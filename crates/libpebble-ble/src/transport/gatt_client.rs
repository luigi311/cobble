//! Reverse PPoGATT v2: the watch hosts the data service, Cobble subscribes and
//! writes to it over the same BLE connection used for pairing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bluer::{
    Device,
    gatt::{
        WriteOp,
        remote::{Characteristic, CharacteristicWriteRequest},
    },
};
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;
use tracing::{debug, warn};

use super::link::StatusGuard;
use super::{LinkState, PpogTransport, TransportMode, TransportStatus};
use super::{
    lifecycle::{Completion, spawn_cleanup, stopped},
    notification::Subscription,
};
use crate::error::PebbleError;
use crate::uuids::{
    PPOGATT_WATCH_SERVER_V2_NOTIFY, PPOGATT_WATCH_SERVER_V2_SERVICE, PPOGATT_WATCH_SERVER_V2_WRITE,
};

pub struct ReverseCharacteristics {
    notify: Characteristic,
    write: Characteristic,
}

/// Match both characteristics inside the exact v2 service. Legacy v1 UUIDs
/// are deliberately left on forward mode, matching upstream's default policy.
pub async fn discover_reverse_v2(
    device: &Device,
) -> Result<Option<ReverseCharacteristics>, PebbleError> {
    for service in device.services().await? {
        if service.uuid().await? != PPOGATT_WATCH_SERVER_V2_SERVICE {
            continue;
        }
        let mut notify = None;
        let mut write = None;
        for characteristic in service.characteristics().await? {
            match characteristic.uuid().await? {
                PPOGATT_WATCH_SERVER_V2_NOTIFY => notify = Some(characteristic),
                PPOGATT_WATCH_SERVER_V2_WRITE => write = Some(characteristic),
                _ => {}
            }
        }
        return match (notify, write) {
            (Some(notify), Some(write)) => Ok(Some(ReverseCharacteristics { notify, write })),
            _ => Err(PebbleError::Other(
                "reverse PPoGATT v2 service has missing characteristics".into(),
            )),
        };
    }
    Ok(None)
}

/// Retain the single serial packet write even if its caller times out or stops.
/// Teardown must await its reply before fallback can send a new reset handshake.
#[derive(Default)]
struct PendingWrite(Mutex<Option<Completion>>);

impl PendingWrite {
    async fn send(
        &self,
        write: impl std::future::Future<Output = Result<(), PebbleError>> + Send + 'static,
    ) -> Result<(), PebbleError> {
        let done = spawn_cleanup(write);
        *self.0.lock().unwrap() = Some(done.clone());
        done.wait().await
    }

    async fn finish(&self) -> Result<(), PebbleError> {
        let pending = self.0.lock().unwrap().clone();
        if let Some(done) = pending {
            // Preserve failures, including ambiguous D-Bus timeouts. They
            // require physical BLE teardown before a new transport can start.
            done.wait().await?;
        }
        Ok(())
    }
}

/// Create an inactive reverse transport whose worker owns subscription setup and teardown.
pub fn start_gatt_client(
    characteristics: ReverseCharacteristics,
    on_data: Arc<dyn Fn(Vec<u8>) + Send + Sync + 'static>,
) -> PpogTransport {
    let (send_tx, send_rx) = mpsc::unbounded_channel();
    let packet_size = Arc::new(Mutex::new(20));
    let (status_tx, status_rx) = watch::channel(TransportStatus::Starting);
    let status = StatusGuard(status_tx);
    let (start_tx, start_rx) = oneshot::channel();
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let task_size = packet_size.clone();
    // Install a handle before beginning any remote subscription. Cancelling
    // setup now requests cooperative shutdown instead of dropping StartNotify.
    let task = tokio::spawn(async move {
        let status = status;
        if start_rx.await.is_err() {
            return Ok(());
        }
        let mut subscription = Subscription::start(characteristics.notify);
        let notifications = subscription.events();
        let notifications = futures::stream::unfold(notifications, |mut rx| async {
            rx.recv().await.map(|packet| (packet, rx))
        });
        let pending_write = PendingWrite::default();
        let result = tokio::select! {
            _ = stopped(&mut stop_rx) => Ok(()),
            result = async {
                subscription.wait_ready().await?;
                // Ordinary GATT writes avoid AcquireWrite's exclusive socket
                // lock, which also blocks WriteValue when left acquired.
                let limit = characteristics.write.mtu().await.unwrap_or(20);
                let op_type = match characteristics.write.flags().await {
                    Ok(flags) if flags.write_without_response => WriteOp::Command,
                    _ => WriteOp::Request,
                };
                debug!("PPoGATT reverse-v2 subscribed; using WriteValue {op_type:?}; GATT value limit={limit}");
                *task_size.lock().unwrap() = limit;
                run_client(notifications, send_rx, task_size, limit, &status.0, on_data, |packet| {
                    let characteristic = characteristics.write.clone();
                    pending_write.send(async move {
                        characteristic.write_ext(&packet, &CharacteristicWriteRequest {
                            op_type,
                            ..Default::default()
                        }).await?;
                        Ok(())
                    })
                }).await
            } => result,
        };
        if let Err(error) = result {
            warn!("PPoGATT reverse-v2 stopped: {error}");
        }
        status.0.send_replace(TransportStatus::Closed);
        let write_result = pending_write.finish().await;
        // StopNotify is awaited even when setup or I/O failed. An error here
        // prevents fallback until the enclosing BLE teardown has completed.
        let subscription_result = subscription.shutdown().await;
        write_result?;
        subscription_result
    });
    PpogTransport::new(
        TransportMode::ReverseV2,
        send_tx,
        packet_size,
        status_rx,
        start_tx,
        stop_tx,
        task,
    )
}

/// Generic over the stream and writer so the actual I/O loop can be exercised
/// with a simulated watch without BlueZ or a physical Bluetooth connection.
async fn run_client<S, F, Fut>(
    notifications: S,
    mut outgoing: mpsc::UnboundedReceiver<Vec<u8>>,
    packet_size: Arc<Mutex<usize>>,
    writer_limit: usize,
    status: &watch::Sender<TransportStatus>,
    on_data: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    mut send: F,
) -> Result<(), PebbleError>
where
    S: Stream<Item = Vec<u8>>,
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<(), PebbleError>>,
{
    let mut state = LinkState::new(TransportMode::ReverseV2);
    let first = state
        .initial_packet()
        .expect("reverse transport initiates reset");
    send_packet(&mut send, first).await?;
    tokio::pin!(notifications);
    loop {
        tokio::select! {
            packet = notifications.next() => {
                let Some(packet) = packet else { return Err(PebbleError::Other("reverse notification stream ended".into())); };
                let was_ready = state.is_ready();
                let output = state.receive(&packet);
                if was_ready && !state.is_ready() {
                    return Err(PebbleError::Other("watch reset the active PPoGATT session".into()));
                }
                for packet in output.packets { send_packet(&mut send, packet).await?; }
                if state.is_ready() && *status.borrow() != TransportStatus::Ready {
                    status.send_replace(TransportStatus::Ready);
                }
                for message in output.messages { on_data(message); }
            }
            message = outgoing.recv() => {
                let Some(message) = message else { return Ok(()); };
                state.enqueue(message);
            }
        }
        let limit = (*packet_size.lock().unwrap()).min(writer_limit);
        while let Some(packet) = state.next_packet(limit) {
            send_packet(&mut send, packet).await?;
        }
    }
}

/// Bound each packet write to five seconds so a stalled backend closes the transport.
async fn send_packet<F, Fut>(send: &mut F, packet: Vec<u8>) -> Result<(), PebbleError>
where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<(), PebbleError>>,
{
    timeout(Duration::from_secs(5), send(packet))
        .await
        .map_err(|_| PebbleError::Timeout("reverse PPoGATT write".into()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::{Endpoint, pebble_pack};
    use crate::transport::wait_ready;

    /// Cancelling a send must not let a late D-Bus write cross the teardown
    /// barrier and reset a replacement session after its handshake has begun.
    #[tokio::test]
    async fn cancelled_reverse_write_finishes_before_transport_cleanup() {
        let pending = Arc::new(PendingWrite::default());
        let (started, writing) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (completed, completion) = oneshot::channel();
        let sender = pending.clone();
        let send = tokio::spawn(async move {
            sender
                .send(async move {
                    let _ = started.send(());
                    let _ = released.await;
                    let _ = completed.send(());
                    Ok(())
                })
                .await
        });
        writing.await.unwrap();
        send.abort();
        assert!(send.await.unwrap_err().is_cancelled());

        let mut cleanup = tokio::spawn(async move { pending.finish().await });
        assert!(
            timeout(Duration::from_millis(10), &mut cleanup)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        timeout(Duration::from_secs(1), cleanup)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        completion.await.unwrap();
    }

    #[tokio::test]
    async fn failed_reverse_write_requires_ble_teardown_before_fallback() {
        let pending = PendingWrite::default();
        let result = pending
            .send(async { Err(PebbleError::Other("simulated WriteValue failure".into())) })
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("WriteValue failure")
        );
        let cleanup = timeout(Duration::from_secs(1), pending.finish())
            .await
            .unwrap();
        assert!(
            cleanup
                .unwrap_err()
                .to_string()
                .contains("WriteValue failure")
        );
    }

    struct SimulatedLink {
        watch: mpsc::UnboundedSender<Vec<u8>>,
        sent: mpsc::UnboundedReceiver<Vec<u8>>,
        outgoing: mpsc::UnboundedSender<Vec<u8>>,
        delivered: mpsc::UnboundedReceiver<Vec<u8>>,
        status: watch::Receiver<TransportStatus>,
        task: tokio::task::JoinHandle<Result<(), PebbleError>>,
    }

    impl SimulatedLink {
        /// Run the production reverse I/O loop with channels and an optional write failure.
        fn new(fail_write: bool) -> Self {
            let (watch, notifications) = mpsc::unbounded_channel();
            let notifications = futures::stream::unfold(notifications, |mut rx| async {
                rx.recv().await.map(|packet| (packet, rx))
            });
            let (send, sent) = mpsc::unbounded_channel();
            let (outgoing, queue) = mpsc::unbounded_channel();
            let (deliver, delivered) = mpsc::unbounded_channel();
            let (status_tx, status) = watch::channel(TransportStatus::Starting);
            let guard = StatusGuard(status_tx);
            let task = tokio::spawn(async move {
                let guard = guard;
                run_client(
                    notifications,
                    queue,
                    Arc::new(Mutex::new(20)),
                    20,
                    &guard.0,
                    Arc::new(move |message| {
                        deliver.send(message).unwrap();
                    }),
                    move |packet| {
                        let send = send.clone();
                        async move {
                            if fail_write {
                                return Err(PebbleError::Other(
                                    "simulated GATT write failure".into(),
                                ));
                            }
                            send.send(packet).unwrap();
                            Ok(())
                        }
                    },
                )
                .await
            });
            Self {
                watch,
                sent,
                outgoing,
                delivered,
                status,
                task,
            }
        }

        async fn packet(&mut self) -> Vec<u8> {
            timeout(Duration::from_secs(1), self.sent.recv())
                .await
                .unwrap()
                .unwrap()
        }
    }

    #[tokio::test]
    async fn reverse_io_handshakes_then_transfers_data_with_flow_control() {
        let mut link = SimulatedLink::new(false);
        assert_eq!(link.packet().await, [2, 1]);
        let outbound = pebble_pack(Endpoint::PutBytes, &[42; 60]).unwrap();
        link.outgoing.send(outbound.clone()).unwrap();
        assert_eq!(*link.status.borrow(), TransportStatus::Starting);
        assert!(link.sent.try_recv().is_err());
        link.watch.send(vec![3, 2, 25]).unwrap();
        assert_eq!(link.packet().await, [3, 25, 25]);
        timeout(Duration::from_secs(1), wait_ready(link.status.clone()))
            .await
            .unwrap()
            .unwrap();
        let first = link.packet().await;
        let second = link.packet().await;
        assert_eq!((first[0], second[0]), (0, 8));
        assert_eq!((first.len(), second.len()), (20, 20));
        assert!(link.sent.try_recv().is_err()); // Peer's receive window is only two.
        link.watch.send(vec![9]).unwrap(); // Cumulative ACK for serial 1.
        let third = link.packet().await;
        let fourth = link.packet().await;
        let assembled: Vec<u8> = [first, second, third, fourth]
            .iter()
            .flat_map(|packet| packet[1..].iter().copied())
            .collect();
        assert_eq!(assembled, outbound);
        let inbound = pebble_pack(Endpoint::Ping, &[0, 1, 2, 3, 4]).unwrap();
        link.watch
            .send([vec![0], inbound.clone()].concat())
            .unwrap();
        assert_eq!(link.packet().await, [1]); // ACK serial zero.
        assert_eq!(
            timeout(Duration::from_secs(1), link.delivered.recv())
                .await
                .unwrap()
                .unwrap(),
            inbound
        );
        drop(link.watch);
        assert!(link.task.await.unwrap().is_err());
        assert_eq!(*link.status.borrow(), TransportStatus::Closed);
    }

    #[tokio::test]
    async fn reverse_write_failure_closes_the_link_without_marking_ready() {
        let link = SimulatedLink::new(true);
        assert!(link.task.await.unwrap().is_err());
        assert_eq!(*link.status.borrow(), TransportStatus::Closed);
        assert!(wait_ready(link.status).await.is_err());
    }

    #[tokio::test]
    async fn active_watch_reset_closes_transport_for_reconnection() {
        let mut link = SimulatedLink::new(false);
        assert_eq!(link.packet().await, [2, 1]);
        link.watch.send(vec![3, 25, 25]).unwrap();
        assert_eq!(link.packet().await, [3, 25, 25]);
        wait_ready(link.status.clone()).await.unwrap();
        link.watch.send(vec![2, 1]).unwrap();
        assert!(link.task.await.unwrap().is_err());
        assert_eq!(*link.status.borrow(), TransportStatus::Closed);
    }
}
