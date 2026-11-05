//! Phone-hosted PPoGATT GATT server (BlueZ peripheral via bluer).
//!
//! We register a GattApplication exporting the PPoGATT service.  The watch,
//! acting as GATT client, writes PPoGATT packets to our WRITE characteristic
//! and receives our notifications on the same characteristic.
//!
//!   service 10000000
//!     char 10000002  READ           — fixed 19-byte blob on read
//!     char 10000001  WRITE_NO_RESP + NOTIFY
//!   service badbadba-...  (Gadgetbridge also registers this; watch expects it)

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use bluer::{
    Adapter,
    gatt::{
        CharacteristicWriter,
        local::{
            Application, Characteristic, CharacteristicControlEvent, CharacteristicNotify,
            CharacteristicNotifyMethod, CharacteristicRead, CharacteristicWrite,
            CharacteristicWriteMethod, Service, characteristic_control,
        },
    },
};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;
use tracing::{debug, warn};

use super::lifecycle::stopped;
use super::link::StatusGuard;
use super::{LinkState, PpogTransport, TransportMode, TransportStatus};
use crate::uuids::{
    PPOGATT_BADBAD_SERVICE, PPOGATT_SERVER_READ_CHARACTERISTIC, PPOGATT_SERVER_SERVICE,
    PPOGATT_SERVER_WRITE_CHARACTERISTIC,
};

// The exact 19-byte response Gadgetbridge returns for a read of 10000002.
const READ_BLOB: [u8; 19] = [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

/// Handle to a forward PPoGATT link. Drop to unregister and stop its I/O.
pub type PebbleGattServerHandle = PpogTransport;

pub async fn start_gatt_server(
    adapter: &Adapter,
    on_data: Arc<dyn Fn(Vec<u8>) + Send + Sync + 'static>,
) -> Result<PpogTransport, bluer::Error> {
    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (ctrl, ctrl_handle) = characteristic_control();
    let packet_size = Arc::new(Mutex::new(20));
    let (status_tx, status_rx) = watch::channel(TransportStatus::Starting);
    let status = StatusGuard(status_tx);
    let (start_tx, start_rx) = oneshot::channel();
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let write_tx_clone = write_tx.clone();
    let write_char = Characteristic {
        uuid: PPOGATT_SERVER_WRITE_CHARACTERISTIC,
        write: Some(CharacteristicWrite {
            write: true,
            write_without_response: true,
            method: CharacteristicWriteMethod::Fun(Box::new(move |value, _req| {
                let tx = write_tx_clone.clone();
                Box::pin(async move {
                    let _ = tx.send(value);
                    Ok(())
                })
            })),
            ..Default::default()
        }),
        notify: Some(CharacteristicNotify {
            notify: true,
            method: CharacteristicNotifyMethod::Io,
            ..Default::default()
        }),
        control_handle: ctrl_handle,
        ..Default::default()
    };

    let read_char = Characteristic {
        uuid: PPOGATT_SERVER_READ_CHARACTERISTIC,
        read: Some(CharacteristicRead {
            read: true,
            fun: Box::new(|_req| Box::pin(async move { Ok(READ_BLOB.to_vec()) })),
            ..Default::default()
        }),
        ..Default::default()
    };

    let (badbad_ctrl, badbad_ctrl_handle) = characteristic_control();
    let badbad_char = Characteristic {
        uuid: PPOGATT_BADBAD_SERVICE,
        read: Some(CharacteristicRead {
            read: true,
            fun: Box::new(|_req| Box::pin(async move { Ok(vec![0x00]) })),
            ..Default::default()
        }),
        control_handle: badbad_ctrl_handle,
        ..Default::default()
    };

    let app = Application {
        services: vec![
            Service {
                uuid: PPOGATT_SERVER_SERVICE,
                primary: true,
                characteristics: vec![read_char, write_char],
                ..Default::default()
            },
            Service {
                uuid: PPOGATT_BADBAD_SERVICE,
                primary: true,
                characteristics: vec![badbad_char],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let app_handle = adapter.serve_gatt_application(app).await?;
    debug!("GATT server registered on adapter {}", adapter.name());

    let packet_size_for_task = packet_size.clone();
    let task = tokio::spawn(async move {
        let status = status;
        if start_rx.await.is_err() {
            return Ok(());
        }
        let mut state = LinkState::new(TransportMode::Forward);
        let mut writer: Option<CharacteristicWriter> = None;
        let mut packets = VecDeque::new();
        tokio::pin!(ctrl);
        tokio::pin!(badbad_ctrl);
        loop {
            tokio::select! {
                _ = stopped(&mut stop_rx) => break,
                event = ctrl.next() => {
                    let Some(event) = event else { break; };
                    if let CharacteristicControlEvent::Notify(next_writer) = event {
                        debug!("watch subscribed to PPoGATT server characteristic");
                        let limit = next_writer.mtu();
                        debug!("PPoGATT forward GATT value limit={limit}");
                        *packet_size_for_task.lock().unwrap() = limit;
                        writer = Some(next_writer);
                    }
                }
                _ = async {
                    match &writer {
                        Some(writer) => { let _ = writer.closed().await; }
                        None => std::future::pending().await,
                    }
                } => { debug!("PPoGATT forward notification channel closed"); break; }
                packet = write_rx.recv() => {
                    let Some(packet) = packet else { break; };
                    let was_ready = state.is_ready();
                    let output = state.receive(&packet);
                    if was_ready && !state.is_ready() {
                        warn!("PPoGATT forward reset during active session; reconnecting");
                        break;
                    }
                    packets.extend(output.packets);
                    for message in output.messages { on_data(message); }
                }
                message = send_rx.recv() => {
                    let Some(message) = message else { break; };
                    state.enqueue(message);
                }
                Some(_) = badbad_ctrl.next() => {}
            }
            if let Some(writer) = &writer {
                let limit = (*packet_size_for_task.lock().unwrap()).min(writer.mtu());
                loop {
                    let packet = packets.pop_front().or_else(|| state.next_packet(limit));
                    let Some(packet) = packet else {
                        break;
                    };
                    if !matches!(
                        timeout(Duration::from_secs(5), writer.send(&packet)).await,
                        Ok(Ok(()))
                    ) {
                        warn!("PPoGATT forward write failed or timed out");
                        return Ok(());
                    }
                }
                if state.is_ready() && *status.0.borrow() != TransportStatus::Ready {
                    status.0.send_replace(TransportStatus::Ready);
                }
            }
        }
        Ok(())
    });
    Ok(PpogTransport::new(
        TransportMode::Forward,
        send_tx,
        packet_size,
        status_rx,
        start_tx,
        stop_tx,
        task,
    )
    .with_application(app_handle))
}
