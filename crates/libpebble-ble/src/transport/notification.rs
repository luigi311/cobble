//! Own each StartNotify/StopNotify exchange until it finishes. bluer's notify()
//! token is created only after StartNotify and cannot await StopNotify on drop.

use super::{
    TaskGuard,
    lifecycle::{Completion, spawn_cleanup, stopped},
};
use crate::error::PebbleError;
use bluer::gatt::remote::Characteristic;
use dbus::{
    arg::{PropMap, RefArg},
    message::MatchRule,
    nonblock::{Proxy, SyncConnection},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};
use tracing::warn;

pub(crate) struct Subscription {
    stop: watch::Sender<bool>,
    ready: watch::Receiver<Option<Result<(), String>>>,
    done: Completion,
    events: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl Subscription {
    pub fn start(characteristic: Characteristic) -> Self {
        let (stop, stop_rx) = watch::channel(false);
        let (ready_tx, ready) = watch::channel(None);
        let (events_tx, events) = mpsc::unbounded_channel();
        let stop_on_io = stop.clone();
        let done = spawn_cleanup(async move {
            let result = run_subscription(
                characteristic,
                stop_rx,
                stop_on_io,
                ready_tx.clone(),
                events_tx,
            )
            .await;
            if ready_tx.borrow().is_none() {
                ready_tx.send_replace(Some(Err(result
                    .as_ref()
                    .err()
                    .map_or_else(|| "subscription stopped".into(), ToString::to_string))));
            }
            result
        });
        Self {
            stop,
            ready,
            done,
            events: Some(events),
        }
    }

    pub fn events(&mut self) -> mpsc::UnboundedReceiver<Vec<u8>> {
        self.events
            .take()
            .expect("notification receiver taken once")
    }
    pub fn stop(&self) {
        self.stop.send_replace(true);
    }

    pub async fn wait_ready(&self) -> Result<(), PebbleError> {
        let mut ready = self.ready.clone();
        loop {
            if let Some(result) = ready.borrow_and_update().clone() {
                return result.map_err(PebbleError::Other);
            }
            ready
                .changed()
                .await
                .map_err(|_| PebbleError::Other("subscription worker stopped".into()))?;
        }
    }

    pub async fn shutdown(self) -> Result<(), PebbleError> {
        self.stop();
        self.done.wait().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.stop();
    }
}

fn error(error: dbus::Error) -> PebbleError {
    PebbleError::Other(format!("BlueZ notification: {error}"))
}

async fn run_subscription(
    characteristic: Characteristic,
    mut stop: watch::Receiver<bool>,
    stop_on_io: watch::Sender<bool>,
    ready: watch::Sender<Option<Result<(), String>>>,
    events: mpsc::UnboundedSender<Vec<u8>>,
) -> Result<(), PebbleError> {
    if *stop.borrow() {
        return Ok(());
    }
    let (resource, connection) = dbus_tokio::connection::new_system_sync().map_err(error)?;
    // This bus connection owns only this subscription. Even an ambiguous
    // StartNotify failure cannot leak ownership into another connection.
    let io = tokio::spawn(async move {
        let _ = resource.await;
        stop_on_io.send_replace(true);
    });
    let io_guard = TaskGuard(io);
    let path = format!(
        "/org/bluez/{}/dev_{}/service{:04x}/char{:04x}",
        characteristic.adapter_name(),
        characteristic
            .device_address()
            .to_string()
            .replace(':', "_"),
        characteristic.service_id(),
        characteristic.id()
    );
    let rule = MatchRule::new_signal("org.freedesktop.DBus.Properties", "PropertiesChanged")
        .with_sender("org.bluez")
        .with_path(path.clone());
    let signal = connection.add_match(rule).await.map_err(error)?.cb(
        move |_, (interface, changed, _): (String, PropMap, Vec<String>)| {
            if interface == "org.bluez.GattCharacteristic1"
                && let Some(value) = changed.get("Value").and_then(|v| v.0.as_iter())
            {
                let bytes: Option<Vec<u8>> = value
                    .map(|item| item.as_u64().and_then(|n| u8::try_from(n).ok()))
                    .collect();
                if let Some(bytes) = bytes {
                    let _ = events.send(bytes);
                }
            }
            true
        },
    );
    let proxy = Proxy::new(
        "org.bluez",
        path,
        Duration::from_secs(30),
        connection.clone(),
    );
    // Never cancel either method after sending it. The caller can request
    // stop while StartNotify is pending; this owner still waits for its reply
    // and then sends and awaits StopNotify on the SAME D-Bus connection.
    let result = owned_exchange(
        &mut stop,
        &ready,
        || notify_call(&proxy, "StartNotify"),
        || notify_call(&proxy, "StopNotify"),
    )
    .await;
    let _ = connection.remove_match(signal.token()).await;
    drop(signal);
    drop(proxy);
    drop(connection);
    drop(io_guard);
    result
}

async fn notify_call(
    proxy: &Proxy<'_, Arc<SyncConnection>>,
    method: &'static str,
) -> Result<(), PebbleError> {
    let result: Result<(), dbus::Error> = proxy
        .method_call("org.bluez.GattCharacteristic1", method, ())
        .await;
    if method == "StopNotify" && result.as_ref().err().is_some_and(already_stopped) {
        // BlueZ no longer has an owned notification session/characteristic.
        return Ok(());
    }
    result.map_err(error)
}

fn already_stopped(error: &dbus::Error) -> bool {
    matches!(error.name(), Some("org.bluez.Error.NotConnected" | "org.bluez.Error.DoesNotExist" |
        "org.freedesktop.DBus.Error.UnknownObject")) ||
        // BlueZ src/gatt-client.c: characteristic_stop_notify removes the
        // caller's client synchronously, or returns this exact absence error.
        (error.name() == Some("org.bluez.Error.Failed") && error.message() == Some("No notify session started"))
}

async fn owned_exchange<Start, StartF, Stop, StopF>(
    stop: &mut watch::Receiver<bool>,
    ready: &watch::Sender<Option<Result<(), String>>>,
    start: Start,
    finish: Stop,
) -> Result<(), PebbleError>
where
    Start: FnOnce() -> StartF,
    StartF: std::future::Future<Output = Result<(), PebbleError>>,
    Stop: FnOnce() -> StopF,
    StopF: std::future::Future<Output = Result<(), PebbleError>>,
{
    let result = start().await;
    if let Err(error) = &result {
        warn!("BlueZ notification setup failed: {error}");
    }
    ready.send_replace(Some(
        result.as_ref().map(|_| ()).map_err(ToString::to_string),
    ));
    if result.is_ok() {
        stopped(stop).await;
    }
    // Also stop after failed setup: a method timeout can hide an accepted
    // StartNotify. Failure to confirm cleanup prevents same-link fallback.
    finish().await
}

#[cfg(test)]
pub(crate) fn test_subscription(
    stopped_tx: tokio::sync::oneshot::Sender<()>,
    release_stop: tokio::sync::oneshot::Receiver<()>,
) -> Subscription {
    let (stop, mut stop_rx) = watch::channel(false);
    let (_ready, ready) = watch::channel(Some(Ok(())));
    let (_, events) = mpsc::unbounded_channel();
    let done = spawn_cleanup(async move {
        stopped(&mut stop_rx).await;
        let _ = stopped_tx.send(());
        let _ = release_stop.await;
        Ok(())
    });
    Subscription {
        stop,
        ready,
        done,
        events: Some(events),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{sync::oneshot, time::timeout};

    #[tokio::test]
    async fn cancellation_during_start_waits_for_the_reply_then_confirms_stop() {
        let (stop, mut stop_rx) = watch::channel(false);
        let (ready, mut ready_rx) = watch::channel(None);
        let (start_entered, entered) = oneshot::channel();
        let (release_start, start_reply) = oneshot::channel();
        let (stop_entered, stopping) = oneshot::channel();
        let (release_stop, stop_reply) = oneshot::channel();
        let done = spawn_cleanup(async move {
            owned_exchange(
                &mut stop_rx,
                &ready,
                || async {
                    start_entered.send(()).unwrap();
                    start_reply.await.unwrap();
                    Ok(())
                },
                || async {
                    stop_entered.send(()).unwrap();
                    stop_reply.await.unwrap();
                    Ok(())
                },
            )
            .await
        });
        entered.await.unwrap();
        stop.send_replace(true); // Parent times out while StartNotify reply is delayed.
        assert!(ready_rx.borrow().is_none());
        let mut waiter = Box::pin(done.wait());
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        // Dropping a cancelled wait does not drop the StartNotify owner.
        drop(waiter);
        release_start.send(()).unwrap();
        stopping.await.unwrap();
        assert_eq!(*ready_rx.borrow_and_update(), Some(Ok(())));
        let mut waiter = Box::pin(done.wait());
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        release_stop.send(()).unwrap();
        timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_fallback_wait_keeps_unsubscribe_ahead_of_later_cleanup() {
        let mut queue = super::super::lifecycle::CleanupQueue::default();
        let (stop_entered, stopping) = oneshot::channel();
        let (release_stop, stop_reply) = oneshot::channel();
        let subscription = test_subscription(stop_entered, stop_reply);
        let first = queue.push(subscription.shutdown());
        stopping.await.unwrap();
        let mut cancelled = Box::pin(first.wait());
        assert!(futures::poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);
        let (fallback_started, mut started) = oneshot::channel();
        let next = queue.push(async move {
            fallback_started.send(()).unwrap();
            Ok(())
        });
        assert!(started.try_recv().is_err());
        release_stop.send(()).unwrap();
        next.wait().await.unwrap();
        started.await.unwrap();
    }

    #[tokio::test]
    async fn ambiguous_failed_start_also_attempts_stop_and_reports_cleanup_failure() {
        let (_stop, mut stop_rx) = watch::channel(false);
        let (ready, ready_rx) = watch::channel(None);
        let (stop_called, called) = oneshot::channel();
        let result = owned_exchange(
            &mut stop_rx,
            &ready,
            || async { Err(PebbleError::Timeout("StartNotify reply".into())) },
            || async {
                stop_called.send(()).unwrap();
                Err(PebbleError::Timeout("StopNotify reply".into()))
            },
        )
        .await;
        called.await.unwrap();
        assert!(ready_rx.borrow().as_ref().unwrap().is_err());
        assert!(
            matches!(result, Err(PebbleError::Timeout(message)) if message == "StopNotify reply")
        );
    }
    #[test]
    fn absent_notify_session_is_safe_but_unknown_cleanup_failure_is_not() {
        assert!(already_stopped(&dbus::Error::new_custom(
            "org.bluez.Error.Failed",
            "No notify session started"
        )));
        assert!(!already_stopped(&dbus::Error::new_custom(
            "org.bluez.Error.Failed",
            "Unsubscribe still pending"
        )));
        assert!(!already_stopped(&dbus::Error::new_custom(
            "org.freedesktop.DBus.Error.NoReply",
            "Timeout"
        )));
    }
}
