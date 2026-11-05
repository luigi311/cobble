//! Cancellation-safe cleanup jobs. Waiting may be cancelled; cleanup may not.

use crate::error::PebbleError;
use std::future::Future;
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct Completion(watch::Receiver<Option<Result<(), String>>>);

impl Completion {
    /// Await a retained job result without cancelling the job when this wait is dropped.
    pub async fn wait(&self) -> Result<(), PebbleError> {
        let mut rx = self.0.clone();
        loop {
            if let Some(result) = rx.borrow_and_update().clone() {
                return result.map_err(PebbleError::Other);
            }
            rx.changed()
                .await
                .map_err(|_| PebbleError::Other("cleanup worker stopped unexpectedly".into()))?;
        }
    }
}

/// Spawn an owned job and retain its outcome for any current or later cleanup waiter.
pub(crate) fn spawn_cleanup(
    future: impl Future<Output = Result<(), PebbleError>> + Send + 'static,
) -> Completion {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        let result = future.await.map_err(|error| error.to_string());
        tx.send_replace(Some(result));
    });
    Completion(rx)
}

#[derive(Default)]
pub(crate) struct CleanupQueue {
    tail: Option<Completion>,
}

impl CleanupQueue {
    pub fn pending(&self) -> Option<Completion> {
        self.tail.clone()
    }

    /// Run a cleanup job after the previous one finishes, even if its predecessor failed.
    pub fn push(
        &mut self,
        future: impl Future<Output = Result<(), PebbleError>> + Send + 'static,
    ) -> Completion {
        let previous = self.tail.clone();
        let next = spawn_cleanup(async move {
            // Still run a physical disconnect even if earlier unsubscribe
            // failed; it is the final barrier that can recover that failure.
            if let Some(previous) = previous {
                let _ = previous.wait().await;
            }
            future.await
        });
        self.tail = Some(next.clone());
        next
    }
}

/// Wait until stop is requested or all senders of the stop channel are dropped.
pub(crate) async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
