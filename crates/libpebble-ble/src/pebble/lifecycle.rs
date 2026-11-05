use crate::error::PebbleError;
use crate::transport::lifecycle::CleanupQueue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;

pub(super) struct ConnectionLifecycle {
    pub operation: Arc<AsyncMutex<()>>,
    pub cleanup: Mutex<CleanupQueue>,
    adapter: String,
    address: String,
}

impl ConnectionLifecycle {
    /// A cancelled Pebble can outlive its caller while finishing BlueZ
    /// teardown. A replacement Pebble for that address must share its barrier.
    pub fn shared(adapter: &str, address: &str) -> Arc<Self> {
        type Registry = Mutex<HashMap<(String, String), Weak<ConnectionLifecycle>>>;
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        let mut registry = REGISTRY.get_or_init(Mutex::default).lock().unwrap();
        registry.retain(|_, lifecycle| lifecycle.strong_count() > 0);
        let key = (adapter.to_owned(), address.to_ascii_uppercase());
        if let Some(lifecycle) = registry.get(&key).and_then(Weak::upgrade) {
            return lifecycle;
        }
        let lifecycle = Arc::new(Self {
            operation: Arc::new(AsyncMutex::new(())),
            cleanup: Mutex::new(CleanupQueue::default()),
            adapter: adapter.into(),
            address: address.into(),
        });
        registry.insert(key, Arc::downgrade(&lifecycle));
        lifecycle
    }

    pub async fn wait_cleanup(self: &Arc<Self>) -> Result<(), PebbleError> {
        let lifecycle = self.clone();
        self.wait_cleanup_with(async move {
            // A prior bus connection may itself be dead. Resolve the physical
            // device through a fresh session before permitting any new Connect.
            let session = bluer::Session::new().await?;
            let adapter = session.adapter(&lifecycle.adapter)?;
            let address = lifecycle.address.parse().map_err(|error| {
                PebbleError::Other(format!("invalid Bluetooth address: {error}"))
            })?;
            disconnect_device(&adapter.device(address)?).await
        })
        .await
    }

    async fn wait_cleanup_with(
        &self,
        recovery: impl std::future::Future<Output = Result<(), PebbleError>> + Send + 'static,
    ) -> Result<(), PebbleError> {
        let pending = self.cleanup.lock().unwrap().pending();
        if let Some(pending) = pending
            && let Err(error) = pending.wait().await
        {
            warn!("retrying unconfirmed Bluetooth teardown: {error}");
            let done = self.cleanup.lock().unwrap().push(recovery);
            done.wait().await?;
        }
        Ok(())
    }
}

fn missing(error: &bluer::Error) -> bool {
    matches!(
        error.kind,
        bluer::ErrorKind::NotFound | bluer::ErrorKind::DoesNotExist
    )
}

pub(super) async fn disconnect_device(device: &bluer::Device) -> Result<(), PebbleError> {
    confirm_disconnect(|| device.disconnect(), || device.is_connected()).await
}

/// Missing BlueZ objects after Forget are already gone. Other errors require
/// a confirmed disconnected state; an unavailable backend remains blocked.
async fn confirm_disconnect<D, DF, C, CF>(disconnect: D, connected: C) -> Result<(), PebbleError>
where
    D: FnOnce() -> DF,
    DF: std::future::Future<Output = Result<(), bluer::Error>>,
    C: FnOnce() -> CF,
    CF: std::future::Future<Output = Result<bool, bluer::Error>>,
{
    match disconnect().await {
        Ok(()) => Ok(()),
        Err(error) if missing(&error) => Ok(()),
        Err(error) => match connected().await {
            Ok(false) => Ok(()),
            Err(check) if missing(&check) => Ok(()),
            _ => Err(error.into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn absent() -> bluer::Error {
        dbus::Error::new_custom(
            "org.freedesktop.DBus.Error.UnknownObject",
            "removed by Forget",
        )
        .into()
    }
    fn unavailable() -> bluer::Error {
        dbus::Error::new_custom(
            "org.freedesktop.DBus.Error.NoReply",
            "backend temporarily unavailable",
        )
        .into()
    }

    #[tokio::test]
    async fn missing_device_disconnect_permits_rediscovery_and_the_next_connect_attempt() {
        let mut attempted = 0;
        for _ in 0..2 {
            confirm_disconnect(
                || async { Err(absent()) },
                || async { panic!("missing object already confirms absence") },
            )
            .await
            .unwrap();
            // The production retry loop reaches its next scan after this
            // exact confirmation, even if Disconnect failed after Forget.
            attempted += 1;
        }
        assert_eq!(attempted, 2);
    }

    #[tokio::test]
    async fn disconnected_or_removed_state_confirms_failed_disconnect() {
        confirm_disconnect(|| async { Err(unavailable()) }, || async { Ok(false) })
            .await
            .unwrap();
        confirm_disconnect(|| async { Err(unavailable()) }, || async { Err(absent()) })
            .await
            .unwrap();
        assert!(
            confirm_disconnect(|| async { Err(unavailable()) }, || async { Ok(true) })
                .await
                .is_err()
        );
        assert!(
            confirm_disconnect(
                || async { Err(unavailable()) },
                || async { Err(unavailable()) }
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn failed_teardown_can_recover_after_backend_availability_returns() {
        let lifecycle = ConnectionLifecycle::shared("council-recovery", "00:00:00:00:00:20");
        let _operation = lifecycle.operation.clone().lock_owned().await;
        lifecycle
            .cleanup
            .lock()
            .unwrap()
            .push(async { Err(PebbleError::Other("original disconnect failed".into())) });
        // Recovery must fail closed while disconnection is unconfirmed.
        assert!(
            lifecycle
                .wait_cleanup_with(async { Err(PebbleError::Other("backend still down".into())) })
                .await
                .is_err()
        );
        let recovered = Arc::new(AtomicBool::new(false));
        let mark = recovered.clone();
        lifecycle
            .wait_cleanup_with(async move {
                mark.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap();
        assert!(recovered.load(Ordering::SeqCst));
        // The failed completion was replaced, so future attempts need no
        // additional physical recovery until another teardown actually fails.
        lifecycle
            .wait_cleanup_with(async { panic!("already recovered") })
            .await
            .unwrap();
    }
}
