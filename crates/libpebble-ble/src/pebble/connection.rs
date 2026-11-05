//! BLE connection lifecycle: scanning, connecting, pairing, and teardown.
//!
//! All async `Pebble::connect*` methods, GATT subscription helpers,
//! and the bonding handshake live here.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bluer::{AdapterEvent, Address, Device};
use futures::StreamExt;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{debug, info, trace, warn};

use super::{Pebble, dispatch};
use crate::endpoints::Endpoint;
use crate::endpoints::blob_db::{BlobDB2Incoming, BlobDBStatus, build_blobdb2_version};
use crate::error::PebbleError;
use crate::transport::agent::build_pairing_agent;
use crate::transport::gatt_client::{discover_reverse_v2, start_gatt_client};
use crate::transport::gatt_server::start_gatt_server;
use crate::transport::{PpogTransport, TaskGuard, TransportMode, TransportStatus, wait_ready};
use crate::uuids::{
    BATTERY_LEVEL_CHARACTERISTIC, CONNECTION_PARAMS_CHARACTERISTIC, CONNECTIVITY_CHARACTERISTIC,
    MTU_CHARACTERISTIC, PAIRING_TRIGGER_CHARACTERISTIC,
};

#[derive(Clone, Copy)]
struct AttemptId {
    epoch: u64,
    generation: u64,
}

impl Pebble {
    // ── scanning ───────────────────────────────────────────────────────

    /// Scan for nearby Pebble devices for `timeout_secs` seconds.
    /// Returns a list of `(address, name)` pairs for every device whose
    /// Bluetooth name contains "pebble" (case-insensitive).
    pub async fn scan(
        adapter_name: &str,
        timeout_secs: f64,
    ) -> Result<Vec<(String, String)>, PebbleError> {
        let session = bluer::Session::new().await?;
        let adapter = session
            .adapter(adapter_name)
            .map_err(|e| PebbleError::Other(format!("adapter {adapter_name}: {e}")))?;

        adapter
            .set_discoverable_timeout(timeout_secs as u32)
            .await?;
        let mut stream = adapter.discover_devices().await?;
        let mut found: Vec<(String, String)> = Vec::new();

        let _ = tokio::time::timeout(Duration::from_secs_f64(timeout_secs), async {
            while let Some(event) = stream.next().await {
                if let AdapterEvent::DeviceAdded(addr) = event
                    && let Ok(device) = adapter.device(addr)
                {
                    let name = device.name().await.ok().flatten().unwrap_or_default();
                    if name.to_lowercase().contains("pebble") {
                        debug!("found Pebble: {addr} \"{name}\"");
                        found.push((addr.to_string(), name));
                    }
                }
            }
        })
        .await;

        Ok(found)
    }

    // ── connect ────────────────────────────────────────────────────────

    pub async fn connect(&self) -> Result<(), PebbleError> {
        self.connect_with_options(true, 30.0, 3, 2.0).await
    }

    /// Serialize startup and prior teardown, retry pairing when needed, and clean up failures.
    pub async fn connect_with_options(
        &self,
        pairing: bool,
        conn_timeout: f64,
        connect_attempts: u32,
        retry_delay: f64,
    ) -> Result<(), PebbleError> {
        let epoch = self.cancel_epoch.load(std::sync::atomic::Ordering::SeqCst);
        let operation = self.lifecycle.operation.clone().lock_owned().await;
        let mut attempt = ConnectionAttemptGuard {
            pebble: self,
            operation: Some(operation),
            finished: false,
        };
        self.clear_connection();
        self.lifecycle.wait_cleanup().await?;
        self.ensure_attempt(epoch)?;
        let mut result = self
            .connect_session(pairing, conn_timeout, connect_attempts, retry_delay, epoch)
            .await;
        if matches!(&result, Err(PebbleError::PairingFailed(_))) {
            warn!("pairing failed; clearing stale bond and retrying with a fresh connection");
            self.ensure_attempt(epoch)?;
            self.clear_connection();
            self.lifecycle.wait_cleanup().await?;
            result = self
                .retry_pairing_after(
                    epoch,
                    tokio::time::sleep(Duration::from_secs(2)),
                    || async {
                        if let Err(error) = self.forget_for_attempt(Some(epoch)).await {
                            self.ensure_attempt(epoch)?;
                            warn!("could not clear stale bond: {error}");
                        }
                        // RemoveDevice invalidates every service/characteristic object.
                        self.connect_session(
                            pairing,
                            conn_timeout,
                            connect_attempts,
                            retry_delay,
                            epoch,
                        )
                        .await
                    },
                )
                .await;
        }
        if result.is_err()
            && let Some(done) = self.clear_connection()
            && let Err(error) = done.wait().await
        {
            warn!("connection teardown failed: {error}");
        }
        attempt.finished = true;
        result
    }

    /// Establish one BLE session, select its transport, and synchronize watch preferences.
    async fn connect_session(
        &self,
        pairing: bool,
        conn_timeout: f64,
        connect_attempts: u32,
        retry_delay: f64,
        epoch: u64,
    ) -> Result<(), PebbleError> {
        let identity = self.capture_attempt(epoch)?;
        let generation = identity.generation;
        let address = Address::from_str(&self.address)
            .map_err(|e| PebbleError::Other(format!("invalid address: {e}")))?;

        let session = bluer::Session::new().await?;
        let adapter = session
            .adapter(&self.adapter_name)
            .map_err(|e| PebbleError::Other(format!("adapter {}: {e}", self.adapter_name)))?;

        // Register pairing agent.
        let agent = build_pairing_agent(address);
        let _agent_handle = session.register_agent(agent).await.map_err(|e| {
            warn!("could not register pairing agent: {e}; relying on a system agent");
            PebbleError::Other(e.to_string())
        });

        // Connect to the watch (with retries).
        let device = self
            .connect_client(
                &adapter,
                address,
                conn_timeout,
                connect_attempts,
                retry_delay,
                identity,
            )
            .await?;
        self.check_identity(identity)?;

        let weak_inner = Arc::downgrade(&self.inner);
        let on_data: Arc<dyn Fn(Vec<u8>) + Send + Sync> = Arc::new(move |message| {
            if let Some(inner) = weak_inner.upgrade() {
                if inner.lock().unwrap().connection_id != generation {
                    return;
                }
                dispatch::on_pebble_message(message, &inner);
            }
        });
        let reverse = match timeout(
            Duration::from_secs_f64(conn_timeout),
            discover_reverse_v2(&device),
        )
        .await
        {
            Ok(Ok(characteristics)) => characteristics,
            Ok(Err(error)) => {
                warn!("reverse PPoGATT v2 discovery failed: {error}; using forward");
                None
            }
            Err(_) => {
                warn!("reverse PPoGATT v2 discovery timed out; using forward");
                None
            }
        };
        // Legacy watches can attempt their subscription during pairing, so
        // publish the forward service before triggering that handshake.
        if reverse.is_none() {
            let transport = start_gatt_server(&adapter, on_data.clone()).await?;
            self.install_transport(transport, generation)?;
        }

        // Pairing / bonding.
        let already_paired = device.is_paired().await.unwrap_or(false);

        if !already_paired && pairing {
            self.do_pairing(&device, identity).await?;
            info!("pairing completed");
            if let Err(error) = device.set_trusted(true).await {
                debug!("could not set Trusted: {error}");
            }
        }

        // Subscribe to fed9 characteristics.
        self.subscribe_fed9(&device).await;

        // Read + subscribe to the standard BLE battery level.
        self.subscribe_battery(&device).await;

        let mode = select_transport(
            reverse.is_some(),
            || async {
                let characteristics = reverse.expect("reverse service was discovered");
                let transport = start_gatt_client(characteristics, on_data.clone());
                let status = self.install_transport(transport, generation)?;
                timeout(Duration::from_secs(22), wait_ready(status))
                    .await
                    .map_err(|_| PebbleError::Timeout("reverse PPoGATT setup/handshake".into()))?
            },
            || async {
                let status = self
                    .prepare_forward(generation, || async {
                        start_gatt_server(&adapter, on_data.clone())
                            .await
                            .map_err(PebbleError::from)
                    })
                    .await?;
                self.ensure_attempt(epoch)?;
                self.write_pairing_trigger(&device).await;
                timeout(Duration::from_secs_f64(conn_timeout), wait_ready(status))
                    .await
                    .map_err(|_| PebbleError::Timeout("forward PPoGATT handshake".into()))?
            },
        )
        .await?;
        info!("PPoGATT transport selected: {mode}");
        let transport_status = self.transport_status(generation)?;

        // Startup synchronization uses the same connected guard as later API calls.
        {
            let inner = self.inner.lock().unwrap();
            self.ensure_attempt(epoch)?;
            if inner.connection_id != generation {
                return Err(PebbleError::NotConnected);
            }
            let _ = self.connected_tx.send(true);
        }
        self.monitor_connection(device, transport_status, generation);

        // BlobDB2 version handshake.
        let blob_db_version = self.negotiate_blobdb2_version().await;
        self.inner.lock().unwrap().blob_db_version = blob_db_version;
        if blob_db_version >= 1 {
            match self.refresh_watch_preferences().await {
                Ok(()) => debug!("BlobDB2 v{blob_db_version}: WatchPrefs sync complete"),
                Err(error) => warn!("initial WatchPrefs sync failed: {error}"),
            }
        }

        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        info!("Pebble connected and ready");
        Ok(())
    }

    /// Reject attempts cancelled by a public disconnect, including across pairing retries.
    fn ensure_attempt(&self, epoch: u64) -> Result<(), PebbleError> {
        if self.cancel_epoch.load(std::sync::atomic::Ordering::SeqCst) != epoch {
            return Err(PebbleError::NotConnected);
        }
        Ok(())
    }

    /// Capture the session generation under the same lock that validates the attempt epoch.
    fn capture_attempt(&self, epoch: u64) -> Result<AttemptId, PebbleError> {
        let inner = self.inner.lock().unwrap();
        self.ensure_attempt(epoch)?;
        Ok(AttemptId {
            epoch,
            generation: inner.connection_id,
        })
    }

    /// Reject a cancelled attempt or an identity belonging to a cleared session.
    fn check_identity(&self, identity: AttemptId) -> Result<(), PebbleError> {
        let inner = self.inner.lock().unwrap();
        self.ensure_attempt(identity.epoch)?;
        if inner.connection_id != identity.generation {
            return Err(PebbleError::NotConnected);
        }
        Ok(())
    }

    /// Register connection-producing RPCs atomically with both identities and
    /// their device. Teardown retains them even if the caller is cancelled.
    fn begin_request(
        &self,
        identity: AttemptId,
        device: Option<Device>,
        future: impl std::future::Future<Output = Result<(), PebbleError>> + Send + 'static,
    ) -> Result<crate::transport::lifecycle::Completion, PebbleError> {
        let mut inner = self.inner.lock().unwrap();
        self.ensure_attempt(identity.epoch)?;
        if inner.connection_id != identity.generation {
            return Err(PebbleError::NotConnected);
        }
        if let Some(device) = device {
            inner.device = Some(device);
        }
        let request = crate::transport::lifecycle::spawn_cleanup(future);
        inner.connection_requests.push(request.clone());
        Ok(request)
    }

    /// Poll bonding with cancellation checks around every property read and retry delay.
    async fn poll_pairing<S, SF, D, DF>(
        &self,
        identity: AttemptId,
        checks: usize,
        mut paired: S,
        mut delay: D,
    ) -> Result<bool, PebbleError>
    where
        S: FnMut() -> SF,
        SF: std::future::Future<Output = Result<bool, bluer::Error>>,
        D: FnMut() -> DF,
        DF: std::future::Future<Output = ()>,
    {
        for _ in 0..checks {
            self.check_identity(identity)?;
            let paired = paired().await.unwrap_or(false);
            self.check_identity(identity)?;
            if paired {
                return Ok(true);
            }
            delay().await;
            self.check_identity(identity)?;
        }
        Ok(false)
    }

    /// Retry pairing after a delay only if the original attempt remains uncancelled.
    async fn retry_pairing_after<D, R, RF>(
        &self,
        epoch: u64,
        delay: D,
        retry: R,
    ) -> Result<(), PebbleError>
    where
        D: std::future::Future<Output = ()>,
        R: FnOnce() -> RF,
        RF: std::future::Future<Output = Result<(), PebbleError>>,
    {
        self.ensure_attempt(epoch)?;
        delay.await;
        self.ensure_attempt(epoch)?;
        retry().await
    }

    /// Return the active transport's status or reject a missing or superseded session.
    fn transport_status(
        &self,
        generation: u64,
    ) -> Result<watch::Receiver<TransportStatus>, PebbleError> {
        let inner = self.inner.lock().unwrap();
        if inner.connection_id != generation {
            return Err(PebbleError::NotConnected);
        }
        inner
            .transport
            .as_ref()
            .map(PpogTransport::status)
            .ok_or(PebbleError::NotConnected)
    }

    /// Reuse forward mode or await reverse teardown before registering its replacement.
    /// Recheck the generation around asynchronous work to reject concurrent disconnects.
    async fn prepare_forward<F, FF>(
        &self,
        generation: u64,
        start: F,
    ) -> Result<watch::Receiver<TransportStatus>, PebbleError>
    where
        F: FnOnce() -> FF,
        FF: std::future::Future<Output = Result<PpogTransport, PebbleError>>,
    {
        {
            let inner = self.inner.lock().unwrap();
            if inner.connection_id != generation {
                return Err(PebbleError::NotConnected);
            }
            if let Some(transport) = &inner.transport
                && transport.mode() == TransportMode::Forward
            {
                return Ok(transport.status());
            }
        }
        self.stop_transport(generation).await?;
        if self.inner.lock().unwrap().connection_id != generation {
            return Err(PebbleError::NotConnected);
        }
        let transport = start().await?;
        self.install_transport(transport, generation)
    }

    /// Install and activate a transport only while its session generation is current.
    fn install_transport(
        &self,
        transport: PpogTransport,
        generation: u64,
    ) -> Result<watch::Receiver<TransportStatus>, PebbleError> {
        let status = transport.status();
        // Activate only after installing the send path. The watch can request
        // PhoneVersion immediately after the reset exchange completes.
        let mut inner = self.inner.lock().unwrap();
        if inner.connection_id != generation {
            return Err(PebbleError::NotConnected);
        }
        inner.transport = Some(transport);
        inner.transport.as_mut().unwrap().activate();
        Ok(status)
    }

    /// Remove the current transport and await its owned cleanup before allowing fallback.
    async fn stop_transport(&self, generation: u64) -> Result<(), PebbleError> {
        let done = {
            let mut inner = self.inner.lock().unwrap();
            if inner.connection_id != generation {
                return Err(PebbleError::NotConnected);
            }
            inner.transport.take().map(|transport| {
                transport.stop();
                let lifecycle = self.lifecycle.clone();
                self.lifecycle.cleanup.lock().unwrap().push(async move {
                    let _lifecycle = lifecycle;
                    transport.shutdown().await
                })
            })
        };
        if let Some(done) = done {
            done.wait().await?;
        }
        Ok(())
    }

    fn track_connection_task(&self, task: tokio::task::JoinHandle<()>) {
        self.inner
            .lock()
            .unwrap()
            .connection_tasks
            .push(TaskGuard(task));
    }

    /// Route transport closure and confirmed BlueZ disconnection through cleanup.
    /// The generation check prevents an old monitor from clearing a replacement session.
    fn monitor_connection(
        &self,
        device: Device,
        transport: watch::Receiver<TransportStatus>,
        generation: u64,
    ) {
        let weak_inner = Arc::downgrade(&self.inner);
        let connected_tx = Arc::clone(&self.connected_tx);
        let lifecycle = self.lifecycle.clone();
        self.track_connection_task(tokio::spawn(async move {
            let started = tokio::time::Instant::now();
            let events = device.events().await;
            let events = match events {
                Ok(events) => Some(Box::pin(events)),
                Err(error) => {
                    warn!(
                        "device event subscription failed: {error}; using connection-status polling"
                    );
                    None
                }
            };
            let reason = wait_for_link_loss(
                transport,
                events,
                tokio::time::interval(Duration::from_secs(60)),
                || device.is_connected(),
            )
            .await;
            if let Some(inner) = weak_inner.upgrade() {
                warn!(
                    reason,
                    uptime_secs = started.elapsed().as_secs(),
                    "PPoGATT/BLE link closed; cleaning up connection"
                );
                Pebble::clear_connection_for(&inner, &connected_tx, &lifecycle, Some(generation));
            }
        }));
    }

    // ── private connection helpers ─────────────────────────────────────

    /// Retry BLE connection with rediscovery and confirmed teardown between attempts.
    /// Retain issued Connect requests so cancellation cannot leave a late connection open.
    async fn connect_client(
        &self,
        adapter: &bluer::Adapter,
        address: Address,
        conn_timeout: f64,
        attempts: u32,
        retry_delay: f64,
        identity: AttemptId,
    ) -> Result<Device, PebbleError> {
        let mut last_err: Option<PebbleError> = None;
        for i in 1..=attempts {
            self.check_identity(identity)?;
            if i > 1 {
                let scan_duration: f64 = 3.0 * i as f64;
                let _ = Pebble::scan(adapter.name(), scan_duration).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            };
            let device = adapter.device(address)?;
            let request_device = device.clone();
            let request = self.begin_request(identity, Some(device.clone()), async move {
                request_device.connect().await.map_err(PebbleError::from)
            })?;
            let result = timeout(Duration::from_secs_f64(conn_timeout), request.wait()).await;
            self.check_identity(identity)?;
            match result {
                Ok(Ok(())) => return Ok(device),
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    if msg.to_lowercase().contains("already connected") {
                        debug!("device was already connected; attaching");
                        return Ok(device);
                    }
                    warn!("connect attempt {i}/{attempts} failed: {e}");
                    last_err = Some(e);
                }
                Err(_) => {
                    warn!("connect attempt {i}/{attempts} timed out");
                    last_err = Some(PebbleError::Timeout(format!(
                        "connect to {} timed out after {conn_timeout}s",
                        self.address
                    )));
                }
            }
            if i < attempts {
                // A timed-out Connect call still owns a BlueZ request. Finish
                // it before disconnecting or starting the next retry.
                let _ = request.wait().await;
                let retry_device = device.clone();
                let lifecycle = self.lifecycle.clone();
                let done = self.lifecycle.cleanup.lock().unwrap().push(async move {
                    let _lifecycle = lifecycle;
                    super::lifecycle::disconnect_device(&retry_device).await
                });
                done.wait().await?;
                tokio::time::sleep(Duration::from_secs_f64(retry_delay * i as f64)).await;
            }
        }
        Err(last_err.unwrap_or_else(|| PebbleError::Other("connect failed".into())))
    }

    /// Prefer watch-initiated bonding, then retain a host Pair request if polling expires.
    /// Cancelled identities cannot initiate pairing; teardown awaits any issued request.
    async fn do_pairing(&self, device: &Device, identity: AttemptId) -> Result<(), PebbleError> {
        self.check_identity(identity)?;
        if let Some(c) = dispatch::find_char(device, PAIRING_TRIGGER_CHARACTERISTIC).await {
            self.check_identity(identity)?;
            let _ = c.read().await;
            self.check_identity(identity)?;
            let _ = c.write(&[0x09]).await;
        }
        info!("waiting for the watch to initiate bonding; CONFIRM ON THE WATCH when prompted");
        if self
            .poll_pairing(
                identity,
                20,
                || device.is_paired(),
                || tokio::time::sleep(Duration::from_millis(500)),
            )
            .await?
        {
            debug!("bonded (watch-initiated)");
            return Ok(());
        }
        debug!("watch did not initiate bonding; calling pair() from the host");
        let pair_device = device.clone();
        let request = self.begin_request(identity, Some(device.clone()), async move {
            pair_device.pair().await.map_err(PebbleError::from)
        })?;
        if let Err(error) = request.wait().await {
            warn!("host-initiated pair() failed: {error}");
        }
        if self
            .poll_pairing(
                identity,
                6,
                || device.is_paired(),
                || tokio::time::sleep(Duration::from_millis(500)),
            )
            .await?
        {
            debug!("bonded (host-initiated)");
            return Ok(());
        }
        Err(PebbleError::PairingFailed("pairing timed out".into()))
    }

    /// Own available fed9 subscriptions and apply the watch's MTU hint to the transport.
    async fn subscribe_fed9(&self, device: &Device) {
        for (char_uuid, label) in [
            (CONNECTIVITY_CHARACTERISTIC, "connectivity"),
            (MTU_CHARACTERISTIC, "MTU"),
            (CONNECTION_PARAMS_CHARACTERISTIC, "connection-params"),
        ] {
            if let Some(c) = dispatch::find_char(device, char_uuid).await {
                let label = label.to_string();
                let mut subscription = crate::transport::notification::Subscription::start(c);
                let mut events = subscription.events();
                self.inner.lock().unwrap().subscriptions.push(subscription);
                self.track_connection_task(tokio::spawn(async move {
                    while let Some(data) = events.recv().await {
                        trace!("{label} update: {} bytes", data.len());
                    }
                }));
            } else {
                trace!("fed9 characteristic {label} not found on device");
            }
        }

        if let Some(c) = dispatch::find_char(device, MTU_CHARACTERISTIC).await
            && let Ok(data) = c.read().await
            && data.len() >= 2
        {
            let watch_mtu = u16::from_le_bytes([data[0], data[1]]) as usize;
            debug!("watch requested MTU: {watch_mtu}");
            if let Some(srv) = &self.inner.lock().unwrap().transport
                && watch_mtu >= 23
            {
                srv.set_mtu(watch_mtu);
            }
        }
    }

    /// Read the watch battery level once and subscribe to change notifications.
    async fn subscribe_battery(&self, device: &Device) {
        self.inner.lock().unwrap().battery_level = None;
        let Some(c) = dispatch::find_char(device, BATTERY_LEVEL_CHARACTERISTIC).await else {
            debug!("no battery characteristic; battery level unavailable");
            return;
        };
        match c.read().await {
            Ok(data) => {
                super::dispatch::update_battery(&self.inner, &data);
            }
            Err(e) => debug!("battery read failed: {e}"),
        }
        let inner = Arc::downgrade(&self.inner);
        let mut subscription = crate::transport::notification::Subscription::start(c);
        let mut events = subscription.events();
        self.inner.lock().unwrap().subscriptions.push(subscription);
        self.track_connection_task(tokio::spawn(async move {
            while let Some(data) = events.recv().await {
                if let Some(inner) = inner.upgrade() {
                    super::dispatch::update_battery(&inner, &data);
                }
            }
        }));
    }

    async fn write_pairing_trigger(&self, device: &Device) {
        if let Some(c) = dispatch::find_char(device, PAIRING_TRIGGER_CHARACTERISTIC).await {
            if let Err(e) = c.write(&[0x09]).await {
                warn!("pairing trigger write failed: {e}");
            } else {
                debug!("wrote 0x09 to pairing trigger (server mode)");
            }
        }
    }

    async fn negotiate_blobdb2_version(&self) -> u8 {
        let token = dispatch::rand_u16();
        let (tx, rx) = tokio::sync::oneshot::channel::<BlobDB2Incoming>();
        self.inner.lock().unwrap().blobdb2_pending.insert(token, tx);
        if self
            .send_pebble(Endpoint::BlobDbV2, &build_blobdb2_version(token))
            .is_err()
        {
            self.inner.lock().unwrap().blobdb2_pending.remove(&token);
            return 0;
        }
        match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(BlobDB2Incoming::VersionResponse {
                status, version, ..
            })) if status == BlobDBStatus::Success as u8 => {
                debug!("BlobDB2 version: {version}");
                version
            }
            _ => {
                self.inner.lock().unwrap().blobdb2_pending.remove(&token);
                debug!("BlobDB2 version query timed out; assuming v0");
                0
            }
        }
    }

    // ── teardown ───────────────────────────────────────────────────────

    /// Remove the Bluetooth bond (unpair). The supervisor re-pairs on the
    /// next reconnect.
    pub async fn forget(&self) -> Result<(), PebbleError> {
        self.forget_for_attempt(None).await
    }

    /// Retain RemoveDevice until it completes, checking the retry epoch when supplied.
    async fn forget_for_attempt(&self, epoch: Option<u64>) -> Result<(), PebbleError> {
        if let Some(epoch) = epoch {
            self.ensure_attempt(epoch)?;
        }
        let address = Address::from_str(&self.address)
            .map_err(|e| PebbleError::Other(format!("invalid address: {e}")))?;
        let session = bluer::Session::new().await?;
        let adapter = session.adapter(&self.adapter_name)?;
        let request = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(epoch) = epoch {
                self.ensure_attempt(epoch)?;
            }
            let request = crate::transport::lifecycle::spawn_cleanup(async move {
                adapter
                    .remove_device(address)
                    .await
                    .map_err(PebbleError::from)
            });
            inner.connection_requests.push(request.clone());
            request
        };
        request.wait().await?;
        info!("removed {} from BlueZ (stale bond cleared)", self.address);
        Ok(())
    }
}

/// A failed property read leaves connection state unknown. Keep monitoring until
/// a transport closes, BlueZ reports disconnection, or the device is removed.
async fn wait_for_link_loss<E, C, CF>(
    mut transport: watch::Receiver<TransportStatus>,
    mut events: Option<E>,
    mut poll: tokio::time::Interval,
    mut connected: C,
) -> &'static str
where
    E: futures::Stream<Item = bluer::DeviceEvent> + Unpin,
    C: FnMut() -> CF,
    CF: std::future::Future<Output = Result<bool, bluer::Error>>,
{
    poll.tick().await; // Skip the immediate first tick.
    loop {
        if *transport.borrow_and_update() == TransportStatus::Closed {
            return "PPoGATT transport closed";
        }
        tokio::select! {
            changed = transport.changed() => {
                if changed.is_err() {
                    return "PPoGATT status stream ended";
                }
            }
            event = async {
                match &mut events {
                    Some(events) => events.next().await,
                    None => std::future::pending().await,
                }
            } => {
                match event {
                    None => return "BlueZ device event stream ended",
                    Some(bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(false))) => {
                        return "BlueZ reported Connected=false";
                    }
                    _ => {}
                }
            }
            _ = poll.tick() => {
                match connected().await {
                    Ok(true) => trace!("BLE connection poll confirmed connected"),
                    Ok(false) => return "BlueZ connection poll reported disconnected",
                    Err(error) if matches!(error.kind, bluer::ErrorKind::NotFound | bluer::ErrorKind::DoesNotExist) => {
                        return "BlueZ device no longer exists";
                    }
                    Err(error) => warn!("BLE connection poll failed: {error}; keeping current session"),
                }
            }
        }
    }
}

/// Keep fallback policy independent of BlueZ so failures and missing services
/// can be tested without modifying any real Bluetooth connection.
async fn select_transport<R, RF, F, FF>(
    has_reverse: bool,
    reverse: R,
    forward: F,
) -> Result<TransportMode, PebbleError>
where
    R: FnOnce() -> RF,
    RF: std::future::Future<Output = Result<(), PebbleError>>,
    F: FnOnce() -> FF,
    FF: std::future::Future<Output = Result<(), PebbleError>>,
{
    if has_reverse {
        debug!("PPoGATT attempting reverse-v2");
        match reverse().await {
            Ok(()) => return Ok(TransportMode::ReverseV2),
            Err(error) => {
                warn!("reverse PPoGATT v2 setup failed: {error}; falling back to forward")
            }
        }
    } else {
        debug!("reverse PPoGATT v2 service unavailable; using forward");
    }
    forward().await?;
    Ok(TransportMode::Forward)
}

struct ConnectionAttemptGuard<'a> {
    pebble: &'a Pebble,
    operation: Option<tokio::sync::OwnedMutexGuard<()>>,
    finished: bool,
}
impl Drop for ConnectionAttemptGuard<'_> {
    /// Hand a cancelled attempt's gate to cleanup so a replacement cannot connect early.
    fn drop(&mut self) {
        if !self.finished {
            let done = self.pebble.clear_connection();
            let operation = self.operation.take();
            // Handoff the lock as well as the cleanup receipt. A replacement
            // Pebble for the same address cannot connect before teardown ends.
            tokio::spawn(async move {
                if let Some(done) = done {
                    let _ = done.wait().await;
                }
                drop(operation);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::ping::build_pong;
    use crate::endpoints::{Endpoint, pebble_pack};
    use crate::transport::test_transport;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A transient polling failure must survive a subsequent successful poll;
    /// confirmed disconnection must still close the link, with or without events.
    #[tokio::test]
    async fn connection_monitor_survives_failed_poll_until_confirmed_disconnection() {
        for with_events in [false, true] {
            let (_status, transport) = watch::channel(TransportStatus::Ready);
            let unavailable = dbus::Error::new_custom(
                "org.freedesktop.DBus.Error.NoReply",
                "temporary backend failure",
            );
            let mut checks = std::collections::VecDeque::from([
                Err(bluer::Error::from(unavailable)),
                Ok(true),
                Ok(false),
            ]);
            let reason = timeout(
                Duration::from_secs(1),
                wait_for_link_loss(
                    transport,
                    with_events.then(futures::stream::pending::<bluer::DeviceEvent>),
                    tokio::time::interval(Duration::from_millis(1)),
                    || std::future::ready(checks.pop_front().expect("unexpected extra poll")),
                ),
            )
            .await
            .unwrap();
            assert_eq!(reason, "BlueZ connection poll reported disconnected");
            assert!(checks.is_empty());
        }
    }

    #[tokio::test]
    async fn connection_monitor_detects_bluez_events_without_waiting_for_poll() {
        let (_status, transport) = watch::channel(TransportStatus::Ready);
        let events = futures::stream::iter([
            bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(true)),
            bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(false)),
        ]);
        let reason = timeout(
            Duration::from_secs(1),
            wait_for_link_loss(
                transport,
                Some(events),
                tokio::time::interval(Duration::from_secs(60)),
                || async { panic!("connection event should close the link before polling") },
            ),
        )
        .await
        .unwrap();
        assert_eq!(reason, "BlueZ reported Connected=false");
    }

    #[tokio::test]
    async fn connection_monitor_detects_transport_closure_after_poll_failure() {
        let (status, transport) = watch::channel(TransportStatus::Ready);
        let reason = timeout(
            Duration::from_secs(1),
            wait_for_link_loss(
                transport,
                Some(futures::stream::pending::<bluer::DeviceEvent>()),
                tokio::time::interval(Duration::from_millis(1)),
                || {
                    status.send_replace(TransportStatus::Closed);
                    std::future::ready(Err(bluer::Error::from(dbus::Error::new_custom(
                        "org.freedesktop.DBus.Error.NoReply",
                        "temporary backend failure",
                    ))))
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(reason, "PPoGATT transport closed");
    }

    #[tokio::test]
    async fn connection_monitor_detects_removed_device_without_events() {
        let (_status, transport) = watch::channel(TransportStatus::Ready);
        let reason = timeout(
            Duration::from_secs(1),
            wait_for_link_loss(
                transport,
                None::<futures::stream::Pending<bluer::DeviceEvent>>,
                tokio::time::interval(Duration::from_millis(1)),
                || {
                    std::future::ready(Err(bluer::Error {
                        kind: bluer::ErrorKind::DoesNotExist,
                        message: "device removed".into(),
                    }))
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(reason, "BlueZ device no longer exists");
    }

    #[tokio::test]
    async fn absent_reverse_service_uses_forward_without_attempting_reverse() {
        assert_eq!(
            select_transport(
                false,
                || async {
                    panic!("reverse service is absent");
                },
                || async { Ok(()) }
            )
            .await
            .unwrap(),
            TransportMode::Forward
        );
    }

    #[tokio::test]
    async fn successful_reverse_setup_does_not_start_forward() {
        assert_eq!(
            select_transport(
                true,
                || async { Ok(()) },
                || async {
                    panic!("forward must not be started");
                }
            )
            .await
            .unwrap(),
            TransportMode::ReverseV2
        );
    }

    /// Check that either reverse startup failure reaches the forward transport.
    #[tokio::test]
    async fn reverse_subscription_and_handshake_failures_fall_back_to_forward() {
        for error in [
            PebbleError::Other("invalid cached GATT handle".into()),
            PebbleError::Timeout("reverse PPoGATT subscription".into()),
            PebbleError::Timeout("reverse PPoGATT handshake".into()),
        ] {
            let attempted = AtomicBool::new(false);
            let mode = select_transport(
                true,
                || async {
                    attempted.store(true, Ordering::SeqCst);
                    Err(error)
                },
                || async {
                    assert!(attempted.load(Ordering::SeqCst));
                    Ok(())
                },
            )
            .await
            .unwrap();
            assert_eq!(mode, TransportMode::Forward);
        }
    }

    #[tokio::test]
    async fn failure_of_both_transports_propagates_the_forward_failure() {
        let result = select_transport(
            true,
            || async { Err(PebbleError::Other("reverse failure".into())) },
            || async { Err(PebbleError::Timeout("forward handshake".into())) },
        )
        .await;
        assert!(
            matches!(result, Err(PebbleError::Timeout(message)) if message == "forward handshake")
        );
    }

    /// Exercise automatic endpoint replies through both selected transport modes.
    #[tokio::test]
    async fn automatic_endpoint_replies_use_the_selected_transport() {
        for mode in [TransportMode::Forward, TransportMode::ReverseV2] {
            let pebble = Pebble::new("00:00:00:00:00:00", "hci0");
            let (transport, mut outgoing) = test_transport(mode);
            pebble.install_transport(transport, 0).unwrap();
            // Ping request: command 0 followed by a big-endian cookie.
            dispatch::on_pebble_message(
                pebble_pack(Endpoint::Ping, &[0, 1, 2, 3, 4]).unwrap(),
                &pebble.inner,
            );
            assert_eq!(
                outgoing.recv().await.unwrap(),
                pebble_pack(Endpoint::Ping, &build_pong(0x01020304)).unwrap()
            );
            let mut open = vec![crate::endpoints::datalog::DATALOG_OPENSESSION, 7];
            open.extend_from_slice(&[0; 16]);
            open.extend_from_slice(&1_700_000_000_u32.to_le_bytes());
            open.extend_from_slice(&81_u32.to_le_bytes());
            open.push(0);
            open.extend_from_slice(&249_u16.to_le_bytes());
            dispatch::on_pebble_message(
                pebble_pack(Endpoint::DataLog, &open).unwrap(),
                &pebble.inner,
            );
            assert_eq!(
                outgoing.recv().await.unwrap(),
                pebble_pack(
                    Endpoint::DataLog,
                    &crate::endpoints::datalog::build_reply(7, true)
                )
                .unwrap()
            );
            assert_eq!(pebble.inner.lock().unwrap().datalog_sessions[&7].tag, 81);
            pebble.connected_tx.send_replace(true);
            pebble.fetch_health_data().unwrap();
            assert_eq!(
                outgoing.recv().await.unwrap(),
                pebble_pack(
                    Endpoint::DataLog,
                    &crate::endpoints::datalog::build_report_sessions()
                )
                .unwrap()
            );
            assert_eq!(
                outgoing.recv().await.unwrap(),
                pebble_pack(
                    Endpoint::HealthSync,
                    &crate::endpoints::health::build_health_sync_request()
                )
                .unwrap()
            );
            pebble.disconnect().await;
            assert!(!pebble.is_connected());
        }
    }

    /// Check that cancelling startup closes its transport and fails pending requests.
    #[tokio::test]
    async fn cancelled_connection_attempt_releases_transport_and_pending_requests() {
        let pebble = Pebble::new("00:00:00:00:00:00", "hci0");
        let (transport, _outgoing) = test_transport(TransportMode::ReverseV2);
        let mut status = pebble.install_transport(transport, 0).unwrap();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        pebble.inner.lock().unwrap().pending.insert(1, sender);
        let attempt = ConnectionAttemptGuard {
            pebble: &pebble,
            operation: None,
            finished: false,
        };
        drop(attempt);
        assert_eq!(receiver.await, Ok(false));
        assert!(pebble.inner.lock().unwrap().transport.is_none());
        timeout(Duration::from_secs(1), async {
            while *status.borrow_and_update() != TransportStatus::Closed {
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    /// Check that disconnect invalidates subsequent installation by the old attempt.
    #[tokio::test]
    async fn stale_connection_attempt_cannot_install_a_transport_after_disconnect() {
        let pebble = Pebble::new("00:00:00:00:00:00", "hci0");
        let (transport, _outgoing) = test_transport(TransportMode::ReverseV2);
        let mut status = transport.status();
        pebble.disconnect().await;
        assert!(pebble.install_transport(transport, 0).is_err());
        assert!(pebble.inner.lock().unwrap().transport.is_none());
        assert!(!pebble.is_connected());
        timeout(Duration::from_secs(1), async {
            while *status.borrow_and_update() != TransportStatus::Closed {
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancelled_startup_holds_the_shared_gate_until_old_disconnect_finishes() {
        let pebble = Arc::new(Pebble::new("00:00:00:00:00:11", "council-cancel"));
        let (release, blocked) = tokio::sync::oneshot::channel();
        let (disconnect_started, disconnecting) = tokio::sync::oneshot::channel();
        pebble.lifecycle.cleanup.lock().unwrap().push(async move {
            disconnect_started.send(()).unwrap();
            blocked.await.unwrap();
            Ok(())
        });
        disconnecting.await.unwrap();
        let (startup_started, started) = tokio::sync::oneshot::channel();
        let old = pebble.clone();
        let startup = tokio::spawn(async move {
            let operation = old.lifecycle.operation.clone().lock_owned().await;
            let _guard = ConnectionAttemptGuard {
                pebble: &old,
                operation: Some(operation),
                finished: false,
            };
            startup_started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        startup.abort();
        assert!(startup.await.unwrap_err().is_cancelled());
        let replacement = Pebble::new("00:00:00:00:00:11", "council-cancel");
        assert!(Arc::ptr_eq(&pebble.lifecycle, &replacement.lifecycle));
        drop(pebble);
        let mut reconnect = Box::pin(replacement.lifecycle.operation.clone().lock_owned());
        assert!(futures::poll!(reconnect.as_mut()).is_pending());
        release.send(()).unwrap();
        let _operation = timeout(Duration::from_secs(1), reconnect).await.unwrap();
    }

    /// Exercise detected link loss without requiring a later public disconnect call.
    #[tokio::test]
    async fn detected_link_loss_cleans_resources_and_nacks_requests_without_public_disconnect() {
        use crate::transport::notification::test_subscription;
        let pebble = Pebble::new("00:00:00:00:00:12", "council-loss");
        let (transport, _outgoing) = test_transport(TransportMode::ReverseV2);
        let mut status = pebble.install_transport(transport, 0).unwrap();
        pebble.connected_tx.send_replace(true);
        let (stop_started, stopping) = tokio::sync::oneshot::channel();
        let (release, stopped) = tokio::sync::oneshot::channel();
        let subscription = test_subscription(stop_started, stopped);
        let (nack, nacked) = tokio::sync::oneshot::channel();
        let (version, version_result) = tokio::sync::oneshot::channel();
        {
            let mut inner = pebble.inner.lock().unwrap();
            inner.pending.insert(42, nack);
            inner.watch_version_pending.push(version);
            inner.subscriptions.push(subscription);
        }
        // Both Closed transport status and Device.Connected=false reach this
        // production monitor cleanup path, with the captured generation.
        let done = Pebble::clear_connection_for(
            &pebble.inner,
            &pebble.connected_tx,
            &pebble.lifecycle,
            Some(0),
        )
        .unwrap();
        assert!(!pebble.is_connected());
        assert_eq!(nacked.await, Ok(false));
        assert!(version_result.await.is_err());
        assert!(pebble.inner.lock().unwrap().transport.is_none());
        assert!(pebble.inner.lock().unwrap().subscriptions.is_empty());
        stopping.await.unwrap();
        while *status.borrow_and_update() != TransportStatus::Closed {
            status.changed().await.unwrap();
        }
        let mut waiting = Box::pin(done.wait());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        release.send(()).unwrap();
        timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn old_link_loss_callback_cannot_clean_a_new_generation() {
        let pebble = Pebble::new("00:00:00:00:00:13", "council-generation");
        pebble.inner.lock().unwrap().connection_id = 1;
        let (transport, _outgoing) = test_transport(TransportMode::Forward);
        pebble.install_transport(transport, 1).unwrap();
        pebble.connected_tx.send_replace(true);
        assert!(
            Pebble::clear_connection_for(
                &pebble.inner,
                &pebble.connected_tx,
                &pebble.lifecycle,
                Some(0)
            )
            .is_none()
        );
        assert!(pebble.is_connected());
        assert!(pebble.inner.lock().unwrap().transport.is_some());
        pebble.disconnect().await;
    }

    #[tokio::test]
    async fn disconnect_waits_for_an_in_flight_connect_request_after_cancellation() {
        let pebble = Pebble::new("00:00:00:00:00:14", "council-request");
        let (release, pending_reply) = tokio::sync::oneshot::channel();
        let request = crate::transport::lifecycle::spawn_cleanup(async move {
            pending_reply.await.unwrap();
            Ok(())
        });
        pebble
            .inner
            .lock()
            .unwrap()
            .connection_requests
            .push(request);
        let done = pebble.clear_connection().unwrap();
        let mut cancelled = Box::pin(done.wait());
        assert!(futures::poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);
        let pending = pebble.lifecycle.cleanup.lock().unwrap().pending().unwrap();
        let mut reconnect = Box::pin(pending.wait());
        assert!(futures::poll!(reconnect.as_mut()).is_pending());
        release.send(()).unwrap();
        timeout(Duration::from_secs(1), reconnect)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn completed_disconnect_cancels_pairing_retry_before_forget_or_connect() {
        use std::sync::atomic::AtomicUsize;
        let pebble = Pebble::new("00:00:00:00:00:21", "council-pairing-retry");
        let (release, wait) = tokio::sync::oneshot::channel();
        let removes = AtomicUsize::new(0);
        let connects = AtomicUsize::new(0);
        let mut retry = Box::pin(pebble.retry_pairing_after(
            0,
            async {
                wait.await.unwrap();
            },
            || async {
                removes.fetch_add(1, Ordering::SeqCst);
                connects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert!(futures::poll!(retry.as_mut()).is_pending());
        pebble.disconnect().await;
        release.send(()).unwrap();
        assert!(matches!(retry.await, Err(PebbleError::NotConnected)));
        assert_eq!(removes.load(Ordering::SeqCst), 0);
        assert_eq!(connects.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn forward_setup_reuses_existing_transport_and_checks_disconnect_without_panicking() {
        let pebble = Pebble::new("00:00:00:00:00:22", "council-forward-reuse");
        let (transport, _outgoing) = test_transport(TransportMode::Forward);
        pebble.install_transport(transport, 0).unwrap();
        pebble
            .prepare_forward(0, || async { panic!("forward already exists") })
            .await
            .unwrap();
        pebble.disconnect().await;
        assert!(matches!(
            pebble.transport_status(0),
            Err(PebbleError::NotConnected)
        ));
        assert!(
            pebble
                .prepare_forward(0, || async { panic!("attempt was disconnected") })
                .await
                .is_err()
        );
        assert!(!pebble.inner.is_poisoned());
    }

    #[tokio::test]
    async fn disconnect_during_forward_registration_returns_error_without_poisoning_state() {
        let pebble = Pebble::new("00:00:00:00:00:23", "council-forward-registration");
        let (release, wait) = tokio::sync::oneshot::channel();
        let mut setup = Box::pin(pebble.prepare_forward(0, || async {
            wait.await.unwrap();
            Ok(test_transport(TransportMode::Forward).0)
        }));
        assert!(futures::poll!(setup.as_mut()).is_pending());
        pebble.disconnect().await;
        release.send(()).unwrap();
        assert!(matches!(setup.await, Err(PebbleError::NotConnected)));
        assert!(!pebble.inner.is_poisoned());
        assert!(pebble.inner.lock().unwrap().transport.is_none());
    }

    /// Block reverse teardown and check that forward registration waits and honours disconnect.
    #[tokio::test]
    async fn actual_forward_setup_waits_for_reverse_cleanup_and_respects_disconnect() {
        use crate::transport::test_transport_with_cleanup;
        for disconnect in [false, true] {
            let pebble = Pebble::new(
                "00:00:00:00:00:24",
                if disconnect {
                    "council-fallback-cancel"
                } else {
                    "council-fallback-order"
                },
            );
            let (entered, stopping) = tokio::sync::oneshot::channel();
            let (release, wait) = tokio::sync::oneshot::channel();
            let (reverse, _outgoing) =
                test_transport_with_cleanup(TransportMode::ReverseV2, async move {
                    entered.send(()).unwrap();
                    wait.await.unwrap();
                });
            pebble.install_transport(reverse, 0).unwrap();
            let started_forward = AtomicBool::new(false);
            let mut setup = Box::pin(pebble.prepare_forward(0, || async {
                started_forward.store(true, Ordering::SeqCst);
                Ok(test_transport(TransportMode::Forward).0)
            }));
            assert!(futures::poll!(setup.as_mut()).is_pending());
            stopping.await.unwrap();
            assert!(!started_forward.load(Ordering::SeqCst));
            if disconnect {
                pebble.clear_connection();
            }
            release.send(()).unwrap();
            let result = timeout(Duration::from_secs(1), setup).await.unwrap();
            assert_eq!(result.is_ok(), !disconnect);
            assert_eq!(started_forward.load(Ordering::SeqCst), !disconnect);
            pebble.disconnect().await;
        }
    }

    /// Disconnect during a pending pairing poll and check that no host Pair is issued.
    #[tokio::test]
    async fn disconnect_during_pairing_poll_prevents_host_pair_from_running() {
        use std::sync::atomic::AtomicUsize;
        let pebble = Pebble::new("00:00:00:00:00:30", "council-pair-poll");
        let identity = pebble.capture_attempt(0).unwrap();
        let (release, wait) = tokio::sync::oneshot::channel();
        let mut wait = Some(wait);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut pairing = Box::pin(async {
            if !pebble
                .poll_pairing(
                    identity,
                    1,
                    || async { Ok(false) },
                    || {
                        let wait = wait.take().expect("one polling delay");
                        async move {
                            let _ = wait.await;
                        }
                    },
                )
                .await?
            {
                let calls = calls.clone();
                let request = pebble.begin_request(identity, None, async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })?;
                request.wait().await?;
            }
            Ok::<(), PebbleError>(())
        });
        assert!(futures::poll!(pairing.as_mut()).is_pending());
        pebble.disconnect().await;
        release.send(()).unwrap();
        assert!(matches!(pairing.await, Err(PebbleError::NotConnected)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn disconnect_retains_pending_pair_until_its_rpc_completes() {
        let pebble = Pebble::new("00:00:00:00:00:31", "council-pair-pending");
        let identity = pebble.capture_attempt(0).unwrap();
        let (started, running) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        let request = pebble
            .begin_request(identity, None, async move {
                started.send(()).unwrap();
                wait.await.unwrap();
                Ok(())
            })
            .unwrap();
        running.await.unwrap();
        let mut disconnect = Box::pin(pebble.disconnect());
        assert!(futures::poll!(disconnect.as_mut()).is_pending());
        assert!(pebble.inner.lock().unwrap().connection_requests.is_empty());
        let mut pending_pair = Box::pin(request.wait());
        assert!(futures::poll!(pending_pair.as_mut()).is_pending());
        release.send(()).unwrap();
        timeout(Duration::from_secs(1), disconnect).await.unwrap();
        pending_pair.await.unwrap();
        assert!(pebble.check_identity(identity).is_err());
    }

    #[tokio::test]
    async fn cancelled_epoch_cannot_adopt_new_generation_or_register_connect() {
        let pebble = Pebble::new("00:00:00:00:00:32", "council-capture");
        pebble.ensure_attempt(0).unwrap(); // Original check passes.
        pebble.disconnect().await; // Deterministic interleaving before capture.
        assert!(matches!(
            pebble.capture_attempt(0),
            Err(PebbleError::NotConnected)
        ));
        let replacement_generation = pebble.inner.lock().unwrap().connection_id;
        let calls = Arc::new(AtomicBool::new(false));
        let mark = calls.clone();
        assert!(
            pebble
                .begin_request(
                    AttemptId {
                        epoch: 0,
                        generation: replacement_generation
                    },
                    None,
                    async move {
                        mark.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                )
                .is_err()
        );
        tokio::task::yield_now().await;
        assert!(!calls.load(Ordering::SeqCst));
        assert!(pebble.inner.lock().unwrap().connection_requests.is_empty());
    }
}
