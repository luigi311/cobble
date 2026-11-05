//! Transport-independent PPoGATT handshake, framing, and task lifetime.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use super::lifecycle::{Completion, spawn_cleanup};
use super::ppogatt::{
    PPOGATT_WINDOW, PPoGATTSession, PPoGATTType, parse_ppogatt_header, ppogatt_header,
};
use crate::error::PebbleError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    Forward,
    ReverseV2,
}

impl fmt::Display for TransportMode {
    /// Format the stable transport label used in connection logs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Forward => "forward",
            Self::ReverseV2 => "reverse-v2",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportStatus {
    Starting,
    Ready,
    Closed,
}

pub(crate) struct TaskGuard(pub JoinHandle<()>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Owns a single transport. Dropping it stops its I/O and releases the local
/// service or remote notification subscription before a later connection.
pub struct PpogTransport {
    mode: TransportMode,
    send_tx: mpsc::UnboundedSender<Vec<u8>>,
    packet_size: Arc<Mutex<usize>>,
    status: watch::Receiver<TransportStatus>,
    start: Option<oneshot::Sender<()>>,
    stop: watch::Sender<bool>,
    done: Completion,
    _app: Option<bluer::gatt::local::ApplicationHandle>,
}

impl PpogTransport {
    /// Own an inactive worker and retain its completion so shutdown can await cleanup.
    pub(crate) fn new(
        mode: TransportMode,
        send_tx: mpsc::UnboundedSender<Vec<u8>>,
        packet_size: Arc<Mutex<usize>>,
        status: watch::Receiver<TransportStatus>,
        start: oneshot::Sender<()>,
        stop: watch::Sender<bool>,
        task: JoinHandle<Result<(), PebbleError>>,
    ) -> Self {
        Self {
            mode,
            send_tx,
            packet_size,
            status,
            start: Some(start),
            stop,
            done: spawn_cleanup(async move {
                task.await
                    .map_err(|error| PebbleError::Other(format!("transport worker: {error}")))?
            }),
            _app: None,
        }
    }

    pub(crate) fn with_application(mut self, app: bluer::gatt::local::ApplicationHandle) -> Self {
        self._app = Some(app);
        self
    }

    /// Queue a whole Pebble Protocol message. False means the link has closed.
    pub fn send(&self, message: Vec<u8>) -> bool {
        if *self.status.borrow() == TransportStatus::Closed || self.send_tx.send(message).is_err() {
            warn!("PPoGATT transport={} send channel closed", self.mode);
            false
        } else {
            true
        }
    }

    pub fn mode(&self) -> TransportMode {
        self.mode
    }

    /// Apply the watch's ATT MTU hint; the I/O task also observes BlueZ's actual
    /// characteristic value limit. Each PPoGATT packet includes one header byte.
    pub fn set_mtu(&self, mtu: usize) {
        *self.packet_size.lock().unwrap() = mtu.saturating_sub(3).max(20);
    }

    /// Start I/O after the caller has installed this handle as its send path.
    pub fn activate(&mut self) {
        if let Some(start) = self.start.take() {
            let _ = start.send(());
        }
    }

    pub(crate) fn status(&self) -> watch::Receiver<TransportStatus> {
        self.status.clone()
    }

    pub(crate) fn stop(&self) {
        self.stop.send_replace(true);
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), PebbleError> {
        self.stop();
        let result = self.done.wait().await;
        self._app.take();
        result
    }
}

impl Drop for PpogTransport {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Publishes closure even when its task is cancelled during setup or fallback.
pub(crate) struct StatusGuard(pub watch::Sender<TransportStatus>);

impl Drop for StatusGuard {
    fn drop(&mut self) {
        self.0.send_replace(TransportStatus::Closed);
    }
}

/// Await the retained handshake status, failing if the transport closes before readiness.
pub(crate) async fn wait_ready(
    mut status: watch::Receiver<TransportStatus>,
) -> Result<(), PebbleError> {
    loop {
        match *status.borrow_and_update() {
            TransportStatus::Ready => return Ok(()),
            TransportStatus::Closed => {
                return Err(PebbleError::Other("PPoGATT closed during handshake".into()));
            }
            TransportStatus::Starting => {}
        }
        status
            .changed()
            .await
            .map_err(|_| PebbleError::Other("PPoGATT handshake task stopped".into()))?;
    }
}

pub(crate) struct LinkOutput {
    pub packets: Vec<Vec<u8>>,
    pub messages: Vec<Vec<u8>>,
}

/// V2 names the GATT service, not a new framing version. It still negotiates
/// PPoGATT framing v1, exactly as upstream PPoG.kt / PPoGPacket.kt do.
pub(crate) struct LinkState {
    mode: TransportMode,
    session: PPoGATTSession,
    pending: VecDeque<Vec<u8>>,
    // Some(true) = we initiated; Some(false) = watch initiated.
    awaiting_complete: Option<bool>,
    version: u8,
    ready: bool,
    tx_window: u8,
}

impl LinkState {
    pub fn new(mode: TransportMode) -> Self {
        Self {
            mode,
            session: PPoGATTSession::new(),
            pending: VecDeque::new(),
            awaiting_complete: (mode == TransportMode::ReverseV2).then_some(true),
            version: 1,
            ready: false,
            tx_window: PPOGATT_WINDOW,
        }
    }

    pub fn initial_packet(&self) -> Option<Vec<u8>> {
        (self.mode == TransportMode::ReverseV2)
            .then(|| vec![ppogatt_header(PPoGATTType::ResetRequest, 0), 1])
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn enqueue(&mut self, message: Vec<u8>) {
        self.pending.push_back(message);
    }

    /// Process resets, DATA, and ACKs, returning control packets and complete messages.
    pub fn receive(&mut self, packet: &[u8]) -> LinkOutput {
        let mut output = LinkOutput {
            packets: Vec::new(),
            messages: Vec::new(),
        };
        let Some(&header) = packet.first() else {
            return output;
        };
        let (cmd, serial) = parse_ppogatt_header(header);
        let body = &packet[1..];
        trace!(
            "PPoGATT rx transport={} cmd={cmd} serial={serial} len={}",
            self.mode,
            body.len()
        );
        match PPoGATTType::from_u8(cmd) {
            Some(PPoGATTType::ResetRequest) => {
                let version = body.first().copied().unwrap_or(0);
                if version > 1 {
                    warn!("unsupported PPoGATT framing version={version}");
                    return output;
                }
                self.session.reset();
                if self.ready {
                    self.pending.clear();
                }
                self.ready = false;
                self.version = version;
                self.awaiting_complete = Some(false);
                output.packets.push(self.reset_complete());
            }
            Some(PPoGATTType::ResetComplete) if self.ready => {
                self.ready = false;
                self.session.reset();
                self.pending.clear();
            }
            Some(PPoGATTType::ResetComplete) if self.awaiting_complete.is_some() => {
                let window = match body {
                    [] => 4, // framing v0 window
                    [rx, tx] if *rx > 0 && *tx > 0 => (*rx).min(PPOGATT_WINDOW),
                    _ => {
                        warn!("invalid PPoGATT reset-complete windows");
                        return output;
                    }
                };
                self.tx_window = window;
                if self.awaiting_complete == Some(true) {
                    output.packets.push(self.reset_complete());
                }
                self.awaiting_complete = None;
                self.ready = true;
                debug!(
                    "PPoGATT reset complete transport={} tx_window={window}",
                    self.mode
                );
            }
            Some(PPoGATTType::Ack) if self.ready => self.session.on_ack(serial),
            Some(PPoGATTType::Data) if self.ready => {
                if let Some(messages) = self.session.on_data(serial, body) {
                    let last = self.session.rx_seq.wrapping_sub(1) & 0x1f;
                    output
                        .packets
                        .push(vec![ppogatt_header(PPoGATTType::Ack, last)]);
                    output.messages = messages;
                }
            }
            _ => trace!("PPoGATT ignored command={cmd} before/after handshake"),
        }
        output
    }

    /// Encode the reset confirmation for the negotiated framing version.
    fn reset_complete(&self) -> Vec<u8> {
        if self.version == 0 {
            vec![ppogatt_header(PPoGATTType::ResetComplete, 0)]
        } else {
            vec![
                ppogatt_header(PPoGATTType::ResetComplete, 0),
                PPOGATT_WINDOW,
                PPOGATT_WINDOW,
            ]
        }
    }

    /// Split only as packets leave the queue, so a negotiated MTU change also
    /// applies to queued messages. Never send DATA before handshake completion.
    pub fn next_packet(&mut self, packet_size: usize) -> Option<Vec<u8>> {
        if !self.ready || self.session.tx_inflight >= self.tx_window {
            return None;
        }
        let message = self.pending.front_mut()?;
        let count = message.len().min(packet_size.saturating_sub(1).max(1));
        let mut packet = vec![ppogatt_header(
            PPoGATTType::Data,
            self.session.next_tx_seq(),
        )];
        packet.extend(message.drain(..count));
        if message.is_empty() {
            self.pending.pop_front();
        }
        Some(packet)
    }
}

#[cfg(test)]
pub(crate) fn test_transport(
    mode: TransportMode,
) -> (PpogTransport, mpsc::UnboundedReceiver<Vec<u8>>) {
    test_transport_with_cleanup(mode, async {})
}

#[cfg(test)]
pub(crate) fn test_transport_with_cleanup(
    mode: TransportMode,
    cleanup: impl std::future::Future<Output = ()> + Send + 'static,
) -> (PpogTransport, mpsc::UnboundedReceiver<Vec<u8>>) {
    let (send, receive) = mpsc::unbounded_channel();
    let (status_tx, status_rx) = watch::channel(TransportStatus::Ready);
    let status = StatusGuard(status_tx);
    let (start_tx, start_rx) = oneshot::channel();
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let _status = status;
        let _ = start_rx.await;
        super::lifecycle::stopped(&mut stop_rx).await;
        cleanup.await;
        Ok(())
    });
    (
        PpogTransport::new(
            mode,
            send,
            Arc::new(Mutex::new(20)),
            status_rx,
            start_tx,
            stop_tx,
            task,
        ),
        receive,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::{Endpoint, pebble_pack};

    /// Complete a simulated reset exchange with the supplied peer receive window.
    fn ready(mode: TransportMode, window: u8) -> LinkState {
        let mut state = LinkState::new(mode);
        if mode == TransportMode::Forward {
            assert_eq!(state.receive(&[2, 1]).packets, [vec![3, 25, 25]]);
        }
        let reply = state.receive(&[3, window, 25]);
        assert_eq!(
            reply.packets,
            if mode == TransportMode::ReverseV2 {
                vec![vec![3, 25, 25]]
            } else {
                vec![]
            }
        );
        assert!(state.is_ready());
        state
    }

    /// Check that each transport completes its reset exchange before sending DATA.
    #[test]
    fn both_transports_use_the_upstream_reset_exchange() {
        for mode in [TransportMode::Forward, TransportMode::ReverseV2] {
            let state = LinkState::new(mode);
            assert_eq!(
                state.initial_packet(),
                (mode == TransportMode::ReverseV2).then_some(vec![2, 1])
            );
            let mut state = ready(mode, 25);
            let message = pebble_pack(Endpoint::Ping, &[0, 1, 2, 3, 4]).unwrap();
            state.enqueue(message.clone());
            assert_eq!(state.next_packet(20), Some([vec![0], message].concat()));
        }
    }

    /// Exercise both legacy reset encodings and the framing-v0 transmit window.
    #[test]
    fn forward_still_negotiates_legacy_framing_v0() {
        for request in [vec![2], vec![2, 0]] {
            let mut state = LinkState::new(TransportMode::Forward);
            assert_eq!(state.receive(&request).packets, [vec![3]]);
            assert!(!state.is_ready());
            assert!(state.receive(&[3]).packets.is_empty());
            assert!(state.is_ready());
            for _ in 0..5 {
                state.enqueue(vec![42]);
            }
            for _ in 0..4 {
                assert!(state.next_packet(20).is_some());
            }
            assert!(state.next_packet(20).is_none());
            state.receive(&[ppogatt_header(PPoGATTType::Ack, 3)]);
            assert!(state.next_packet(20).is_some());
        }
    }

    #[test]
    fn reverse_handles_watch_initiating_a_crossed_reset() {
        let mut state = LinkState::new(TransportMode::ReverseV2);
        assert_eq!(state.receive(&[2, 1]).packets, [vec![3, 25, 25]]);
        assert!(!state.is_ready());
        assert!(state.receive(&[3, 4, 4]).packets.is_empty());
        assert!(state.is_ready());
    }

    /// Check that an unexpected reset confirmation invalidates either active transport.
    #[test]
    fn in_session_reset_complete_marks_both_transports_not_ready() {
        for mode in [TransportMode::Forward, TransportMode::ReverseV2] {
            let mut state = ready(mode, 25);
            state.enqueue(vec![42]);
            let output = state.receive(&[3, 25, 25]);
            assert!(!state.is_ready());
            assert!(output.messages.is_empty());
            assert!(state.next_packet(20).is_none());
        }
    }

    /// Reject malformed resets and early DATA while preserving queued outbound messages.
    #[test]
    fn malformed_handshakes_and_early_data_do_not_establish_a_link() {
        let mut state = LinkState::new(TransportMode::ReverseV2);
        state.enqueue(vec![42]);
        for packet in [
            &[][..],
            &[3, 25],
            &[3, 0, 25],
            &[3, 25, 0],
            &[2, 2],
            &[0, 99],
            &[7],
        ] {
            let output = state.receive(packet);
            assert!(output.messages.is_empty());
            assert!(!state.is_ready());
            assert!(state.next_packet(20).is_none());
        }
        state.receive(&[3, 25, 25]);
        assert_eq!(state.next_packet(20), Some(vec![0, 42]));
    }

    /// Exercise window limits, cumulative ACKs, and sequence wrap across repeated sends.
    #[test]
    fn negotiated_transmit_windows_and_sequence_wrap_are_respected() {
        for mode in [TransportMode::Forward, TransportMode::ReverseV2] {
            let mut state = ready(mode, 3);
            for n in 0..39 {
                state.enqueue(vec![n]);
            }
            for group in 0..13 {
                for offset in 0..3 {
                    let n = group * 3 + offset;
                    assert_eq!(
                        state.next_packet(20),
                        Some(vec![ppogatt_header(PPoGATTType::Data, n & 31), n])
                    );
                }
                assert!(state.next_packet(20).is_none());
                // An ACK beyond the transmitted range cannot release the window.
                state.receive(&[ppogatt_header(PPoGATTType::Ack, (group * 3 + 3) & 31)]);
                assert!(state.next_packet(20).is_none());
                state.receive(&[ppogatt_header(PPoGATTType::Ack, (group * 3 + 2) & 31)]);
            }
            assert!(state.next_packet(20).is_none());
        }
    }

    /// Check packet size limits and exact message reconstruction at several GATT sizes.
    #[test]
    fn segmentation_fits_the_characteristic_at_default_and_large_mtu() {
        let message = pebble_pack(Endpoint::PutBytes, &vec![42; 300]).unwrap();
        for mode in [TransportMode::Forward, TransportMode::ReverseV2] {
            for limit in [20, 244] {
                let mut state = ready(mode, 25);
                state.enqueue(message.clone());
                let mut assembled = Vec::new();
                while let Some(packet) = state.next_packet(limit) {
                    assert!(packet.len() <= limit);
                    assembled.extend_from_slice(&packet[1..]);
                }
                assert_eq!(assembled, message);
            }
        }
    }

    /// Exercise out-of-order buffering, contiguous ACKs, and duplicate suppression.
    #[test]
    fn receive_reassembles_out_of_order_fragments_and_acknowledges_only_contiguous_data() {
        for mode in [TransportMode::Forward, TransportMode::ReverseV2] {
            let mut state = ready(mode, 25);
            let message = pebble_pack(Endpoint::Ping, &[0, 1, 2, 3, 4]).unwrap();
            let second = [
                vec![ppogatt_header(PPoGATTType::Data, 1)],
                message[3..].to_vec(),
            ]
            .concat();
            let first = [vec![0], message[..3].to_vec()].concat();
            assert!(state.receive(&second).packets.is_empty());
            let output = state.receive(&first);
            assert_eq!(output.messages, [message]);
            assert_eq!(output.packets, [vec![ppogatt_header(PPoGATTType::Ack, 1)]]);
            assert!(state.receive(&first).messages.is_empty());
        }
    }

    #[test]
    fn reset_discards_old_fragments_and_queued_messages() {
        let mut state = ready(TransportMode::Forward, 25);
        state.enqueue(vec![1, 2, 3]);
        state.receive(&[0, 0]); // Incomplete Pebble Protocol header.
        state.receive(&[2, 1]);
        assert!(!state.is_ready());
        state.receive(&[3, 25, 25]);
        assert!(state.next_packet(20).is_none());
        let message = pebble_pack(Endpoint::Ping, &[0]).unwrap();
        assert_eq!(
            state.receive(&[vec![0], message.clone()].concat()).messages,
            [message]
        );
    }

    #[tokio::test]
    async fn readiness_is_retained_when_handshake_finishes_before_the_waiter() {
        let (status, rx) = watch::channel(TransportStatus::Starting);
        status.send_replace(TransportStatus::Ready);
        wait_ready(rx).await.unwrap();
        status.send_replace(TransportStatus::Closed);
        assert!(wait_ready(status.subscribe()).await.is_err());
        let (status, rx) = watch::channel(TransportStatus::Starting);
        drop(status);
        assert!(wait_ready(rx).await.is_err());
    }

    #[tokio::test]
    async fn shutdown_aborts_transport_tasks_and_reports_closed() {
        let (mut transport, _outgoing) = test_transport(TransportMode::ReverseV2);
        let status = transport.status();
        transport.activate();
        transport.shutdown().await.unwrap();
        assert_eq!(*status.borrow(), TransportStatus::Closed);
    }
}
