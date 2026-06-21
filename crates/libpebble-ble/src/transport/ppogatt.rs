//! PPoGATT — "Pebble Protocol over GATT" transport framing.
//!
//! Each PPoGATT packet: 1-byte header followed by an optional payload.
//!
//!   bits 0-2 : command  (3-bit, see PPoGATTType)
//!   bits 3-7 : serial   (5-bit sequence number, wraps at 32)
//!
//! DATA packets carry one Pebble Protocol message, possibly split across
//! several DATA packets if it exceeds the negotiated ATT MTU.
//!
//! Reset handshake:
//!   watch sends RESET_REQUEST (0x02). We reply {0x03, 0x19, 0x19} if the
//!   request carried a payload, else {0x03}. The 0x19 bytes advertise our
//!   rx/tx window sizes (25 packets each).
//!   ACK header: (serial << 3) | 1.

/// Window size we advertise in the reset reply and honor on our TX side.
pub const PPOGATT_WINDOW: u8 = 0x19;

/// Drop the reassembly buffer if it grows beyond this (framing desync).
pub const MAX_REASSEMBLY: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PPoGATTType {
    Data = 0,
    Ack = 1,
    ResetRequest = 2,
    ResetComplete = 3,
}

impl PPoGATTType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Data),
            1 => Some(Self::Ack),
            2 => Some(Self::ResetRequest),
            3 => Some(Self::ResetComplete),
            _ => None,
        }
    }
}

pub fn ppogatt_header(packet_type: PPoGATTType, seq: u8) -> u8 {
    (packet_type as u8 & 0x07) | ((seq & 0x1F) << 3)
}

/// Returns (command_byte, serial). Command is the raw 3-bit field; the
/// caller must convert to PPoGATTType and handle unknowns gracefully.
pub fn parse_ppogatt_header(byte: u8) -> (u8, u8) {
    (byte & 0x07, (byte >> 3) & 0x1F)
}

/// Sequence, window, and reassembly state for one PPoGATT link.
///
/// TX flow control: callers check `can_send()` before emitting a DATA
/// packet, take a serial from `next_tx_seq()`, and feed every inbound ACK
/// to `on_ack()`.
///
/// RX dedup: we track the expected inbound 5-bit serial. Out-of-sequence
/// DATA (a retransmit whose ACK we already sent) must still be ACKed but
/// NOT appended to the reassembly buffer.
pub struct PPoGATTSession {
    pub tx_seq: u8,
    pub tx_ack_seq: u8,
    pub tx_inflight: u8,
    pub rx_seq: u8,
    reassembly: Vec<u8>,
}

impl PPoGATTSession {
    pub fn new() -> Self {
        Self { tx_seq: 0, tx_ack_seq: 0, tx_inflight: 0, rx_seq: 0, reassembly: Vec::new() }
    }

    pub fn reset(&mut self) {
        self.tx_seq = 0;
        self.tx_ack_seq = 0;
        self.tx_inflight = 0;
        self.rx_seq = 0;
        self.reassembly.clear();
    }

    // ---- TX side ----
    pub fn can_send(&self) -> bool {
        self.tx_inflight < PPOGATT_WINDOW
    }

    pub fn next_tx_seq(&mut self) -> u8 {
        let seq = self.tx_seq;
        self.tx_seq = (self.tx_seq + 1) & 0x1F;
        self.tx_inflight += 1;
        seq
    }

    /// Cumulative ACK: the watch ACKs the highest serial it has received,
    /// confirming every in-flight packet up to and including it.
    pub fn on_ack(&mut self, serial: u8) {
        let covered = ((serial.wrapping_sub(self.tx_ack_seq)) & 0x1F) + 1;
        let covered = covered.min(self.tx_inflight);
        self.tx_inflight -= covered;
        self.tx_ack_seq = (serial + 1) & 0x1F;
    }

    // ---- RX side ----
    /// Feed one inbound DATA packet. Returns `None` if the packet was a
    /// duplicate/out-of-order and must be dropped (after ACKing). Otherwise
    /// returns the list of complete Pebble Protocol messages available.
    pub fn on_data(&mut self, serial: u8, body: &[u8]) -> Option<Vec<Vec<u8>>> {
        if serial != self.rx_seq {
            tracing::warn!(
                "PPoGATT DATA serial={serial}, expected {} — dropping (dup/out-of-order)",
                self.rx_seq
            );
            return None;
        }
        self.rx_seq = (self.rx_seq + 1) & 0x1F;
        self.reassembly.extend_from_slice(body);
        Some(self.drain())
    }

    fn drain(&mut self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            if self.reassembly.len() < 4 {
                break;
            }
            let length = u16::from_be_bytes([self.reassembly[0], self.reassembly[1]]) as usize;
            let total = 4 + length;
            if total > MAX_REASSEMBLY {
                tracing::error!(
                    "PPoGATT framing desync (claimed length {length}); dropping reassembly buffer"
                );
                self.reassembly.clear();
                return out;
            }
            if self.reassembly.len() < total {
                break;
            }
            out.push(self.reassembly[..total].to_vec());
            self.reassembly.drain(..total);
        }
        if self.reassembly.len() > MAX_REASSEMBLY {
            tracing::error!("PPoGATT reassembly buffer overflow; dropping buffer");
            self.reassembly.clear();
        }
        out
    }
}

impl Default for PPoGATTSession {
    fn default() -> Self {
        Self::new()
    }
}
