//! Newline-delimited JSON IPC between `cobbled` and `cobbled-pkjs`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeCommand {
    AppMessage {
        data: BTreeMap<String, Value>,
    },
    AppMessageResult {
        request_id: u64,
        success: bool,
    },
    ShowConfiguration,
    WebviewClosed {
        response: String,
    },
    LocationResult {
        request_id: u64,
        latitude: Option<f64>,
        longitude: Option<f64>,
        error: Option<String>,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEvent {
    Ready,
    SendAppMessage {
        request_id: u64,
        data: BTreeMap<String, Value>,
    },
    OpenUrl {
        url: String,
    },
    RequestLocation {
        request_id: u64,
    },
    Log {
        level: String,
        message: String,
    },
    Fatal {
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_round_trips_tagged_messages() {
        let event = RuntimeEvent::SendAppMessage {
            request_id: 7,
            data: BTreeMap::from([("1".into(), Value::String("hello".into()))]),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(serde_json::from_str::<RuntimeEvent>(&json).unwrap(), event);

        let command = RuntimeCommand::LocationResult {
            request_id: 3,
            latitude: Some(35.1),
            longitude: Some(-106.6),
            error: None,
        };
        let json = serde_json::to_string(&command).unwrap();
        assert_eq!(
            serde_json::from_str::<RuntimeCommand>(&json).unwrap(),
            command
        );
    }
}
