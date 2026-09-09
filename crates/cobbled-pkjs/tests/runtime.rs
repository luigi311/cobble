use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
};

use cobble_pkjs_protocol::{RuntimeCommand, RuntimeEvent};
use serde_json::json;
use tempfile::tempdir;
use zip::{ZipWriter, write::SimpleFileOptions};

#[test]
fn helper_runs_bridge_and_handles_app_message_result() {
    let temporary = tempdir().unwrap();
    let pbw_path = temporary.path().join("test.pbw");
    let file = File::create(&pbw_path).unwrap();
    let mut archive = ZipWriter::new(file);
    archive
        .start_file("appinfo.json", SimpleFileOptions::default())
        .unwrap();
    archive
        .write_all(
            br#"{
        "uuid":"01234567-89ab-cdef-0123-456789abcdef",
        "shortName":"Runtime Test",
        "versionLabel":"1.0",
        "appKeys":{"answer":7}
    }"#,
        )
        .unwrap();
    archive
        .start_file("pebble-js-app.js", SimpleFileOptions::default())
        .unwrap();
    archive
        .write_all(
            br#"
        Pebble.addEventListener('ready', function() {
          localStorage.setItem('started', 'yes');
          console.log('ready handler ran');
          Pebble.sendAppMessage({answer: 42}, function() { console.log('message acked'); });
          navigator.geolocation.getCurrentPosition(function(position) {
            coordinates = position.coords;
            console.log('location ' + coordinates.latitude);
            throw new Error('location callback failed');
          });
          setTimeout(function() { console.log('timer fired'); }, 1);
        });
        Pebble.addEventListener('appmessage', function(event) {
          console.log('incoming ' + event.payload.answer);
        });
        Pebble.addEventListener('showConfiguration', function() {
          Pebble.openURL('https://example.com/config');
        });
        Pebble.addEventListener('webviewclosed', function(event) {
          console.log('configured ' + event.response);
        });
    "#,
        )
        .unwrap();
    archive.finish().unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_cobbled-pkjs"))
        .arg("--pbw")
        .arg(&pbw_path)
        .arg("--storage-dir")
        .arg(temporary.path().join("storage"))
        .arg("--platform")
        .arg("basalt")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());

    let mut request_id = None;
    let mut location_request_id = None;
    let mut ready = false;
    for _ in 0..8 {
        let event = read_event(&mut output);
        match event {
            RuntimeEvent::Ready => ready = true,
            RuntimeEvent::SendAppMessage {
                request_id: id,
                data,
            } => {
                assert_eq!(data["7"], json!(42));
                request_id = Some(id);
            }
            RuntimeEvent::RequestLocation { request_id } => {
                location_request_id = Some(request_id);
            }
            _ => {}
        }
        if ready && request_id.is_some() && location_request_id.is_some() {
            break;
        }
    }
    assert!(ready);
    let request_id = request_id.expect("ready handler should send an AppMessage");

    write_command(
        &mut input,
        &RuntimeCommand::AppMessageResult {
            request_id,
            success: true,
        },
    );
    write_command(
        &mut input,
        &RuntimeCommand::LocationResult {
            request_id: location_request_id.expect("ready handler should request location"),
            latitude: Some(35.1),
            longitude: Some(-106.6),
            error: None,
        },
    );
    write_command(
        &mut input,
        &RuntimeCommand::AppMessage {
            data: BTreeMap::from([("7".into(), json!(43))]),
        },
    );
    let mut observed = Vec::new();
    for _ in 0..6 {
        if let RuntimeEvent::Log { message, .. } = read_event(&mut output) {
            observed.push(message);
            if observed.iter().any(|message| message == "message acked")
                && observed.iter().any(|message| message == "incoming 43")
                && observed.iter().any(|message| message == "timer fired")
                && observed.iter().any(|message| message == "location 35.1")
                && observed
                    .iter()
                    .any(|message| message.contains("location callback failed"))
            {
                break;
            }
        }
    }
    assert!(observed.iter().any(|message| message == "message acked"));
    assert!(observed.iter().any(|message| message == "incoming 43"));
    assert!(observed.iter().any(|message| message == "timer fired"));
    assert!(observed.iter().any(|message| message == "location 35.1"));
    assert!(
        observed
            .iter()
            .any(|message| message.contains("location callback failed"))
    );

    write_command(&mut input, &RuntimeCommand::ShowConfiguration);
    let mut configuration_url = None;
    for _ in 0..8 {
        if let RuntimeEvent::OpenUrl { url } = read_event(&mut output) {
            configuration_url = Some(url);
            break;
        }
    }
    assert_eq!(
        configuration_url.as_deref(),
        Some("https://example.com/config")
    );

    write_command(
        &mut input,
        &RuntimeCommand::WebviewClosed {
            response: r#"{"weather":"openweathermap"}"#.into(),
        },
    );
    let mut configuration_closed = false;
    for _ in 0..8 {
        if let RuntimeEvent::Log { message, .. } = read_event(&mut output)
            && message == r#"configured {"weather":"openweathermap"}"#
        {
            configuration_closed = true;
            break;
        }
    }
    assert!(configuration_closed);

    write_command(&mut input, &RuntimeCommand::Shutdown);
    assert!(child.wait().unwrap().success());
    let storage = std::fs::read_to_string(
        temporary
            .path()
            .join("storage/01234567-89ab-cdef-0123-456789abcdef.json"),
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&storage).unwrap()["started"],
        "yes"
    );
}

#[test]
fn javascript_failure_is_reported_by_the_helper_process() {
    let temporary = tempdir().unwrap();
    let pbw_path = temporary.path().join("broken.pbw");
    let file = File::create(&pbw_path).unwrap();
    let mut archive = ZipWriter::new(file);
    archive
        .start_file("appinfo.json", SimpleFileOptions::default())
        .unwrap();
    archive
        .write_all(
            br#"{
                "uuid":"01234567-89ab-cdef-0123-456789abcdef",
                "shortName":"Broken Runtime Test",
                "versionLabel":"1.0"
            }"#,
        )
        .unwrap();
    archive
        .start_file("pebble-js-app.js", SimpleFileOptions::default())
        .unwrap();
    archive
        .write_all(b"throw new Error('expected failure');")
        .unwrap();
    archive.finish().unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_cobbled-pkjs"))
        .arg("--pbw")
        .arg(&pbw_path)
        .arg("--storage-dir")
        .arg(temporary.path().join("storage"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let RuntimeEvent::Fatal { message } = read_event(&mut output) else {
        panic!("expected a fatal runtime event");
    };
    assert!(message.contains("expected failure"), "{message}");
    assert!(!child.wait().unwrap().success());
}

fn read_event(reader: &mut impl BufRead) -> RuntimeEvent {
    let mut line = String::new();
    assert_ne!(
        reader.read_line(&mut line).unwrap(),
        0,
        "helper closed stdout"
    );
    serde_json::from_str(&line).unwrap()
}

fn write_command(writer: &mut impl Write, command: &RuntimeCommand) {
    serde_json::to_writer(&mut *writer, command).unwrap();
    writer.write_all(b"\n").unwrap();
    writer.flush().unwrap();
}
