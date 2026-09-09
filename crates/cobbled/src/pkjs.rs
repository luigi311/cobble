//! Lifecycle manager for the out-of-process PebbleKit JS runtime.
//!
//! JavaScript and PBW parsing execute in `cobbled-pkjs`, never in the daemon.
//! This actor only owns the helper's stdio and translates its small IPC
//! protocol to watch operations.

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use cobble_db::AppDb;
use cobble_pkjs_protocol::{RuntimeCommand, RuntimeEvent};
use libpebble_ble::{AppMessageValue, Pebble};
use serde_json::Value;
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, info, warn};

const MAX_IPC_LINE_BYTES: usize = 1024 * 1024;
const CONFIGURATION_URL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct PkjsManager {
    sender: mpsc::UnboundedSender<ManagerCommand>,
}

enum ManagerCommand {
    AppStarted {
        uuid: String,
        pebble: Weak<Pebble>,
    },
    AppStopped {
        uuid: String,
    },
    AppMessage {
        uuid: String,
        data: HashMap<u32, AppMessageValue>,
    },
    RequestConfiguration {
        uuid: String,
        pebble: Weak<Pebble>,
        done: oneshot::Sender<Result<String, String>>,
    },
    CancelConfiguration {
        uuid: String,
    },
    SubmitConfiguration {
        uuid: String,
        response: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    Disconnected,
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

enum SessionEvent {
    Protocol {
        generation: u64,
        event: RuntimeEvent,
    },
    Exited {
        generation: u64,
        result: anyhow::Result<std::process::ExitStatus>,
    },
}

struct ActiveSession {
    uuid: String,
    generation: u64,
    pebble: Weak<Pebble>,
    commands: mpsc::Sender<SessionCommand>,
    task: JoinHandle<()>,
}

struct SessionCommand {
    command: RuntimeCommand,
    written: Option<oneshot::Sender<Result<(), String>>>,
}

impl From<RuntimeCommand> for SessionCommand {
    fn from(command: RuntimeCommand) -> Self {
        Self {
            command,
            written: None,
        }
    }
}

struct PendingConfiguration {
    uuid: String,
    generation: u64,
    done: oneshot::Sender<Result<String, String>>,
}

impl PkjsManager {
    pub fn start(db: Option<Arc<Mutex<AppDb>>>, storage_dir: PathBuf) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_manager(receiver, db, storage_dir));
        Self { sender }
    }

    #[cfg(test)]
    pub(crate) fn unavailable() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        drop(receiver);
        Self { sender }
    }

    pub fn app_started(&self, uuid: String, pebble: Weak<Pebble>) {
        let _ = self
            .sender
            .send(ManagerCommand::AppStarted { uuid, pebble });
    }

    pub fn app_stopped(&self, uuid: String) {
        let _ = self.sender.send(ManagerCommand::AppStopped { uuid });
    }

    pub fn app_message(&self, uuid: String, data: HashMap<u32, AppMessageValue>) {
        let _ = self.sender.send(ManagerCommand::AppMessage { uuid, data });
    }

    pub fn disconnected(&self) {
        let _ = self.sender.send(ManagerCommand::Disconnected);
    }

    pub async fn request_configuration(
        &self,
        uuid: String,
        pebble: Weak<Pebble>,
    ) -> anyhow::Result<String> {
        let (done, receiver) = oneshot::channel();
        self.sender
            .send(ManagerCommand::RequestConfiguration {
                uuid: uuid.clone(),
                pebble,
                done,
            })
            .map_err(|_| anyhow::anyhow!("PKJS manager is unavailable"))?;
        match tokio::time::timeout(CONFIGURATION_URL_TIMEOUT, receiver).await {
            Ok(Ok(Ok(url))) => Ok(url),
            Ok(Ok(Err(error))) => Err(anyhow::anyhow!(error)),
            Ok(Err(_)) => Err(anyhow::anyhow!("PKJS configuration request was cancelled")),
            Err(_) => {
                let _ = self
                    .sender
                    .send(ManagerCommand::CancelConfiguration { uuid });
                Err(anyhow::anyhow!("PKJS did not provide a configuration URL"))
            }
        }
    }

    pub async fn submit_configuration(&self, uuid: String, response: String) -> anyhow::Result<()> {
        let (done, receiver) = oneshot::channel();
        self.sender
            .send(ManagerCommand::SubmitConfiguration {
                uuid,
                response,
                done,
            })
            .map_err(|_| anyhow::anyhow!("PKJS manager is unavailable"))?;
        match tokio::time::timeout(Duration::from_secs(2), receiver).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(anyhow::anyhow!(error)),
            Ok(Err(_)) => Err(anyhow::anyhow!("PKJS configuration response was cancelled")),
            Err(_) => Err(anyhow::anyhow!("PKJS configuration response timed out")),
        }
    }

    pub async fn shutdown(&self) {
        let (done, receiver) = oneshot::channel();
        if self.sender.send(ManagerCommand::Shutdown { done }).is_ok() {
            let _ = receiver.await;
        }
    }
}

async fn run_manager(
    mut receiver: mpsc::UnboundedReceiver<ManagerCommand>,
    db: Option<Arc<Mutex<AppDb>>>,
    storage_dir: PathBuf,
) {
    // Bounded so a broken script that floods console output cannot grow the
    // daemon without limit. The helper's stdout pipe supplies backpressure.
    let (session_events, mut event_receiver) = mpsc::channel(128);
    let mut active: Option<ActiveSession> = None;
    let mut pending_configuration: Option<PendingConfiguration> = None;
    let mut generation = 0_u64;

    loop {
        tokio::select! {
            command = receiver.recv() => {
                let Some(command) = command else { break };
                match command {
                    ManagerCommand::AppStarted { uuid, pebble } => {
                        if active.as_ref().is_some_and(|session| session.uuid == uuid) {
                            continue;
                        }
                        fail_pending_configuration(
                            &mut pending_configuration,
                            "PKJS session changed",
                        );
                        if let Err(error) = start_session(
                            &mut active,
                            &mut generation,
                            &db,
                            &storage_dir,
                            &session_events,
                            uuid.clone(),
                            pebble,
                            false,
                        ).await {
                            debug!("not starting PKJS for launched app {uuid}: {error:#}");
                        }
                    }
                    ManagerCommand::AppStopped { uuid } => {
                        if active.as_ref().is_some_and(|session| session.uuid == uuid) {
                            fail_pending_configuration(
                                &mut pending_configuration,
                                "app stopped while opening its configuration",
                            );
                            stop_active(&mut active).await;
                        }
                    }
                    ManagerCommand::AppMessage { uuid, data } => {
                        if let Some(session) = active.as_ref().filter(|session| session.uuid == uuid) {
                            let data = data.into_iter().map(|(key, value)| (key.to_string(), app_value_to_json(value))).collect();
                            if session.commands.try_send(RuntimeCommand::AppMessage { data }.into()).is_err() {
                                warn!("PKJS helper command queue is unavailable for {uuid}");
                            }
                        }
                    }
                    ManagerCommand::RequestConfiguration { uuid, pebble, done } => {
                        if pending_configuration.is_some() {
                            let _ = done.send(Err("another app configuration request is active".into()));
                            continue;
                        }
                        if !active.as_ref().is_some_and(|session| session.uuid == uuid)
                            && let Err(error) = start_session(
                                &mut active,
                                &mut generation,
                                &db,
                                &storage_dir,
                                &session_events,
                                uuid.clone(),
                                pebble,
                                true,
                            ).await
                        {
                            let _ = done.send(Err(error.to_string()));
                            continue;
                        }
                        let Some(session) = active.as_ref() else {
                            let _ = done.send(Err("PKJS session is unavailable".into()));
                            continue;
                        };
                        let session_generation = session.generation;
                        pending_configuration = Some(PendingConfiguration {
                            uuid: uuid.clone(),
                            generation: session_generation,
                            done,
                        });
                        if session.commands.try_send(RuntimeCommand::ShowConfiguration.into()).is_err() {
                            fail_pending_configuration(
                                &mut pending_configuration,
                                "PKJS helper command queue is unavailable",
                            );
                            continue;
                        }
                        debug!("requested PKJS configuration URL for {uuid}");
                    }
                    ManagerCommand::CancelConfiguration { uuid } => {
                        if pending_configuration.as_ref().is_some_and(|pending| pending.uuid == uuid) {
                            fail_pending_configuration(
                                &mut pending_configuration,
                                "PKJS configuration request timed out",
                            );
                        }
                    }
                    ManagerCommand::SubmitConfiguration { uuid, response, done } => {
                        if let Some(session) = active.as_ref().filter(|session| session.uuid == uuid) {
                            let command = SessionCommand {
                                command: RuntimeCommand::WebviewClosed { response },
                                written: Some(done),
                            };
                            if let Err(error) = session.commands.try_send(command)
                                && let Some(done) = error.into_inner().written
                            {
                                let _ = done.send(Err(
                                    "PKJS helper command queue is unavailable".into(),
                                ));
                            }
                        } else {
                            let _ = done.send(Err(
                                "the configured PKJS session is no longer active".into(),
                            ));
                        }
                    }
                    ManagerCommand::Disconnected => {
                        fail_pending_configuration(
                            &mut pending_configuration,
                            "watch disconnected while opening app configuration",
                        );
                        stop_active(&mut active).await;
                    }
                    ManagerCommand::Shutdown { done } => {
                        fail_pending_configuration(
                            &mut pending_configuration,
                            "PKJS manager is shutting down",
                        );
                        stop_active(&mut active).await;
                        let _ = done.send(());
                        break;
                    }
                }
            }
            event = event_receiver.recv() => {
                let Some(event) = event else { continue };
                match event {
                    SessionEvent::Protocol { generation: event_generation, event }
                        if active.as_ref().is_some_and(|session| session.generation == event_generation) =>
                    {
                        handle_runtime_event(
                            active.as_ref().unwrap(),
                            event,
                            db.clone(),
                            &mut pending_configuration,
                        );
                    }
                    SessionEvent::Protocol { .. } => {}
                    SessionEvent::Exited { generation: event_generation, result } => {
                        if active.as_ref().is_some_and(|session| session.generation == event_generation) {
                            fail_pending_configuration(
                                &mut pending_configuration,
                                "PKJS helper exited while opening app configuration",
                            );
                            match result {
                                Ok(status) if status.success() => debug!("PKJS helper exited normally"),
                                Ok(status) => warn!("PKJS helper exited with {status}; cobbled remains available"),
                                Err(error) => warn!("PKJS helper failed: {error:#}; cobbled remains available"),
                            }
                            active = None;
                        }
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_session(
    active: &mut Option<ActiveSession>,
    generation: &mut u64,
    db: &Option<Arc<Mutex<AppDb>>>,
    storage_dir: &Path,
    session_events: &mpsc::Sender<SessionEvent>,
    uuid: String,
    pebble: Weak<Pebble>,
    require_configurable: bool,
) -> anyhow::Result<()> {
    stop_active(active).await;
    let cached = db
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("app database is unavailable"))?
        .lock()
        .unwrap()
        .load_cached_pbw_app(&uuid)?
        .ok_or_else(|| anyhow::anyhow!("no retained PBW for app {uuid}"))?;
    if require_configurable && !cached.app.configurable {
        anyhow::bail!("app {uuid} is not configurable");
    }
    *generation = generation.wrapping_add(1);
    let (commands, task) = spawn_session(
        *generation,
        &uuid,
        &cached.app.platform,
        &cached.pbw,
        storage_dir,
        session_events.clone(),
    )
    .await?;
    *active = Some(ActiveSession {
        uuid,
        generation: *generation,
        pebble,
        commands,
        task,
    });
    Ok(())
}

fn fail_pending_configuration(pending: &mut Option<PendingConfiguration>, message: &str) {
    if let Some(pending) = pending.take() {
        let _ = pending.done.send(Err(message.into()));
    }
}

async fn stop_active(active: &mut Option<ActiveSession>) {
    if let Some(session) = active.take() {
        let _ = session.commands.send(RuntimeCommand::Shutdown.into()).await;
        if let Err(error) = session.task.await {
            warn!("PKJS session task failed during shutdown: {error}");
        }
    }
}

fn handle_runtime_event(
    session: &ActiveSession,
    event: RuntimeEvent,
    db: Option<Arc<Mutex<AppDb>>>,
    pending_configuration: &mut Option<PendingConfiguration>,
) {
    match event {
        RuntimeEvent::Ready => info!("PKJS ready for {}", session.uuid),
        RuntimeEvent::SendAppMessage { request_id, data } => {
            let commands = session.commands.clone();
            let pebble = session.pebble.clone();
            let uuid = session.uuid.clone();
            tokio::spawn(async move {
                let success = match (pebble.upgrade(), json_to_app_message(data)) {
                    (Some(pebble), Ok(data)) => pebble
                        .send_app_message(&uuid, data, true, 5.0)
                        .await
                        .is_ok(),
                    (None, _) => false,
                    (_, Err(error)) => {
                        warn!("PKJS produced an invalid AppMessage for {uuid}: {error}");
                        false
                    }
                };
                let _ = commands
                    .send(
                        RuntimeCommand::AppMessageResult {
                            request_id,
                            success,
                        }
                        .into(),
                    )
                    .await;
            });
        }
        RuntimeEvent::RequestLocation { request_id } => {
            let commands = session.commands.clone();
            tokio::spawn(async move {
                let command = match crate::location::get_location(db).await {
                    Ok((latitude, longitude, _)) => RuntimeCommand::LocationResult {
                        request_id,
                        latitude: Some(latitude),
                        longitude: Some(longitude),
                        error: None,
                    },
                    Err(error) => RuntimeCommand::LocationResult {
                        request_id,
                        latitude: None,
                        longitude: None,
                        error: Some(error.to_string()),
                    },
                };
                let _ = commands.send(command.into()).await;
            });
        }
        RuntimeEvent::OpenUrl { url } => {
            if pending_configuration.as_ref().is_some_and(|pending| {
                pending.uuid == session.uuid && pending.generation == session.generation
            }) {
                let pending = pending_configuration.take().unwrap();
                debug!(
                    "received requested PKJS configuration URL for {}",
                    session.uuid
                );
                let _ = pending.done.send(Ok(url));
            } else {
                debug!(
                    "ignoring unsolicited PKJS configuration URL for {}",
                    session.uuid
                );
            }
        }
        RuntimeEvent::Log { level, message } => {
            // App-controlled console text may contain tokens or location data.
            // Keep it out of normal logs and expose it only with --verbose.
            debug!("PKJS {} [{level}]: {message}", session.uuid);
        }
        RuntimeEvent::Fatal { message } => {
            warn!("PKJS {} stopped with a runtime error", session.uuid);
            debug!("PKJS {} runtime error: {message}", session.uuid);
        }
    }
}

async fn spawn_session(
    generation: u64,
    uuid: &str,
    platform: &str,
    pbw: &[u8],
    storage_dir: &Path,
    events: mpsc::Sender<SessionEvent>,
) -> anyhow::Result<(mpsc::Sender<SessionCommand>, JoinHandle<()>)> {
    let temporary = tempfile::Builder::new().prefix("cobbled-pkjs-").tempdir()?;
    let pbw_path = temporary.path().join("app.pbw");
    tokio::fs::write(&pbw_path, pbw).await?;
    tokio::fs::create_dir_all(storage_dir).await?;

    let mut child = Command::new(helper_binary())
        .arg("--pbw")
        .arg(&pbw_path)
        .arg("--storage-dir")
        .arg(storage_dir)
        .arg("--platform")
        .arg(platform)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("helper stdin was not piped"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("helper stdout was not piped"))?;
    let (commands, command_receiver) = mpsc::channel(32);
    let task = tokio::spawn(run_session(
        generation,
        uuid.to_owned(),
        child,
        stdin,
        stdout,
        command_receiver,
        events,
        temporary,
    ));
    Ok((commands, task))
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    generation: u64,
    uuid: String,
    mut child: Child,
    mut stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    mut commands: mpsc::Receiver<SessionCommand>,
    events: mpsc::Sender<SessionEvent>,
    _temporary: TempDir,
) {
    let mut stdout = BufReader::new(stdout);
    let mut report_exit = true;
    let result = loop {
        tokio::select! {
            line = read_capped_line(&mut stdout) => match line {
                Ok(CappedLine::Line(line)) => {
                    match serde_json::from_str(&line) {
                        Ok(event) => {
                            if events.try_send(SessionEvent::Protocol { generation, event }).is_err() {
                                warn!("PKJS event queue overflow for {uuid}; terminating helper");
                                break terminate_child(&mut child).await;
                            }
                        }
                        Err(error) => warn!("invalid PKJS event for {uuid}: {error}"),
                    }
                }
                Ok(CappedLine::Oversized) => {
                    warn!("PKJS event for {uuid} exceeded the IPC limit");
                    break terminate_child(&mut child).await;
                }
                Ok(CappedLine::Eof) => break child.wait().await.map_err(Into::into),
                Err(error) => break Err(error.into()),
            },
            command = commands.recv() => {
                let shutdown = command.as_ref().is_none_or(|command| {
                    matches!(command.command, RuntimeCommand::Shutdown)
                });
                if shutdown {
                    // The manager already removed and will join this session.
                    report_exit = false;
                }
                if let Some(command) = command {
                    let result = write_command(&mut stdin, &command.command).await;
                    if let Some(done) = command.written {
                        let acknowledgement = result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        let _ = done.send(acknowledgement);
                    }
                    if let Err(error) = result {
                        break Err(error);
                    }
                }
                if shutdown {
                    drop(stdin);
                    break match tokio::time::timeout(Duration::from_secs(1), child.wait()).await {
                        Ok(result) => result.map_err(Into::into),
                        Err(_) => terminate_child(&mut child).await,
                    };
                }
            }
        }
    };
    if report_exit {
        let _ = events
            .send(SessionEvent::Exited { generation, result })
            .await;
    }
}

enum CappedLine {
    Line(String),
    Eof,
    Oversized,
}

async fn read_capped_line<R>(reader: &mut R) -> std::io::Result<CappedLine>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = Vec::with_capacity(MAX_IPC_LINE_BYTES.min(8192));
    let mut capped = (&mut *reader).take((MAX_IPC_LINE_BYTES + 1) as u64);
    let read = capped.read_until(b'\n', &mut bytes).await?;
    if read == 0 {
        return Ok(CappedLine::Eof);
    }

    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    } else if bytes.len() > MAX_IPC_LINE_BYTES {
        return Ok(CappedLine::Oversized);
    }
    if bytes.len() > MAX_IPC_LINE_BYTES {
        return Ok(CappedLine::Oversized);
    }

    String::from_utf8(bytes)
        .map(CappedLine::Line)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

async fn terminate_child(child: &mut Child) -> anyhow::Result<std::process::ExitStatus> {
    child.kill().await?;
    Ok(child.wait().await?)
}

async fn write_command(
    stdin: &mut tokio::process::ChildStdin,
    command: &RuntimeCommand,
) -> anyhow::Result<()> {
    let encoded = encode_command(command)?;
    stdin.write_all(&encoded).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(())
}

fn encode_command(command: &RuntimeCommand) -> anyhow::Result<Vec<u8>> {
    let encoded = serde_json::to_vec(command)?;
    if encoded.len() > MAX_IPC_LINE_BYTES {
        anyhow::bail!("PKJS command exceeds the IPC limit");
    }
    Ok(encoded)
}

fn helper_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("COBBLED_PKJS_BIN") {
        return path.into();
    }
    if let Ok(executable) = std::env::current_exe()
        && let Some(parent) = executable.parent()
    {
        let sibling = parent.join("cobbled-pkjs");
        if sibling.is_file() {
            return sibling;
        }
    }
    PathBuf::from("cobbled-pkjs")
}

fn app_value_to_json(value: AppMessageValue) -> Value {
    match value {
        AppMessageValue::U8(value) => Value::from(value),
        AppMessageValue::U16(value) => Value::from(value),
        AppMessageValue::U32(value) => Value::from(value),
        AppMessageValue::I8(value) => Value::from(value),
        AppMessageValue::I16(value) => Value::from(value),
        AppMessageValue::I32(value) => Value::from(value),
        AppMessageValue::Uint(value) => Value::from(value),
        AppMessageValue::Int(value) => Value::from(value),
        AppMessageValue::Str(value) => Value::from(value),
        AppMessageValue::Bytes(value) => Value::Array(value.into_iter().map(Value::from).collect()),
    }
}

fn json_to_app_message(
    data: BTreeMap<String, Value>,
) -> anyhow::Result<HashMap<u32, AppMessageValue>> {
    data.into_iter()
        .map(|(key, value)| {
            let key = key
                .parse::<u32>()
                .map_err(|_| anyhow::anyhow!("invalid numeric app key {key:?}"))?;
            let value = match value {
                Value::Bool(value) => AppMessageValue::I16(i16::from(value)),
                Value::String(value) => AppMessageValue::Str(value),
                Value::Number(value) if value.as_i64().is_some() => {
                    let value = value.as_i64().unwrap();
                    let value = i32::try_from(value)
                        .map_err(|_| anyhow::anyhow!("integer for key {key} is out of range"))?;
                    AppMessageValue::I32(value)
                }
                Value::Number(value) if value.as_u64().is_some() => {
                    let value = value.as_u64().unwrap();
                    let value = u32::try_from(value)
                        .map_err(|_| anyhow::anyhow!("integer for key {key} is out of range"))?;
                    AppMessageValue::U32(value)
                }
                Value::Array(values) => AppMessageValue::Bytes(
                    values
                        .into_iter()
                        .map(|value| {
                            value
                                .as_u64()
                                .and_then(|value| u8::try_from(value).ok())
                                .ok_or_else(|| {
                                    anyhow::anyhow!("byte array for key {key} is invalid")
                                })
                        })
                        .collect::<anyhow::Result<_>>()?,
                ),
                _ => anyhow::bail!("value for key {key} is not supported by AppMessage"),
            };
            Ok((key, value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_message_conversion_preserves_supported_values() {
        let values = BTreeMap::from([
            ("1".into(), Value::from("hello")),
            ("2".into(), Value::from(-7)),
            ("3".into(), Value::from(vec![1, 2, 255])),
            ("4".into(), Value::from(true)),
        ]);
        let converted = json_to_app_message(values).unwrap();
        assert_eq!(converted[&1], AppMessageValue::Str("hello".into()));
        assert_eq!(converted[&2], AppMessageValue::I32(-7));
        assert_eq!(converted[&3], AppMessageValue::Bytes(vec![1, 2, 255]));
        assert_eq!(converted[&4], AppMessageValue::I16(1));
    }

    #[test]
    fn app_message_conversion_rejects_fractional_numbers() {
        let values = BTreeMap::from([("1".into(), Value::from(1.5))]);
        assert!(json_to_app_message(values).is_err());
    }

    #[tokio::test]
    async fn capped_line_reader_keeps_limits_per_line() {
        let input = b"first\nsecond\r\n";
        let mut reader = BufReader::new(&input[..]);
        assert!(matches!(
            read_capped_line(&mut reader).await.unwrap(),
            CappedLine::Line(line) if line == "first"
        ));
        assert!(matches!(
            read_capped_line(&mut reader).await.unwrap(),
            CappedLine::Line(line) if line == "second"
        ));
        assert!(matches!(
            read_capped_line(&mut reader).await.unwrap(),
            CappedLine::Eof
        ));

        let oversized = vec![b'x'; MAX_IPC_LINE_BYTES + 1];
        let mut reader = BufReader::new(&oversized[..]);
        assert!(matches!(
            read_capped_line(&mut reader).await.unwrap(),
            CappedLine::Oversized
        ));
    }

    #[test]
    fn serialized_configuration_command_must_fit_ipc_limit() {
        let command = RuntimeCommand::WebviewClosed {
            response: "\0".repeat(MAX_IPC_LINE_BYTES),
        };
        assert!(encode_command(&command).is_err());
    }
}
