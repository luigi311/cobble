use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

use anyhow::{Context as _, bail};
use clap::Parser;
use cobble_pkjs_protocol::{RuntimeCommand, RuntimeEvent};
use rquickjs::{
    CatchResultExt, Context, Ctx, Function, Runtime, context::EvalOptions, function::Func,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;
use tracing_subscriber::EnvFilter;
use zip::ZipArchive;

const MAX_PBW_BYTES: u64 = 32 * 1024 * 1024;
const MAX_SCRIPT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_IPC_BYTES: usize = 1024 * 1024;
const MAX_HTTP_BYTES: usize = 4 * 1024 * 1024;

#[derive(Parser)]
#[command(about = "Isolated PebbleKit JS runtime used by cobbled")]
struct Cli {
    #[arg(long)]
    pbw: PathBuf,
    #[arg(long)]
    storage_dir: PathBuf,
    #[arg(long, default_value = "unknown")]
    platform: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AppInfo {
    uuid: String,
    #[serde(default)]
    app_keys: HashMap<String, u32>,
}

enum Input {
    Command(RuntimeCommand),
    Invalid(String),
    Closed,
    Schedule {
        id: u64,
        delay: Duration,
        repeat: bool,
    },
    Cancel(u64),
}

#[derive(Clone, Copy)]
struct Timer {
    deadline: Instant,
    interval: Duration,
    repeat: bool,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(io::stderr)
        .init();

    let output = Arc::new(Mutex::new(BufWriter::new(io::stdout())));
    if let Err(error) = run(Cli::parse(), Arc::clone(&output)) {
        let _ = emit(
            &output,
            &RuntimeEvent::Fatal {
                message: format!("{error:#}"),
            },
        );
        std::process::exit(1);
    }
}

fn run(cli: Cli, output: Arc<Mutex<BufWriter<io::Stdout>>>) -> anyhow::Result<()> {
    let metadata = fs::metadata(&cli.pbw).context("read PBW metadata")?;
    if metadata.len() > MAX_PBW_BYTES {
        bail!("PBW exceeds the {MAX_PBW_BYTES}-byte runtime limit");
    }
    let Some((script, app_info)) = read_pbw(&cli.pbw)? else {
        return Ok(());
    };
    fs::create_dir_all(&cli.storage_dir).context("create PKJS storage directory")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&cli.storage_dir, fs::Permissions::from_mode(0o700))
            .context("secure PKJS storage directory")?;
    }
    let storage_path = cli.storage_dir.join(format!("{}.json", app_info.uuid));
    let initial_storage = fs::read_to_string(&storage_path).unwrap_or_else(|_| "{}".into());

    let (input_tx, input_rx) = mpsc::channel();
    spawn_input_reader(input_tx.clone());

    let runtime = Runtime::new().context("create QuickJS runtime")?;
    runtime.set_memory_limit(64 * 1024 * 1024);
    runtime.set_max_stack_size(1024 * 1024);
    let context = Context::full(&runtime).context("create QuickJS context")?;

    context.with(|ctx| -> anyhow::Result<()> {
        let globals = ctx.globals();
        globals.set("__cobbleInitialStorage", initial_storage)?;
        globals.set(
            "__cobbleWatchInfo",
            serde_json::to_string(&json!({
                "platform": cli.platform,
                "model": cli.platform,
                "language": "en_US",
                "firmware": { "major": 0, "minor": 0, "patch": 0, "suffix": "" }
            }))?,
        )?;

        let log_output = Arc::clone(&output);
        globals.set(
            "__cobbleLog",
            Func::from(move |level: String, message: String| {
                let _ = emit(&log_output, &RuntimeEvent::Log { level, message });
            }),
        )?;

        let event_output = Arc::clone(&output);
        let app_keys = app_info.app_keys.clone();
        let next_request = Arc::new(std::sync::atomic::AtomicU64::new(1));
        globals.set(
            "__cobbleSendAppMessage",
            Func::from(move |encoded: String| -> i64 {
                match decode_app_message(&encoded, &app_keys) {
                    Ok(data) => {
                        let request_id =
                            next_request.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if emit(
                            &event_output,
                            &RuntimeEvent::SendAppMessage { request_id, data },
                        )
                        .is_ok()
                        {
                            request_id as i64
                        } else {
                            -1
                        }
                    }
                    Err(error) => {
                        let _ = emit(
                            &event_output,
                            &RuntimeEvent::Log {
                                level: "error".into(),
                                message: format!("invalid AppMessage: {error}"),
                            },
                        );
                        -1
                    }
                }
            }),
        )?;

        let url_output = Arc::clone(&output);
        globals.set(
            "__cobbleOpenUrl",
            Func::from(move |url: String| {
                let _ = emit(&url_output, &RuntimeEvent::OpenUrl { url });
            }),
        )?;

        let location_output = Arc::clone(&output);
        let next_location = Arc::new(std::sync::atomic::AtomicU64::new(1));
        globals.set(
            "__cobbleRequestLocation",
            Func::from(move || -> i64 {
                let request_id = next_location.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if emit(
                    &location_output,
                    &RuntimeEvent::RequestLocation { request_id },
                )
                .is_ok()
                {
                    request_id as i64
                } else {
                    -1
                }
            }),
        )?;

        globals.set(
            "__cobbleSaveStorage",
            Func::from(move |encoded: String| {
                if encoded.len() > MAX_IPC_BYTES
                    || serde_json::from_str::<BTreeMap<String, String>>(&encoded).is_err()
                {
                    warn!("refusing invalid PKJS local storage update");
                    return;
                }
                if let Err(error) = write_storage(&storage_path, encoded.as_bytes()) {
                    warn!("could not persist PKJS local storage: {error:#}");
                }
            }),
        )?;

        let schedule_tx = input_tx.clone();
        globals.set(
            "__cobbleScheduleTimer",
            Func::from(move |id: u64, delay_ms: f64, repeat: bool| {
                let millis = if delay_ms.is_finite() {
                    delay_ms.clamp(0.0, u32::MAX as f64) as u64
                } else {
                    0
                };
                let _ = schedule_tx.send(Input::Schedule {
                    id,
                    delay: Duration::from_millis(millis),
                    repeat,
                });
            }),
        )?;
        let cancel_tx = input_tx;
        globals.set(
            "__cobbleCancelTimer",
            Func::from(move |id: u64| {
                let _ = cancel_tx.send(Input::Cancel(id));
            }),
        )?;

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("create HTTP client")?;
        globals.set(
            "__cobbleHttpRequest",
            Func::from(
                move |method: String, url: String, headers: String, body: String| -> String {
                    http_request(&client, &method, &url, &headers, &body).to_string()
                },
            ),
        )?;

        catch_js(&ctx, ctx.eval::<(), _>(include_str!("bridge.js")))
            .context("evaluate PKJS bridge")?;
        let mut app_eval_options = EvalOptions::default();
        // PebbleKit JS files are classic scripts. Do not force strict mode: old
        // apps commonly create globals through undeclared assignments. An app
        // can still opt into strict mode with its own `use strict` directive.
        app_eval_options.strict = false;
        catch_js(
            &ctx,
            ctx.eval_with_options::<(), _>(script.as_str(), app_eval_options),
        )
        .context("evaluate pebble-js-app.js")?;
        catch_js(&ctx, ctx.eval::<(), _>("globalThis.__cobbleReady()"))
            .context("dispatch PKJS ready event")?;
        Ok(())
    })?;
    emit(&output, &RuntimeEvent::Ready)?;

    let mut timers = HashMap::<u64, Timer>::new();
    loop {
        drain_jobs(&runtime)?;
        let timeout = timers
            .values()
            .map(|timer| timer.deadline.saturating_duration_since(Instant::now()))
            .min();
        let input = match timeout {
            Some(timeout) => match input_rx.recv_timeout(timeout) {
                Ok(input) => Some(input),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => Some(Input::Closed),
            },
            None => Some(input_rx.recv().unwrap_or(Input::Closed)),
        };

        if let Some(input) = input {
            match input {
                Input::Command(RuntimeCommand::Shutdown) | Input::Closed => break,
                Input::Command(command) => {
                    dispatch_command(&context, &command, &app_info.app_keys)?
                }
                Input::Invalid(error) => warn!("invalid command from cobbled: {error}"),
                Input::Schedule { id, delay, repeat } => {
                    let interval = delay.max(Duration::from_millis(1));
                    timers.insert(
                        id,
                        Timer {
                            deadline: Instant::now() + interval,
                            interval,
                            repeat,
                        },
                    );
                }
                Input::Cancel(id) => {
                    timers.remove(&id);
                }
            }
        }

        let now = Instant::now();
        let due: Vec<u64> = timers
            .iter()
            .filter_map(|(id, timer)| (timer.deadline <= now).then_some(*id))
            .collect();
        for id in due {
            if let Some(timer) = timers.get_mut(&id) {
                if timer.repeat {
                    timer.deadline = now + timer.interval;
                } else {
                    timers.remove(&id);
                }
            }
            context.with(|ctx| -> anyhow::Result<()> {
                let callback: Function = catch_js(&ctx, ctx.globals().get("__cobbleFireTimer"))?;
                catch_js(&ctx, callback.call::<_, ()>((id,)))
            })?;
        }
    }
    Ok(())
}

fn read_pbw(path: &Path) -> anyhow::Result<Option<(String, AppInfo)>> {
    let file = fs::File::open(path).context("open PBW")?;
    let mut archive = ZipArchive::new(file).context("open PBW ZIP")?;
    let script = {
        let mut entry = match archive.by_name("pebble-js-app.js") {
            Ok(entry) => entry,
            Err(zip::result::ZipError::FileNotFound) => return Ok(None),
            Err(error) => return Err(error).context("open pebble-js-app.js"),
        };
        if entry.size() > MAX_SCRIPT_BYTES {
            bail!("PebbleKit JS is too large");
        }
        let mut data = String::new();
        entry
            .read_to_string(&mut data)
            .context("read pebble-js-app.js")?;
        data
    };
    let app_info: AppInfo = {
        let mut entry = archive
            .by_name("appinfo.json")
            .context("PBW has no appinfo.json")?;
        if entry.size() > MAX_SCRIPT_BYTES {
            bail!("PBW appinfo.json is too large");
        }
        let mut data = String::new();
        entry
            .read_to_string(&mut data)
            .context("read appinfo.json")?;
        let mut app_info: AppInfo = serde_json::from_str(&data).context("parse appinfo.json")?;
        app_info.uuid = uuid::Uuid::parse_str(&app_info.uuid)
            .context("invalid app UUID")?
            .to_string();
        app_info
    };
    Ok(Some((script, app_info)))
}

fn decode_app_message(
    encoded: &str,
    app_keys: &HashMap<String, u32>,
) -> anyhow::Result<BTreeMap<String, Value>> {
    let object = serde_json::from_str::<serde_json::Map<String, Value>>(encoded)?;
    object
        .into_iter()
        .filter_map(|(key, value)| {
            let key = app_keys.get(&key).copied().or_else(|| key.parse().ok())?;
            match &value {
                Value::Bool(_) | Value::String(_) | Value::Number(_) => {}
                Value::Array(values)
                    if values
                        .iter()
                        .all(|value| value.as_u64().is_some_and(|value| value <= 255)) => {}
                _ => {
                    return Some(Err(anyhow::anyhow!(
                        "app key {key} has an unsupported value"
                    )));
                }
            }
            Some(Ok((key.to_string(), value)))
        })
        .collect()
}

fn dispatch_command(
    context: &Context,
    command: &RuntimeCommand,
    app_keys: &HashMap<String, u32>,
) -> anyhow::Result<()> {
    let command = match command {
        RuntimeCommand::AppMessage { data } => {
            let reverse: HashMap<u32, &str> = app_keys
                .iter()
                .map(|(name, id)| (*id, name.as_str()))
                .collect();
            let data = data
                .iter()
                .map(|(id, value)| {
                    let numeric_id = id.parse::<u32>().ok();
                    (
                        numeric_id
                            .and_then(|id| reverse.get(&id).copied())
                            .map(str::to_owned)
                            .unwrap_or_else(|| id.clone()),
                        value.clone(),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            serde_json::to_string(&json!({ "type": "app_message", "data": data }))?
        }
        _ => serde_json::to_string(command)?,
    };
    context.with(|ctx| -> anyhow::Result<()> {
        let dispatch: Function = catch_js(&ctx, ctx.globals().get("__cobbleDispatch"))?;
        catch_js(&ctx, dispatch.call::<_, ()>((command,)))
    })
}

fn catch_js<'js, T>(ctx: &Ctx<'js>, result: rquickjs::Result<T>) -> anyhow::Result<T> {
    result
        .catch(ctx)
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}

fn spawn_input_reader(sender: mpsc::Sender<Input>) {
    std::thread::spawn(move || {
        for line in BufReader::new(io::stdin()).lines() {
            let input = match line {
                Ok(line) if line.len() <= MAX_IPC_BYTES => serde_json::from_str(&line)
                    .map(Input::Command)
                    .unwrap_or_else(|error| Input::Invalid(error.to_string())),
                Ok(_) => Input::Invalid("IPC command exceeds size limit".into()),
                Err(_) => break,
            };
            if sender.send(input).is_err() {
                return;
            }
        }
        let _ = sender.send(Input::Closed);
    });
}

fn emit(output: &Arc<Mutex<BufWriter<io::Stdout>>>, event: &RuntimeEvent) -> anyhow::Result<()> {
    let encoded = serde_json::to_vec(event)?;
    if encoded.len() > MAX_IPC_BYTES {
        bail!("runtime event exceeds IPC size limit");
    }
    let mut output = output.lock().unwrap();
    output.write_all(&encoded)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn write_storage(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(temporary, path)?;
    Ok(())
}

fn drain_jobs(runtime: &Runtime) -> anyhow::Result<()> {
    while runtime.is_job_pending() {
        if let Err(error) = runtime.execute_pending_job() {
            let message = error.0.with(|ctx| {
                rquickjs::CaughtError::from_error(&ctx, rquickjs::Error::Exception).to_string()
            });
            bail!("JavaScript promise job failed: {message}");
        }
    }
    Ok(())
}

fn http_request(
    client: &reqwest::blocking::Client,
    method: &str,
    url: &str,
    headers: &str,
    body: &str,
) -> Value {
    let result = (|| -> anyhow::Result<Value> {
        let method =
            reqwest::Method::from_bytes(method.as_bytes()).context("invalid HTTP method")?;
        let parsed = reqwest::Url::parse(url).context("invalid URL")?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!("only HTTP and HTTPS URLs are allowed");
        }
        let mut request = client.request(method, parsed);
        let headers: HashMap<String, String> =
            serde_json::from_str(headers).context("invalid request headers")?;
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if !body.is_empty() {
            request = request.body(body.to_owned());
        }
        let mut response = request.send().context("HTTP request failed")?;
        let status = response.status();
        let mut bytes = Vec::new();
        response
            .by_ref()
            .take((MAX_HTTP_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .context("read HTTP response")?;
        if bytes.len() > MAX_HTTP_BYTES {
            bail!("HTTP response exceeds size limit");
        }
        Ok(json!({
            "ok": status.is_success(), "status": status.as_u16(),
            "status_text": status.canonical_reason().unwrap_or(""),
            "body": String::from_utf8_lossy(&bytes),
        }))
    })();
    result
        .unwrap_or_else(|error| json!({ "ok": false, "status": 0, "error": format!("{error:#}") }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_message_resolves_named_and_numeric_keys() {
        let keys = HashMap::from([("temperature".into(), 7)]);
        let decoded = decode_app_message(r#"{"temperature":21,"8":[1,2,255]}"#, &keys).unwrap();
        assert_eq!(decoded["7"], json!(21));
        assert_eq!(decoded["8"], json!([1, 2, 255]));
    }

    #[test]
    fn app_message_skips_unknown_keys_and_accepts_booleans() {
        let keys = HashMap::from([("enabled".into(), 1)]);
        let decoded = decode_app_message(r#"{"NaN":0,"enabled":true}"#, &keys).unwrap();
        assert_eq!(decoded, BTreeMap::from([("1".into(), json!(true))]));
    }

    #[test]
    fn app_message_rejects_nested_values() {
        let keys = HashMap::new();
        assert!(decode_app_message(r#"{"1":{"nested":true}}"#, &keys).is_err());
    }
}
