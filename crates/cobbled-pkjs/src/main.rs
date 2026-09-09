use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
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
    HttpResponse {
        request_id: u64,
        response: String,
    },
}

struct HostHttpRequest {
    request_id: u64,
    method: String,
    url: String,
    headers: String,
    body: String,
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
    let http_requests = spawn_http_worker(input_tx.clone())?;

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

        globals.set(
            "__cobbleHttpRequest",
            Func::from(
                move |method: String, url: String, headers: String, body: String| -> String {
                    http_request(&method, &url, &headers, &body).to_string()
                },
            ),
        )?;
        let next_http_request = Arc::new(std::sync::atomic::AtomicU64::new(1));
        globals.set(
            "__cobbleStartHttpRequest",
            Func::from(
                move |method: String, url: String, headers: String, body: String| -> i64 {
                    let request_id =
                        next_http_request.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if request_id > i64::MAX as u64 {
                        return -1;
                    }
                    if http_requests
                        .try_send(HostHttpRequest {
                            request_id,
                            method,
                            url,
                            headers,
                            body,
                        })
                        .is_ok()
                    {
                        request_id as i64
                    } else {
                        -1
                    }
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
                Input::HttpResponse {
                    request_id,
                    response,
                } => context.with(|ctx| -> anyhow::Result<()> {
                    let callback: Function =
                        catch_js(&ctx, ctx.globals().get("__cobbleHttpResponse"))?;
                    catch_js(&ctx, callback.call::<_, ()>((request_id, response)))
                })?,
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

fn spawn_http_worker(
    input: mpsc::Sender<Input>,
) -> anyhow::Result<mpsc::SyncSender<HostHttpRequest>> {
    let (requests, receiver) = mpsc::sync_channel::<HostHttpRequest>(32);
    std::thread::Builder::new()
        .name("pkjs-http".into())
        .spawn(move || {
            while let Ok(request) = receiver.recv() {
                let response = http_request(
                    &request.method,
                    &request.url,
                    &request.headers,
                    &request.body,
                )
                .to_string();
                if input
                    .send(Input::HttpResponse {
                        request_id: request.request_id,
                        response,
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
        .context("start PKJS HTTP worker")?;
    Ok(requests)
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
            warn!("JavaScript promise job failed: {message}");
        }
    }
    Ok(())
}

fn http_request(method: &str, url: &str, headers: &str, body: &str) -> Value {
    let result = (|| -> anyhow::Result<Value> {
        let mut method =
            reqwest::Method::from_bytes(method.as_bytes()).context("invalid HTTP method")?;
        let mut current_url = reqwest::Url::parse(url).context("invalid URL")?;
        let headers: HashMap<String, String> =
            serde_json::from_str(headers).context("invalid request headers")?;
        let mut request_body = body.to_owned();
        let mut response = None;

        for redirect_count in 0..=10 {
            let client = client_for_url(&current_url)?;
            let mut request = client.request(method.clone(), current_url.clone());
            for (name, value) in &headers {
                request = request.header(name, value);
            }
            if !request_body.is_empty() {
                request = request.body(request_body.clone());
            }
            let next_response = request.send().context("HTTP request failed")?;
            if !next_response.status().is_redirection() {
                response = Some(next_response);
                break;
            }
            if redirect_count == 10 {
                bail!("HTTP redirect limit exceeded");
            }
            let location = next_response
                .headers()
                .get(reqwest::header::LOCATION)
                .context("HTTP redirect did not include a location")?
                .to_str()
                .context("HTTP redirect location is not valid text")?;
            current_url = current_url
                .join(location)
                .context("invalid HTTP redirect location")?;
            if next_response.status() == reqwest::StatusCode::SEE_OTHER
                || ((next_response.status() == reqwest::StatusCode::MOVED_PERMANENTLY
                    || next_response.status() == reqwest::StatusCode::FOUND)
                    && method == reqwest::Method::POST)
            {
                method = reqwest::Method::GET;
                request_body.clear();
            }
        }

        let mut response = response.context("HTTP request did not produce a response")?;
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

fn client_for_url(url: &reqwest::Url) -> anyhow::Result<reqwest::blocking::Client> {
    if !matches!(url.scheme(), "http" | "https") {
        bail!("only HTTP and HTTPS URLs are allowed");
    }
    let host = url.host_str().context("URL is missing a hostname")?;
    let resolution_host = host.trim_start_matches('[').trim_end_matches(']');
    let port = url
        .port_or_known_default()
        .context("URL is missing a port")?;
    let addresses: Vec<SocketAddr> = (resolution_host, port)
        .to_socket_addrs()
        .context("resolve URL hostname")?
        .collect();
    if addresses.is_empty() {
        bail!("URL hostname did not resolve");
    }
    if addresses.iter().any(|address| !is_public_ip(address.ip())) {
        bail!("URL hostname resolves to a non-public address");
    }

    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .resolve_to_addrs(resolution_host, &addresses)
        .build()
        .context("create HTTP client")
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => {
            if let Some(ip) = ip.to_ipv4_mapped() {
                return is_public_ipv4(ip);
            }
            is_public_ipv6(ip)
        }
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    // Globally routable IPv6 unicast space is currently 2000::/3. Keep the
    // allow-list conservative and exclude the documentation range.
    (segments[0] & 0xe000) == 0x2000 && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
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

    #[test]
    fn rejects_non_public_http_targets() {
        for address in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "172.16.0.1",
            "192.168.0.1",
            "224.0.0.1",
            "::",
            "::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_public_ip(address.parse().unwrap()), "{address}");
        }
        for address in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(is_public_ip(address.parse().unwrap()), "{address}");
        }
        assert!(client_for_url(&reqwest::Url::parse("http://127.0.0.1/").unwrap()).is_err());
        assert!(client_for_url(&reqwest::Url::parse("file:///etc/passwd").unwrap()).is_err());
    }

    #[test]
    fn promise_job_failure_does_not_stop_later_jobs() {
        let runtime = Runtime::new().unwrap();
        let context = Context::full(&runtime).unwrap();
        context.with(|ctx| {
            ctx.eval::<(), _>(
                "globalThis.completed = false;\n\
                 Promise.resolve().then(() => { throw new Error('expected'); });\n\
                 Promise.resolve().then(() => { globalThis.completed = true; });",
            )
            .unwrap();
        });

        drain_jobs(&runtime).unwrap();
        context.with(|ctx| {
            assert!(ctx.globals().get::<_, bool>("completed").unwrap());
        });
    }

    #[test]
    fn asynchronous_xhr_defers_completion_and_uses_late_handler() {
        let runtime = Runtime::new().unwrap();
        let context = Context::full(&runtime).unwrap();
        let scheduled = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Mutex::new(Vec::new()));
        context.with(|ctx| {
            let globals = ctx.globals();
            globals.set("__cobbleInitialStorage", "{}").unwrap();
            globals
                .set("__cobbleLog", Func::from(|_: String, _: String| {}))
                .unwrap();
            globals
                .set("__cobbleSaveStorage", Func::from(|_: String| {}))
                .unwrap();
            let scheduled_ids = Arc::clone(&scheduled);
            globals
                .set(
                    "__cobbleScheduleTimer",
                    Func::from(move |id: u64, _: f64, _: bool| {
                        scheduled_ids.lock().unwrap().push(id);
                    }),
                )
                .unwrap();
            globals
                .set("__cobbleCancelTimer", Func::from(|_: u64| {}))
                .unwrap();
            globals
                .set(
                    "__cobbleHttpRequest",
                    Func::from(|_: String, _: String, _: String, _: String| {
                        r#"{"ok":true,"status":200,"status_text":"OK","body":"done"}"#.to_string()
                    }),
                )
                .unwrap();
            let started_requests = Arc::clone(&started);
            globals
                .set(
                    "__cobbleStartHttpRequest",
                    Func::from(
                        move |method: String, url: String, headers: String, body: String| -> i64 {
                            started_requests
                                .lock()
                                .unwrap()
                                .push((method, url, headers, body));
                            7
                        },
                    ),
                )
                .unwrap();
            ctx.eval::<(), _>(include_str!("bridge.js")).unwrap();
            ctx.eval::<(), _>(
                "globalThis.loaded = false;\n\
                 const request = new XMLHttpRequest();\n\
                 request.open('GET', 'https://old.example.com');\n\
                 request.setRequestHeader('Old', 'value');\n\
                 request.status = 204; request.statusText = 'Old';\n\
                 request.response = request.responseText = 'old';\n\
                 request.open('POST', 'https://example.com');\n\
                 request.send();\n\
                 globalThis.stateAfterSend = request.readyState;\n\
                 globalThis.resetBeforeSend = request.status === 0 &&\n\
                   request.statusText === '' && request.response === '' &&\n\
                   request.responseText === '';\n\
                 request.onload = () => { globalThis.loaded = true; };",
            )
            .unwrap();
            assert_eq!(globals.get::<_, i32>("stateAfterSend").unwrap(), 1);
            assert!(globals.get::<_, bool>("resetBeforeSend").unwrap());
            assert!(!globals.get::<_, bool>("loaded").unwrap());
            ctx.eval::<(), _>(
                "globalThis.syncLoaded = false;\n\
                 const syncRequest = new XMLHttpRequest();\n\
                 syncRequest.open('GET', 'https://example.com', false);\n\
                 syncRequest.onload = () => { globalThis.syncLoaded = true; };\n\
                 syncRequest.send();",
            )
            .unwrap();
            assert!(globals.get::<_, bool>("syncLoaded").unwrap());
        });

        assert_eq!(
            *started.lock().unwrap(),
            vec![(
                "POST".into(),
                "https://example.com".into(),
                "{}".into(),
                "".into()
            )]
        );
        assert!(scheduled.lock().unwrap().is_empty());
        context.with(|ctx| {
            ctx.globals()
                .get::<_, Function>("__cobbleHttpResponse")
                .unwrap()
                .call::<_, ()>((
                    7_u64,
                    r#"{"ok":true,"status":200,"status_text":"OK","body":"done"}"#,
                ))
                .unwrap();
        });
        let timer_id = scheduled.lock().unwrap()[0];
        context.with(|ctx| {
            ctx.globals()
                .get::<_, Function>("__cobbleFireTimer")
                .unwrap()
                .call::<_, ()>((timer_id,))
                .unwrap();
            assert!(ctx.globals().get::<_, bool>("loaded").unwrap());
        });
    }
}
