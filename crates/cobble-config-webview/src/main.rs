use std::{
    fs,
    io::{self, Read, Write},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
use gtk::prelude::{GtkWindowExt, WidgetExt};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use tao::{
    dpi::LogicalSize,
    event::{Event, WindowEvent},
    event_loop::{ControlFlow, EventLoopBuilder},
    platform::unix::{EventLoopBuilderExtUnix, WindowExtUnix},
    window::{UserAttentionType, WindowBuilder},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;
use uuid::Uuid;
use wry::{
    NewWindowResponse, PageLoadEvent, PermissionResponse, WebContext, WebViewBuilder,
    WebViewBuilderExtUnix, WebViewExtUnix,
};

const MAX_REQUEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CONFIG_WEBVIEW_MESSAGE_BYTES: usize = 1024 * 1024;
// A JSON string byte can expand to six bytes (for example, `\u0000`). Keep
// enough room for the submitted-result wrapper and its trailing newline.
const SUBMITTED_RESPONSE_OVERHEAD_BYTES: usize = 35;
const MAX_RESPONSE_BYTES: usize =
    (MAX_CONFIG_WEBVIEW_MESSAGE_BYTES - SUBMITTED_RESPONSE_OVERHEAD_BYTES) / 6;

#[derive(Deserialize)]
struct ConfigRequest {
    uuid: String,
    title: String,
    url: String,
}

#[derive(Clone)]
enum UserEvent {
    Submitted(String),
    Cancelled,
    Fatal(String),
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ConfigResult {
    Submitted { response: String },
    Cancelled,
    Fatal { message: String },
}

fn main() {
    init_logging();
    if let Err(error) = run() {
        let message = format!("{error:#}");
        error!(%message, "configuration helper failed");
        let _ = emit_result(&ConfigResult::Fatal { message });
        std::process::exit(1);
    }
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("cobble_config_webview=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .try_init();
}

fn run() -> Result<()> {
    let request = read_request()?;
    Uuid::parse_str(&request.uuid).context("invalid app UUID")?;
    let initial_url = normalize_initial_url(&request.url)?;
    let storage_path = storage_path(&request.uuid)?;
    info!(
        pid = std::process::id(),
        uuid = %request.uuid,
        title = %request.title,
        url = %describe_url(&initial_url),
        storage = %storage_path.display(),
        display = ?std::env::var("DISPLAY").ok(),
        wayland_display = ?std::env::var("WAYLAND_DISPLAY").ok(),
        session_type = ?std::env::var("XDG_SESSION_TYPE").ok(),
        "starting configuration helper"
    );

    let mut event_loop_builder = EventLoopBuilder::<UserEvent>::with_user_event();
    event_loop_builder.with_app_id("org.cobble.ConfigWebview");
    let event_loop = event_loop_builder.build();
    let proxy = event_loop.create_proxy();
    info!("configuration helper event loop created");
    let window = WindowBuilder::new()
        .with_title(format!("{} Settings", request.title))
        .with_inner_size(LogicalSize::new(420.0, 720.0))
        .with_visible(false)
        .with_always_on_top(true)
        .build(&event_loop)
        .context("create configuration window")?;
    info!(
        window_id = ?window.id(),
        scale_factor = window.scale_factor(),
        size = ?window.inner_size(),
        "configuration window created hidden"
    );

    let mut web_context = WebContext::new(Some(storage_path));
    let navigation_proxy = proxy.clone();
    let webview = WebViewBuilder::new_with_web_context(&mut web_context)
        .with_url(&initial_url)
        .with_navigation_handler(move |url| {
            if let Some(response) = close_response(&url) {
                info!(url = %describe_url(&url), "configuration page requested close");
                let event = match response {
                    Ok(Some(response)) => UserEvent::Submitted(response),
                    Ok(None) => UserEvent::Cancelled,
                    Err(message) => UserEvent::Fatal(message.into()),
                };
                let _ = navigation_proxy.send_event(event);
                return false;
            }
            let allowed = allowed_navigation(&url);
            debug!(url = %describe_url(&url), allowed, "configuration navigation requested");
            allowed
        })
        .with_permission_handler(|kind| {
            warn!(?kind, "denying configuration-page permission request");
            PermissionResponse::Deny
        })
        .with_new_window_req_handler(|url, features| {
            warn!(
                url = %describe_url(&url),
                ?features,
                "denying configuration-page popup request"
            );
            NewWindowResponse::Deny
        })
        .with_on_page_load_handler(|event, url| {
            let stage = match event {
                PageLoadEvent::Started => "started",
                PageLoadEvent::Finished => "finished",
            };
            info!(stage, url = %describe_url(&url), "configuration page load event");
        })
        .build_gtk(
            window
                .default_vbox()
                .context("configuration window has no GTK container")?,
        )
        .context("create configuration webview")?;
    info!("configuration WebKit view created");

    // The helper is a separate process, so it cannot be transient-for the
    // Slint window. Present the underlying GTK window directly in addition to
    // Tao's queued requests. This ensures the map request reaches Wayland even
    // when Phosh declines focus activation for a child process.
    let gtk_window = window.gtk_window();
    let native_webview = webview.webview();
    gtk_window.set_accept_focus(true);
    gtk_window.set_focus_on_map(true);
    gtk_window.show_all();
    gtk_window.deiconify();
    gtk_window.present();
    native_webview.grab_focus();
    window.set_visible(true);
    window.set_focus();
    window.request_user_attention(Some(UserAttentionType::Informational));
    info!(
        tao_visible = window.is_visible(),
        tao_focused = window.is_focused(),
        gtk_visible = gtk_window.is_visible(),
        gtk_mapped = gtk_window.is_mapped(),
        gtk_realized = gtk_window.is_realized(),
        webview_visible = native_webview.is_visible(),
        webview_mapped = native_webview.is_mapped(),
        "configuration window presentation requested"
    );

    let resources = (window, webview, web_context);
    let mut finished = false;
    event_loop.run(move |event, _, control_flow| {
        let _ = &resources;
        *control_flow = ControlFlow::Wait;
        let result = match event {
            Event::UserEvent(UserEvent::Submitted(response)) => {
                Some(ConfigResult::Submitted { response })
            }
            Event::UserEvent(UserEvent::Cancelled)
            | Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => Some(ConfigResult::Cancelled),
            Event::UserEvent(UserEvent::Fatal(message)) => Some(ConfigResult::Fatal { message }),
            Event::LoopDestroyed => {
                info!("configuration helper event loop destroyed");
                None
            }
            _ => None,
        };
        if let Some(result) = result {
            if !finished {
                let _ = emit_result(&result);
                finished = true;
            }
            *control_flow = ControlFlow::Exit;
        }
    });
}

fn describe_url(input: &str) -> String {
    let Ok(url) = Url::parse(input) else {
        return "<invalid URL>".into();
    };
    if url.scheme() == "data" {
        return format!("data:<{} bytes>", input.len());
    }
    let mut description = format!("{}:", url.scheme());
    if let Some(host) = url.host_str() {
        description.push_str("//");
        description.push_str(host);
    }
    description.push_str(url.path());
    if url.query().is_some() {
        description.push_str("?<redacted>");
    }
    if url.fragment().is_some() {
        description.push_str("#<redacted>");
    }
    description
}

fn read_request() -> Result<ConfigRequest> {
    let mut bytes = Vec::new();
    io::stdin()
        .take(MAX_REQUEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("read configuration request")?;
    if bytes.len() as u64 > MAX_REQUEST_BYTES {
        bail!("configuration request is too large");
    }
    serde_json::from_slice(&bytes).context("decode configuration request")
}

fn normalize_initial_url(input: &str) -> Result<String> {
    let mut url = Url::parse(input).context("invalid configuration URL")?;
    match url.scheme() {
        "http" | "https" | "data" => {}
        _ => bail!("unsupported configuration URL scheme"),
    }

    // RawGit was retired, but many classic Pebble apps still return its old URLs.
    if matches!(url.host_str(), Some("rawgit.com" | "cdn.rawgit.com")) {
        url.set_host(Some("raw.githack.com"))
            .map_err(|_| anyhow::anyhow!("invalid legacy configuration URL"))?;
    }
    Ok(url.into())
}

fn close_response(url: &str) -> Option<Result<Option<String>, &'static str>> {
    let encoded = if let Some(encoded) = url.strip_prefix("pebblejs://close#") {
        encoded
    } else if let Some(encoded) = url.strip_prefix("pebblejs://close/") {
        encoded.strip_prefix('?').unwrap_or(encoded)
    } else if url == "pebblejs://close" {
        ""
    } else {
        return None;
    };
    if encoded.is_empty() {
        return Some(Ok(None));
    }
    let response = match percent_decode_str(encoded).decode_utf8() {
        Ok(response) => response.into_owned(),
        Err(_) => return Some(Err("configuration response is not valid UTF-8")),
    };
    if response.len() > MAX_RESPONSE_BYTES {
        Some(Err("configuration response exceeds the size limit"))
    } else {
        Some(Ok(Some(response)))
    }
}

fn allowed_navigation(input: &str) -> bool {
    Url::parse(input)
        .map(|url| matches!(url.scheme(), "http" | "https" | "data" | "about" | "blob"))
        .unwrap_or(false)
}

fn storage_path(uuid: &str) -> Result<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .context("XDG_DATA_HOME and HOME are unavailable")?;
    let path = base.join("cobble/config-webview").join(uuid);
    fs::create_dir_all(&path).context("create configuration storage directory")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .context("protect configuration storage directory")?;
    }
    Ok(path)
}

fn emit_result(result: &ConfigResult) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, result).context("encode configuration result")?;
    stdout
        .write_all(b"\n")
        .context("write configuration result")?;
    stdout.flush().context("flush configuration result")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_close_urls() {
        assert_eq!(
            close_response("pebblejs://close#%7B%22color%22%3A%22red%22%7D"),
            Some(Ok(Some(r#"{"color":"red"}"#.into())))
        );
        assert_eq!(
            close_response("pebblejs://close/?name=Mario%20Watch"),
            Some(Ok(Some("name=Mario Watch".into())))
        );
        assert_eq!(close_response("pebblejs://close"), Some(Ok(None)));
        assert_eq!(close_response("https://example.com/close"), None);
    }

    #[test]
    fn accepts_only_web_navigation() {
        assert!(allowed_navigation("https://example.com/settings"));
        assert!(allowed_navigation("about:blank"));
        assert!(!allowed_navigation("file:///etc/passwd"));
        assert!(!allowed_navigation("javascript:alert(1)"));
    }

    #[test]
    fn rewrites_retired_rawgit_hosts() {
        assert_eq!(
            normalize_initial_url("https://rawgit.com/example/app/master/config.html").unwrap(),
            "https://raw.githack.com/example/app/master/config.html"
        );
    }

    #[test]
    fn redacts_url_values_in_diagnostics() {
        assert_eq!(
            describe_url("https://example.com/config?token=secret#result"),
            "https://example.com/config?<redacted>#<redacted>"
        );
        assert_eq!(describe_url("not a URL"), "<invalid URL>");
    }

    #[test]
    fn maximum_response_fits_parent_message_limit_after_json_escaping() {
        let result = ConfigResult::Submitted {
            response: "\0".repeat(MAX_RESPONSE_BYTES),
        };
        let mut encoded = serde_json::to_vec(&result).unwrap();
        encoded.push(b'\n');
        assert!(encoded.len() <= MAX_CONFIG_WEBVIEW_MESSAGE_BYTES);

        let oversized = ConfigResult::Submitted {
            response: "\0".repeat(MAX_RESPONSE_BYTES + 1),
        };
        let mut encoded = serde_json::to_vec(&oversized).unwrap();
        encoded.push(b'\n');
        assert!(encoded.len() > MAX_CONFIG_WEBVIEW_MESSAGE_BYTES);
    }
}
