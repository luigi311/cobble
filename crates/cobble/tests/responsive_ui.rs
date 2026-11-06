//! Native software-renderer fixtures. No daemon, watch, or display server is used.
//! Set COBBLE_UI_SNAPSHOTS=/absolute/path to save PNGs for visual review.
slint::include_modules!();

slint::slint! {
    import { NumericSetting, PeriodControl } from "../ui/widgets.slint";
    export component StepperProbe inherits Window {
        in-out property <int> value: 50;
        NumericSetting {
            label: "Sample"; minimum: 0; maximum: 100;
            value <=> root.value;
        }
    }
    export component PeriodProbe inherits Window {
        in-out property <bool> can-forward: false;
        in-out property <int> forwards: 0;
        PeriodControl {
            can-forward: root.can-forward;
            go-forward => { root.forwards += 1; }
        }
    }
}

use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
use slint::platform::{Platform, PlatformError, PointerEventButton, WindowAdapter, WindowEvent};
use slint::{ComponentHandle, LogicalPosition, LogicalSize, ModelRc, PhysicalSize, VecModel};
use std::{cell::Cell, path::Path, rc::Rc, time::Duration};

struct Headless(Rc<MinimalSoftwareWindow>, Rc<Cell<u64>>);
impl Platform for Headless {
    fn create_window_adapter(&self) -> Result<Rc<dyn WindowAdapter>, PlatformError> {
        Ok(self.0.clone())
    }
    fn duration_since_start(&self) -> Duration {
        Duration::from_millis(self.1.get())
    }
}

fn m<T: Clone + 'static>(v: Vec<T>) -> ModelRc<T> {
    ModelRc::new(VecModel::from(v))
}
fn fill(w: &AppWindow) {
    w.set_daemon_running(true);
    w.set_watch_connected(true);
    w.set_battery_level(72);
    w.set_wi_model("Pebble Time 2".into());
    w.set_wi_firmware("v4.9.127-rebble3".into());
    w.set_wi_color("Black/Red".into());
    w.set_wi_board("asterix_evt2".into());
    w.set_wi_serial("Q402445E00AB".into());
    w.set_wi_bt("E6:94:0A:D4:D5:DC".into());
    w.set_wi_language("en_US v12".into());
    let days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    w.set_daily_steps(m(days
        .iter()
        .enumerate()
        .map(|(i, d)| DaySteps {
            label: (*d).into(),
            steps_label: format!("{}k", 4 + i).into(),
            fraction: 0.3 + 0.1 * i as f32,
            bar_start: i as i32,
            bar_end: i as i32 + 1,
        })
        .collect()));
    w.set_steps_avg_label("Avg 7,214 steps/day".into());
    w.set_steps_delta_label("+12% vs last week".into());
    w.set_sessions(m((0..5)
        .map(|i| HealthSession {
            type_name: "Walk".into(),
            start_label: format!("Nov {} 7:1{} AM", i + 1, i).into(),
            duration_label: "42m".into(),
            has_metrics: i % 2 == 0,
            metrics_label: "3,812 steps · 2.8 km · 214 kcal".into(),
        })
        .collect()));
    w.set_sleep_bars(m(days
        .iter()
        .enumerate()
        .map(|(i, d)| SleepBar {
            label: (*d).into(),
            bar_start: i as i32,
            bar_end: i as i32 + 1,
            light_fraction: 0.5,
            deep_fraction: 0.2,
            total_label: "7h 30m".into(),
            deep_label: "2h 15m deep".into(),
        })
        .collect()));
    w.set_nap_bars(m(days
        .iter()
        .enumerate()
        .map(|(i, d)| SleepBar {
            label: (*d).into(),
            bar_start: i as i32,
            bar_end: i as i32 + 1,
            light_fraction: 0.05,
            deep_fraction: 0.05,
            total_label: "20m".into(),
            deep_label: "".into(),
        })
        .collect()));
    w.set_has_sleep(true);
    w.set_has_naps(true);
    w.set_sleep_max_label("8h 12m".into());
    w.set_sleep_mid_label("4h 6m".into());
    w.set_sleep_avg_label("Avg 7h 12m".into());
    w.set_sleep_delta_label("-8m".into());
    w.set_nap_summary(NapSummary {
        available: true,
        count: 3,
        scope_label: "This week".into(),
        total_label: "1h 10m".into(),
        avg_label: "23m".into(),
        latest_label: "Latest: Tue 2:10 PM, 25m".into(),
    });
    w.set_sleep_stats(SleepStats {
        deep_avg_label: "1h 52m".into(),
        light_pct: 61.,
        deep_pct: 26.,
        awake_pct: 13.,
        avg_bedtime: "11:24 PM".into(),
        avg_wakeup: "6:52 AM".into(),
        highest_dur: "8h 12m".into(),
        lowest_dur: "5h 40m".into(),
    });
    w.set_heart_stats(HeartStats {
        average_label: "71 bpm".into(),
        resting_label: "58 bpm".into(),
        sleeping_label: "54 bpm".into(),
        lowest_label: "47 bpm".into(),
        highest_label: "162 bpm".into(),
        samples_label: "1,204".into(),
    });
    w.set_heart_trend_points(m(days
        .iter()
        .map(|d| HeartTrendPoint { label: (*d).into() })
        .collect()));
    w.set_heart_average_path("M 7 40 L 21 45 M 21 45 L 35 30 M 35 30 L 50 50 M 50 50 L 64 35 M 64 35 L 78 42 M 78 42 L 92 38".into());
    w.set_heart_resting_path("M 7 70 L 21 72 M 21 72 L 35 68 M 35 68 L 50 74 M 50 74 L 64 70 M 64 70 L 78 71 M 78 71 L 92 69".into());
    w.set_heart_trend_max_label("170".into());
    w.set_heart_trend_mid_label("105".into());
    w.set_heart_trend_min_label("40".into());
    w.set_heart_trend_has_data(true);
    w.set_cfg_address("E6:94:0A:D4:D5:DC".into());
    w.set_scan_results(m(vec![WatchDevice {
        name: "Pebble Time 2 D5DC".into(),
        address: "E6:94:0A:D4:D5:DC".into(),
    }]));
    w.set_installed_apps(m((0..3)
        .map(|index| InstalledApp {
            uuid: format!("fixture-{index}").into(),
            name: "Weather Station Deluxe Edition".into(),
            version: "2.14".into(),
            platform: "emery".into(),
            state: "installed".into(),
            watchface: true,
            configurable: true,
        })
        .collect()));
    w.set_dc_state("ready".into());
    w.set_dc_status("Settings read from watch.".into());
    w.set_dc_watch_id("Q402445E00AB (asterix_evt2)".into());
    w.set_dc_platform("emery".into());
    w.set_dc_firmware("v4.9.127-rebble3".into());
    w.set_dc_last_read("2026-11-05 14:02:11".into());
    w.set_dc_height("175 cm".into());
    w.set_dc_units_available(true);
    w.set_dc_health_available(true);
    w.set_dc_height_display("175 cm / 5' 9\"".into());
    w.set_dc_weight_display("72.5 kg / 159.8 lb".into());
    w.set_dc_light_timeout_available(true);
    w.set_dc_light_timeout_ms(10000);
    w.set_dc_hrm_interval_available(true);
    w.set_dc_hrm_interval_options(m(vec![
        "Every 10 minutes during background sampling".into(),
    ]));
    w.set_dc_preferences(m(vec![DeviceConfigEntry {
        group: "Other".into(),
        label: "Notification vibration pattern".into(),
        value: "Standard short double pulse".into(),
        status: "available".into(),
    }]));
}

fn resize(sw: &MinimalSoftwareWindow, width: u32, height: u32, scale: f32) {
    sw.dispatch_event(WindowEvent::ScaleFactorChanged {
        scale_factor: scale,
    });
    sw.set_size(LogicalSize::new(width as f32, height as f32));
}

fn click(sw: &MinimalSoftwareWindow, x: f32, y: f32) {
    let position = LogicalPosition::new(x, y);
    sw.dispatch_event(WindowEvent::PointerPressed {
        position,
        button: PointerEventButton::Left,
    });
    sw.dispatch_event(WindowEvent::PointerReleased {
        position,
        button: PointerEventButton::Left,
    });
}

fn scroll(sw: &MinimalSoftwareWindow, x: f32, y: f32, amount: f32) {
    sw.dispatch_event(WindowEvent::PointerScrolled {
        position: LogicalPosition::new(x, y),
        delta_x: 0.,
        delta_y: amount,
    });
}

/// Settle animations and render the window, optionally saving a named PNG fixture.
fn snapshot(
    w: &AppWindow,
    clock: &Cell<u64>,
    name: &str,
) -> slint::SharedPixelBuffer<slint::Rgba8Pixel> {
    clock.set(clock.get() + 500);
    slint::platform::update_timers_and_animations();
    let image = w.window().take_snapshot().unwrap();
    if let Some(dir) = std::env::var_os("COBBLE_UI_SNAPSHOTS")
        && name != "reset"
        && name != "navigation"
    {
        std::fs::create_dir_all(&dir).unwrap();
        let file = std::fs::File::create(Path::new(&dir).join(format!("{name}.png"))).unwrap();
        let mut encoder =
            png::Encoder::new(std::io::BufWriter::new(file), image.width(), image.height());
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_compression(png::Compression::Fast);
        encoder
            .write_header()
            .unwrap()
            .write_image_data(image.as_bytes())
            .unwrap();
    }
    image
}

#[test]
fn disabled_period_controls_ignore_taps() {
    let sw = MinimalSoftwareWindow::new(RepaintBufferType::NewBuffer);
    slint::platform::set_platform(Box::new(Headless(sw.clone(), Rc::new(Cell::new(0))))).unwrap();
    let period = PeriodProbe::new().unwrap();
    period.show().unwrap();
    resize(&sw, 360, 124, 1.);
    period.window().take_snapshot().unwrap();
    click(&sw, 320., 92.);
    assert_eq!(period.get_forwards(), 0, "disabled Next must ignore taps");
    period.set_can_forward(true);
    period.window().take_snapshot().unwrap();
    click(&sw, 320., 92.);
    assert_eq!(period.get_forwards(), 1, "enabled Next must respond");
}

/// Preserve open Settings forms and staged edits when navigating during an action.
#[test]
fn settings_navigation_remains_available_during_actions() {
    let sw = MinimalSoftwareWindow::new(RepaintBufferType::NewBuffer);
    let clock = Rc::new(Cell::new(0));
    slint::platform::set_platform(Box::new(Headless(sw.clone(), clock.clone()))).unwrap();
    let w = AppWindow::new().unwrap();
    w.set_cfg_adapter("hci-staged".into());
    w.set_dc_dirty(true);
    w.set_action_busy(true);
    w.set_action_cancellable(true);
    w.set_action_status("Waiting for app configuration…".into());
    w.show().unwrap();

    for (width, height) in [(360, 720), (720, 360)] {
        resize(&sw, width, height, 2.);
        let (device_x, device_y, settings_x, settings_y) = if width > height {
            (40., 40., 40., 296.)
        } else {
            (36., 688., 324., 688.)
        };
        for page in [1, 2, 3] {
            w.set_active_tab(4);
            w.set_settings_page(page);
            snapshot(&w, &clock, "navigation");
            click(&sw, device_x, device_y);
            assert_eq!(w.get_active_tab(), 0);
            snapshot(&w, &clock, "navigation");
            click(&sw, settings_x, settings_y);
            assert_eq!(
                w.get_active_tab(),
                4,
                "Settings must remain reachable while busy"
            );
            assert_eq!(w.get_settings_page(), page);
            snapshot(&w, &clock, "navigation");
            click(&sw, settings_x, settings_y);
            assert_eq!(
                w.get_settings_page(),
                page,
                "reselecting Settings must preserve its form"
            );
            assert_eq!(w.get_cfg_adapter(), "hci-staged");
            assert!(w.get_dc_dirty());
            assert!(w.get_action_busy());
            assert!(w.get_action_cancellable());
            assert_eq!(w.get_action_status(), "Waiting for app configuration…");
        }
    }
}

/// Exercise responsive rendering, navigation, scrolling, and state preservation across scales.
#[test]
fn responsive_pages_and_rotation() {
    let sw = MinimalSoftwareWindow::new(RepaintBufferType::NewBuffer);
    let clock = Rc::new(Cell::new(0));
    slint::platform::set_platform(Box::new(Headless(sw.clone(), clock.clone()))).unwrap();
    let w = AppWindow::new().unwrap();
    fill(&w);
    w.show().unwrap();
    // Physical phone and Comet sizes are also exercised at compositor scale 2.
    let sizes = [
        (320, 568, 1.),
        (360, 720, 2.),
        (720, 360, 2.),
        (540, 620, 2.),
        (620, 540, 2.),
        (1080, 1240, 1.),
        (1240, 1080, 1.),
        (1280, 800, 1.),
        (640, 280, 1.),
        (432, 496, 2.5),
        (496, 432, 2.5),
        (360, 413, 3.),
        (413, 360, 3.),
        (720, 150, 1.),
    ];
    let pages = [
        ("device", 0, 0),
        ("steps", 1, 0),
        ("sleep", 2, 0),
        ("heart", 3, 0),
        ("settings", 4, 0),
        ("daemon", 4, 1),
        ("device-config", 4, 3),
        ("actions", 4, 2),
    ];
    for (width, height, scale) in sizes {
        resize(&sw, width, height, scale);
        let rail =
            (width >= 680 && (height < 500 || width >= 900)) || (width >= 480 && height < 280);
        // Exercise the actual navigation hit areas, including the first and last tab.
        for index in 0..5 {
            w.set_active_tab((index + 1) % 5);
            snapshot(&w, &clock, "navigation");
            let (x, y) = if rail {
                scroll(&sw, 40., height as f32 / 2., 100_000.);
                let row_height = if height < 336 { 48. } else { 64. };
                let mut y = 8. + (index as f32 + 0.5) * row_height;
                if y > height as f32 - 8. {
                    scroll(&sw, 40., height as f32 / 2., -100_000.);
                    y -= (16. + 5. * row_height - height as f32).max(0.);
                }
                (40., y)
            } else {
                (
                    (index as f32 + 0.5) * width as f32 / 5.,
                    height as f32 - 32.,
                )
            };
            click(&sw, x, y);
            assert_eq!(w.get_active_tab(), index, "navigation at {width}x{height}");
        }
        for (name, tab, page) in pages {
            // Recreate the page's Flickable before each fixture.
            w.set_active_tab(-1);
            snapshot(&w, &clock, "reset");
            w.set_settings_page(page);
            w.set_active_tab(tab);
            let prefix = format!("{width}x{height}-{name}");
            let top = snapshot(&w, &clock, &prefix);
            // The fixed header must stay at the top, outside the page scroller.
            let content_left = if rail {
                if width >= 1100 { 168 } else { 88 }
            } else {
                0
            };
            let pixel = top.as_slice()[((12. * scale) as u32 * top.width()
                + ((content_left + 3) as f32 * scale) as u32)
                as usize];
            assert_eq!(
                (pixel.r, pixel.g, pixel.b),
                (0x14, 0x14, 0x28),
                "header at {prefix}"
            );
            assert_eq!(
                w.window().size(),
                PhysicalSize::new(
                    (width as f32 * scale) as u32,
                    (height as f32 * scale) as u32
                )
            );
            assert_eq!(
                (top.width(), top.height()),
                (w.window().size().width, w.window().size().height)
            );
            scroll(&sw, (width / 2) as f32, (height / 2) as f32, -100_000.);
            let bottom = snapshot(&w, &clock, &format!("{prefix}-bottom"));
            if height <= 720
                && ["steps", "heart", "daemon", "device-config", "actions"].contains(&name)
            {
                assert!(
                    top.as_bytes() != bottom.as_bytes(),
                    "page must scroll at {prefix}"
                );
            }
            if page != 0 {
                click(&sw, content_left as f32 + 36., 24.);
                assert_eq!(
                    w.get_settings_page(),
                    0,
                    "Back stays reachable after scrolling {prefix}"
                );
            }
        }
    }
    // Dense hourly data must retain distinct selectable slots, not shrink targets.
    resize(&sw, 360, 720, 2.);
    w.set_active_tab(-1);
    snapshot(&w, &clock, "reset");
    w.set_daily_steps(m((0..24)
        .map(|hour| DaySteps {
            label: format!("{hour:02}").into(),
            steps_label: "420 steps".into(),
            fraction: 0.5,
            bar_start: hour * 3600,
            bar_end: (hour + 1) * 3600,
        })
        .collect()));
    w.set_active_tab(1);
    let top = snapshot(&w, &clock, "360x720-hourly");
    sw.dispatch_event(WindowEvent::PointerScrolled {
        position: LogicalPosition::new(180., 320.),
        delta_x: -100_000.,
        delta_y: 0.,
    });
    let end = snapshot(&w, &clock, "360x720-hourly-end");
    assert!(
        top.as_bytes() != end.as_bytes(),
        "hourly chart must pan horizontally"
    );

    // Heart Day has 24 points and four sparse hour labels. They must stay on
    // one line and align with the same plot as the weekly view.
    w.set_heart_trend_points(m((0..24)
        .map(|hour| HeartTrendPoint {
            label: match hour {
                0 => "12a",
                6 => "6a",
                12 => "12p",
                18 => "6p",
                _ => "",
            }
            .into(),
        })
        .collect()));
    w.set_active_tab(3);
    snapshot(&w, &clock, "360x720-heart-day");

    // Applying staged settings is reachable even at the top of a long form.
    let applied = Rc::new(Cell::new(false));
    let called = applied.clone();
    w.on_apply_device_config(move || called.set(true));
    w.set_active_tab(4);
    w.set_settings_page(3);
    w.set_dc_dirty(true);
    snapshot(&w, &clock, "360x720-staged-settings");
    click(&sw, 270., 624.);
    assert!(
        applied.get(),
        "Apply remains reachable without scrolling the form"
    );
    w.set_dc_dirty(false);

    // Confirmations block navigation and remain reachable in a short viewport.
    resize(&sw, 720, 300, 1.);
    w.set_settings_page(2);
    w.set_confirm_action("factory".into());
    snapshot(&w, &clock, "720x300-confirmation");
    click(&sw, 40., 40.);
    assert_eq!(
        w.get_active_tab(),
        4,
        "modal must block the navigation rail"
    );
    assert_eq!(w.get_confirm_action(), "factory");
    click(&sw, 250., 200.);
    assert!(
        w.get_confirm_action().is_empty(),
        "Cancel is reachable in the dialog"
    );

    // Rotation must not discard the open form, entered values, or staged watch edits.
    w.set_active_tab(4);
    w.set_settings_page(1);
    w.set_cfg_adapter("hci-test".into());
    w.set_dc_dirty(true);
    w.set_dc_age_value(37);
    for (width, height) in [(720, 360), (540, 620), (620, 540), (360, 720)] {
        resize(&sw, width, height, 2.);
        snapshot(&w, &clock, &format!("{width}x{height}-rotation"));
        assert_eq!(w.get_active_tab(), 4);
        assert_eq!(w.get_settings_page(), 1);
        assert_eq!(w.get_cfg_adapter(), "hci-test");
        assert!(w.get_dc_dirty());
        assert_eq!(w.get_dc_age_value(), 37);
    }
    // Empty/disconnected states must render too.
    w.set_watch_connected(false);
    w.set_daemon_running(false);
    w.set_daily_steps(m(vec![]));
    w.set_sessions(m(vec![]));
    w.set_has_sleep(false);
    w.set_has_naps(false);
    w.set_sleep_bars(m(vec![]));
    w.set_nap_bars(m(vec![]));
    w.set_heart_trend_has_data(false);
    for tab in 0..5 {
        w.set_active_tab(tab);
        w.set_settings_page(0);
        snapshot(&w, &clock, &format!("360x720-empty-{tab}"));
    }

    w.hide().unwrap();
    drop(w);
    let stepper = StepperProbe::new().unwrap();
    stepper.show().unwrap();
    resize(&sw, 280, 120, 1.);
    // Rendering resolves the control layout before dispatching the tap.
    stepper.window().take_snapshot().unwrap();
    click(&sw, 244., 80.);
    assert_eq!(
        stepper.get_value(),
        51,
        "stepper + must act on the first tap"
    );
    click(&sw, 36., 80.);
    assert_eq!(
        stepper.get_value(),
        50,
        "stepper - must act on the first tap"
    );
}
