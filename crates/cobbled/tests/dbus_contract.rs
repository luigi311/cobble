use std::process::Command;

#[test]
fn exported_dbus_contract_contains_all_client_settings_apis() {
    let root = std::env::temp_dir().join(format!("cobbled-dbus-contract-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("create isolated contract-test directory");
    let config = root.join("config.toml");
    let data = root.join("data");
    std::fs::write(&config, "").expect("write isolated empty config");

    let script = r#"
set -eu
"$COBBLED_TEST_BIN" --config "$COBBLED_TEST_CONFIG" >/dev/null 2>&1 &
daemon_pid=$!
trap 'kill "$daemon_pid" 2>/dev/null || true; wait "$daemon_pid" 2>/dev/null || true' EXIT
i=0
while [ "$i" -lt 100 ]; do
  if gdbus introspect --session --dest org.cobble.Daemon --object-path /org/cobble/Daemon --xml 2>/dev/null; then
    exit 0
  fi
  i=$((i + 1))
  sleep 0.05
done
exit 1
"#;
    let output = Command::new("dbus-run-session")
        .args(["--", "sh", "-c", script])
        .env("COBBLED_TEST_BIN", env!("CARGO_BIN_EXE_cobbled"))
        .env("COBBLED_TEST_CONFIG", &config)
        .env("XDG_DATA_HOME", &data)
        .output()
        .expect("run daemon on private session bus");
    let _ = std::fs::remove_dir_all(&root);
    assert!(output.status.success(), "private-bus introspection failed: {}", String::from_utf8_lossy(&output.stderr));
    let xml = String::from_utf8(output.stdout).expect("introspection is UTF-8");

    for method in [
        "GetDaemonConfig", "UpdateDaemonConfig", "GetDeviceConfig", "RefreshDeviceConfig",
        "UpdateDeviceConfig", "ResetDeviceConfigDefaults", "SyncWellness",
        "GetWellnessSyncStatus", "ReloadConfig", "RebootWatch", "ResetIntoRecovery",
        "CreateCoreDump", "FactoryReset", "Forget",
    ] {
        assert!(xml.contains(&format!("<method name=\"{method}\">")), "missing D-Bus method {method}");
    }
    for signal in ["DaemonConfigChanged", "DeviceConfigChanged", "ConnectionChanged"] {
        assert!(xml.contains(&format!("<signal name=\"{signal}\">")), "missing D-Bus signal {signal}");
    }
    assert!(xml.contains("<arg name=\"expected_revision\" type=\"t\" direction=\"in\"/>"));
    assert!(xml.contains("<arg name=\"patch\" type=\"a{sv}\" direction=\"in\"/>"));
}
