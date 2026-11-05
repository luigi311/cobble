//! Enforce one daemon across login sessions, configurations, and installations.

use std::fs;
use std::io;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{SocketAddr, UnixListener};
use std::path::Path;

/// A duplicate launch is an intentional refusal, not a crash to restart.
pub const ALREADY_RUNNING_EXIT_CODE: u8 = 73;
const SOCKET_NAME: &[u8] = b"org.cobble.cobbled.instance";

/// The kernel releases this abstract socket even after SIGKILL. No socket or
/// PID file is left on disk, and child processes do not inherit the descriptor.
pub struct InstanceGuard {
    _socket: UnixListener,
}

impl InstanceGuard {
    pub fn acquire() -> io::Result<Self> {
        Self::acquire_at(SOCKET_NAME, Path::new("/proc"))
    }

    fn acquire_at(socket_name: &[u8], proc_root: &Path) -> io::Result<Self> {
        let own_pid = std::process::id();
        let address = SocketAddr::from_abstract_name(socket_name)?;
        let socket = UnixListener::bind_addr(&address).map_err(|error| {
            if error.kind() == io::ErrorKind::AddrInUse {
                already_running(find_other_daemon(proc_root, own_pid, None).ok().flatten())
            } else {
                error
            }
        })?;
        let guard = Self { _socket: socket };

        // Older installed builds do not own the socket. Detect them before any
        // database or D-Bus work. Same-executable contenders already use this
        // guard; ignoring them avoids rejecting the winner of concurrent starts
        // while a losing process is still exiting.
        let own_executable = fs::metadata("/proc/self/exe")?;
        let identity = (own_executable.dev(), own_executable.ino());
        if let Some(pid) = find_other_daemon(proc_root, own_pid, Some(identity))? {
            return Err(already_running(Some(pid)));
        }
        Ok(guard)
    }
}

fn already_running(pid: Option<u32>) -> io::Error {
    let owner = pid.map(|pid| format!(" (PID {pid})")).unwrap_or_default();
    io::Error::new(
        io::ErrorKind::AddrInUse,
        format!(
            "another cobbled instance is already running{owner}; stop the existing daemon before starting another instance"
        ),
    )
}

fn find_other_daemon(
    proc_root: &Path,
    own_pid: u32,
    guarded_executable: Option<(u64, u64)>,
) -> io::Result<Option<u32>> {
    for entry in fs::read_dir(proc_root)? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == own_pid {
            continue;
        }
        let path = entry.path();
        let Ok(name) = fs::read_to_string(path.join("comm")) else {
            continue;
        };
        if name.trim_end() != "cobbled" {
            continue;
        }
        match fs::metadata(path.join("exe")) {
            Ok(executable) if guarded_executable == Some((executable.dev(), executable.ino())) => {}
            // An exited/zombie process has no executable and cannot own BLE.
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            _ => return Ok(Some(pid)),
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, Write};
    use std::os::unix::fs::symlink;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, mpsc};

    fn socket_name() -> String {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        format!(
            "cobbled.test.{}.{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
    }

    #[test]
    fn concurrent_starts_allow_only_one_daemon_and_release_on_drop() {
        let name = socket_name();
        let proc_root = tempfile::tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(16));
        let (send, receive) = mpsc::channel();
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let barrier = barrier.clone();
                let send = send.clone();
                let name = &name;
                let proc_root = proc_root.path();
                scope.spawn(move || {
                    barrier.wait();
                    send.send(InstanceGuard::acquire_at(name.as_bytes(), proc_root))
                        .unwrap();
                });
            }
        });
        drop(send);
        let results: Vec<_> = receive.into_iter().collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        for error in results.iter().filter_map(|result| result.as_ref().err()) {
            assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        }
        drop(results);
        assert!(InstanceGuard::acquire_at(name.as_bytes(), proc_root.path()).is_ok());
    }

    #[test]
    fn legacy_daemon_is_rejected_and_identified_by_pid() {
        let name = socket_name();
        let directory = tempfile::tempdir().unwrap();
        let proc_root = directory.path().join("proc");
        let pid = std::process::id() + 1;
        let legacy = proc_root.join(pid.to_string());
        fs::create_dir_all(&legacy).unwrap();
        fs::write(legacy.join("comm"), "cobbled\n").unwrap();
        let old_executable = directory.path().join("old-cobbled");
        fs::write(&old_executable, b"old executable").unwrap();
        symlink(&old_executable, legacy.join("exe")).unwrap();

        let error = InstanceGuard::acquire_at(name.as_bytes(), &proc_root)
            .err()
            .expect("legacy daemon must prevent startup");
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        assert!(error.to_string().contains(&format!("PID {pid}")));

        fs::remove_dir_all(legacy).unwrap();
        assert!(InstanceGuard::acquire_at(name.as_bytes(), &proc_root).is_ok());
    }

    #[test]
    fn own_process_guarded_contenders_and_exited_processes_do_not_block_startup() {
        let name = socket_name();
        let directory = tempfile::tempdir().unwrap();
        let own_pid = std::process::id();
        for pid in [own_pid, own_pid + 1, own_pid + 2] {
            let process = directory.path().join(pid.to_string());
            fs::create_dir(&process).unwrap();
            fs::write(process.join("comm"), "cobbled\n").unwrap();
            if pid != own_pid + 2 {
                symlink("/proc/self/exe", process.join("exe")).unwrap();
            }
        }
        assert!(InstanceGuard::acquire_at(name.as_bytes(), directory.path()).is_ok());
    }

    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn guard_spans_dbus_sessions_and_is_released_after_sigkill() {
        let name = socket_name();
        let proc_root = tempfile::tempdir().unwrap();
        let parent_name = socket_name();
        let parent_guard =
            InstanceGuard::acquire_at(parent_name.as_bytes(), proc_root.path()).unwrap();
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "instance::tests::child_instance_holder",
                    "--ignored",
                    "--nocapture",
                ])
                .env("COBBLED_TEST_SOCKET", &name)
                .env("COBBLED_TEST_PROC", proc_root.path())
                .env("DBUS_SESSION_BUS_ADDRESS", "unix:path=/different-session")
                .env("XDG_RUNTIME_DIR", proc_root.path())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let output = child.0.stdout.take().unwrap();
        let (ready, started) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in io::BufReader::new(output).lines() {
                if line.unwrap().contains("INSTANCE_READY") {
                    let _ = ready.send(());
                }
            }
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // An exec'd helper must not retain its parent's guard descriptor.
        drop(parent_guard);
        assert!(InstanceGuard::acquire_at(parent_name.as_bytes(), proc_root.path()).is_ok());
        let error = InstanceGuard::acquire_at(name.as_bytes(), proc_root.path())
            .err()
            .expect("another process owns the guard");
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);

        child.0.kill().unwrap();
        child.0.wait().unwrap();
        reader.join().unwrap();
        assert!(InstanceGuard::acquire_at(name.as_bytes(), proc_root.path()).is_ok());
    }

    #[test]
    #[ignore = "subprocess helper for the cross-session instance test"]
    fn child_instance_holder() {
        let name = std::env::var("COBBLED_TEST_SOCKET").unwrap();
        let proc_root = std::env::var_os("COBBLED_TEST_PROC").unwrap();
        let _guard = InstanceGuard::acquire_at(name.as_bytes(), Path::new(&proc_root)).unwrap();
        println!("INSTANCE_READY");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
    }
}
