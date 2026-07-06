mod app_db;
mod queries;
mod schema;
mod time;
mod types;

pub use app_db::AppDb;
pub use queries::*;
pub use time::*;
pub use types::*;

use anyhow::Context;
use rusqlite::Connection;
use std::path::Path;

/// Open the database for reading only (used by the GUI and other consumers that
/// don't own the schema). Sets `busy_timeout` so reads don't fail with
/// `SQLITE_BUSY` when the daemon is concurrently writing.
pub fn connect_readonly(path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("open app DB at {}", path.display()))?;
    schema::apply_read_pragmas(&conn)?;
    Ok(conn)
}
