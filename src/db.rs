//! Storage of accepted reports.
//!
//! One SQLite file, opened once and shared. Reports arrive a handful per
//! install per run, so a single connection behind a mutex is ample; every use
//! of it happens on a blocking thread so the reactor is never held up by the
//! disk.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::event::Event;

/// Reports kept per install per rolling 24 hours. The install id is the only
/// identity in a report, so it is also the only handle on a client that
/// reports in a loop.
const MAX_PER_INSTALL_PER_DAY: i64 = 50;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS event (
    id          INTEGER PRIMARY KEY,
    received_at INTEGER NOT NULL,
    install     TEXT    NOT NULL,
    event       TEXT    NOT NULL,
    body        TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS event_received_at ON event (received_at);
CREATE INDEX IF NOT EXISTS event_install ON event (install, received_at);
";

/// Columns every report has, whatever mayara sends.
const FIXED_COLUMNS: [&str; 5] = ["id", "received_at", "install", "event", "body"];

/// The optional fields of a report, each in a column named after the field it
/// holds. This is the one place a field is named: a column listed here is
/// created on the next start and filled from the bodies already stored, and a
/// column no longer listed is dropped. Every report is kept verbatim in
/// `body`, so nothing is lost either way.
const REPORT_COLUMNS: [(&str, &str); 12] = [
    ("version", "TEXT"),
    ("os", "TEXT"),
    ("arch", "TEXT"),
    ("deployment", "TEXT"),
    ("brand", "TEXT"),
    ("model", "TEXT"),
    ("build", "TEXT"),
    ("control", "TEXT"),
    ("radars", "INTEGER"),
    ("dual_range", "INTEGER"),
    ("transmit_hours", "INTEGER"),
    ("secs_to_first_spoke", "INTEGER"),
];

#[derive(Clone)]
pub(crate) struct Db {
    connection: Arc<Mutex<Connection>>,
}

/// A stored report, as the public API returns it.
#[derive(Debug, Serialize)]
pub(crate) struct StoredEvent {
    pub id: i64,
    pub received_at: String,
    pub report: serde_json::Value,
}

impl Db {
    pub(crate) fn open(path: &Path) -> Result<Db> {
        let connection = Connection::open(path)
            .with_context(|| format!("cannot open database '{}'", path.display()))?;
        Db::prepare(connection)
    }

    #[cfg(test)]
    pub(crate) fn in_memory() -> Result<Db> {
        Db::prepare(Connection::open_in_memory()?)
    }

    fn prepare(connection: Connection) -> Result<Db> {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "busy_timeout", 5000)?;
        connection
            .execute_batch(SCHEMA)
            .context("cannot create database schema")?;
        migrate(&connection).context("cannot bring the database up to date")?;
        Ok(Db {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Run `f` against the connection on the calling thread.
    #[cfg(test)]
    pub(crate) fn with<T>(&self, f: impl FnOnce(&Connection) -> T) -> T {
        let connection = self.connection.lock().expect("database mutex poisoned");
        f(&connection)
    }

    /// Run `f` against the connection on a blocking thread.
    pub(crate) async fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let connection = connection.lock().expect("database mutex poisoned");
            f(&connection)
        })
        .await
        .context("database task failed")?
        .context("database query failed")
    }
}

/// Reconcile the table with the fields this collector knows about. A column
/// carries the report field of the same name, so one that has just been added
/// can be filled from the bodies already stored, and one that has gone out of
/// use can be dropped: the report itself is never touched.
fn migrate(connection: &Connection) -> rusqlite::Result<()> {
    let present: Vec<String> = {
        let mut statement = connection.prepare("SELECT name FROM pragma_table_info('event')")?;
        let rows = statement.query_map([], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };

    // Every name below is either one of this module's own literals or a
    // column name read back from the table this module created.
    for (column, kind) in REPORT_COLUMNS {
        if present.iter().any(|name| name == column) {
            continue;
        }
        connection.execute_batch(&format!(
            "ALTER TABLE event ADD COLUMN {column} {kind};
             UPDATE event SET {column} = json_extract(body, '$.{column}');"
        ))?;
    }

    let wanted = |name: &String| {
        FIXED_COLUMNS.contains(&name.as_str())
            || REPORT_COLUMNS.iter().any(|(column, _)| column == name)
    };
    for column in present.iter().filter(|name| !wanted(name)) {
        connection.execute_batch(&format!("ALTER TABLE event DROP COLUMN {column};"))?;
    }
    Ok(())
}

/// Store a report, unless this install has already filled its day. Returns
/// whether the report was stored.
pub(crate) fn insert(connection: &Connection, now: i64, event: &Event) -> rusqlite::Result<bool> {
    let today: i64 = connection.query_row(
        "SELECT COUNT(*) FROM event
         WHERE install = ?1 AND received_at >= ?2",
        params![event.install, now - 24 * 60 * 60],
        |row| row.get(0),
    )?;
    if today >= MAX_PER_INSTALL_PER_DAY {
        return Ok(false);
    }

    connection.execute(
        "INSERT INTO event (
             received_at, install, event, version, os, arch, deployment, brand, model,
             build, control, radars, dual_range, transmit_hours, secs_to_first_spoke, body
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![
            now,
            event.install,
            event.event,
            event.version,
            event.os,
            event.arch,
            event.deployment,
            event.brand,
            event.model,
            event.build,
            event.control,
            event.radars,
            event.dual_range,
            event.transmit_hours,
            event.secs_to_first_spoke,
            event.body,
        ],
    )?;
    Ok(true)
}

/// The most recent reports, newest first. Reports are anonymous by
/// construction, so they are handed out as they were received.
pub(crate) fn recent(connection: &Connection, limit: i64) -> rusqlite::Result<Vec<StoredEvent>> {
    let mut statement = connection.prepare(
        "SELECT id, datetime(received_at, 'unixepoch'), body
         FROM event ORDER BY id DESC LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit], |row| {
        let body: String = row.get(2)?;
        Ok(StoredEvent {
            id: row.get(0)?,
            received_at: row.get(1)?,
            report: serde_json::from_str(&body).unwrap_or(serde_json::Value::Null),
        })
    })?;
    rows.collect()
}

/// When the newest report arrived, for the health check.
pub(crate) fn last_received(connection: &Connection) -> rusqlite::Result<Option<String>> {
    connection
        .query_row(
            "SELECT datetime(MAX(received_at), 'unixepoch') FROM event",
            [],
            |row| row.get(0),
        )
        .optional()
        .map(Option::flatten)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event;

    fn event(install: &str, kind: &str) -> Event {
        event::parse(
            serde_json::json!({
                "install": install,
                "event": kind,
                "version": "3.10.0",
                "os": "linux",
                "brand": "Navico",
                "build": "official",
                "transmit_hours": 1234,
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap()
    }

    #[test]
    fn a_stored_report_comes_back_as_it_was_sent() {
        let db = Db::in_memory().unwrap();

        db.with(|connection| {
            assert!(insert(connection, 1_700_000_000, &event("a", "spokes")).unwrap());

            let stored = recent(connection, 10).unwrap();
            assert_eq!(stored.len(), 1);
            assert_eq!(stored[0].report["install"], "a");
            assert_eq!(stored[0].received_at, "2023-11-14 22:13:20");
        });
    }

    #[test]
    fn recent_returns_the_newest_reports_first_within_the_limit() {
        let db = Db::in_memory().unwrap();

        db.with(|connection| {
            for i in 0..5 {
                insert(connection, 1_700_000_000 + i, &event("a", "spokes")).unwrap();
            }

            let stored = recent(connection, 2).unwrap();
            assert_eq!(stored.len(), 2);
            assert!(stored[0].id > stored[1].id);
        });
    }

    #[test]
    fn one_install_cannot_fill_the_database() {
        let db = Db::in_memory().unwrap();
        let now = 1_700_000_000;

        db.with(|connection| {
            for _ in 0..MAX_PER_INSTALL_PER_DAY {
                assert!(insert(connection, now, &event("loud", "spokes")).unwrap());
            }
            assert!(!insert(connection, now, &event("loud", "spokes")).unwrap());

            // Another install is unaffected, and once the day's reports have
            // aged out the budget is back.
            assert!(insert(connection, now, &event("quiet", "spokes")).unwrap());
            assert!(insert(connection, now + 24 * 60 * 60 + 1, &event("loud", "spokes")).unwrap());
        });
    }

    fn columns(connection: &Connection) -> Vec<String> {
        let mut statement = connection
            .prepare("SELECT name FROM pragma_table_info('event')")
            .unwrap();
        let rows = statement.query_map([], |row| row.get(0)).unwrap();
        rows.collect::<rusqlite::Result<_>>().unwrap()
    }

    /// A database written by an older collector keeps its reports: the
    /// columns it never had are filled from the bodies it stored, and the
    /// columns nothing reads any more go away.
    #[test]
    fn an_older_database_is_brought_forward_from_the_bodies_it_stored() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE event (
                     id          INTEGER PRIMARY KEY,
                     received_at INTEGER NOT NULL,
                     install     TEXT    NOT NULL,
                     event       TEXT    NOT NULL,
                     version     TEXT,
                     host        TEXT,
                     features    TEXT,
                     body        TEXT    NOT NULL
                 );",
            )
            .unwrap();
        let body = serde_json::json!({
            "install": "a",
            "event": "spokes",
            "version": "3.12.3",
            "deployment": "container",
            "build": "official",
            "transmit_hours": 1234,
        })
        .to_string();
        connection
            .execute(
                "INSERT INTO event (received_at, install, event, version, features, body)
                 VALUES (1, 'a', 'spokes', '3.12.3', 'furuno,navico', ?1)",
                params![body],
            )
            .unwrap();

        let db = Db::prepare(connection).unwrap();

        db.with(|connection| {
            let names = columns(connection);
            assert!(!names.iter().any(|n| n == "host" || n == "features"));

            let (deployment, build, hours): (String, String, i64) = connection
                .query_row(
                    "SELECT deployment, build, transmit_hours FROM event",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(deployment, "container");
            assert_eq!(build, "official");
            assert_eq!(hours, 1234);
        });
    }

    /// Opening the same database twice must not try to add the columns again.
    #[test]
    fn bringing_a_database_forward_twice_changes_nothing() {
        let db = Db::in_memory().unwrap();
        let before = db.with(columns);

        db.with(|connection| migrate(connection).unwrap());

        assert_eq!(db.with(columns), before);
    }

    #[test]
    fn an_empty_database_has_no_last_report() {
        let db = Db::in_memory().unwrap();

        assert_eq!(db.with(last_received).unwrap(), None);
    }
}
