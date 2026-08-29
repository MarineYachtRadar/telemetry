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
    id                  INTEGER PRIMARY KEY,
    received_at         INTEGER NOT NULL,
    install             TEXT    NOT NULL,
    event               TEXT    NOT NULL,
    version             TEXT,
    os                  TEXT,
    arch                TEXT,
    host                TEXT,
    brand               TEXT,
    model               TEXT,
    radars              INTEGER,
    dual_range          INTEGER,
    features            TEXT,
    secs_to_first_spoke INTEGER,
    control             TEXT,
    body                TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS event_received_at ON event (received_at);
CREATE INDEX IF NOT EXISTS event_install ON event (install, received_at);
";

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
             received_at, install, event, version, os, arch, host, brand, model,
             radars, dual_range, features, secs_to_first_spoke, control, body
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            now,
            event.install,
            event.event,
            event.version,
            event.os,
            event.arch,
            event.host,
            event.brand,
            event.model,
            event.radars,
            event.dual_range,
            event.features,
            event.secs_to_first_spoke,
            event.control,
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

    #[test]
    fn an_empty_database_has_no_last_report() {
        let db = Db::in_memory().unwrap();

        assert_eq!(db.with(last_received).unwrap(), None);
    }
}
