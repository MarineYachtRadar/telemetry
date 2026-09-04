//! The aggregate view of what has been collected.
//!
//! Every breakdown counts installs as well as reports: a single install that
//! restarts often would otherwise outweigh a brand a hundred boats use.

use rusqlite::{Connection, params};
use serde::Serialize;

/// Window used when a caller does not ask for one.
pub(crate) const DEFAULT_DAYS: i64 = 90;

/// Longest window a caller may ask for, in days.
pub(crate) const MAX_DAYS: i64 = 3650;

/// Distinct values reported per breakdown. Long enough for every brand, model
/// and version in use, short enough that a flood of junk cannot bloat the
/// answer.
const MAX_BUCKETS: i64 = 100;

/// Where the transmit time buckets divide, in hours. A lifetime counter runs
/// from a radar switched on once to a magnetron near the end of its life, so
/// the buckets widen as they go until they settle at a thousand hours apiece;
/// the last one is everything above the last bound.
const TRANSMIT_BOUNDS: [i64; 14] = [
    10, 50, 100, 500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
];

#[derive(Debug, Serialize)]
pub(crate) struct Stats {
    pub generated_at: String,
    pub days: i64,
    pub totals: Totals,
    pub versions: Vec<Bucket>,
    pub brands: Vec<Bucket>,
    pub models: Vec<Bucket>,
    pub builds: Vec<Bucket>,
    pub os: Vec<Bucket>,
    pub arch: Vec<Bucket>,
    pub deployments: Vec<Bucket>,
    pub events: Vec<Bucket>,
    pub controls: Vec<Bucket>,
    pub transmit_hours: Vec<Bucket>,
    pub daily: Vec<Day>,
}

#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct Totals {
    /// Reports and installs over all time.
    pub events: i64,
    pub installs: i64,
    /// Reports and installs within the requested window.
    pub events_in_window: i64,
    pub installs_in_window: i64,
    pub first_event: Option<String>,
    pub last_event: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct Bucket {
    pub key: String,
    pub installs: i64,
    pub events: i64,
}

#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct Day {
    pub day: String,
    pub installs: i64,
    pub events: i64,
}

pub(crate) fn collect(connection: &Connection, now: i64, days: i64) -> rusqlite::Result<Stats> {
    let days = days.clamp(1, MAX_DAYS);
    let since = now - days * 24 * 60 * 60;

    Ok(Stats {
        generated_at: chrono::DateTime::from_timestamp(now, 0)
            .unwrap_or_default()
            .to_rfc3339(),
        days,
        totals: totals(connection, since)?,
        versions: breakdown(connection, "version", since)?,
        brands: breakdown(connection, "brand", since)?,
        models: breakdown(connection, "model", since)?,
        builds: breakdown(connection, "build", since)?,
        os: breakdown(connection, "os", since)?,
        arch: breakdown(connection, "arch", since)?,
        deployments: breakdown(connection, "deployment", since)?,
        events: breakdown(connection, "event", since)?,
        controls: breakdown(connection, "control", since)?,
        transmit_hours: transmit_hours(connection, since)?,
        daily: daily(connection, since)?,
    })
}

fn totals(connection: &Connection, since: i64) -> rusqlite::Result<Totals> {
    let (events, installs, first_event, last_event) = connection.query_row(
        "SELECT COUNT(*), COUNT(DISTINCT install),
                datetime(MIN(received_at), 'unixepoch'),
                datetime(MAX(received_at), 'unixepoch')
         FROM event",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let (events_in_window, installs_in_window) = connection.query_row(
        "SELECT COUNT(*), COUNT(DISTINCT install) FROM event WHERE received_at >= ?1",
        params![since],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    Ok(Totals {
        events,
        installs,
        events_in_window,
        installs_in_window,
        first_event,
        last_event,
    })
}

/// Installs and reports per distinct value of one column. Rows that never had
/// a value for the column are left out rather than bucketed as "unknown":
/// `control` is only present on a control report, and counting the reports
/// that are not about a control as a control would be a lie.
fn breakdown(connection: &Connection, column: &str, since: i64) -> rusqlite::Result<Vec<Bucket>> {
    // `column` is one of this module's own literals, never caller input.
    let sql = format!(
        "SELECT {column}, COUNT(DISTINCT install), COUNT(*)
         FROM event WHERE received_at >= ?1 AND {column} IS NOT NULL
         GROUP BY {column} ORDER BY 2 DESC, 3 DESC, 1 LIMIT {MAX_BUCKETS}"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![since], |row| {
        Ok(Bucket {
            key: row.get(0)?,
            installs: row.get(1)?,
            events: row.get(2)?,
        })
    })?;
    rows.collect()
}

/// Installs and reports by how long their radar has transmitted. Exact hour
/// counts are as good as unique, so they are grouped into bands and left in
/// the order the bands run rather than sorted by size.
fn transmit_hours(connection: &Connection, since: i64) -> rusqlite::Result<Vec<Bucket>> {
    let mut band = String::from("CASE");
    for (index, bound) in TRANSMIT_BOUNDS.iter().enumerate() {
        band.push_str(&format!(" WHEN transmit_hours < {bound} THEN {index}"));
    }
    band.push_str(&format!(" ELSE {} END", TRANSMIT_BOUNDS.len()));

    let sql = format!(
        "SELECT {band}, COUNT(DISTINCT install), COUNT(*)
         FROM event WHERE received_at >= ?1 AND transmit_hours IS NOT NULL
         GROUP BY 1 ORDER BY 1"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![since], |row| {
        Ok(Bucket {
            key: transmit_band(row.get::<_, i64>(0)? as usize),
            installs: row.get(1)?,
            events: row.get(2)?,
        })
    })?;
    rows.collect()
}

fn transmit_band(index: usize) -> String {
    let from = index.checked_sub(1).map_or(0, |i| TRANSMIT_BOUNDS[i]);
    match TRANSMIT_BOUNDS.get(index) {
        Some(to) => format!("{from}\u{2013}{to} h"),
        None => format!("{from}+ h"),
    }
}

fn daily(connection: &Connection, since: i64) -> rusqlite::Result<Vec<Day>> {
    let mut statement = connection.prepare(
        "SELECT date(received_at, 'unixepoch'), COUNT(DISTINCT install), COUNT(*)
         FROM event WHERE received_at >= ?1
         GROUP BY 1 ORDER BY 1",
    )?;
    let rows = statement.query_map(params![since], |row| {
        Ok(Day {
            day: row.get(0)?,
            installs: row.get(1)?,
            events: row.get(2)?,
        })
    })?;
    rows.collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Db, insert};
    use crate::event;

    const NOW: i64 = 1_700_000_000;
    const DAY: i64 = 24 * 60 * 60;

    fn report(install: &str, brand: &str, control: Option<&str>) -> event::Event {
        with_hours(install, brand, control, None)
    }

    fn with_hours(
        install: &str,
        brand: &str,
        control: Option<&str>,
        transmit_hours: Option<i64>,
    ) -> event::Event {
        event::parse(
            serde_json::json!({
                "install": install,
                "event": if control.is_some() { "control" } else { "spokes" },
                "version": "3.10.0",
                "os": "linux",
                "brand": brand,
                "build": "official",
                "control": control,
                "transmit_hours": transmit_hours,
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap()
    }

    fn filled() -> Db {
        let db = Db::in_memory().unwrap();
        db.with(|connection| {
            insert(connection, NOW, &report("a", "Navico", None)).unwrap();
            insert(connection, NOW, &report("a", "Navico", Some("range"))).unwrap();
            insert(connection, NOW - DAY, &report("b", "Navico", None)).unwrap();
            insert(connection, NOW - 100 * DAY, &report("old", "Furuno", None)).unwrap();
        });
        db
    }

    fn stats(db: &Db, days: i64) -> Stats {
        db.with(|connection| collect(connection, NOW, days))
            .unwrap()
    }

    #[test]
    fn totals_separate_all_time_from_the_requested_window() {
        let stats = stats(&filled(), 90);

        assert_eq!(stats.totals.events, 4);
        assert_eq!(stats.totals.installs, 3);
        assert_eq!(stats.totals.events_in_window, 3);
        assert_eq!(stats.totals.installs_in_window, 2);
        assert_eq!(
            stats.totals.last_event.as_deref(),
            Some("2023-11-14 22:13:20")
        );
    }

    #[test]
    fn a_breakdown_counts_installs_not_just_reports() {
        let stats = stats(&filled(), 90);

        assert_eq!(
            stats.brands,
            vec![Bucket {
                key: "Navico".to_string(),
                installs: 2,
                events: 3,
            }]
        );
    }

    #[test]
    fn a_wider_window_reaches_the_older_reports() {
        let stats = stats(&filled(), 365);

        let brands: Vec<&str> = stats.brands.iter().map(|b| b.key.as_str()).collect();
        assert_eq!(brands, vec!["Navico", "Furuno"]);
    }

    #[test]
    fn reports_that_are_not_about_a_control_are_left_out_of_the_control_breakdown() {
        let stats = stats(&filled(), 90);

        assert_eq!(
            stats.controls,
            vec![Bucket {
                key: "range".to_string(),
                installs: 1,
                events: 1,
            }]
        );
    }

    /// Radars are grouped by how long they have transmitted, in the order
    /// the bands run, and a report with no reading is left out of them.
    #[test]
    fn transmit_time_is_grouped_into_bands_in_ascending_order() {
        let db = Db::in_memory().unwrap();
        db.with(|connection| {
            insert(connection, NOW, &with_hours("a", "Navico", None, Some(9))).unwrap();
            insert(connection, NOW, &with_hours("b", "Navico", None, Some(60))).unwrap();
            insert(connection, NOW, &with_hours("c", "Navico", None, Some(70))).unwrap();
            insert(
                connection,
                NOW,
                &with_hours("d", "Navico", None, Some(9999)),
            )
            .unwrap();
            insert(connection, NOW, &report("e", "Navico", None)).unwrap();
        });

        let stats = stats(&db, 90);
        let bands: Vec<(&str, i64)> = stats
            .transmit_hours
            .iter()
            .map(|b| (b.key.as_str(), b.installs))
            .collect();
        assert_eq!(
            bands,
            vec![
                ("0\u{2013}10 h", 1),
                ("50\u{2013}100 h", 2),
                ("9000\u{2013}10000 h", 1)
            ]
        );
    }

    #[test]
    fn daily_has_one_row_per_day_that_saw_a_report() {
        let stats = stats(&filled(), 90);

        assert_eq!(stats.daily.len(), 2);
        assert_eq!(stats.daily[0].day, "2023-11-13");
        assert_eq!(stats.daily[1].installs, 1);
        assert_eq!(stats.daily[1].events, 2);
    }

    #[test]
    fn an_empty_collector_answers_with_zeroes() {
        let db = Db::in_memory().unwrap();
        let stats = stats(&db, DEFAULT_DAYS);

        assert_eq!(stats.totals.events, 0);
        assert_eq!(stats.totals.first_event, None);
        assert!(stats.brands.is_empty());
        assert!(stats.daily.is_empty());
    }

    #[test]
    fn an_absurd_window_is_clamped_rather_than_refused() {
        assert_eq!(stats(&filled(), 0).days, 1);
        assert_eq!(stats(&filled(), i64::MAX).days, MAX_DAYS);
    }
}
