//! Validation of an incoming report.
//!
//! The collector is a public endpoint, so a report is trusted no further than
//! it has been checked: it must be a JSON object, it must be small, and every
//! field it claims must have the shape the field is stored in. Fields mayara
//! does not send are ignored, but the body is kept verbatim so a report that
//! grows a field before this collector knows about it is not lost.

use serde::Serialize;
use serde_json::Value;

/// Largest report accepted. A real report is a few hundred bytes; anything
/// near this limit is a mistake or an attempt to fill the disk.
pub(crate) const MAX_BODY: usize = 20 * 1024;

/// Longest accepted value of any single text field.
const MAX_FIELD: usize = 200;

/// A report that passed validation, split into the columns it is stored in.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Event {
    pub install: String,
    pub event: String,
    pub version: Option<String>,
    pub os: Option<String>,
    pub arch: Option<String>,
    pub deployment: Option<String>,
    pub brand: Option<String>,
    pub model: Option<String>,
    pub build: Option<String>,
    pub radars: Option<i64>,
    pub dual_range: Option<bool>,
    pub transmit_hours: Option<i64>,
    pub secs_to_first_spoke: Option<i64>,
    pub control: Option<String>,
    /// The report exactly as received.
    pub body: String,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Invalid {
    TooLarge,
    NotJson,
    NotAnObject,
    Missing(&'static str),
    BadField(&'static str),
}

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Invalid::TooLarge => write!(f, "report larger than {MAX_BODY} bytes"),
            Invalid::NotJson => write!(f, "report is not valid JSON"),
            Invalid::NotAnObject => write!(f, "report is not a JSON object"),
            Invalid::Missing(field) => write!(f, "report has no '{field}'"),
            Invalid::BadField(field) => write!(f, "report has an unusable '{field}'"),
        }
    }
}

pub(crate) fn parse(body: &[u8]) -> Result<Event, Invalid> {
    if body.len() > MAX_BODY {
        return Err(Invalid::TooLarge);
    }
    let value: Value = serde_json::from_slice(body).map_err(|_| Invalid::NotJson)?;
    let object = value.as_object().ok_or(Invalid::NotAnObject)?;

    let text = |field: &'static str| -> Result<Option<String>, Invalid> {
        match object.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if !s.is_empty() && s.len() <= MAX_FIELD => Ok(Some(s.clone())),
            Some(_) => Err(Invalid::BadField(field)),
        }
    };
    let required = |field: &'static str| -> Result<String, Invalid> {
        text(field)?.ok_or(Invalid::Missing(field))
    };
    // Every number a report carries counts something -- radars, hours,
    // seconds -- so none of them can be negative. A negative one is a report
    // that went wrong somewhere, not a radar that transmitted backwards, and
    // it would otherwise settle into the lowest bucket of a breakdown as if
    // it meant something.
    let count = |field: &'static str| -> Result<Option<i64>, Invalid> {
        match object.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(n)) => match n.as_i64() {
                Some(n) if n >= 0 => Ok(Some(n)),
                _ => Err(Invalid::BadField(field)),
            },
            Some(_) => Err(Invalid::BadField(field)),
        }
    };
    let flag = |field: &'static str| -> Result<Option<bool>, Invalid> {
        match object.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(_) => Err(Invalid::BadField(field)),
        }
    };

    Ok(Event {
        install: required("install")?,
        event: required("event")?,
        version: text("version")?,
        os: text("os")?,
        arch: text("arch")?,
        deployment: text("deployment")?,
        brand: text("brand")?,
        model: text("model")?,
        build: text("build")?,
        radars: count("radars")?,
        dual_range: flag("dual_range")?,
        transmit_hours: count("transmit_hours")?,
        secs_to_first_spoke: count("secs_to_first_spoke")?,
        control: text("control")?,
        body: String::from_utf8_lossy(body).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A report as mayara's telemetry module sends it.
    fn report() -> String {
        serde_json::json!({
            "install": "11111111-2222-3333-4444-555555555555",
            "version": "3.10.0",
            "os": "linux",
            "arch": "aarch64",
            "deployment": "standalone",
            "event": "spokes",
            "brand": "Navico",
            "model": "HALO",
            "build": "official",
            "radars": 2,
            "dual_range": true,
            "transmit_hours": 1234,
            "secs_to_first_spoke": 12
        })
        .to_string()
    }

    #[test]
    fn a_mayara_report_keeps_every_field_it_sent() {
        let event = parse(report().as_bytes()).unwrap();

        assert_eq!(event.install, "11111111-2222-3333-4444-555555555555");
        assert_eq!(event.event, "spokes");
        assert_eq!(event.version.as_deref(), Some("3.10.0"));
        assert_eq!(event.deployment.as_deref(), Some("standalone"));
        assert_eq!(event.brand.as_deref(), Some("Navico"));
        assert_eq!(event.model.as_deref(), Some("HALO"));
        assert_eq!(event.build.as_deref(), Some("official"));
        assert_eq!(event.radars, Some(2));
        assert_eq!(event.dual_range, Some(true));
        assert_eq!(event.transmit_hours, Some(1234));
        assert_eq!(event.secs_to_first_spoke, Some(12));
        assert_eq!(event.control, None);
        assert_eq!(event.body, report());
    }

    /// A report from a mayara older than the fields this collector knows
    /// about is still a report; the fields it does not carry are simply
    /// absent.
    #[test]
    fn a_report_that_omits_the_optional_fields_is_still_accepted() {
        let event = parse(br#"{"install":"i","event":"spokes","brand":"Navico"}"#).unwrap();

        assert_eq!(event.brand.as_deref(), Some("Navico"));
        assert_eq!(event.build, None);
        assert_eq!(event.transmit_hours, None);
        assert_eq!(event.deployment, None);
    }

    #[test]
    fn a_report_without_install_or_event_is_refused() {
        assert_eq!(
            parse(br#"{"event":"spokes"}"#),
            Err(Invalid::Missing("install"))
        );
        assert_eq!(parse(br#"{"install":"i"}"#), Err(Invalid::Missing("event")));
    }

    #[test]
    fn a_body_that_is_not_a_json_object_is_refused() {
        assert_eq!(parse(b"not json"), Err(Invalid::NotJson));
        assert_eq!(parse(b"[1,2,3]"), Err(Invalid::NotAnObject));
        assert_eq!(parse(b"\"install\""), Err(Invalid::NotAnObject));
    }

    #[test]
    fn a_field_of_the_wrong_type_is_refused() {
        assert_eq!(
            parse(br#"{"install":"i","event":"e","radars":"many"}"#),
            Err(Invalid::BadField("radars"))
        );
        assert_eq!(
            parse(br#"{"install":"i","event":"e","dual_range":"yes"}"#),
            Err(Invalid::BadField("dual_range"))
        );
        assert_eq!(
            parse(br#"{"install":"i","event":"e","version":7}"#),
            Err(Invalid::BadField("version"))
        );
        assert_eq!(
            parse(br#"{"install":"i","event":"e","transmit_hours":"lots"}"#),
            Err(Invalid::BadField("transmit_hours"))
        );
        assert_eq!(
            parse(br#"{"install":"i","event":"e","build":["official"]}"#),
            Err(Invalid::BadField("build"))
        );
    }

    /// Every number in a report counts something, so a negative one is not a
    /// small reading -- it is a broken one, and must not be bucketed as if it
    /// were a radar that has barely transmitted.
    #[test]
    fn a_negative_count_is_refused() {
        assert_eq!(
            parse(br#"{"install":"i","event":"e","transmit_hours":-1}"#),
            Err(Invalid::BadField("transmit_hours"))
        );
        assert_eq!(
            parse(br#"{"install":"i","event":"e","radars":-1}"#),
            Err(Invalid::BadField("radars"))
        );
        assert_eq!(
            parse(br#"{"install":"i","event":"e","secs_to_first_spoke":-1}"#),
            Err(Invalid::BadField("secs_to_first_spoke"))
        );

        let event = parse(br#"{"install":"i","event":"e","transmit_hours":0}"#).unwrap();
        assert_eq!(event.transmit_hours, Some(0));
    }

    #[test]
    fn an_oversized_field_or_body_is_refused() {
        let long = "x".repeat(MAX_FIELD + 1);
        let body = format!(r#"{{"install":"i","event":"e","model":"{long}"}}"#);
        assert_eq!(parse(body.as_bytes()), Err(Invalid::BadField("model")));

        let padding = "x".repeat(MAX_BODY);
        let body = format!(r#"{{"install":"i","event":"e","model":"{padding}"}}"#);
        assert_eq!(parse(body.as_bytes()), Err(Invalid::TooLarge));
    }

    #[test]
    fn unknown_fields_are_ignored_but_kept_in_the_stored_body() {
        let body = br#"{"install":"i","event":"e","something_new":{"deep":[1]}}"#;
        let event = parse(body).unwrap();

        assert_eq!(event.install, "i");
        assert_eq!(event.body, String::from_utf8_lossy(body));
    }
}
