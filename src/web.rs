//! The HTTP surface: one endpoint to report to, two to read from, and the
//! page that reads them.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use crate::db::{self, Db};
use crate::event::{self, Invalid, MAX_BODY};
use crate::ratelimit::RateLimit;
use crate::stats::{self, DEFAULT_DAYS};

/// Reports returned by `/v1/events` when the caller does not ask for a count.
const DEFAULT_EVENT_LIMIT: i64 = 100;

/// Most reports `/v1/events` will return in one answer.
const MAX_EVENT_LIMIT: i64 = 1000;

const INDEX: &str = include_str!("../static/index.html");

#[derive(Clone)]
pub(crate) struct AppState {
    pub db: Db,
    pub limit: Arc<RateLimit>,
}

pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route(
            "/v1/event",
            post(post_event).layer(DefaultBodyLimit::max(MAX_BODY)),
        )
        .route("/v1/stats", get(get_stats))
        .route("/v1/events", get(get_events))
        .fallback(not_found)
        .with_state(state)
}

async fn index() -> impl IntoResponse {
    Html(INDEX)
}

async fn health(State(state): State<AppState>) -> Response {
    match state.db.call(db::last_received).await {
        Ok(last_event) => public(Json(serde_json::json!({
            "status": "ok",
            "last_event": last_event,
        })))
        .into_response(),
        Err(e) => {
            warn!("Health check failed: {e:#}");
            error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable")
        }
    }
}

async fn post_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    connect: Option<Extension<ConnectInfo<SocketAddr>>>,
    body: Bytes,
) -> Response {
    let address = client_address(&headers, connect.map(|Extension(ConnectInfo(peer))| peer));
    if !state.limit.allow(address) {
        debug!("Report from {address} refused: too many reports");
        return error(StatusCode::TOO_MANY_REQUESTS, "too many reports");
    }

    let event = match event::parse(&body) {
        Ok(event) => event,
        Err(e) => {
            debug!("Report from {address} refused: {e}");
            let status = match e {
                Invalid::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
                _ => StatusCode::BAD_REQUEST,
            };
            return error(status, &e.to_string());
        }
    };

    let now = crate::now();
    let install = event.install.clone();
    let kind = event.event.clone();
    match state.db.call(move |c| db::insert(c, now, &event)).await {
        Ok(true) => {
            debug!("Report '{kind}' from install {install} stored");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => {
            debug!("Report '{kind}' from install {install} refused: install over its daily cap");
            error(StatusCode::TOO_MANY_REQUESTS, "too many reports")
        }
        Err(e) => {
            warn!("Report '{kind}' from install {install} not stored: {e:#}");
            error(StatusCode::INTERNAL_SERVER_ERROR, "cannot store report")
        }
    }
}

#[derive(Deserialize)]
struct StatsQuery {
    days: Option<i64>,
}

async fn get_stats(State(state): State<AppState>, Query(query): Query<StatsQuery>) -> Response {
    let days = query.days.unwrap_or(DEFAULT_DAYS);
    let now = crate::now();
    match state.db.call(move |c| stats::collect(c, now, days)).await {
        Ok(stats) => public(Json(stats)).into_response(),
        Err(e) => {
            warn!("Cannot collect stats: {e:#}");
            error(StatusCode::INTERNAL_SERVER_ERROR, "cannot collect stats")
        }
    }
}

#[derive(Deserialize)]
struct EventsQuery {
    limit: Option<i64>,
}

async fn get_events(State(state): State<AppState>, Query(query): Query<EventsQuery>) -> Response {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_EVENT_LIMIT)
        .clamp(1, MAX_EVENT_LIMIT);
    match state.db.call(move |c| db::recent(c, limit)).await {
        Ok(events) => public(Json(events)).into_response(),
        Err(e) => {
            warn!("Cannot read reports: {e:#}");
            error(StatusCode::INTERNAL_SERVER_ERROR, "cannot read reports")
        }
    }
}

async fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "no such endpoint")
}

#[derive(Serialize)]
struct ApiError<'a> {
    error: &'a str,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, public(Json(ApiError { error: message }))).into_response()
}

/// The collected data is anonymous and meant to be read by anyone, so every
/// answer is readable from a browser on any origin.
fn public<T: IntoResponse>(body: T) -> impl IntoResponse {
    ([(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")], body)
}

/// The address the reverse proxy accepted the report from.
///
/// nginx appends the peer it saw to `X-Forwarded-For`, so the last entry is
/// the only one this server did not take on the sender's word. A report that
/// arrives without the header is counted under the connecting peer, and one
/// with neither shares a single bucket.
fn client_address(headers: &HeaderMap, peer: Option<SocketAddr>) -> IpAddr {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit(',').next())
        .and_then(|value| value.trim().parse().ok())
        .or_else(|| peer.map(|peer| peer.ip()))
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

pub(crate) fn announce(listen: SocketAddr, database: &std::path::Path) {
    info!("Listening on http://{listen}");
    info!("Storing reports in {}", database.display());
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::time::Duration;
    use tower::ServiceExt;

    fn app() -> Router {
        router(AppState {
            db: Db::in_memory().unwrap(),
            limit: Arc::new(RateLimit::new(100, Duration::from_secs(60))),
        })
    }

    async fn send(app: &Router, request: Request<Body>) -> (StatusCode, String) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn report(body: &str) -> Request<Body> {
        Request::post("/v1/event")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn valid() -> &'static str {
        r#"{"install":"11111111-2222-3333-4444-555555555555","event":"spokes","brand":"Navico","version":"3.10.0"}"#
    }

    #[tokio::test]
    async fn a_report_is_accepted_and_shows_up_in_the_stats() {
        let app = app();

        let (status, _) = send(&app, report(valid())).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, body) =
            send(&app, Request::get("/v1/stats").body(Body::empty()).unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let stats: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(stats["totals"]["installs"], 1);
        assert_eq!(stats["brands"][0]["key"], "Navico");
    }

    #[tokio::test]
    async fn a_report_that_is_not_a_json_object_is_refused() {
        let app = app();

        assert_eq!(
            send(&app, report("garbage")).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(send(&app, report("[]")).await.0, StatusCode::BAD_REQUEST);
        assert_eq!(send(&app, report("{}")).await.0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_report_over_the_size_limit_is_refused_without_being_read() {
        let app = app();
        let body = format!(
            r#"{{"install":"i","event":"e","pad":"{}"}}"#,
            "x".repeat(MAX_BODY)
        );

        assert_eq!(
            send(&app, report(&body)).await.0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[tokio::test]
    async fn an_address_that_reports_too_often_is_turned_away() {
        let app = router(AppState {
            db: Db::in_memory().unwrap(),
            limit: Arc::new(RateLimit::new(1, Duration::from_secs(60))),
        });
        let from = |address: &str| {
            Request::post("/v1/event")
                .header("x-forwarded-for", address)
                .body(Body::from(valid().to_string()))
                .unwrap()
        };

        assert_eq!(
            send(&app, from("192.0.2.1")).await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            send(&app, from("192.0.2.1")).await.0,
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            send(&app, from("192.0.2.2")).await.0,
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn a_spoofed_forwarded_for_cannot_buy_a_fresh_budget() {
        let app = router(AppState {
            db: Db::in_memory().unwrap(),
            limit: Arc::new(RateLimit::new(1, Duration::from_secs(60))),
        });
        let from = |header: &str| {
            Request::post("/v1/event")
                .header("x-forwarded-for", header)
                .body(Body::from(valid().to_string()))
                .unwrap()
        };

        // Only the entry nginx appended -- the last one -- is believed.
        assert_eq!(
            send(&app, from("192.0.2.9")).await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            send(&app, from("10.0.0.1, 192.0.2.9")).await.0,
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[tokio::test]
    async fn stored_reports_are_readable_and_the_answers_are_cross_origin() {
        let app = app();
        send(&app, report(valid())).await;

        let response = app
            .clone()
            .oneshot(
                Request::get("/v1/events?limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .unwrap(),
            "*"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let events: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(events[0]["report"]["brand"], "Navico");
    }

    #[tokio::test]
    async fn the_ui_and_health_check_answer() {
        let app = app();

        let (status, body) = send(&app, Request::get("/").body(Body::empty()).unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("mayara"));

        let (status, body) = send(&app, Request::get("/health").body(Body::empty()).unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"status\":\"ok\""));
    }

    #[tokio::test]
    async fn an_unknown_path_answers_with_json() {
        let app = app();

        let (status, body) = send(&app, Request::get("/nope").body(Body::empty()).unwrap()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("no such endpoint"));
    }
}
