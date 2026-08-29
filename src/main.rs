//! Collector for the anonymous "it works" reports mayara sends.
//!
//! It listens on a loopback port behind nginx, which terminates TLS for
//! telemetry.keversoft.com and forwards to it. What it stores is what mayara
//! sends: a random install id, a version, a platform, a radar brand and model.
//! No address of the sender is ever written to disk.

mod db;
mod event;
mod ratelimit;
mod stats;
mod web;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use log::info;

use crate::db::Db;
use crate::ratelimit::RateLimit;
use crate::web::AppState;

/// Window a client's report budget is measured over.
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60 * 60);

#[derive(Parser, Debug)]
#[command(version, about = "Collector for anonymous mayara usage reports")]
struct Cli {
    /// Address to listen on. Keep this on loopback and let nginx do TLS.
    #[arg(short, long, default_value = "127.0.0.1:8099")]
    listen: SocketAddr,

    /// SQLite database holding the reports; created if it does not exist.
    #[arg(short, long, default_value = "telemetry.db")]
    database: PathBuf,

    /// Reports accepted per client address per hour.
    #[arg(long, default_value_t = 60)]
    rate_limit: u32,
}

/// Seconds since the epoch, the form every timestamp is stored in.
pub(crate) fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Cli::parse();
    let state = AppState {
        db: Db::open(&args.database)?,
        limit: Arc::new(RateLimit::new(args.rate_limit, RATE_LIMIT_WINDOW)),
    };

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("cannot listen on {}", args.listen))?;
    web::announce(args.listen, &args.database);

    axum::serve(
        listener,
        web::router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown())
    .await
    .context("server failed")
}

async fn shutdown() {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("cannot listen for ctrl-c");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("cannot listen for SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }
    info!("Shutting down");
}
