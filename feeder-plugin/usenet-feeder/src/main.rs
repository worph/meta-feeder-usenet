//! `usenet-feeder` — single-source Usenet feeder sidecar over an nntmux
//! header-scan catalog.
//!
//! One plugin (`usenet`) served through [`meta_feeder_sdk::serve_feeders`].
//!
//! Operator config — the nntmux database URL and NZB store path, and the
//! per-host Newznab indexer keys used to redeem `nzb-release` locators — is read
//! from the persisted `config.json` (dashboard-written), with the env vars below
//! as a **first-boot seed only**: the file wins on the next restart, and there
//! is no hot reload (gateway invariant 12). Indexer keys have no env seed; set
//! them on the config page.
//!
//! Env (seed): `NNTMUX_DB_URL`, `NNTMUX_NZB_PATH`.
//! Env (infra): `META_FEEDER_HTTP_LISTEN` (default `0.0.0.0:8080`),
//! `META_FEEDER_STATE_DIR` (default `/data/meta-feeder`), `RUST_LOG`.

use std::net::SocketAddr;

use meta_feeder_sdk::plugin::FeederPlugin;
use meta_feeder_sdk::serve_feeders;
use tracing::info;
use tracing_subscriber::EnvFilter;

use usenet_feeder::usenet::UsenetPlugin;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let listen: SocketAddr = std::env::var("META_FEEDER_HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
    let state_dir =
        std::env::var("META_FEEDER_STATE_DIR").unwrap_or_else(|_| "/data/meta-feeder".to_string());

    let plugins: Vec<Box<dyn FeederPlugin>> = vec![Box::new(UsenetPlugin::new())];

    info!(target: "meta-feeder", loaded = plugins.len(), "usenet feeder starting");

    serve_feeders(plugins, state_dir, listen).await
}
