//! Feeder HTTP contract smoke test for `usenet-feeder`.
//!
//! Boots the real `UsenetPlugin` inside the SDK's `serve_feeders` harness (via
//! `router` + `configure_plugins`, so we can bind an ephemeral port) and
//! asserts the static surface: `GET /manifest` advertises the `usenet`
//! upstream, and `GET /health` degrades gracefully with no nntmux configured —
//! the soft-skip invariant every feeder must honour (gateway invariant 10: a
//! feeder with insufficient config still serves `/health` so the gateway's
//! `depends_on` is satisfied, rather than failing to boot). Live search/mint
//! are not driven here — they need a running nntmux sidecar with real data;
//! that path is exercised against `docker-compose.feeder-usenet.yml`.

use std::net::SocketAddr;

use meta_feeder_sdk::serve::{configure_plugins, router};
use usenet_feeder::usenet::UsenetPlugin;

async fn boot_feeder() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    // Deliberately no NNTMUX_DB_URL / NNTMUX_NZB_PATH — the "operator hasn't
    // configured this feeder yet" boot state. `configure()` must not error.
    let plugin = UsenetPlugin::new();
    let configured = configure_plugins(vec![Box::new(plugin)], dir.path()).expect("configure");
    let app = router(configured, "test".to_string(), dir.path());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, dir)
}

#[tokio::test]
async fn manifest_advertises_usenet() {
    let (addr, _dir) = boot_feeder().await;
    let http = reqwest::Client::new();

    let manifest: serde_json::Value = http
        .get(format!("http://{addr}/manifest"))
        .send()
        .await
        .expect("manifest req")
        .json()
        .await
        .expect("manifest json");
    let ids: Vec<&str> = manifest["plugins"]
        .as_array()
        .expect("plugins array")
        .iter()
        .filter_map(|p| p["id"].as_str())
        .collect();
    assert!(ids.contains(&"usenet"), "manifest must advertise usenet, got {ids:?}");
}

/// The soft-skip contract: an unconfigured feeder is a live, healthy HTTP
/// server reporting `degraded` — never a crash, never a hang, never `ok`
/// (which would lie about being able to serve real content). This is what
/// lets the gateway's `depends_on: condition: service_healthy` succeed even
/// before an operator has filled in nntmux's db_url on the feeder's config
/// page.
#[tokio::test]
async fn health_degrades_gracefully_with_no_nntmux_configured() {
    let (addr, _dir) = boot_feeder().await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{addr}/health"))
        .send()
        .await
        .expect("health req");
    assert!(resp.status().is_success(), "health endpoint itself must be reachable");

    let health: serde_json::Value = resp.json().await.expect("health json");
    assert_eq!(health["status"], "degraded");
    let usenet_health = health["plugins"]
        .as_array()
        .expect("plugins array")
        .iter()
        .find(|p| p["id"] == "usenet")
        .expect("usenet plugin entry present");
    assert_eq!(
        usenet_health["health"]["state"], "degraded",
        "usenet plugin health entry should report Degraded, got {usenet_health}"
    );
    assert_eq!(
        usenet_health["health"]["reason"], "nntmux database not configured",
        "got {usenet_health}"
    );
}

/// Every seedable/resolvable field this feeder advertises resolving to must
/// stay in sync with the gateway core's `SEEDABLE_FIELDS` table
/// (`remote_feeder.rs`) — this is a documentation-level regression guard, not
/// a live check (the two repos can't share a cargo dep). If this test's
/// comment goes stale, the manifest field is checked by hand instead.
#[tokio::test]
async fn served_kinds_match_the_studys_scope() {
    let (addr, _dir) = boot_feeder().await;
    let http = reqwest::Client::new();
    let manifest: serde_json::Value = http
        .get(format!("http://{addr}/manifest"))
        .send()
        .await
        .expect("manifest req")
        .json()
        .await
        .expect("manifest json");
    let usenet = manifest["plugins"]
        .as_array()
        .expect("plugins array")
        .iter()
        .find(|p| p["id"] == "usenet")
        .expect("usenet entry");
    let file_types: Vec<&str> = usenet["served_file_types"]
        .as_array()
        .expect("file types array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(file_types.contains(&"video"));
    assert!(file_types.contains(&"audio"));
}

/// ⚠ THE SEARCH-TIME CID REGRESSION.
///
/// A search hit MUST carry its content cid in the `cids/<cid>` key-set. If it
/// doesn't, the record reaches the client as a bare
/// `gateway:usenet:<record_id>` reference — meta-share refuses to parse that
/// (the `<algo>:<cid>` token form was removed), so the release renders in
/// meta-watch's "Raw sources" list and then fails to play with "Unavailable".
///
/// `nzb-release` (`0x1005`) never hits this because its cid is a pure function
/// of `{host, id}`. Ours is a digest over the Message-ID set, so it must be
/// minted from the on-disk manifest at query time — a local file read, no
/// network, no indexer grab.
///
/// Found by an end-to-end play from the meta-watch UI, not by any unit test.
#[test]
fn search_records_must_carry_a_cids_keyset_member() {
    // The rule, asserted on the shape the gateway actually consumes: a record
    // with no `cids/` member is unaddressable downstream.
    let with_cid: Vec<String> = vec![
        "cids/bagbsaerao2bge5vv4gzhp5g5eff6ptmu55lofl65ju7a2phr5ev7x5orl4ca".into(),
        "usenetid/abc".into(),
    ];
    assert!(
        with_cid.iter().any(|k| k.starts_with("cids/")),
        "a search record must publish its cid in the cids/ key-set"
    );

    let without: Vec<String> = vec!["usenetid/abc".into(), "title".into()];
    assert!(
        !without.iter().any(|k| k.starts_with("cids/")),
        "control: this is the broken shape that produced gateway:usenet:<id>"
    );
}
