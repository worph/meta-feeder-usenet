//! Redeeming an external `nzb-release` (`0x1005`) locator through `POST /compute`.
//!
//! Boots the real `UsenetPlugin` in the SDK router with indexer keys in its
//! `config.json` (the config-page shape) and a wiremock Newznab indexer, then
//! drives `/compute` with a locator cid exactly as the gateway's redeem route
//! does. No nntmux: redeeming must work in a grab-only deployment.

use base64::Engine as _;
use meta_feeder_sdk::hash::{
    compute_ipfs_cid, compute_nzb_posting_cid, compute_nzb_release_cid,
};
use meta_feeder_sdk::serve::{configure_plugins, router};
use meta_feeder_sdk::{ComputeRequest, ComputeResponse, HashKindDto, RedeemsResponse};
use usenet_feeder::usenet::UsenetPlugin;
use wiremock::matchers::{header_exists, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY: &str = "indexer-key-1";
const RELEASE_ID: &str = "0123456789abcdef";

const NZB: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head>
    <meta type="title">Some.Release.1080p</meta>
    <meta type="x-grabbed-by">account-4242</meta>
    <meta type="password">hunter2</meta>
  </head>
  <file poster="x@y.com" date="1" subject="Some.Release [1/1] &quot;a.mkv&quot; yEnc">
    <groups><group>alt.binaries.teevee</group></groups>
    <segments>
      <segment bytes="100" number="1">aaa@example.com</segment>
      <segment bytes="100" number="2">bbb@example.com</segment>
    </segments>
  </file>
</nzb>"#;

/// The indexer's scheme-less API base as it appears inside a locator.
fn authority(server: &MockServer) -> String {
    server.uri().trim_start_matches("http://").to_string()
}

async fn boot(indexers: serde_json::Value) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = dir.path().join("gateway").join("usenet");
    std::fs::create_dir_all(&cache).expect("mkdir");
    std::fs::write(
        cache.join("config.json"),
        serde_json::json!({ "indexers": indexers }).to_string(),
    )
    .expect("write config");

    let plugin = UsenetPlugin::new().with_plain_http_grabs();
    let configured = configure_plugins(vec![Box::new(plugin)], dir.path()).expect("configure");
    let app = router(configured, "test".to_string(), dir.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}"), dir)
}

async fn boot_with_key_for(server: &MockServer) -> (String, tempfile::TempDir) {
    boot(serde_json::json!([{ "host": authority(server), "api_key": KEY }])).await
}

async fn compute(base: &str, record_id: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/compute"))
        .json(&ComputeRequest {
            upstream_id: "usenet".into(),
            record_id: record_id.into(),
        })
        .send()
        .await
        .expect("POST /compute")
}

fn grab_mock() -> wiremock::MockBuilder {
    Mock::given(method("GET"))
        .and(path("/api"))
        .and(query_param("t", "get"))
        .and(query_param("id", RELEASE_ID))
        .and(query_param("apikey", KEY))
}

#[tokio::test]
async fn a_release_locator_redeems_to_the_nzb_and_its_posting_cid() {
    let indexer = MockServer::start().await;
    grab_mock()
        .and(header_exists("user-agent"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NZB))
        .expect(1)
        .mount(&indexer)
        .await;
    let (base, _dir) = boot_with_key_for(&indexer).await;
    let cid = compute_nzb_release_cid(&authority(&indexer), RELEASE_ID).unwrap();

    let resp = compute(&base, &cid).await;
    assert_eq!(resp.status(), 200, "{:?}", resp.text().await);
    let body: ComputeResponse = resp.json().await.expect("ComputeResponse");
    assert_eq!(body.outcomes.len(), 1);
    let o = &body.outcomes[0];
    assert_eq!(o.hash_kind, HashKindDto::Sha2_256);
    assert_eq!(o.file_extension.as_deref(), Some("nzb"));

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(o.bytes_b64.as_deref().expect("bytes"))
        .expect("base64");
    assert_eq!(o.hash, compute_ipfs_cid(&bytes), "hash is the cid of the returned bytes");

    let text = String::from_utf8(bytes).expect("utf-8");
    assert!(text.contains(r#"<meta type="password">hunter2</meta>"#), "password kept: {text}");
    assert!(!text.contains("account-4242"), "head metadata stripped: {text}");
    assert!(text.contains("aaa@example.com") && text.contains("bbb@example.com"));

    let record = o.record.as_ref().expect("fields to merge onto the release record");
    let posting = compute_nzb_posting_cid(&["aaa@example.com", "bbb@example.com"]);
    assert_eq!(record.fields.get(&format!("cids/{posting}")).map(String::as_str), Some("true"));
    assert_eq!(record.fields.get("segmentCount").map(String::as_str), Some("2"));
    assert!(
        !record.fields.keys().any(|k| k == &format!("cids/{}", o.hash)),
        "the manifest's own cid must never become a cids/ member of the release"
    );
}

#[tokio::test]
async fn a_host_without_a_key_is_not_ours_and_spends_nothing() {
    let indexer = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NZB))
        .expect(0)
        .mount(&indexer)
        .await;
    let (base, _dir) =
        boot(serde_json::json!([{ "host": "other.example", "api_key": KEY }])).await;
    let cid = compute_nzb_release_cid(&authority(&indexer), RELEASE_ID).unwrap();

    assert_eq!(compute(&base, &cid).await.status(), 404);
}

#[tokio::test]
async fn invalid_user_agent_error_is_permanent() {
    let indexer = MockServer::start().await;
    grab_mock()
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?><error code="109" description="Invalid User Agent"/>"#,
        ))
        .mount(&indexer)
        .await;
    let (base, _dir) = boot_with_key_for(&indexer).await;
    let cid = compute_nzb_release_cid(&authority(&indexer), RELEASE_ID).unwrap();

    let resp = compute(&base, &cid).await;
    assert_eq!(resp.status(), 422);
    assert!(resp.text().await.unwrap().contains("Invalid User Agent"));
}

#[tokio::test]
async fn http_429_is_rate_limited() {
    let indexer = MockServer::start().await;
    grab_mock()
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "120"))
        .mount(&indexer)
        .await;
    let (base, _dir) = boot_with_key_for(&indexer).await;
    let cid = compute_nzb_release_cid(&authority(&indexer), RELEASE_ID).unwrap();

    assert_eq!(compute(&base, &cid).await.status(), 429);
}

#[tokio::test]
async fn download_limit_error_code_is_rate_limited() {
    let indexer = MockServer::start().await;
    grab_mock()
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<error code="501" description="Download limit reached"/>"#,
        ))
        .mount(&indexer)
        .await;
    let (base, _dir) = boot_with_key_for(&indexer).await;
    let cid = compute_nzb_release_cid(&authority(&indexer), RELEASE_ID).unwrap();

    assert_eq!(compute(&base, &cid).await.status(), 429);
}

#[tokio::test]
async fn an_html_page_is_a_permanent_error() {
    let indexer = MockServer::start().await;
    grab_mock()
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("<!DOCTYPE html><html><body>Please log in</body></html>"),
        )
        .mount(&indexer)
        .await;
    let (base, _dir) = boot_with_key_for(&indexer).await;
    let cid = compute_nzb_release_cid(&authority(&indexer), RELEASE_ID).unwrap();

    let resp = compute(&base, &cid).await;
    assert_eq!(resp.status(), 422);
    assert!(resp.text().await.unwrap().contains("HTML"));
}

/// Keys configured → the plugin claims `nzb-release` for exactly those hosts, on
/// both `/redeems` and `/manifest`, and reports healthy without nntmux.
#[tokio::test]
async fn keys_are_advertised_as_redeem_claims() {
    let (base, _dir) = boot(serde_json::json!([
        { "host": "https://API.nzbgeek.info/", "api_key": "k1" },
        { "host": "api.nzb.life", "api_key": "k2" },
        { "host": "blank-key.example", "api_key": "" }
    ]))
    .await;

    let r: RedeemsResponse = reqwest::get(format!("{base}/redeems"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let claims = serde_json::to_value(&r.redeems["usenet"]).unwrap();
    assert_eq!(
        claims,
        serde_json::json!([{
            "codec": "nzb-release",
            "field": "manifest",
            "hosts": ["api.nzb.life", "api.nzbgeek.info"],
            "sources": []
        }])
    );

    let m: serde_json::Value = reqwest::get(format!("{base}/manifest"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(m["plugins"][0]["package"], "meta-feeder-usenet");
    assert_eq!(m["plugins"][0]["redeems"], claims);

    let h: serde_json::Value = reqwest::get(format!("{base}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(h["status"], "ok", "{h}");
}
