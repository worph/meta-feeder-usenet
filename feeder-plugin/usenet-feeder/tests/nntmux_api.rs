//! The self-scan path against a mock nntmux, over its Newznab API.
//!
//! Boots the real `UsenetPlugin` in the SDK router with `nntmux_url` +
//! `nntmux_api_key` in its `config.json` (the config-page shape) and a wiremock
//! nntmux answering `/api/v1/api?t=…` and `/getnzb`, then drives `/query`,
//! `/blob` and `/health` exactly as the gateway does.

use meta_feeder_sdk::hash::compute_nzb_posting_cid;
use meta_feeder_sdk::serve::{configure_plugins, router};
use meta_feeder_sdk::{GatewayQuery, QueryRequest, QueryResponse};
use usenet_feeder::usenet::UsenetPlugin;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY: &str = "metamesh-key";

const CAPS: &str = r#"<?xml version="1.0" encoding="UTF-8"?><caps><limits max="100" default="100"/></caps>"#;

fn item(guid: &str, title: &str, cat: u32) -> String {
    format!(
        r#"<item><title>{title}</title><guid isPermaLink="true">http://localhost/details/{guid}</guid>
<newznab:attr name="category" value="{}"/><newznab:attr name="category" value="{cat}"/>
<newznab:attr name="size" value="734003200"/>
<newznab:attr name="usenetdate" value="Fri, 02 Oct 2026 08:30:00 +0000"/></item>"#,
        cat / 1000 * 1000
    )
}

fn rss(items: &[String]) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/"><channel><title>NNTmux</title>{}</channel></rss>"#,
        items.join("")
    )
}

const NZB: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="x@y.com" date="1" subject="Ghost [1/1] &quot;a.mkv&quot; yEnc">
    <groups><group>alt.binaries.teevee</group></groups>
    <segments>
      <segment bytes="100" number="1">aaa@example.com</segment>
      <segment bytes="100" number="2">bbb@example.com</segment>
    </segments>
  </file>
</nzb>"#;

fn posting() -> String {
    compute_nzb_posting_cid(&["aaa@example.com".to_string(), "bbb@example.com".to_string()])
}

async fn boot(nntmux: &MockServer) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = dir.path().join("gateway").join("usenet");
    std::fs::create_dir_all(&cache).expect("mkdir");
    std::fs::write(
        cache.join("config.json"),
        serde_json::json!({ "nntmux_url": nntmux.uri(), "nntmux_api_key": KEY }).to_string(),
    )
    .expect("write config");

    let configured =
        configure_plugins(vec![Box::new(UsenetPlugin::new())], dir.path()).expect("configure");
    let app = router(configured, "test".to_string(), dir.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}"), dir)
}

async fn caps_ok(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/v1/api"))
        .and(query_param("t", "caps"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPS))
        .mount(server)
        .await;
}

async fn search_answers(server: &MockServer, body: String) {
    Mock::given(method("GET"))
        .and(path("/api/v1/api"))
        .and(query_param("t", "search"))
        .and(query_param("apikey", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

async fn query(base: &str, text: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/query"))
        .json(&QueryRequest {
            upstream_id: "usenet".into(),
            query: GatewayQuery::from_free_text(text),
            max_results: 10,
        })
        .send()
        .await
        .expect("POST /query")
}

async fn records(base: &str, text: &str) -> Vec<meta_feeder_sdk::DiscoveryRecord> {
    let r = query(base, text).await;
    assert!(r.status().is_success(), "query status {}", r.status());
    r.json::<QueryResponse>().await.expect("query json").records
}

/// Search hits carry their portable cid, minted from an `.nzb` downloaded ONCE:
/// a repeat search and the core's `/blob` fetch are served from the cache.
#[tokio::test]
async fn hits_carry_the_posting_cid_and_each_nzb_is_downloaded_once() {
    let server = MockServer::start().await;
    caps_ok(&server).await;
    search_answers(
        &server,
        rss(&[
            item("abc111", "Celebrity.Ghost.Stories.S05E08.720p.HDTV.x264-DHD", 5040),
            item("abc222", "Ghost.Not.Collated.Yet", 2040),
        ]),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/getnzb"))
        .and(query_param("id", "abc111.nzb"))
        .and(query_param("r", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_string(NZB))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/getnzb"))
        .and(query_param("id", "abc222.nzb"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let (base, _dir) = boot(&server).await;
    let recs = records(&base, "ghost").await;
    assert_eq!(recs.len(), 1, "a release without a usable .nzb is dropped, got {recs:?}");
    let r = &recs[0];
    assert_eq!(r.record_id, "abc111");
    assert_eq!(r.fields.get(&format!("cids/{}", posting())).map(String::as_str), Some("true"));
    assert_eq!(r.fields["manifest_url"], "/blob/usenet/abc111.nzb");
    assert_eq!(r.fields["contentKind"], "episode");
    assert_eq!(r.fields["fileType"], "video");
    assert_eq!(r.fields["sizeByte"], "734003200");
    assert_eq!(r.fields["publishedAt"], "1790929800");

    // Repeat search: no second download (the `.expect(1)` above is verified on drop).
    assert_eq!(records(&base, "ghost").await.len(), 1);

    // The core seeds the manifest from /blob — served from the cache.
    let blob = reqwest::get(format!("{base}/blob/usenet/abc111.nzb")).await.expect("blob");
    assert!(blob.status().is_success());
    assert_eq!(blob.text().await.unwrap(), NZB);
}

/// nntmux's daily download cap (`<error code="501">`, answered as HTTP 200):
/// stop asking for the rest of this query instead of burning a request per hit.
#[tokio::test]
async fn a_spent_download_cap_stops_the_query_early() {
    let server = MockServer::start().await;
    caps_ok(&server).await;
    search_answers(&server, rss(&[item("aaa1", "One", 2040), item("aaa2", "Two", 2040)])).await;
    Mock::given(method("GET"))
        .and(path("/getnzb"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"<error code="501" description="Download limit reached"/>"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let (base, _dir) = boot(&server).await;
    assert!(records(&base, "one").await.is_empty());
}

/// A refused key (`<error code="100">`) fails the query and shows on /health.
#[tokio::test]
async fn a_refused_key_degrades_health() {
    let server = MockServer::start().await;
    caps_ok(&server).await;
    search_answers(
        &server,
        r#"<error code="100" description="Incorrect user credentials (wrong API key)"/>"#.into(),
    )
    .await;

    let (base, _dir) = boot(&server).await;
    assert!(!query(&base, "ghost").await.status().is_success());

    let health: serde_json::Value = reqwest::get(format!("{base}/health")).await.unwrap().json().await.unwrap();
    let usenet = health["plugins"].as_array().unwrap().iter().find(|p| p["id"] == "usenet").unwrap();
    assert_eq!(usenet["health"]["state"], "degraded", "{usenet}");
    assert!(usenet["health"]["reason"].as_str().unwrap().contains("API key"), "{usenet}");
}

/// A blank query never reaches nntmux (the gateway's routing gate owns structure).
#[tokio::test]
async fn a_blank_query_is_answered_locally() {
    let server = MockServer::start().await;
    caps_ok(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v1/api"))
        .and(query_param("t", "search"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss(&[])))
        .expect(0)
        .mount(&server)
        .await;

    let (base, _dir) = boot(&server).await;
    assert!(records(&base, "   ").await.is_empty());
}

/// A configured, reachable nntmux reports healthy.
#[tokio::test]
async fn a_reachable_nntmux_is_healthy() {
    let server = MockServer::start().await;
    caps_ok(&server).await;
    let (base, _dir) = boot(&server).await;
    // Let the boot-time t=caps probe land.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let health: serde_json::Value = reqwest::get(format!("{base}/health")).await.unwrap().json().await.unwrap();
    let usenet = health["plugins"].as_array().unwrap().iter().find(|p| p["id"] == "usenet").unwrap();
    assert_eq!(usenet["health"]["state"], "ok", "{usenet}");
}

/// A path-traversal id never reaches the cache or nntmux.
#[tokio::test]
async fn a_hostile_blob_id_is_refused() {
    let server = MockServer::start().await;
    caps_ok(&server).await;
    Mock::given(method("GET")).and(path("/getnzb")).respond_with(ResponseTemplate::new(200)).expect(0).mount(&server).await;
    let (base, _dir) = boot(&server).await;
    let r = reqwest::get(format!("{base}/blob/usenet/..%2F..%2Fconfig.json")).await.unwrap();
    assert!(!r.status().is_success());
}
