//! Show-page queries (`tmdbid:<id> season:<n>`, no free text): releases are
//! matched to the show BY NAME against its card in meta-core, and stamped as a
//! title match. Own binary because the card source is read from
//! `META_CORE_URL`, set once here.

use std::collections::BTreeMap;

use meta_feeder_sdk::hash::compute_card_cid;
use meta_feeder_sdk::serve::{configure_plugins, router};
use meta_feeder_sdk::{GatewayQuery, QueryRequest, QueryResponse};
use usenet_feeder::usenet::UsenetPlugin;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY: &str = "metamesh-key";

fn item(guid: &str, title: &str, cat: u32) -> String {
    format!(
        r#"<item><title>{title}</title><guid isPermaLink="true">http://localhost/details/{guid}</guid>
<newznab:attr name="category" value="{cat}"/><newznab:attr name="size" value="443121252"/></item>"#
    )
}

fn rss(items: &[String]) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/"><channel>{}</channel></rss>"#,
        items.join("")
    )
}

const NZB: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="x@y.com" date="1" subject="TT [1/1] &quot;a.mkv&quot; yEnc">
    <groups><group>alt.binaries.teevee</group></groups>
    <segments><segment bytes="100" number="1">ttt@example.com</segment></segments>
  </file>
</nzb>"#;

fn anchored(filters: &[(&str, &[&str])]) -> GatewayQuery {
    let mut q = GatewayQuery::from_free_text("");
    q.filters = filters
        .iter()
        .map(|(k, vs)| (k.to_string(), vs.iter().map(|s| s.to_string()).collect()))
        .collect::<BTreeMap<_, _>>();
    q
}

async fn records(base: &str, q: GatewayQuery) -> Vec<meta_feeder_sdk::DiscoveryRecord> {
    let r = reqwest::Client::new()
        .post(format!("{base}/query"))
        .json(&QueryRequest { upstream_id: "usenet".into(), query: q, max_results: 50 })
        .send()
        .await
        .expect("POST /query");
    assert!(r.status().is_success(), "query status {}", r.status());
    r.json::<QueryResponse>().await.expect("query json").records
}

#[tokio::test]
async fn a_show_page_query_matches_releases_by_name_against_the_card() {
    let nntmux = MockServer::start().await;
    let core = MockServer::start().await;
    std::env::set_var("META_CORE_URL", core.uri());

    // The card the card feeder stored for tv 112442.
    let locator = compute_card_cid("tmdb", "tv:112442").unwrap();
    Mock::given(method("GET"))
        .and(path(format!("/meta/{locator}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "hashId": locator,
            "metadata": {
                "title": "Trash Truck",
                "fileType": "card",
                "poster": "bafkreiposter",
                "description/eng": "Hank and his best pal, a giant trash truck.",
                "imdbid": "tt9288860",
                "genres/Kids": "true"
            }
        })))
        .mount(&core)
        .await;
    // No card for any other id.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .with_priority(10)
        .mount(&core)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/api"))
        .and(query_param("t", "caps"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<caps/>"))
        .mount(&nntmux)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/api"))
        .and(query_param("t", "search"))
        .and(query_param("q", "Trash Truck"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss(&[
            item("aaa111", "Trash.Truck.S02E16.Mint.Choco.Boom.1080p.NF.WEB-DL.DDP5.1.H.264-NTb", 5040),
            item("bbb222", "Trash.Truck.S01E02.Slumber.Party.1080p.NF.WEB-DL.DDP5.1.x264-LAZY", 5040),
            item("ccc333", "Trash.Truck.Fan.Made.Toy.Unboxing.Compilation.Vol.3.1080p", 5040),
        ])))
        .mount(&nntmux)
        .await;
    // Only the release that matches name AND season is ever downloaded.
    Mock::given(method("GET"))
        .and(path("/getnzb"))
        .and(query_param("id", "aaa111.nzb"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NZB))
        .expect(1)
        .mount(&nntmux)
        .await;
    Mock::given(method("GET"))
        .and(path("/getnzb"))
        .respond_with(ResponseTemplate::new(200).set_body_string(NZB))
        .with_priority(10)
        .expect(0)
        .mount(&nntmux)
        .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let cache = dir.path().join("gateway").join("usenet");
    std::fs::create_dir_all(&cache).expect("mkdir");
    std::fs::write(
        cache.join("config.json"),
        serde_json::json!({ "nntmux_url": nntmux.uri(), "nntmux_api_key": KEY }).to_string(),
    )
    .expect("write config");
    let configured = configure_plugins(vec![Box::new(UsenetPlugin::new())], dir.path()).expect("configure");
    let app = router(configured, "test".to_string(), dir.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    // The season walk's shape: id + season + the TV kinds, no free text.
    let recs = records(
        &base,
        anchored(&[
            ("tmdbid", &["112442"]),
            ("season", &["2"]),
            ("contentKind", &["series", "episode", "pack"]),
        ]),
    )
    .await;
    assert_eq!(recs.len(), 1, "only the S02 release whose name is the show, got {recs:?}");
    let f = &recs[0].fields;
    assert_eq!(recs[0].record_id, "aaa111");
    assert_eq!(f["tmdbid"], "112442");
    assert_eq!(f["anchored"], "true");
    assert_eq!(f["anchorMethod"], "title", "a name match, never an id-confirmed anchor");
    assert_eq!(f["imdbid"], "tt9288860");
    assert_eq!(f["poster"], "bafkreiposter");
    assert_eq!(f["description/eng"], "Hank and his best pal, a giant trash truck.");
    assert!(!f.contains_key("genres/Kids"));
    assert_eq!(f["season"], "2");
    assert_eq!(f["episode"], "16");
    assert_eq!(f["indexer"], "nntmux");
    assert_eq!(f["source/nntmux"], "true");
    assert_eq!(f["fileName"], "Trash.Truck.S02E16.Mint.Choco.Boom.1080p.NF.WEB-DL.DDP5.1.H.264-NTb");

    // No card on the mesh for this id → nothing to match by name.
    assert!(records(&base, anchored(&[("tmdbid", &["999"])])).await.is_empty());
}
