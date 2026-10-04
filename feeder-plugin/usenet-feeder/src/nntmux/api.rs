//! Client for **our own** nntmux, over its Newznab HTTP API — the only way this
//! feeder reaches it.
//!
//! nntmux is a separate app (worph/AppStore `NNTmux`). Until 1.1 this feeder
//! read nntmux's MariaDB with SQL and gunzipped `<guid>.nzb.gz` straight off
//! its storage volume; that tied it to nntmux's schema, its DB password and its
//! shard layout, across two apps. Now it uses what any Newznab client uses:
//!
//! | call | route | cost (nntmux per-user counters) |
//! |---|---|---|
//! | [`NntmuxApi::search`] | `GET {base}/api/v1/api?t=search&q=…&extended=1` | 1 `apirequests` |
//! | [`NntmuxApi::details`] | `GET {base}/api/v1/api?t=details&id=<guid>` | 1 `apirequests` |
//! | [`NntmuxApi::nzb`] | `GET {base}/getnzb?id=<guid>.nzb&r=<key>` | 1 `downloadrequests` |
//! | [`NntmuxApi::caps`] | `GET {base}/api/v1/api?t=caps` | free, no key |
//!
//! ⚠ The API lives under **`/api/v1/api`** (Laravel's `api` prefix) — plain
//! `/api` is a 404. `t=get` is not used: in this nntmux it *redirects* to
//! `/getnzb` on the **public** `APP_URL` host, which a container on `pcs` should
//! not round-trip through; `/getnzb` on the internal host is the same handler.
//!
//! ⚠ nntmux throttles the whole `/api` group to 60 requests/minute and caps
//! each role per 24 h (`apirequests`, `downloadrequests`: Admin 1000, User 10).
//! The key should belong to a dedicated user whose role has raised caps (the
//! NNTmux app provisions `metamesh`); [`Pacer`] keeps us under the per-minute
//! throttle, and the per-guid cache ([`super::cache`]) means each release's
//! `.nzb` is downloaded once, ever.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use meta_feeder_sdk::types::GatewayError;
use tokio::sync::Mutex;

use super::catalog::Release;
use crate::newznab::{classify, fetch_nzb, looks_like_html, newznab_error, status_error};

/// Default base URL: the NNTmux app's web container on the shared `pcs` network.
pub const DEFAULT_NNTMUX_URL: &str = "http://nntmux";

/// Newznab's own page-size ceiling (`<limits max="100">`).
pub const MAX_PAGE: usize = 100;

/// Requests allowed per [`PACE_WINDOW`] — under nntmux's 60/min throttle, with
/// headroom for a human browsing nntmux with the same key.
const PACE_BUDGET: usize = 50;
const PACE_WINDOW: Duration = Duration::from_secs(60);

/// Sliding-window limiter: at most [`PACE_BUDGET`] calls per [`PACE_WINDOW`].
/// A cold search (one `t=search` + one `/getnzb` per new hit) bursts freely up
/// to the budget, then waits instead of earning an HTTP 429.
#[derive(Default)]
pub struct Pacer {
    sent: Mutex<VecDeque<Instant>>,
}

impl Pacer {
    pub async fn wait(&self) {
        loop {
            let mut sent = self.sent.lock().await;
            let now = Instant::now();
            while sent.front().is_some_and(|t| now.duration_since(*t) >= PACE_WINDOW) {
                sent.pop_front();
            }
            if sent.len() < PACE_BUDGET {
                sent.push_back(now);
                return;
            }
            let wait = PACE_WINDOW - now.duration_since(*sent.front().unwrap());
            drop(sent);
            tokio::time::sleep(wait).await;
        }
    }
}

pub struct NntmuxApi {
    http: reqwest::Client,
    /// Scheme + authority (+ optional path prefix), no trailing slash.
    base: String,
    key: String,
    pacer: Pacer,
}

impl NntmuxApi {
    pub fn new(http: reqwest::Client, base: &str, key: &str) -> Self {
        let base = base.trim().trim_end_matches('/');
        let base = if base.starts_with("http://") || base.starts_with("https://") {
            base.to_string()
        } else {
            format!("http://{base}")
        };
        Self {
            http,
            base,
            key: key.trim().to_string(),
            pacer: Pacer::default(),
        }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    fn api_url(&self) -> String {
        format!("{}/api/v1/api", self.base)
    }

    /// Free-text search, newest first. `q` is passed as typed — nntmux tokenises
    /// it (Manticore full-text when present, SQL otherwise).
    pub async fn search(&self, q: &str, limit: usize) -> Result<Vec<Release>, GatewayError> {
        let limit = limit.clamp(1, MAX_PAGE).to_string();
        let xml = self
            .get_xml(&[("t", "search"), ("q", q), ("limit", &limit), ("extended", "1")])
            .await?;
        parse_items(&xml).map_err(|e| GatewayError::Transient(format!("nntmux search answer: {e}")))
    }

    /// One release's metadata, by guid (`None` = nntmux doesn't have it).
    pub async fn details(&self, guid: &str) -> Result<Option<Release>, GatewayError> {
        let xml = match self.get_xml(&[("t", "details"), ("id", guid), ("extended", "1")]).await {
            Err(GatewayError::NotFound) => return Ok(None),
            other => other?,
        };
        let items = parse_items(&xml)
            .map_err(|e| GatewayError::Transient(format!("nntmux details answer: {e}")))?;
        Ok(items.into_iter().find(|r| r.guid == guid))
    }

    /// The release's `.nzb` (plain XML). Spends one `downloadrequests` unit —
    /// callers go through the cache.
    pub async fn nzb(&self, guid: &str) -> Result<Vec<u8>, GatewayError> {
        self.pacer.wait().await;
        let url = format!("{}/getnzb?id={guid}.nzb&r={}", self.base, self.key);
        fetch_nzb(&self.http, &url, &self.base).await
    }

    /// Reachability probe (`t=caps` needs no key and counts against nothing).
    pub async fn caps(&self) -> Result<(), GatewayError> {
        let url = format!("{}?t=caps", self.api_url());
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("nntmux {} unreachable: {}", self.base, e.without_url())))?;
        if !resp.status().is_success() {
            return Err(status_error(&resp, &self.base));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| GatewayError::Transient(format!("nntmux caps: {}", e.without_url())))?;
        if body.contains("<caps") {
            Ok(())
        } else {
            Err(GatewayError::Permanent(format!(
                "{} did not answer t=caps like a Newznab server — is this the nntmux base URL?",
                self.base
            )))
        }
    }

    /// GET the API with the key; Newznab errors (answered as HTTP 200) are mapped
    /// through [`classify`]. The URL is never echoed — it carries the key.
    async fn get_xml(&self, params: &[(&str, &str)]) -> Result<String, GatewayError> {
        self.pacer.wait().await;
        let mut query: Vec<(&str, &str)> = params.to_vec();
        query.push(("apikey", &self.key));
        let resp = self
            .http
            .get(self.api_url())
            .query(&query)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("nntmux {}: {}", self.base, e.without_url())))?;
        if !resp.status().is_success() {
            return Err(status_error(&resp, &self.base));
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| GatewayError::Transient(format!("nntmux {}: {}", self.base, e.without_url())))?;
        if looks_like_html(&body) {
            return Err(GatewayError::Permanent(format!(
                "{} answered the Newznab API with an HTML page — check the nntmux base URL",
                self.base
            )));
        }
        let text = String::from_utf8_lossy(&body).into_owned();
        if let Some(err) = newznab_error(&text) {
            return Err(classify(&err));
        }
        Ok(text)
    }
}

/// Parse a Newznab RSS answer into releases. Items without a guid are skipped.
pub fn parse_items(xml: &str) -> anyhow::Result<Vec<Release>> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    #[derive(Default)]
    struct Item {
        title: String,
        guid: String,
        size: u64,
        enclosure_len: u64,
        category: i32,
        usenetdate: Option<i64>,
        pubdate: Option<i64>,
    }

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut out = Vec::new();
    let mut item: Option<Item> = None;
    let mut field: Option<&'static str> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"item" => item = Some(Item::default()),
                b"title" if item.is_some() => field = Some("title"),
                b"guid" if item.is_some() => field = Some("guid"),
                b"pubDate" if item.is_some() => field = Some("pubDate"),
                _ => field = None,
            },
            Ok(Event::Empty(e)) => {
                let Some(it) = item.as_mut() else { continue };
                let name = e.local_name();
                let attr = |k: &[u8]| -> Option<String> {
                    e.attributes()
                        .flatten()
                        .find(|a| a.key.local_name().as_ref() == k)
                        .and_then(|a| a.unescape_value().ok().map(|v| v.into_owned()))
                };
                match name.as_ref() {
                    b"attr" => {
                        let (Some(n), Some(v)) = (attr(b"name"), attr(b"value")) else { continue };
                        match n.as_str() {
                            // Several: the parent (2000) and the subcategory (2040).
                            "category" => {
                                if let Ok(c) = v.trim().parse::<i32>() {
                                    it.category = it.category.max(c);
                                }
                            }
                            "size" => it.size = v.trim().parse().unwrap_or(0),
                            "usenetdate" => it.usenetdate = rfc2822(&v),
                            "guid" if it.guid.is_empty() => it.guid = v.trim().to_string(),
                            _ => {}
                        }
                    }
                    b"enclosure" => {
                        it.enclosure_len = attr(b"length").and_then(|l| l.trim().parse().ok()).unwrap_or(0);
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(t)) => {
                let (Some(it), Some(f)) = (item.as_mut(), field) else { continue };
                let v = t.unescape().map(|c| c.into_owned()).unwrap_or_default();
                match f {
                    "title" => it.title = v,
                    "guid" => it.guid = guid_of(&v),
                    "pubDate" => it.pubdate = rfc2822(&v),
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                if e.local_name().as_ref() == b"item" {
                    if let Some(it) = item.take() {
                        if !it.guid.is_empty() {
                            out.push(Release {
                                guid: it.guid,
                                search_name: it.title,
                                size: if it.size > 0 { it.size } else { it.enclosure_len },
                                category_id: it.category,
                                post_date: it.usenetdate.or(it.pubdate),
                            });
                        }
                    }
                }
                field = None;
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(anyhow::anyhow!("newznab xml: {e}")),
            _ => {}
        }
    }
    Ok(out)
}

/// nntmux's `<guid>` is a details URL (`{server}/details/<guid>`); the release
/// guid is its last path segment. A bare guid passes through.
fn guid_of(raw: &str) -> String {
    let t = raw.trim().trim_end_matches('/');
    t.rsplit('/').next().unwrap_or(t).to_string()
}

fn rfc2822(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc2822(s.trim()).ok().map(|d| d.timestamp())
}

/// A release guid as nntmux mints it (hex/uuid): the only shape this feeder will
/// put into a URL or a cache path. Guards `get_blob`, whose id comes off the wire.
pub fn is_guid(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEARCH: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
<channel>
 <title>NNTmux</title>
 <newznab:response offset="0" total="2"/>
 <item>
  <title>Celebrity.Ghost.Stories.S05E08.720p.HDTV.x264-DHD</title>
  <guid isPermaLink="true">http://localhost/details/97a2c0ffee1234</guid>
  <link>http://localhost/getnzb?id=97a2c0ffee1234.nzb&amp;r=KEY</link>
  <pubDate>Sat, 03 Oct 2026 12:00:00 +0000</pubDate>
  <category>TV &gt; HD</category>
  <enclosure url="http://localhost/getnzb?id=97a2c0ffee1234.nzb&amp;r=KEY" length="734003200" type="application/x-nzb"/>
  <newznab:attr name="category" value="5000"/>
  <newznab:attr name="category" value="5040"/>
  <newznab:attr name="size" value="734003200"/>
  <newznab:attr name="usenetdate" value="Fri, 02 Oct 2026 08:30:00 +0000"/>
 </item>
 <item>
  <title>no-guid-item</title>
 </item>
 <item>
  <title>Some.Movie.2024.1080p</title>
  <guid>abcd-1234</guid>
  <enclosure url="x" length="42" type="application/x-nzb"/>
  <newznab:attr name="category" value="2040"/>
 </item>
</channel>
</rss>"#;

    #[test]
    fn parses_items_guids_categories_sizes_and_dates() {
        let r = parse_items(SEARCH).unwrap();
        assert_eq!(r.len(), 2, "an item without a guid is skipped");
        assert_eq!(r[0].guid, "97a2c0ffee1234");
        assert_eq!(r[0].search_name, "Celebrity.Ghost.Stories.S05E08.720p.HDTV.x264-DHD");
        assert_eq!(r[0].category_id, 5040, "the most specific category wins");
        assert_eq!(r[0].size, 734_003_200);
        assert_eq!(r[0].post_date, Some(1_790_929_800), "usenetdate beats pubDate");
        assert_eq!(r[1].guid, "abcd-1234");
        assert_eq!(r[1].size, 42, "enclosure length is the size fallback");
        assert_eq!(r[1].post_date, None);
    }

    #[test]
    fn an_empty_channel_is_no_releases() {
        assert!(parse_items("<rss><channel></channel></rss>").unwrap().is_empty());
    }

    #[test]
    fn guid_guard() {
        assert!(is_guid("97a2c0ffee1234"));
        assert!(is_guid("0d1b2c3d-aaaa-bbbb-cccc-000000000000"));
        assert!(!is_guid("../etc/passwd"));
        assert!(!is_guid("a/b"));
        assert!(!is_guid(""));
    }

    #[test]
    fn base_urls_are_normalised() {
        let c = reqwest::Client::new();
        assert_eq!(NntmuxApi::new(c.clone(), "nntmux", "k").base(), "http://nntmux");
        assert_eq!(NntmuxApi::new(c, " http://nntmux:80/ ", "k").base(), "http://nntmux:80");
    }

    #[tokio::test]
    async fn the_pacer_lets_a_burst_through() {
        let p = Pacer::default();
        let t = Instant::now();
        for _ in 0..PACE_BUDGET {
            p.wait().await;
        }
        assert!(t.elapsed() < Duration::from_secs(1));
    }
}
