//! Redeeming an **external** Newznab release — an `nzb-release` (`0x1005`)
//! locator minted by `meta-feeder-torznab` from a Prowlarr/Newznab search hit.
//!
//! The cid embeds the indexer's API base and the release id; redeeming it is a
//! `t=get` grab against that indexer with *this feeder's* key for that host.
//! The grab is **metered** by the indexer (a per-account daily download cap), so
//! it only ever runs from `compute_outcomes`, which the gateway calls on a real
//! play — never at search time, never speculatively.
//!
//! Moved here from meta-share's `nzb/manifest_resolve.rs::grab_newznab`: the
//! indexer keys now live on this feeder, meta-share keeps only the NNTP pool.

use std::borrow::Cow;

use anyhow::{anyhow, Context};
use meta_feeder_sdk::types::GatewayError;
use serde::{Deserialize, Serialize};

/// `retry_after_s` handed back when an indexer reports its request/download cap
/// is spent. Newznab carries no reset time, and the caps are daily.
pub const QUOTA_RETRY_SECS: u32 = 3600;

/// Refuse a `.nzb` bigger than this. A 1329-segment posting is ~200 KB; 16 MiB
/// is a ~100k-segment monster and still parseable. Same bound meta-share uses.
pub const MAX_NZB_BYTES: usize = 16 * 1024 * 1024;

/// One configured indexer key. `host` is the bare authority the key is for
/// (`api.nzbgeek.info`); the API path comes from the cid, not from here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexerCred {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub api_key: String,
}

/// Normalise an indexer host for matching: drop any `http(s)://` scheme, trim a
/// trailing slash and surrounding whitespace, lowercase. Both the configured
/// host and the cid-decoded authority go through this before an equality
/// compare, so an operator may enter `api.example.com` or `https://api.example.com/`.
pub fn normalize_indexer_host(raw: &str) -> String {
    raw.trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_ascii_lowercase()
}

/// The credential-matching key for anything host-shaped an operator or a
/// locator supplies: normalised (scheme, trailing slash, case), then reduced to
/// its authority — `https://API.nzbgeek.info/api/` and `api.nzbgeek.info/api`
/// both become `api.nzbgeek.info`.
pub fn host_key(raw: &str) -> String {
    authority(&normalize_indexer_host(raw)).to_string()
}

/// The authority part of a scheme-less API base (`api.nzbgeek.info/api` →
/// `api.nzbgeek.info`). This — never the full base — is the credential key:
/// operators configure plain hosts, and the locator may carry a path.
pub fn authority(api_base: &str) -> &str {
    match api_base.split_once('/') {
        Some((authority, _path)) => authority,
        None => api_base,
    }
}

/// A Newznab `<error code=".." description=".."/>` answer to `t=get`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewznabError {
    pub code: Option<u32>,
    pub description: Option<String>,
}

impl std::fmt::Display for NewznabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.code, &self.description) {
            (Some(c), Some(d)) => write!(f, "code {c}: {d}"),
            (None, Some(d)) => write!(f, "{d}"),
            (Some(c), None) => write!(f, "code {c}"),
            (None, None) => write!(f, "unspecified error"),
        }
    }
}

/// Pull a Newznab `<error>` out of a `t=get` response, if that is what came
/// back instead of an `.nzb`.
///
/// Deliberately a substring scrape rather than a parse: the body is tiny, the
/// shape is fixed across indexers, and the point is to turn a silent
/// zero-file manifest into a readable reason. Only the head is scanned, so an
/// `<error>` deep inside a large legitimate NZB is never mistaken for a refusal.
pub fn newznab_error(xml: &str) -> Option<NewznabError> {
    let head = &xml[..floor_char_boundary(xml, 1024)];
    let at = head.find("<error ")?;
    let rest = &head[at..];
    let end = rest.find("/>").or_else(|| rest.find('>'))? + 1;
    let tag = &rest[..end];
    let grab = |k: &str| -> Option<&str> {
        let i = tag.find(k)? + k.len();
        let t = &tag[i..];
        let q = t.find('"')? + 1;
        let e = t[q..].find('"')? + q;
        Some(&t[q..e])
    };
    Some(NewznabError {
        code: grab("code=").and_then(|c| c.trim().parse().ok()),
        description: grab("description=").map(str::to_string),
    })
}

fn floor_char_boundary(s: &str, max: usize) -> usize {
    let mut i = s.len().min(max);
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Map an indexer's refusal onto the feeder error contract. The Newznab API
/// spec's codes: `100`–`102` credentials/account, `300` no such item, `429` /
/// `500` request limit, `501` download limit, `910` API disabled.
pub fn classify(err: &NewznabError) -> GatewayError {
    match err.code {
        Some(300) => GatewayError::NotFound,
        Some(429) | Some(500) | Some(501) => GatewayError::RateLimited {
            retry_after_s: QUOTA_RETRY_SECS,
        },
        Some(100..=102) | Some(910) => GatewayError::Permanent(format!(
            "indexer refused the .nzb grab ({err}) — check this host's API key on the feeder config page"
        )),
        _ => GatewayError::Permanent(format!("indexer refused the .nzb grab ({err})")),
    }
}

/// True when an indexer answered a grab with an HTML page — a login wall, a
/// Cloudflare challenge, or a permalink host that 302s to its website
/// (`https://nzbgeek.info/api?t=get` does). Without this the grab dies later as
/// an unhelpful "no segments".
pub fn looks_like_html(body: &[u8]) -> bool {
    let head = &body[..body.len().min(512)];
    let text = String::from_utf8_lossy(head);
    let t = text.trim_start_matches('\u{feff}').trim_start().to_ascii_lowercase();
    t.starts_with("<!doctype html") || t.starts_with("<html")
}

/// Grab a release's `.nzb` from `{scheme}://{api_base}/api?t=get`.
///
/// Errors never echo the request URL — it carries the API key.
pub async fn grab(
    http: &reqwest::Client,
    scheme: &str,
    api_base: &str,
    id: &str,
    api_key: &str,
) -> Result<Vec<u8>, GatewayError> {
    let base = api_base.trim_end_matches('/');
    let url = format!("{scheme}://{base}/api?t=get&id={id}&apikey={api_key}");
    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| GatewayError::Transient(format!("newznab grab {base}: {}", e.without_url())))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(match status.as_u16() {
            404 => GatewayError::NotFound,
            429 => GatewayError::RateLimited {
                retry_after_s: resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(QUOTA_RETRY_SECS),
            },
            _ if status.is_server_error() => {
                GatewayError::Transient(format!("newznab grab {base}: HTTP {status}"))
            }
            _ => GatewayError::Permanent(format!("newznab grab {base}: HTTP {status}")),
        });
    }

    let body = resp
        .bytes()
        .await
        .map_err(|e| GatewayError::Transient(format!("newznab grab {base}: {}", e.without_url())))?;
    if body.len() > MAX_NZB_BYTES {
        return Err(GatewayError::Permanent(format!(
            "newznab grab {base}: .nzb is {} bytes, over the {MAX_NZB_BYTES}-byte ceiling",
            body.len()
        )));
    }
    if looks_like_html(&body) {
        return Err(GatewayError::Permanent(format!(
            "indexer {base} answered the .nzb grab with an HTML page — check the API base and key"
        )));
    }
    let text: Cow<str> = String::from_utf8_lossy(&body);
    if let Some(err) = newznab_error(&text) {
        return Err(classify(&err));
    }
    Ok(text.into_owned().into_bytes())
}

/// Drop the NZB `<head>` except `<meta type="password">`.
///
/// The grabbed file is seeded to the whole mesh, and indexers put
/// account-identifying bits in `<head>` metadata. The password must survive:
/// meta-share needs it to unpack a protected RAR set (`NzbManifest.password`).
/// `<file>` / `<segment>` content is copied byte-for-byte, so the Message-IDs —
/// and the `nzb-posting` cid minted from them — are unchanged. Output is UTF-8.
pub fn strip_head_keep_password(xml: &[u8]) -> anyhow::Result<Vec<u8>> {
    use quick_xml::events::{BytesEnd, BytesStart, Event};
    use quick_xml::{Reader, Writer};

    let text = String::from_utf8_lossy(xml);
    let mut reader = Reader::from_str(&text);
    let mut writer = Writer::new(Vec::with_capacity(xml.len()));

    // Nesting depth inside <head> (0 = outside), and inside a kept password meta.
    let mut head_depth = 0usize;
    let mut password_depth = 0usize;
    let mut head_start: Option<BytesStart<'static>> = None;
    let mut kept: Vec<Event<'static>> = Vec::new();

    loop {
        let ev = reader.read_event().map_err(|e| anyhow!("nzb xml: {e}"))?;
        if matches!(ev, Event::Eof) {
            break;
        }
        if head_depth == 0 {
            match &ev {
                Event::Start(e) if e.local_name().as_ref() == b"head" => {
                    head_depth = 1;
                    head_start = Some(e.clone().into_owned());
                    kept.clear();
                }
                Event::Empty(e) if e.local_name().as_ref() == b"head" => {}
                _ => writer
                    .write_event(ev)
                    .map_err(|e| anyhow!("nzb xml write: {e}"))?,
            }
            continue;
        }

        match &ev {
            Event::Start(e) => {
                if password_depth > 0 {
                    password_depth += 1;
                    kept.push(ev.into_owned());
                } else if is_password_meta(e) {
                    password_depth = 1;
                    kept.push(ev.into_owned());
                } else {
                    head_depth += 1;
                }
            }
            Event::End(e) => {
                if password_depth > 0 {
                    password_depth -= 1;
                    kept.push(ev.into_owned());
                } else if head_depth == 1 && e.local_name().as_ref() == b"head" {
                    head_depth = 0;
                    let start = head_start.take().context("head start tag")?;
                    if !kept.is_empty() {
                        let name = String::from_utf8_lossy(start.name().as_ref()).into_owned();
                        writer
                            .write_event(Event::Start(start))
                            .map_err(|e| anyhow!("nzb xml write: {e}"))?;
                        for k in kept.drain(..) {
                            writer
                                .write_event(k)
                                .map_err(|e| anyhow!("nzb xml write: {e}"))?;
                        }
                        writer
                            .write_event(Event::End(BytesEnd::new(name)))
                            .map_err(|e| anyhow!("nzb xml write: {e}"))?;
                    }
                } else {
                    head_depth -= 1;
                }
            }
            Event::Empty(e) => {
                if password_depth > 0 || is_password_meta(e) {
                    kept.push(ev.into_owned());
                }
            }
            _ => {
                if password_depth > 0 {
                    kept.push(ev.into_owned());
                }
            }
        }
    }
    Ok(writer.into_inner())
}

fn is_password_meta(e: &quick_xml::events::BytesStart<'_>) -> bool {
    e.local_name().as_ref() == b"meta"
        && e.attributes().flatten().any(|a| {
            a.key.local_name().as_ref() == b"type" && a.value.eq_ignore_ascii_case(b"password")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact body nzbgeek returns for an anonymous `t=get` — 200 OK, valid
    /// XML, zero files. Reported as "nzb has no files/segments" until this
    /// existed, which is a parser-shaped message for a credentials-shaped fault.
    #[test]
    fn reports_the_indexers_own_reason() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<error code="109" description="Invalid User Agent"/>"#;
        let err = newznab_error(xml).expect("an error");
        assert_eq!(err.to_string(), "code 109: Invalid User Agent");
        assert!(matches!(classify(&err), GatewayError::Permanent(_)));
    }

    #[test]
    fn a_real_nzb_is_not_an_error() {
        let xml = r#"<?xml version="1.0"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file subject="x"><segments>
<segment number="1" bytes="10">abc@d</segment></segments></file></nzb>"#;
        assert_eq!(newznab_error(xml), None);
    }

    #[test]
    fn only_the_head_is_scanned() {
        let xml = format!(
            "<nzb>{}<error code=\"1\" description=\"late\"/></nzb>",
            "x".repeat(2000)
        );
        assert_eq!(newznab_error(&xml), None);
    }

    #[test]
    fn limit_codes_are_rate_limited_and_300_is_not_found() {
        for code in [429, 500, 501] {
            let err = NewznabError { code: Some(code), description: None };
            assert!(
                matches!(classify(&err), GatewayError::RateLimited { retry_after_s } if retry_after_s == QUOTA_RETRY_SECS),
                "{code}"
            );
        }
        let gone = NewznabError { code: Some(300), description: Some("No such item".into()) };
        assert!(matches!(classify(&gone), GatewayError::NotFound));
        let bad_key = NewznabError { code: Some(100), description: Some("Incorrect user credentials".into()) };
        assert!(matches!(classify(&bad_key), GatewayError::Permanent(m) if m.contains("API key")));
    }

    #[test]
    fn html_pages_are_detected() {
        assert!(looks_like_html(b"<!DOCTYPE html><html><body>login</body></html>"));
        assert!(looks_like_html(b"\n  <html lang=\"en\">"));
        assert!(!looks_like_html(b"<?xml version=\"1.0\"?><nzb/>"));
    }

    #[test]
    fn hosts_normalise_and_authority_drops_the_path() {
        assert_eq!(normalize_indexer_host(" https://API.Example.com/ "), "api.example.com");
        assert_eq!(authority("api.nzbgeek.info/api"), "api.nzbgeek.info");
        assert_eq!(authority("127.0.0.1:8080"), "127.0.0.1:8080");
        // Normalise BEFORE splitting, or a scheme's `//` is taken as the path.
        assert_eq!(host_key("https://API.nzbgeek.info/api/"), "api.nzbgeek.info");
        assert_eq!(host_key("api.nzbgeek.info/api"), "api.nzbgeek.info");
    }

    const WITH_HEAD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head>
    <meta type="title">Some.Release.1080p</meta>
    <meta type="x-grabbed-by">user-12345</meta>
    <meta type="password">s3cr&amp;t</meta>
  </head>
  <file poster="x@y.com" date="1" subject="a [1/1] &quot;a.mkv&quot; yEnc">
    <groups><group>alt.binaries.teevee</group></groups>
    <segments><segment bytes="100" number="1">aaa@example.com</segment></segments>
  </file>
</nzb>"#;

    #[test]
    fn head_strip_keeps_only_the_password() {
        let out = String::from_utf8(strip_head_keep_password(WITH_HEAD.as_bytes()).unwrap()).unwrap();
        assert!(out.contains(r#"<meta type="password">s3cr&amp;t</meta>"#), "{out}");
        assert!(!out.contains("user-12345"), "{out}");
        assert!(!out.contains("Some.Release.1080p"), "{out}");
        assert!(out.contains("<head>") && out.contains("</head>"), "{out}");
        // The file body is untouched.
        assert!(out.contains(r#"<segment bytes="100" number="1">aaa@example.com</segment>"#), "{out}");
        assert!(out.contains(r#"subject="a [1/1] &quot;a.mkv&quot; yEnc""#), "{out}");
    }

    #[test]
    fn head_without_password_is_dropped_entirely() {
        let xml = WITH_HEAD.replace(r#"<meta type="password">s3cr&amp;t</meta>"#, "");
        let out = String::from_utf8(strip_head_keep_password(xml.as_bytes()).unwrap()).unwrap();
        assert!(!out.contains("<head"), "{out}");
        assert!(out.contains("aaa@example.com"), "{out}");
    }

    #[test]
    fn no_head_is_a_no_op_on_content() {
        let xml = r#"<nzb><file subject="s"><segments><segment number="1">id@x</segment></segments></file></nzb>"#;
        let out = strip_head_keep_password(xml.as_bytes()).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), xml);
    }
}
