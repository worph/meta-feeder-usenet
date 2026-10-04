//! Name matching for anchored queries — how a self-scanned release lands on a
//! meta-watch show page **matched by title**, never anchored.
//!
//! A show page asks the gateway for `tmdbid:<id> season:<n> …` with no free
//! text, and keeps a release only when the release itself names that `tmdbid`
//! (and carries a poster + description). NNTmux knows release names, not TMDB
//! ids, and this feeder makes no TMDB calls. The show's identity is already on
//! the mesh, though: the card feeder stores every show it surfaced in the
//! gateway's meta-core as a **card record** keyed by its locator cid
//! (`compute_card_cid("tmdb", "tv:<id>")`, the same address meta-watch routes
//! by). So:
//!
//! 1. read the card (title, AKAs, poster, description) from meta-core;
//! 2. search NNTmux by the card's title;
//! 3. keep the releases whose cleaned name resembles one of the card's names
//!    and whose numbering agrees with the query;
//! 4. stamp them with the query's id, `anchored=true` + `anchorMethod=title`,
//!    and the card's poster/description.
//!
//! `anchorMethod=title` is what keeps this honest: meta-watch grades it
//! *Matched* ("By title"), below an id-confirmed release, and the gateway files
//! it as a durable `matchedBy/tmdb:<id>` claim rather than `anchoredBy/`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meta_feeder_sdk::filename_meta::{clean_title, extract_season_episode};
use meta_feeder_sdk::hash::compute_card_cid;
use meta_feeder_sdk::query::GatewayQuery;
use tracing::{debug, warn};

/// A release name must be *about* the show, not merely mention it: the card's
/// tokens must all appear and cover this share of the release's tokens. Same
/// rule and threshold as the torznab feeder's anchor guard
/// (`tokens_resemble_anchor`), which is crate-private there.
const MIN_TITLE_COVERAGE: f64 = 0.6;

/// A card found in meta-core is good for an hour; a miss is retried sooner,
/// since the card feeder may persist it any moment.
const CARD_TTL: Duration = Duration::from_secs(3600);
const MISS_TTL: Duration = Duration::from_secs(300);

/// What a show page's query is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowKind {
    Tv,
    Movie,
}

impl ShowKind {
    fn locator_prefix(self) -> &'static str {
        match self {
            ShowKind::Tv => "tv",
            ShowKind::Movie => "movie",
        }
    }
}

/// The anchor a query carries: the TMDB id, the kinds it may be, and the
/// numbering it asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryAnchor {
    pub tmdbid: String,
    /// Most likely first. Both when the query does not say.
    pub kinds: Vec<ShowKind>,
    pub season: Option<String>,
    pub episode: Option<String>,
}

fn first_filter<'a>(q: &'a GatewayQuery, key: &str) -> Option<&'a str> {
    q.filters
        .get(key)
        .and_then(|v| v.first())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
}

/// `numeric` → normalised (`"02"` → `"2"`), else `None`.
fn number(s: &str) -> Option<String> {
    s.trim().parse::<u32>().ok().map(|n| n.to_string())
}

impl QueryAnchor {
    /// `None` unless the query names a numeric `tmdbid`.
    pub fn from_query(q: &GatewayQuery) -> Option<Self> {
        let tmdbid = first_filter(q, "tmdbid").and_then(number)?;
        let kinds_said: Vec<&str> = q
            .filters
            .get("contentKind")
            .map(|v| v.iter().map(|s| s.trim()).collect())
            .unwrap_or_default();
        // `pack` says nothing: a film page asks for `movie OR pack`, a show
        // page for `series OR episode OR pack`.
        let tv = kinds_said.iter().any(|k| matches!(*k, "episode" | "series" | "tv"));
        let movie = kinds_said.contains(&"movie");
        let kinds = match (tv, movie) {
            (true, false) => vec![ShowKind::Tv],
            (false, true) => vec![ShowKind::Movie],
            _ => vec![ShowKind::Tv, ShowKind::Movie],
        };
        Some(Self {
            tmdbid,
            kinds,
            season: first_filter(q, "season").and_then(number),
            episode: first_filter(q, "episode").and_then(number),
        })
    }
}

/// The parts of a card record this feeder uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Card {
    pub kind: ShowKind,
    /// Display title first, then `searchTitle` and the `titles/<lang3>/<name>`
    /// AKAs. Deduped, never empty.
    pub names: Vec<String>,
    /// Copied onto matched releases: poster, every `description/<lang>`, and
    /// the cross-source ids.
    pub stamp: BTreeMap<String, String>,
    /// The AKA the card feeder picked for searching (`3%` → `3 percent`).
    pub search_title: Option<String>,
}

impl Card {
    /// `None` when the record has no usable title.
    pub fn from_fields(kind: ShowKind, f: &BTreeMap<String, String>) -> Option<Self> {
        let mut names: Vec<String> = Vec::new();
        let mut push = |s: &str| {
            let s = s.trim();
            if !s.is_empty() && !names.iter().any(|n| n.eq_ignore_ascii_case(s)) {
                names.push(s.to_string());
            }
        };
        push(f.get("title")?);
        if let Some(s) = f.get("searchTitle") {
            push(s);
        }
        for k in f.keys() {
            if let Some(rest) = k.strip_prefix("titles/") {
                if let Some((_, name)) = rest.split_once('/') {
                    push(name);
                }
            }
        }
        let stamp = f
            .iter()
            .filter(|(k, _)| {
                matches!(k.as_str(), "poster" | "imdbid" | "tvdbid") || k.starts_with("description/")
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let search_title = f.get("searchTitle").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        Some(Self { kind, names, stamp, search_title })
    }

    /// The name to search NNTmux with (the display title), plus `searchTitle`
    /// when the card has one (a title NNTmux could not match as typed, `3%`).
    pub fn search_terms(&self) -> Vec<String> {
        let mut out = vec![self.names[0].clone()];
        if let Some(s) = &self.search_title {
            if !out.iter().any(|o| o.eq_ignore_ascii_case(s)) {
                out.push(s.clone());
            }
        }
        out
    }
}

fn norm_tokens(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

fn tokens_resemble(release: &[String], name: &str) -> bool {
    let want = norm_tokens(name);
    if want.is_empty() || release.is_empty() || !want.iter().all(|t| release.contains(t)) {
        return false;
    }
    let covered = release.iter().filter(|t| want.contains(t)).count();
    covered as f64 >= release.len() as f64 * MIN_TITLE_COVERAGE
}

/// Is this release name about one of these show names?
pub fn name_matches(release_name: &str, names: &[String]) -> bool {
    let tokens = norm_tokens(&clean_title(release_name));
    names.iter().any(|n| tokens_resemble(&tokens, n))
}

/// Does the release's own numbering agree with what the query asks for? A
/// number the release does not state never disqualifies it.
pub fn numbering_agrees(release_name: &str, anchor: &QueryAnchor) -> bool {
    let se = extract_season_episode(release_name);
    if let (Some(want), Some(have)) = (&anchor.season, se.season.as_deref().and_then(number)) {
        if se.season_explicit && *want != have {
            return false;
        }
    }
    if let Some(want) = &anchor.episode {
        if let Some(have) = se.episode.as_deref().and_then(number) {
            return *want == have;
        }
        // A pack carrying an explicit range: inside it or not.
        if let (Some(a), Some(b)) = (
            se.episode_start.as_deref().and_then(|s| s.parse::<u32>().ok()),
            se.episode_end.as_deref().and_then(|s| s.parse::<u32>().ok()),
        ) {
            let w: u32 = want.parse().unwrap_or(0);
            return (a..=b).contains(&w);
        }
    }
    true
}

/// A cached lookup: the card, or `None` for a remembered miss.
type CardSlot = Option<Arc<Card>>;

/// Reads card records out of the gateway's meta-core, with a small cache.
pub struct CardBook {
    http: reqwest::Client,
    meta_core_url: Option<String>,
    cache: Mutex<HashMap<String, (Instant, CardSlot)>>,
    warned_unset: std::sync::atomic::AtomicBool,
}

impl CardBook {
    pub fn new(http: reqwest::Client, meta_core_url: Option<String>) -> Self {
        Self {
            http,
            meta_core_url: meta_core_url.map(|u| u.trim_end_matches('/').to_string()).filter(|u| !u.is_empty()),
            cache: Mutex::new(HashMap::new()),
            warned_unset: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The card for this anchor, trying each kind the
    /// query allows. `None` = no card on the mesh yet: nothing to match by.
    pub async fn card_for(&self, anchor: &QueryAnchor) -> CardSlot {
        let Some(base) = self.meta_core_url.as_deref() else {
            if !self.warned_unset.swap(true, std::sync::atomic::Ordering::Relaxed) {
                warn!(target: "meta-feeder", "usenet: META_CORE_URL is unset — show-page (tmdbid:) queries get no name matches");
            }
            return None;
        };
        for kind in &anchor.kinds {
            let id = format!("{}:{}", kind.locator_prefix(), anchor.tmdbid);
            let Some(locator) = compute_card_cid("tmdb", &id) else {
                continue;
            };
            if let Some(hit) = self.cached(&locator) {
                match hit {
                    Some(card) => return Some(card),
                    None => continue,
                }
            }
            let found = match meta_feeder_sdk::meta_core::get_record(&self.http, base, &locator).await {
                Ok(Some(f)) => Card::from_fields(*kind, &f).map(Arc::new),
                Ok(None) => None,
                Err(e) => {
                    // Not cached: a meta-core blip should not hide a show for minutes.
                    debug!(target: "meta-feeder", %locator, error = %e, "usenet: card lookup failed");
                    continue;
                }
            };
            self.cache.lock().unwrap().insert(locator.clone(), (Instant::now(), found.clone()));
            if found.is_some() {
                return found;
            }
        }
        None
    }

    fn cached(&self, locator: &str) -> Option<CardSlot> {
        let cache = self.cache.lock().unwrap();
        let (at, v) = cache.get(locator)?;
        let ttl = if v.is_some() { CARD_TTL } else { MISS_TTL };
        (at.elapsed() < ttl).then(|| v.clone())
    }
}

/// Stamp a release the anchor matched by name.
pub fn stamp_match(fields: &mut BTreeMap<String, String>, anchor: &QueryAnchor, card: &Card) {
    fields.insert("tmdbid".into(), anchor.tmdbid.clone());
    fields.insert("anchored".into(), "true".into());
    fields.insert("anchorMethod".into(), "title".into());
    for (k, v) in &card.stamp {
        fields.entry(k.clone()).or_insert_with(|| v.clone());
    }
    // A release NNTmux left uncategorised (a season pack in "Other") still
    // belongs to the show page once its name matched: give it the routing axes
    // the page filters on (`domain:screen`, a TV/movie kind).
    if !fields.contains_key("contentKind") {
        let kind = match card.kind {
            ShowKind::Movie => "movie",
            ShowKind::Tv if fields.contains_key("episode") => "episode",
            ShowKind::Tv => "pack",
        };
        set_kind(fields, kind);
    }
}

/// Write `contentKind` with its routing axes. `pack` is the one kind the SDK
/// cannot route on its own (a season pack and an album are the same shape), so
/// the writer says it: every pack this feeder emits is a screen work.
pub fn set_kind(fields: &mut BTreeMap<String, String>, kind: &str) {
    fields.insert("contentKind".into(), kind.into());
    let domain = meta_feeder_sdk::domain::domain_for_content_kind(kind).or((kind == "pack").then_some("screen"));
    if let Some(d) = domain {
        fields.insert("domain".into(), d.to_string());
    }
    let form = meta_feeder_sdk::domain::work_form_for_content_kind(kind).or((kind == "pack").then_some("serial"));
    if let Some(wf) = form {
        fields.insert("workForm".into(), wf.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(filters: &[(&str, &[&str])]) -> GatewayQuery {
        let mut g = GatewayQuery::from_free_text("");
        for (k, vs) in filters {
            g.filters.insert(k.to_string(), vs.iter().map(|s| s.to_string()).collect());
        }
        g
    }

    fn names(ns: &[&str]) -> Vec<String> {
        ns.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn anchor_reads_id_kind_and_numbering() {
        let a = QueryAnchor::from_query(&q(&[
            ("tmdbid", &["112442"]),
            ("season", &["02"]),
            ("contentKind", &["series", "episode", "pack"]),
        ]))
        .unwrap();
        assert_eq!(a.tmdbid, "112442");
        assert_eq!(a.kinds, vec![ShowKind::Tv]);
        assert_eq!(a.season.as_deref(), Some("2"));
        assert_eq!(a.episode, None);

        let film = QueryAnchor::from_query(&q(&[("tmdbid", &["603"]), ("contentKind", &["movie", "pack"])])).unwrap();
        assert_eq!(film.kinds, vec![ShowKind::Movie]);
        let unsaid = QueryAnchor::from_query(&q(&[("tmdbid", &["603"])])).unwrap();
        assert_eq!(unsaid.kinds, vec![ShowKind::Tv, ShowKind::Movie]);

        assert!(QueryAnchor::from_query(&q(&[])).is_none());
        assert!(QueryAnchor::from_query(&q(&[("tmdbid", &["tt0133093"])])).is_none());
    }

    #[test]
    fn release_names_match_the_show_not_a_mention_of_it() {
        let n = names(&["Trash Truck"]);
        assert!(name_matches("Trash.Truck.S02E16.Mint.Choco.Boom.1080p.NF.WEB-DL.DDP5.1.H.264-NTb", &n));
        assert!(name_matches("Trash.Truck.S01E02.Slumber.Party.1080p.NF.WEB-DL.DDP5.1.x264-LAZY", &n));
        assert!(!name_matches("A.Trash.Truck.Christmas.Special.Behind.The.Scenes.Making.Of.2020.1080p", &n));
        assert!(!name_matches("Regular.Show.S02E19.Grave.Sights.1080p.DD2.0.VC-1.REMUX-FraMeSToR", &n));
        // An AKA matches as well as the display title.
        assert!(name_matches("Pikappu.S01E03.1080p.WEB.h264-X", &names(&["Trash Truck", "Pikappu"])));
    }

    #[test]
    fn numbering_must_agree_when_the_release_states_it() {
        let a = |season: Option<&str>, episode: Option<&str>| QueryAnchor {
            tmdbid: "1".into(),
            kinds: vec![ShowKind::Tv],
            season: season.map(str::to_string),
            episode: episode.map(str::to_string),
        };
        let rel = "Trash.Truck.S02E16.Mint.Choco.Boom.1080p.NF.WEB-DL-NTb";
        assert!(numbering_agrees(rel, &a(Some("2"), None)));
        assert!(numbering_agrees(rel, &a(Some("2"), Some("16"))));
        assert!(!numbering_agrees(rel, &a(Some("1"), None)));
        assert!(!numbering_agrees(rel, &a(Some("2"), Some("5"))));
        // Says nothing about the episode → kept.
        assert!(numbering_agrees("Trash.Truck.S02.1080p.NF.WEB-DL-NTb", &a(Some("2"), Some("5"))));
        assert!(numbering_agrees(rel, &a(None, None)));
    }

    #[test]
    fn card_collects_names_and_stamp() {
        let mut f = BTreeMap::new();
        f.insert("title".into(), "Trash Truck".into());
        f.insert("titles/jpn/Gomi Torakku".into(), "true".into());
        f.insert("titles/eng/trash truck".into(), "true".into());
        f.insert("poster".into(), "bafkpost".into());
        f.insert("description/eng".into(), "Hank and a truck.".into());
        f.insert("imdbid".into(), "tt9288860".into());
        f.insert("genres/Kids".into(), "true".into());
        let c = Card::from_fields(ShowKind::Tv, &f).unwrap();
        assert_eq!(c.names, names(&["Trash Truck", "Gomi Torakku"]));
        assert_eq!(c.stamp.len(), 3);
        assert!(!c.stamp.contains_key("genres/Kids"));
        assert!(Card::from_fields(ShowKind::Tv, &BTreeMap::new()).is_none());
    }

    #[test]
    fn stamp_marks_a_title_match_and_fills_display_fields_only_when_absent() {
        let mut f = BTreeMap::new();
        f.insert("title".into(), "Trash Truck".into());
        f.insert("poster".into(), "bafkpost".into());
        f.insert("description/eng".into(), "Hank.".into());
        let card = Card::from_fields(ShowKind::Tv, &f).unwrap();
        let anchor = QueryAnchor { tmdbid: "112442".into(), kinds: vec![ShowKind::Tv], season: None, episode: None };

        let mut rec = BTreeMap::new();
        rec.insert("poster".to_string(), "own".to_string());
        rec.insert("episode".to_string(), "16".to_string());
        stamp_match(&mut rec, &anchor, &card);
        assert_eq!(rec["tmdbid"], "112442");
        assert_eq!(rec["anchored"], "true");
        assert_eq!(rec["anchorMethod"], "title");
        assert_eq!(rec["poster"], "own");
        assert_eq!(rec["description/eng"], "Hank.");
        // Uncategorised by NNTmux → routed to the show page anyway.
        assert_eq!(rec["contentKind"], "episode");
        assert_eq!(rec["domain"], "screen");
    }
}
