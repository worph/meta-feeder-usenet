//! The `usenet` upstream — a `FeederPlugin` over an nntmux header-scan catalog.
//!
//! # What makes this different from `meta-feeder-indexer`
//!
//! indexer-feeder proxies an **external** Newznab indexer and mints an
//! `nzb-release` (`0x1005`) cid that *embeds that indexer's host*. Only a peer
//! holding that indexer's credential can ever redeem it.
//!
//! This feeder scans Usenet itself, so it already holds the article Message-IDs
//! at collation time — no grab, no scrape. That lets it mint a **portable**
//! `nzb-posting` (`0x1003`) cid: a digest over the Message-ID set, embedding no
//! host, redeemable by any peer with a plain NNTP provider.
//!
//! The two coexist. This ADDS a Usenet metadata source; it removes nothing.
//!
//! # The manifest hand-off (the one piece of new machinery)
//!
//! A `0x1003` cid is a *hash*, not a locator: it is not reversible to the
//! Message-IDs, and it can never be bitswap-fetched under itself (bitswap
//! derives a block's cid by hashing the block). So the `.nzb` must travel
//! separately. We emit `manifest_url` — a **relative** URL onto our own
//! `/blob/...` route — and the gateway core fetches it, seeds it as an ordinary
//! sha2-256 IPFS cid, publishes it as a first-class `fileType=nzb` meta-core
//! record, and rewrites the field to that cid. Identical to how posters are
//! handled. See the study's §5.3.
//!
//! # Redeeming external releases
//!
//! `compute_outcomes` also accepts an `nzb-release` (`0x1005`) cid — minted by
//! meta-feeder-torznab from a Newznab search hit — and grabs its `.nzb` with the
//! key configured for that indexer host ([`crate::newznab`]). It returns ONE
//! `Sha2_256` outcome carrying the `.nzb` (head stripped, password kept); the
//! gateway stores it under `/files/plugin/meta-feeder-usenet/`, writes its cid
//! onto the release record as `manifest`, and merges the outcome record's
//! `cids/<nzb-posting>` so the release becomes portable to any NNTP-only peer.
//! Works without nntmux configured.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use meta_feeder_sdk::common::build_http_client;
use meta_feeder_sdk::config::{ConfigField as F, ConfigSchema};
use meta_feeder_sdk::hash::{
    codec_of, compute_ipfs_cid, compute_nzb_posting_cid, decode_nzb_release_cid, NZB_RELEASE_CODEC,
};
use meta_feeder_sdk::plugin::{ConfigError, FeederPlugin, HashKind, HashOutcome, RedeemClaim};
use meta_feeder_sdk::query::GatewayQuery;
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, Hash, PluginHealth};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::newznab::{self, IndexerCred};
use crate::nntmux::db::{content_kind_for_category, file_type_for_category, Db, Release};
use crate::nntmux::nzb;

pub const UPSTREAM_ID: &str = "usenet";

/// `/files/plugin/<PACKAGE>/` — where the gateway stores the `.nzb` files this
/// plugin redeems.
pub const PACKAGE: &str = "meta-feeder-usenet";

/// Indexers answer an anonymous `t=get` with `<error code="109" description=
/// "Invalid User Agent"/>` (nzbgeek does), so always send a named client.
const USER_AGENT: &str = concat!("meta-feeder-usenet/", env!("CARGO_PKG_VERSION"));

/// Wall-clock budget for one `.nzb` grab. The viewer is waiting on it.
const GRAB_TIMEOUT_SECS: u64 = 60;

/// Operator-supplied settings, persisted to `<cache_dir>/config.json` by the
/// SDK's config surface. Env is a **seed only** — the file wins on the next
/// restart (invariant 12; there is no hot reload).
#[derive(Debug, Clone, Default)]
pub struct Settings {
    /// `mysql://user:pass@host:3306/nntmux`
    pub db_url: String,
    /// Root of nntmux's NZB store, shared with the sidecar as a volume. The
    /// directory nntmux's `PATH_TO_NZBS` points at.
    pub nzb_root: String,
    /// Per-host Newznab keys for redeeming `nzb-release` locators. Config page
    /// only — no env seed.
    pub indexers: Vec<IndexerCred>,
}

impl Settings {
    fn from_env() -> Self {
        Self {
            db_url: std::env::var("NNTMUX_DB_URL").unwrap_or_default(),
            nzb_root: std::env::var("NNTMUX_NZB_PATH")
                .unwrap_or_else(|_| "/nntmux-nzb".to_string()),
            indexers: Vec::new(),
        }
    }

    fn merge_file(&mut self, cache_dir: &Path) {
        let path = cache_dir.join("config.json");
        let Ok(raw) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) else {
            warn!(target: "meta-feeder", path = %path.display(), "config.json is not valid json; using env seed");
            return;
        };
        if let Some(s) = v.get("db_url").and_then(|x| x.as_str()) {
            if !s.is_empty() {
                self.db_url = s.to_string();
            }
        }
        if let Some(s) = v.get("nzb_root").and_then(|x| x.as_str()) {
            if !s.is_empty() {
                self.nzb_root = s.to_string();
            }
        }
        if let Some(rows) = v.get("indexers").and_then(|x| x.as_array()) {
            self.indexers = rows
                .iter()
                .filter_map(|row| serde_json::from_value::<IndexerCred>(row.clone()).ok())
                .filter(|c| !c.host.trim().is_empty() && !c.api_key.trim().is_empty())
                .collect();
        }
    }
}

pub struct UsenetPlugin {
    settings: Settings,
    /// `None` until a successful connect. A feeder with unreachable config
    /// **must still serve `/health`** so the gateway's `depends_on` is satisfied
    /// — it soft-skips its upstream instead of failing to boot (invariant 10).
    db: Arc<RwLock<Option<Db>>>,
    nzb_root: PathBuf,
    /// Client for `.nzb` grabs (named User-Agent, follows redirects so an HTML
    /// landing page is detected rather than surfacing as a bare 302).
    http: reqwest::Client,
    /// `https` in production — a locator carries no scheme and every public
    /// Newznab API is TLS. Tests point it at a plain-http mock.
    grab_scheme: &'static str,
}

impl Default for UsenetPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl UsenetPlugin {
    pub fn new() -> Self {
        Self {
            settings: Settings::default(),
            db: Arc::new(RwLock::new(None)),
            nzb_root: PathBuf::new(),
            http: build_http_client(GRAB_TIMEOUT_SECS, USER_AGENT, None),
            grab_scheme: "https",
        }
    }

    /// Grab over plain `http` instead of `https`. For tests against a local
    /// mock indexer only — a locator carries no scheme, and a real indexer is TLS.
    pub fn with_plain_http_grabs(mut self) -> Self {
        self.grab_scheme = "http";
        self
    }

    /// The configured key for an indexer authority, if any.
    fn indexer_key(&self, authority: &str) -> Option<&str> {
        let want = newznab::host_key(authority);
        self.settings
            .indexers
            .iter()
            .find(|c| newznab::host_key(&c.host) == want)
            .map(|c| c.api_key.as_str())
    }

    /// Redeem an `nzb-release` locator: grab its `.nzb` with this feeder's key
    /// for that host, strip the head (password kept), and mint the portable
    /// `nzb-posting` cid from its Message-IDs.
    ///
    /// ⚠ **Spends indexer quota.** Reached only through `/compute`, which the
    /// gateway calls on a real play and never for a release it already holds
    /// the manifest of. `NotFound` = "not mine" (no key for that host), so the
    /// gateway can try another claimer without anything being spent.
    async fn redeem_release(&self, cid: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let Some((api_base, id)) = decode_nzb_release_cid(cid) else {
            return Err(GatewayError::NotFound);
        };
        let host = newznab::authority(&api_base);
        let Some(key) = self.indexer_key(host) else {
            debug!(target: "meta-feeder", %host, "usenet: no indexer key for this host; not ours to redeem");
            return Err(GatewayError::NotFound);
        };

        info!(target: "meta-feeder", %host, %id, "usenet: grabbing .nzb (indexer quota)");
        let raw = newznab::grab(&self.http, self.grab_scheme, &api_base, &id, key).await?;
        let nzb = newznab::strip_head_keep_password(&raw)
            .map_err(|e| GatewayError::Permanent(format!("nzb from {host} is not parseable: {e}")))?;
        let message_ids = nzb::extract_message_ids(&nzb)
            .map_err(|e| GatewayError::Permanent(format!("nzb from {host} is not parseable: {e}")))?;
        if message_ids.is_empty() {
            return Err(GatewayError::Permanent(format!(
                "nzb from {host} has no segments"
            )));
        }

        let posting = compute_nzb_posting_cid(&message_ids);
        let mut fields = BTreeMap::new();
        // Merged onto the release record by the gateway: the posting cid
        // outranks the locator, so the release becomes redeemable by any peer
        // with a plain NNTP provider — no indexer key anywhere after this grab.
        fields.insert(format!("cids/{posting}"), "true".to_string());
        fields.insert("segmentCount".to_string(), message_ids.len().to_string());

        Ok(vec![HashOutcome {
            hash: Hash(compute_ipfs_cid(&nzb)),
            hash_kind: HashKind::Sha2_256,
            bytes: Some(nzb.into()),
            record: Some(DiscoveryRecord {
                upstream_id: UPSTREAM_ID.to_string(),
                record_id: cid.to_string(),
                fields,
            }),
            file_extension: Some("nzb".to_string()),
        }])
    }

    async fn db(&self) -> Result<Db, GatewayError> {
        self.db
            .read()
            .await
            .clone()
            .ok_or_else(|| GatewayError::Permanent("nntmux database not configured".into()))
    }

    /// Mint a release's `nzb-posting` cid by reading its manifest off disk.
    ///
    /// `None` when the `.nzb.gz` is absent (a release row can exist before
    /// `createNZBs()` has written it) or unreadable — the caller then emits a
    /// record without a cid, which is honest: we genuinely cannot address that
    /// posting yet.
    fn posting_cid(&self, guid: &str) -> Option<String> {
        match nzb::load(&self.nzb_root, guid) {
            Ok(Some(m)) => Some(compute_nzb_posting_cid(&m.message_ids)),
            Ok(None) => None,
            Err(e) => {
                warn!(target: "meta-feeder", %guid, error = %e, "usenet: manifest unreadable; record will carry no cid");
                None
            }
        }
    }

    /// Build the discovery record for one release. Metadata only — the bytes
    /// live on Usenet and are fetched by meta-share at playback, never by us.
    ///
    /// ⚠ **`cid` must be `Some` on the SEARCH path, not just on compute.** A
    /// record whose fields carry no `cids/<cid>` member reaches the client as a
    /// bare `gateway:<upstream>:<record_id>` reference, which meta-share
    /// refuses to parse (the `<algo>:<cid>` token form was removed) — so the
    /// title renders but is unplayable. `nzb-release` (`0x1005`) avoids this
    /// for free because its cid is derivable from `{host, id}` alone; ours is a
    /// digest over the Message-ID set, so it has to be minted from the manifest
    /// — which is a **local file read**, no network and no indexer grab, and
    /// therefore fine to do per search hit. Caught by an end-to-end play from
    /// meta-watch that landed on "Unavailable".
    fn record_for(&self, r: &Release, cid: Option<&str>) -> DiscoveryRecord {
        let mut fields: BTreeMap<String, String> = BTreeMap::new();
        // The bare-cid key-set (`cids/<cid>` = "true") — the ONLY shape
        // meta-core indexes and the one meta-search elects the canonical cid
        // from (METADATA_KEYS §2). A flat `cid` field would be inert.
        if let Some(c) = cid {
            fields.insert(format!("cids/{c}"), "true".to_string());
            // ⚠ EMITTED ON THE SEARCH PATH, NOT JUST ON COMPUTE. The gateway
            // core rewrites this to a content-addressed `manifest` cid (its
            // SEEDABLE_FIELDS table) and persists the record — and it does that
            // for *search* hits, because the search path is the one that
            // actually runs: `GatewayOp::Hash` (compute) is not sent by any
            // client in the fleet today, so anything emitted only from
            // `compute_outcomes` is never seen. Without this on search, the
            // stored record has no `manifest`, and meta-share cannot resolve
            // the posting at playback ("Unavailable" in meta-watch).
            //
            // Relative on purpose: we do not know our own externally-reachable
            // address; the core resolves it against the URL it discovered us
            // on (`resolve_seed_url`).
            fields.insert(
                "manifest_url".to_string(),
                format!("/blob/{UPSTREAM_ID}/{}.nzb", r.guid),
            );
        }
        // Canonical per-upstream id field (`<upstream_id>id`) — required of
        // every record by the SDK's field-naming convention.
        fields.insert(format!("{UPSTREAM_ID}id"), r.guid.clone());
        fields.insert("title".to_string(), r.search_name.clone());
        fields.insert("fileName".to_string(), r.search_name.clone());
        if r.size > 0 {
            fields.insert("sizeByte".to_string(), r.size.to_string());
        }
        fields.insert(
            "fileType".to_string(),
            file_type_for_category(r.category_id).to_string(),
        );
        if let Some(kind) = content_kind_for_category(r.category_id) {
            fields.insert("contentKind".to_string(), kind.to_string());
            // The routing axes, co-written with the kind (METADATA_KEYS.md §1):
            // `fileType` says what the bytes are, `domain` says which app wants
            // the record, `workForm` whether it stands alone or is one
            // instalment. Without the domain the row never reaches a wall.
            if let Some(d) = meta_feeder_sdk::domain::domain_for_content_kind(kind) {
                fields.insert("domain".to_string(), d.to_string());
            }
            if let Some(wf) = meta_feeder_sdk::domain::work_form_for_content_kind(kind) {
                fields.insert("workForm".to_string(), wf.to_string());
            }
        }
        if let Some(ts) = r.post_date {
            fields.insert("publishedAt".to_string(), ts.to_string());
        }
        // Provenance: this catalog is ours, not an external indexer's. Kept
        // distinct from indexer-feeder's `indexer` field on purpose — an
        // operator reading a record should be able to tell at a glance whether
        // its metadata came from a third party or from our own scan.
        fields.insert("source/gateway:usenet-scan".to_string(), "true".to_string());
        DiscoveryRecord {
            upstream_id: UPSTREAM_ID.to_string(),
            record_id: r.guid.clone(),
            fields,
        }
    }
}

#[async_trait]
impl FeederPlugin for UsenetPlugin {
    fn upstream_id(&self) -> &'static str {
        UPSTREAM_ID
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        let mut settings = Settings::from_env();
        settings.merge_file(cache_dir);
        self.nzb_root = PathBuf::from(&settings.nzb_root);

        if settings.db_url.is_empty() {
            // Soft-skip, not a hard failure: serve /health, report Degraded.
            warn!(
                target: "meta-feeder",
                "usenet: no nntmux database url configured — upstream will soft-skip \
                 (set it in the feeder's config page, or seed NNTMUX_DB_URL)"
            );
            self.settings = settings;
            return Ok(());
        }

        // Connect eagerly so a bad url is visible at boot rather than on the
        // first query, but never fatally — the sidecar may still be starting.
        let url = settings.db_url.clone();
        let slot = self.db.clone();
        tokio::spawn(async move {
            match Db::connect(&url).await {
                Ok(db) => {
                    match db.release_count().await {
                        Ok(0) => warn!(
                            target: "meta-feeder",
                            "usenet: nntmux catalog is EMPTY — check the scan actually ran \
                             (PATH_TO_NZBS must be non-empty, and groups need active=1)"
                        ),
                        Ok(n) => info!(target: "meta-feeder", releases = n, "usenet: nntmux catalog connected"),
                        Err(e) => warn!(target: "meta-feeder", error = %e, "usenet: catalog count failed"),
                    }
                    *slot.write().await = Some(db);
                }
                Err(e) => warn!(target: "meta-feeder", error = %e, "usenet: nntmux connect failed; upstream soft-skips"),
            }
        });
        self.settings = settings;
        Ok(())
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // `free_text` is the bare-word leaves only — the structured filters
        // (contentKind:, fileType:) are the gateway's routing gate, not a search
        // term, and passing them to a LIKE would match nothing.
        let term = query.free_text.clone();
        if term.trim().is_empty() {
            return Ok(vec![]);
        }
        if self.settings.db_url.is_empty() {
            // Grab-only deployment (indexer keys, no nntmux): nothing to search.
            return Ok(vec![]);
        }
        let db = self.db().await?;
        let releases = db
            .search(term.trim(), max_results)
            .await
            .map_err(|e| GatewayError::Transient(format!("nntmux search: {e}")))?;

        // Mint each hit's cid here, from its on-disk manifest — see the
        // `record_for` doc for why this cannot wait for compute. A release
        // whose manifest isn't written yet is DROPPED rather than surfaced
        // uncid'd: an unplayable row in the client's "raw sources" list is
        // worse than one fewer row.
        let mut out = Vec::with_capacity(releases.len());
        let mut skipped = 0usize;
        for r in &releases {
            match self.posting_cid(&r.guid) {
                Some(cid) => out.push(self.record_for(r, Some(&cid))),
                None => skipped += 1,
            }
        }
        debug!(
            target: "meta-feeder", %term, hits = out.len(), skipped,
            "usenet: catalog search"
        );
        if skipped > 0 {
            debug!(
                target: "meta-feeder", skipped,
                "usenet: releases skipped — no .nzb.gz on disk yet (scan still collating?)"
            );
        }
        Ok(out)
    }

    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        // An external release locator, not one of our nntmux guids. Checked
        // first: it needs no database.
        if codec_of(record_id) == Some(NZB_RELEASE_CODEC) {
            return self.redeem_release(record_id).await;
        }
        let db = self.db().await?;
        let release = db
            .by_guid(record_id)
            .await
            .map_err(|e| GatewayError::Transient(format!("nntmux lookup: {e}")))?
            .ok_or(GatewayError::NotFound)?;

        // The Message-IDs survive only in the on-disk `.nzb.gz` — nntmux purges
        // collections/binaries/parts once it has written the file.
        let manifest = nzb::load(&self.nzb_root, record_id)
            .map_err(|e| GatewayError::Permanent(format!("nzb read: {e}")))?
            .ok_or(GatewayError::NotFound)?;

        let cid = compute_nzb_posting_cid(&manifest.message_ids);

        // Same cid the search path already published for this release — the
        // client is committing to a cid it saw in the results, so the two must
        // agree. They do by construction: both mint from the same manifest.
        let mut record = self.record_for(&release, Some(&cid));
        // `manifest_url` is already set by `record_for` — see its doc for why
        // it must be on the search path, not only here.
        record
            .fields
            .insert("segmentCount".to_string(), manifest.message_ids.len().to_string());

        Ok(vec![HashOutcome {
            hash: Hash(cid),
            hash_kind: HashKind::NzbPosting,
            // Metadata-only: we hold no media bytes. meta-share fetches the
            // articles from Usenet at playback.
            bytes: None,
            record: Some(record),
            file_extension: None,
        }])
    }

    /// Serve a release's `.nzb` so the core can seed it. The cid segment is the
    /// release guid (optionally `.nzb`-suffixed so the core's extension sniffing
    /// names the stored blob correctly).
    async fn get_blob(&self, cid: &str) -> Option<Vec<u8>> {
        let guid = cid.strip_suffix(".nzb").unwrap_or(cid);
        match nzb::load(&self.nzb_root, guid) {
            Ok(Some(m)) => Some(m.xml),
            Ok(None) => None,
            Err(e) => {
                warn!(target: "meta-feeder", %guid, error = %e, "usenet: blob read failed");
                None
            }
        }
    }

    fn health(&self) -> PluginHealth {
        if self.settings.db_url.is_empty() && self.settings.indexers.is_empty() {
            return PluginHealth::Degraded {
                reason: "neither an nntmux database nor an indexer key is configured".into(),
            };
        }
        PluginHealth::Ok
    }

    fn package(&self) -> Option<&'static str> {
        Some(PACKAGE)
    }

    /// Claims `nzb-release` for every host with a key; nothing without keys.
    fn redeems(&self) -> Vec<RedeemClaim> {
        let mut hosts: Vec<String> = self
            .settings
            .indexers
            .iter()
            .map(|c| newznab::host_key(&c.host))
            .collect();
        hosts.sort();
        hosts.dedup();
        if hosts.is_empty() {
            return Vec::new();
        }
        vec![RedeemClaim::nzb_release(hosts)]
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        &["video", "audio", "document", "archive"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["movie", "tv"]
    }

    fn config_schema(&self) -> ConfigSchema {
        ConfigSchema {
            fields: vec![
                F::text("db_url", "nntmux database URL")
                    .with_help(
                        "MariaDB DSN of the nntmux sidecar's catalog, e.g. \
                         mysql://nntmux:secret@nntmux-db:3306/nntmux. Blank → the \
                         self-scan search is off (indexer-key redeems still work). Takes \
                         effect on the next feeder restart (no hot reload).",
                    ),
                F::text("nzb_root", "NZB store path")
                    .with_help(
                        "Path to nntmux's NZB directory as mounted INTO this container \
                         — it must be the same volume nntmux's PATH_TO_NZBS points at. \
                         Each release's ordered Message-IDs survive only in the on-disk \
                         <guid>.nzb.gz, so a wrong path here means every release \
                         resolves to NotFound. Point it at the STORE ROOT, not a shard: \
                         nntmux nests each file one directory per leading guid character, \
                         `nzbsplitlevel` deep (the image ships 4 → 9/7/a/2/97a2….nzb.gz), \
                         and the depth is detected automatically.",
                    ),
                F::record_array(
                    "indexers",
                    "Indexer keys (redeem nzb-release)",
                    vec![
                        F::text("host", "Host").required().with_help(
                            "The indexer's bare host, e.g. api.nzbgeek.info — no scheme, no /api. \
                             Matched against the host inside each nzb-release cid.",
                        ),
                        F::secret("api_key", "API key").required().with_help(
                            "The indexer account's API key. Every grab spends that account's \
                             daily download quota — once per release, on a real play only.",
                        ),
                    ],
                )
                .with_help(
                    "Newznab keys used to grab the .nzb of releases minted by the torznab feeder \
                     (Prowlarr search). Takes effect on the next feeder restart.",
                ),
            ],
        }
    }

    fn config_values(&self) -> serde_json::Value {
        serde_json::json!({
            "db_url": self.settings.db_url,
            "nzb_root": self.settings.nzb_root,
            "indexers": self.settings.indexers,
        })
    }
}
