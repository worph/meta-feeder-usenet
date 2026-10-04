//! Per-release `.nzb` cache, so each nntmux release is downloaded **once**.
//!
//! Every search hit must carry its portable `nzb-posting` cid (see
//! `usenet::record_for`), and that cid is a digest over the `.nzb`'s
//! Message-IDs — so a hit costs one `/getnzb` the first time it is seen. nntmux
//! counts each against the key's daily `downloadrequests`; without this cache a
//! popular term would re-download the same postings on every query. A collated
//! nntmux release never changes, so an entry never goes stale.
//!
//! Layout: `<feeder state>/nntmux-nzb/<guid>.nzb` (plain XML, written
//! atomically). The same bytes are what `get_blob` serves the gateway core for
//! seeding. An in-memory map remembers the minted cid so a cached hit is not
//! even re-parsed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use meta_feeder_sdk::hash::compute_nzb_posting_cid;

use super::api::is_guid;
use super::nzb::extract_message_ids;

/// Upper bound on the in-memory cid memo (the files themselves are unbounded).
const MEMO_MAX: usize = 50_000;

/// A release's minted identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    pub cid: String,
    pub segment_count: usize,
}

pub struct NzbCache {
    dir: PathBuf,
    memo: Mutex<HashMap<String, Posting>>,
}

impl NzbCache {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            memo: Mutex::new(HashMap::new()),
        }
    }

    fn path(&self, guid: &str) -> Option<PathBuf> {
        is_guid(guid).then(|| self.dir.join(format!("{guid}.nzb")))
    }

    /// The cached `.nzb`, if any.
    pub fn get(&self, guid: &str) -> Option<Vec<u8>> {
        std::fs::read(self.path(guid)?).ok()
    }

    /// Store a downloaded `.nzb` (tmp + rename, so a reader never sees half a file).
    pub fn put(&self, guid: &str, xml: &[u8]) -> std::io::Result<()> {
        let Some(path) = self.path(guid) else {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a release guid"));
        };
        std::fs::create_dir_all(&self.dir)?;
        let tmp = path.with_extension("nzb.tmp");
        std::fs::write(&tmp, xml)?;
        std::fs::rename(&tmp, &path)
    }

    /// Mint (or recall) the posting identity for `guid` from its `.nzb`.
    /// `None` when the document has no segments or doesn't parse.
    pub fn posting(&self, guid: &str, xml: &[u8]) -> Option<Posting> {
        if let Some(p) = self.memo.lock().unwrap().get(guid) {
            return Some(p.clone());
        }
        let ids = extract_message_ids(xml).ok()?;
        if ids.is_empty() {
            return None;
        }
        let p = Posting {
            cid: compute_nzb_posting_cid(&ids),
            segment_count: ids.len(),
        };
        let mut memo = self.memo.lock().unwrap();
        if memo.len() >= MEMO_MAX {
            memo.clear();
        }
        memo.insert(guid.to_string(), p.clone());
        Some(p)
    }

    /// The memoised identity only (no file read) — the search hot path.
    pub fn known(&self, guid: &str) -> Option<Posting> {
        self.memo.lock().unwrap().get(guid).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NZB: &[u8] = br#"<?xml version="1.0"?><nzb><file><segments>
        <segment bytes="1" number="1">a@b</segment><segment bytes="1" number="2">c@d</segment>
        </segments></file></nzb>"#;

    #[test]
    fn put_get_and_mint() {
        let dir = tempfile::tempdir().unwrap();
        let c = NzbCache::new(&dir.path().join("nntmux-nzb"));
        assert!(c.get("abc123").is_none());
        c.put("abc123", NZB).unwrap();
        assert_eq!(c.get("abc123").unwrap(), NZB);
        let p = c.posting("abc123", NZB).unwrap();
        assert_eq!(p.segment_count, 2);
        assert_eq!(p.cid, compute_nzb_posting_cid(&["a@b".to_string(), "c@d".to_string()]));
        assert_eq!(c.known("abc123"), Some(p));
    }

    #[test]
    fn a_hostile_guid_never_becomes_a_path() {
        let dir = tempfile::tempdir().unwrap();
        let c = NzbCache::new(dir.path());
        assert!(c.put("../escape", NZB).is_err());
        assert!(c.get("../escape").is_none());
    }

    #[test]
    fn a_segmentless_nzb_mints_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let c = NzbCache::new(dir.path());
        assert!(c.posting("g", b"<nzb></nzb>").is_none());
    }
}
