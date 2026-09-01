//! Read-only access to the nntmux sidecar's release catalog (MariaDB).
//!
//! We are a **consumer** of nntmux's schema, never a writer: the scanner owns
//! that database, and everything in it is re-derivable by re-scanning. Treat it
//! as persistent cache, not as a source of truth (the Tribler precedent — a
//! stateful sidecar the gateway still sees as a stateless HTTP feeder).
//!
//! Only a narrow, long-stable column set is selected (`guid`, `searchname`,
//! `name`, `size`, `postdate`, `categories_id`, `totalpart`). nntmux carries
//! ~80 columns on `releases`, most of them scanner bookkeeping; binding a wide
//! row here would make us fragile to upstream schema churn for no gain.

use anyhow::{Context, Result};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::Row;

/// One collated nntmux release — the metadata half of a Usenet posting.
#[derive(Debug, Clone)]
pub struct Release {
    /// nntmux's public id, and the basename of the on-disk `<guid>.nzb.gz`.
    /// This is our `record_id`.
    pub guid: String,
    /// The collator's cleaned, scene-style name
    /// (`Celebrity.Ghost.Stories.S05E08.720p.HDTV.x264-DHD`). Falls back to the
    /// raw posted `name` when the collator could not improve on it.
    pub search_name: String,
    /// Total posted size in bytes (RAR + par2 included, so an over-estimate of
    /// the final media file).
    pub size: u64,
    /// nntmux category id (`categories.id`). Coarse: 2000s = movies, 5000s = TV,
    /// 3000s = audio, 7000s = books — the Newznab category tree.
    pub category_id: i32,
    /// Unix seconds of the original posting date, when nntmux recorded one.
    pub post_date: Option<i64>,
}

/// Connection pool to nntmux's MariaDB. Small by design — this feeder issues a
/// handful of short SELECTs per query, and the scanner needs the headroom.
#[derive(Clone)]
pub struct Db {
    pool: MySqlPool,
}

impl Db {
    /// Open a pool. Does **not** verify the schema — a feeder must come up and
    /// serve `/health` even when its upstream is unreachable, so the gateway's
    /// `depends_on` is satisfied and discovery does not soft-skip us forever.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(url)
            .await
            .with_context(|| "connect to nntmux mariadb")?;
        Ok(Self { pool })
    }

    /// Free-text search over the collated release names.
    ///
    /// `LIKE %term%` on `searchname` — deliberately simple. nntmux can be built
    /// with Manticore/Elasticsearch for real full-text search, but both are
    /// optional in the minimum stack and we do not want to require them. If
    /// search quality becomes the bottleneck, that is the upgrade path.
    pub async fn search(&self, term: &str, limit: usize) -> Result<Vec<Release>> {
        // ⚠ TOKENISE, don't match the phrase. Scene releases are dot-separated
        // (`Breaking.Bad.S01E01.1080p...`) while users type spaces ("breaking
        // bad"), so a single `LIKE %breaking bad%` matches essentially NOTHING
        // — the separator never lines up. Split into words and AND them, which
        // makes the separator irrelevant on both sides.
        //
        // Found by end-to-end search from meta-watch; the unit tests missed it
        // because they only exercised the escaping, never a realistic
        // scene-style name against a natural-language query.
        let words = search_tokens(term);
        if words.is_empty() {
            return Ok(vec![]);
        }
        let mut sql = String::from(
            "SELECT guid, searchname, name, size, categories_id, \
                    UNIX_TIMESTAMP(postdate) AS postdate_unix \
             FROM releases WHERE ",
        );
        sql.push_str(
            &words
                .iter()
                .map(|_| "searchname LIKE ?")
                .collect::<Vec<_>>()
                .join(" AND "),
        );
        sql.push_str(" ORDER BY postdate DESC LIMIT ?");

        let mut q = sqlx::query(&sql);
        for w in &words {
            q = q.bind(format!("%{}%", escape_like(w)));
        }
        let rows = q
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .context("query releases by name")?;
        Ok(rows.iter().map(row_to_release).collect())
    }

    /// Look one release up by guid — the `compute_outcomes` path, where the
    /// record_id came back to us from the gateway.
    pub async fn by_guid(&self, guid: &str) -> Result<Option<Release>> {
        let row = sqlx::query(
            "SELECT guid, searchname, name, size, categories_id, \
                    UNIX_TIMESTAMP(postdate) AS postdate_unix \
             FROM releases WHERE guid = ? LIMIT 1",
        )
        .bind(guid)
        .fetch_optional(&self.pool)
        .await
        .context("query release by guid")?;
        Ok(row.as_ref().map(row_to_release))
    }

    /// Cheap liveness probe for `health()` — confirms the catalog is reachable
    /// *and* non-empty. An empty `releases` table is the visible symptom of both
    /// nntmux silent-zero traps (an unset `PATH_TO_NZBS`, or groups left
    /// `active=0`), so reporting it as degraded turns a silent failure into a
    /// legible one.
    pub async fn release_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS n FROM releases")
            .fetch_one(&self.pool)
            .await
            .context("count releases")?;
        Ok(row.try_get::<i64, _>("n").unwrap_or(0))
    }
}

fn row_to_release(row: &sqlx::mysql::MySqlRow) -> Release {
    let search_name: String = row
        .try_get::<String, _>("searchname")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| row.try_get::<String, _>("name").ok())
        .unwrap_or_default();
    Release {
        guid: row.try_get::<String, _>("guid").unwrap_or_default(),
        search_name,
        // nntmux stores size as an unsigned bigint; it arrives as u64 or
        // (older schemas) a decimal string.
        size: row
            .try_get::<u64, _>("size")
            .ok()
            .or_else(|| {
                row.try_get::<String, _>("size")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0),
        category_id: row.try_get::<i32, _>("categories_id").unwrap_or(0),
        post_date: row.try_get::<i64, _>("postdate_unix").ok(),
    }
}

/// Split a user query into match tokens.
///
/// Anything that is not alphanumeric is a separator — which folds the scene
/// world's `.`/`_`/`-` and the human world's space onto the same footing, so
/// "breaking bad", "Breaking.Bad" and "breaking-bad" all produce the same
/// tokens and all match `Breaking.Bad.S01E01.1080p.BluRay.x264-SCENE`.
///
/// Single characters are dropped: they match nearly every row and cost a full
/// scan for no selectivity.
fn search_tokens(term: &str) -> Vec<String> {
    term.split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.chars().count() > 1)
        .map(|w| w.to_string())
        .collect()
}

/// Escape the `LIKE` wildcards so a user searching for `100%` does not match
/// everything. Backslash is MySQL's default escape character.
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Map an nntmux/Newznab category id onto MetaMesh's `fileType`.
///
/// The Newznab tree is coarse and stable: 1000s console, 2000s movies, 3000s
/// audio, 4000s PC, 5000s TV, 6000s XXX, 7000s books. We only need the buckets
/// `METADATA_KEYS.md` §1 defines.
pub fn file_type_for_category(category_id: i32) -> &'static str {
    match category_id / 1000 {
        2 | 5 | 6 => "video",
        3 => "audio",
        7 => "document",
        _ => "archive",
    }
}

/// `contentKind` refinement of [`file_type_for_category`]. Only asserted where
/// the category tree is unambiguous — a wrong `contentKind` is worse than none,
/// because meta-watch's curated rows gate on it.
pub fn content_kind_for_category(category_id: i32) -> Option<&'static str> {
    match category_id / 1000 {
        2 => Some("movie"),
        // ⚠ `episode`, not `tv`. `tv` is not a `contentKind` at all — it was a
        // `domain` value, and it isn't even that any more (film + tv merged
        // into `screen`, METADATA_KEYS.md §14.17). A kind outside the registry
        // vocabulary resolves to no domain and no workForm, so the row is
        // stamped with neither and is invisible to every client wall.
        5 => Some("episode"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ⚠ THE DOT-VS-SPACE REGRESSION. Scene releases are dot-separated and
    /// users type spaces; matching the raw phrase finds nothing. Caught by an
    /// end-to-end search from meta-watch that returned 0 results against a
    /// catalog that plainly contained the show.
    #[test]
    fn a_spaced_query_tokenises_to_match_a_dotted_scene_name() {
        let tokens = search_tokens("breaking bad");
        assert_eq!(tokens, vec!["breaking", "bad"]);

        // Every token must be a substring of the real scene name — that is
        // exactly what the AND-ed LIKEs check.
        let scene = "Breaking.Bad.S01E01.1080p.BluRay.x264-SCENE".to_lowercase();
        assert!(
            tokens.iter().all(|t| scene.contains(&t.to_lowercase())),
            "tokens {tokens:?} must all appear in {scene}"
        );

        // The naive whole-phrase form does NOT — this is the bug, pinned.
        assert!(
            !scene.contains("breaking bad"),
            "if this ever passes, the dotted-name assumption changed"
        );
    }

    /// Separators are interchangeable in both directions.
    #[test]
    fn scene_and_human_separators_produce_the_same_tokens() {
        let expect = vec!["breaking".to_string(), "bad".to_string()];
        assert_eq!(search_tokens("breaking bad"), expect);
        assert_eq!(search_tokens("Breaking.Bad").iter().map(|s| s.to_lowercase()).collect::<Vec<_>>(), expect);
        assert_eq!(search_tokens("breaking-bad"), expect);
        assert_eq!(search_tokens("breaking_bad"), expect);
        assert_eq!(search_tokens("  breaking   bad  "), expect);
    }

    /// Single characters are dropped (no selectivity, full scan) and an
    /// all-noise query yields nothing rather than matching the whole catalog.
    #[test]
    fn noise_only_queries_yield_no_tokens() {
        assert!(search_tokens("").is_empty());
        assert!(search_tokens("   ").is_empty());
        assert!(search_tokens("- . _").is_empty());
        assert_eq!(search_tokens("a bc"), vec!["bc"]);
    }

    #[test]
    fn like_wildcards_are_escaped() {
        assert_eq!(escape_like("100%"), "100\\%");
        assert_eq!(escape_like("a_b"), "a\\_b");
        assert_eq!(escape_like("c:\\d"), "c:\\\\d");
        assert_eq!(escape_like("naruto"), "naruto");
    }

    #[test]
    fn newznab_categories_map_to_file_types() {
        assert_eq!(file_type_for_category(2040), "video"); // movies HD
        assert_eq!(file_type_for_category(5040), "video"); // tv HD
        assert_eq!(file_type_for_category(3010), "audio");
        assert_eq!(file_type_for_category(7020), "document"); // ebook
        assert_eq!(file_type_for_category(4000), "archive"); // pc
    }

    #[test]
    fn content_kind_only_where_unambiguous() {
        assert_eq!(content_kind_for_category(2040), Some("movie"));
        assert_eq!(content_kind_for_category(5040), Some("episode"));
        assert_eq!(content_kind_for_category(3010), None);
        assert_eq!(content_kind_for_category(7020), None);
    }
}
