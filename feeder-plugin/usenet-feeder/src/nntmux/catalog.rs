//! An nntmux release as the feeder sees it, and the Newznab category mapping.
//!
//! Filled from nntmux's Newznab search answer ([`super::api`]); nothing here
//! knows how it was fetched.

/// One collated nntmux release — the metadata half of a Usenet posting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    /// nntmux's public id (the last path segment of the item's `<guid>`). This
    /// is our `record_id`, and the id `/getnzb` takes.
    pub guid: String,
    /// The collator's cleaned, scene-style name
    /// (`Celebrity.Ghost.Stories.S05E08.720p.HDTV.x264-DHD`) — the item `<title>`.
    pub search_name: String,
    /// Total posted size in bytes (RAR + par2 included, so an over-estimate of
    /// the final media file). `0` when the answer carried none.
    pub size: u64,
    /// nntmux category id — the most specific `newznab:attr name="category"`.
    /// Coarse: 2000s = movies, 5000s = TV, 3000s = audio, 7000s = books.
    pub category_id: i32,
    /// Unix seconds of the original posting date (`usenetdate`), else the
    /// item's `pubDate`, when either parses.
    pub post_date: Option<i64>,
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

    #[test]
    fn newznab_categories_map_to_file_types() {
        assert_eq!(file_type_for_category(2040), "video"); // movies HD
        assert_eq!(file_type_for_category(5040), "video"); // tv HD
        assert_eq!(file_type_for_category(3010), "audio");
        assert_eq!(file_type_for_category(7020), "document"); // ebook
        assert_eq!(file_type_for_category(4000), "archive"); // pc
        assert_eq!(file_type_for_category(10), "archive"); // nntmux "Other > Misc"
    }

    #[test]
    fn content_kind_only_where_unambiguous() {
        assert_eq!(content_kind_for_category(2040), Some("movie"));
        assert_eq!(content_kind_for_category(5040), Some("episode"));
        assert_eq!(content_kind_for_category(3010), None);
        assert_eq!(content_kind_for_category(7020), None);
    }
}
