//! Reading an nntmux release's manifest off disk and extracting its ordered
//! article Message-IDs.
//!
//! # Why the file and not the database
//!
//! You **cannot** read a finished release's Message-IDs out of nntmux's
//! database. `NzbService::writeNzbForReleaseId()` writes the release's NZB and
//! then *unconditionally* purges collections → binaries → parts (all FKs are
//! `ON DELETE CASCADE`, and there is no retention setting). Those tables are
//! transient staging for the header scan; for any *collated* release the
//! ordered Message-IDs survive **only** in the on-disk gzip NZB at
//! `<nzb_root>/<split>/<guid>.nzb.gz`.
//!
//! This was confirmed on the live path during the spike (the offline
//! `import-nzbs` path never stages collections/binaries/parts at all, so it
//! cannot be used to check this). It is the single load-bearing fact behind the
//! whole feeder: lose it and you are back to grabbing `.nzb` files from an
//! external indexer, which is exactly what this design exists to avoid.

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use flate2::read::GzDecoder;

/// A parsed nntmux manifest: the raw NZB XML plus the Message-IDs in posting
/// order.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// The decompressed NZB XML. Handed to the gateway verbatim so it can seed
    /// it as an ordinary sha2-256 cid — the consumer parses this exact document.
    pub xml: Vec<u8>,
    /// Every `<segment>` Message-ID across every `<file>`, in document order.
    /// Bare form (no surrounding `<>`), matching how NZB stores them and how
    /// meta-share's `NzbSegment.message_id` holds them.
    pub message_ids: Vec<String>,
}

/// nntmux shards its NZB storage one level deep by the guid's **first
/// character** (`storage/nzb/a/abcdef….nzb.gz`). Mirrored here rather than
/// globbed: a scan of a large store is slow and this is on the compute path.
pub fn nzb_path(nzb_root: &Path, guid: &str) -> Option<PathBuf> {
    let first = guid.chars().next()?;
    Some(nzb_root.join(first.to_string()).join(format!("{guid}.nzb.gz")))
}

/// Read + gunzip a release's `.nzb.gz` and pull out its Message-IDs.
///
/// Returns `Ok(None)` when the file is absent — a normal, non-fatal state: a
/// release row can exist before `createNZBs()` has written its manifest, and
/// the caller simply skips that record rather than failing the whole query.
pub fn load(nzb_root: &Path, guid: &str) -> Result<Option<Manifest>> {
    let Some(path) = nzb_path(nzb_root, guid) else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(&path).with_context(|| format!("read nzb {}", path.display()))?;
    let mut xml = Vec::new();
    GzDecoder::new(&raw[..])
        .read_to_end(&mut xml)
        .with_context(|| format!("gunzip nzb {}", path.display()))?;

    let message_ids = extract_message_ids(&xml)
        .with_context(|| format!("parse nzb {}", path.display()))?;
    if message_ids.is_empty() {
        anyhow::bail!("nzb {} has no segments", path.display());
    }
    Ok(Some(Manifest { xml, message_ids }))
}

/// Pull every `<segment>` body out of an NZB document, in order.
///
/// Deliberately a light scan rather than a full deserialize: we need exactly
/// one thing from this document, the consumer (meta-share) does its own real
/// parse of the same bytes, and a strict schema here would reject NZBs that
/// meta-share would happily play. Surrounding `<>` are stripped if a producer
/// included them — the mint normalises this too, but keeping the stored form
/// canonical means the two never disagree about what was hashed.
fn extract_message_ids(xml: &[u8]) -> Result<Vec<String>> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let text = std::str::from_utf8(xml).context("nzb is not utf-8")?;
    let mut reader = Reader::from_str(text);
    reader.config_mut().trim_text(true);

    let mut ids = Vec::new();
    let mut in_segment = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) if e.local_name().as_ref() == b"segment" => in_segment = true,
            Ok(Event::End(e)) if e.local_name().as_ref() == b"segment" => in_segment = false,
            Ok(Event::Text(t)) if in_segment => {
                let raw = t.unescape().context("unescape segment text")?;
                let id = raw
                    .trim()
                    .trim_start_matches('<')
                    .trim_end_matches('>')
                    .to_string();
                if !id.is_empty() {
                    ids.push(id);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(anyhow::anyhow!("nzb xml: {e}")),
            _ => {}
        }
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0" encoding="iso-8859-1" ?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="x@y.com" date="1" subject="Show.S01E01 [01/02] &quot;a.r00&quot; yEnc">
    <groups><group>alt.binaries.teevee</group></groups>
    <segments>
      <segment bytes="100" number="1">aaa@astraweb.com</segment>
      <segment bytes="100" number="2">bbb@astraweb.com</segment>
    </segments>
  </file>
  <file poster="x@y.com" date="1" subject="Show.S01E01 [02/02] &quot;a.r01&quot; yEnc">
    <segments>
      <segment bytes="100" number="1">&lt;ccc@astraweb.com&gt;</segment>
    </segments>
  </file>
</nzb>"#;

    #[test]
    fn extracts_ids_in_document_order() {
        let ids = extract_message_ids(SAMPLE.as_bytes()).expect("parse");
        assert_eq!(
            ids,
            vec![
                "aaa@astraweb.com".to_string(),
                "bbb@astraweb.com".to_string(),
                "ccc@astraweb.com".to_string(),
            ]
        );
    }

    /// Angle brackets are stripped so the stored form matches meta-share's
    /// `NzbSegment.message_id` (bare) and the mint's normalisation.
    #[test]
    fn strips_angle_brackets() {
        let ids = extract_message_ids(SAMPLE.as_bytes()).expect("parse");
        assert!(ids.iter().all(|i| !i.contains('<') && !i.contains('>')));
    }

    #[test]
    fn nzb_path_shards_on_first_guid_char() {
        let p = nzb_path(Path::new("/s/nzb"), "abc123").expect("path");
        assert_eq!(p, Path::new("/s/nzb/a/abc123.nzb.gz"));
    }

    /// A release row can exist before `createNZBs()` has written the manifest.
    /// That is normal — the caller skips the record, it is not an error.
    #[test]
    fn missing_file_is_none_not_error() {
        let dir = tempfile::tempdir().expect("tmp");
        let got = load(dir.path(), "deadbeef").expect("no error");
        assert!(got.is_none());
    }

    #[test]
    fn round_trips_a_gzipped_nzb() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let dir = tempfile::tempdir().expect("tmp");
        std::fs::create_dir_all(dir.path().join("a")).expect("mkdir");
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(SAMPLE.as_bytes()).expect("gz write");
        let gz = enc.finish().expect("gz finish");
        std::fs::write(dir.path().join("a/abc.nzb.gz"), gz).expect("write");

        let m = load(dir.path(), "abc").expect("load").expect("present");
        assert_eq!(m.message_ids.len(), 3);
        assert_eq!(m.xml, SAMPLE.as_bytes());
    }
}
