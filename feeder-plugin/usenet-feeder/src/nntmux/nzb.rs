//! Extracting an NZB's ordered article Message-IDs — the input of the portable
//! `nzb-posting` (`0x1003`) cid.
//!
//! # Why the `.nzb` and not nntmux's metadata
//!
//! You **cannot** read a finished release's Message-IDs out of nntmux's
//! database: `NzbService::writeNzbForReleaseId()` writes the release's NZB and
//! then purges collections → binaries → parts. For any *collated* release the
//! ordered Message-IDs survive **only** in its `.nzb` — which this feeder now
//! downloads over nntmux's Newznab API (`/getnzb`, see [`super::api`]) and
//! caches ([`super::cache`]), instead of gunzipping it off nntmux's storage.

use anyhow::{Context, Result};

/// Pull every `<segment>` body out of an NZB document, in order.
///
/// Deliberately a light scan rather than a full deserialize: we need exactly
/// one thing from this document, the consumer (meta-share) does its own real
/// parse of the same bytes, and a strict schema here would reject NZBs that
/// meta-share would happily play. Surrounding `<>` are stripped if a producer
/// included them — the mint normalises this too, but keeping the stored form
/// canonical means the two never disagree about what was hashed.
pub fn extract_message_ids(xml: &[u8]) -> Result<Vec<String>> {
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
}
