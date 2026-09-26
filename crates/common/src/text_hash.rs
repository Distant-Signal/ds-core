//! The one hash of an incident's extractable prose shared by every service
//! that stamps or checks `incidents.source_text_hash`. Lives here rather than
//! in `enricher` because the aggregator must compute the *identical* digest to
//! decide whether a stored extraction still describes the incident's current
//! text -- two copies of this function drifting apart would silently make
//! every extraction look stale (or, worse, every stale one look current).

use sha2::{Digest, Sha256};

/// Deterministic hash of an incident's extractable prose. Used by the
/// enricher to stamp `source_text_hash` after a successful extraction and to
/// detect, during the sweep, which incidents' text has moved since their last
/// extraction; and by the aggregator to ignore extraction output whose
/// `source_text_hash` no longer matches the incident's current text.
pub fn text_hash(summary: &str, description: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(summary.as_bytes());
    hasher.update(b"\0");
    hasher.update(description.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_input_hashes_identically() {
        assert_eq!(text_hash("a", "b"), text_hash("a", "b"));
    }

    #[test]
    fn different_summary_hashes_differently() {
        assert_ne!(text_hash("a", "b"), text_hash("a2", "b"));
    }

    #[test]
    fn the_separator_prevents_boundary_collisions() {
        // Without a separator "ab" + "" and "a" + "b" would hash identically.
        assert_ne!(text_hash("ab", ""), text_hash("a", "b"));
    }
}
