//! [`SmallStr`]: a string stored inline, with no heap allocation, when it
//! fits in `N` bytes -- which every short, fixed-width CIF field this crate
//! decodes always does.
//!
//! **Why this exists (2026-09-26).** A full CIF `MCA` extract decodes to
//! ~7.9M [`crate::records::CallingPoint`]s, all held at once in a
//! [`crate::resolve::ScheduleIndex`]. With `tiploc`, `activity` and `platform`
//! as plain `String`s, each calling point cost 128 bytes of struct plus up to
//! three separate heap allocations of 2-7 bytes each (32 bytes apiece under
//! glibc's malloc) -- ~2.3GiB resident for the index alone, which is what
//! `OOMKilled` `schedule-reference`'s 3Gi `reference` container once
//! `platform` was added. The same three fields as `SmallStr` are 16 bytes
//! each with no allocation at all.
//!
//! **Not a fixed-capacity type.** A value longer than `N` bytes is still
//! stored, just on the heap (behind a thin pointer, so that rare case does
//! not widen the common one). Everything CIF-derived fits inline by
//! construction; the heap fallback exists because the SAME types are
//! deserialized back out of `schedule_line_population` JSON blobs, and
//! nothing about that JSON's schema promises a 7-byte TIPLOC -- the api's
//! own DB tests seed it with ones like `"TEST-PUBSM-KGX-TP"`. Rejecting or
//! truncating such a value would turn a memory optimisation into a
//! behaviour change.
//!
//! Serializes and deserializes as a plain JSON string, exactly as `String`
//! does, so every published row and stored blob stays byte-identical.

use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// An inline length, `0..=15`. A dedicated enum rather than a `u8` purely
/// for its niche: the 240 unused byte values let the compiler encode
/// [`SmallStr`]'s heap variant, and an `Option<SmallStr<N>>` around it, in
/// the same byte, keeping both at 16 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum InlineLen {
    L0,
    L1,
    L2,
    L3,
    L4,
    L5,
    L6,
    L7,
    L8,
    L9,
    L10,
    L11,
    L12,
    L13,
    L14,
    L15,
}

impl InlineLen {
    const ALL: [InlineLen; 16] = [
        InlineLen::L0,
        InlineLen::L1,
        InlineLen::L2,
        InlineLen::L3,
        InlineLen::L4,
        InlineLen::L5,
        InlineLen::L6,
        InlineLen::L7,
        InlineLen::L8,
        InlineLen::L9,
        InlineLen::L10,
        InlineLen::L11,
        InlineLen::L12,
        InlineLen::L13,
        InlineLen::L14,
        InlineLen::L15,
    ];
}

#[derive(Clone)]
enum Repr<const N: usize> {
    Inline { len: InlineLen, buf: [u8; N] },
    // `Box<Box<str>>`, not `Box<str>`: a thin 8-byte pointer, so this rare
    // variant fits alongside the inline one in 16 bytes.
    Heap(Box<Box<str>>),
}

/// A string of at most `N` bytes stored inline (`N <= 15`), or longer on the
/// heap. Derefs to `str`; see the module docs for why it exists.
#[derive(Clone)]
pub struct SmallStr<const N: usize>(Repr<N>);

impl<const N: usize> SmallStr<N> {
    const CAPACITY_FITS_INLINE_LEN: () = assert!(N <= 15, "SmallStr supports N <= 15");

    /// Stores `value`, inline when it is at most `N` bytes.
    pub fn new(value: &str) -> Self {
        let () = Self::CAPACITY_FITS_INLINE_LEN;
        let bytes = value.as_bytes();
        if bytes.len() <= N {
            let mut buf = [0u8; N];
            buf[..bytes.len()].copy_from_slice(bytes);
            Self(Repr::Inline {
                len: InlineLen::ALL[bytes.len()],
                buf,
            })
        } else {
            Self(Repr::Heap(Box::new(value.into())))
        }
    }

    pub fn as_str(&self) -> &str {
        match &self.0 {
            Repr::Inline { len, buf } => {
                // Always valid: `buf[..len]` is a verbatim copy of a whole
                // `&str` (see `new`), so it cannot split a character.
                std::str::from_utf8(&buf[..*len as usize]).unwrap_or_default()
            }
            Repr::Heap(boxed) => boxed,
        }
    }
}

impl<const N: usize> Default for SmallStr<N> {
    fn default() -> Self {
        Self::new("")
    }
}

impl<const N: usize> Deref for SmallStr<N> {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<const N: usize> AsRef<str> for SmallStr<N> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<const N: usize> Borrow<str> for SmallStr<N> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<const N: usize> From<&str> for SmallStr<N> {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl<const N: usize> From<String> for SmallStr<N> {
    fn from(value: String) -> Self {
        Self::new(&value)
    }
}

impl<const N: usize> From<&String> for SmallStr<N> {
    fn from(value: &String) -> Self {
        Self::new(value)
    }
}

impl<const N: usize> From<SmallStr<N>> for String {
    fn from(value: SmallStr<N>) -> Self {
        value.as_str().to_owned()
    }
}

impl<const N: usize> PartialEq for SmallStr<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<const N: usize> Eq for SmallStr<N> {}

impl<const N: usize> PartialEq<str> for SmallStr<N> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<const N: usize> PartialEq<&str> for SmallStr<N> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<const N: usize> PartialEq<String> for SmallStr<N> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<const N: usize> PartialEq<SmallStr<N>> for str {
    fn eq(&self, other: &SmallStr<N>) -> bool {
        self == other.as_str()
    }
}

impl<const N: usize> PartialEq<SmallStr<N>> for &str {
    fn eq(&self, other: &SmallStr<N>) -> bool {
        *self == other.as_str()
    }
}

impl<const N: usize> PartialEq<SmallStr<N>> for String {
    fn eq(&self, other: &SmallStr<N>) -> bool {
        self == other.as_str()
    }
}

impl<const N: usize> PartialOrd for SmallStr<N> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<const N: usize> Ord for SmallStr<N> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl<const N: usize> Hash for SmallStr<N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

/// Formats exactly like the equivalent `String` (quoted), so assertion
/// failure messages and `{:?}` logs read the same as before.
impl<const N: usize> fmt::Debug for SmallStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl<const N: usize> fmt::Display for SmallStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

impl<const N: usize> Serialize for SmallStr<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de, const N: usize> Deserialize<'de> for SmallStr<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<const N: usize>;
        impl<const N: usize> serde::de::Visitor<'_> for Visitor<N> {
            type Value = SmallStr<N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(SmallStr::new(value))
            }
        }
        deserializer.deserialize_str(Visitor::<N>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_width_this_crate_uses_is_sixteen_bytes_including_the_option() {
        assert_eq!(size_of::<SmallStr<3>>(), 16);
        assert_eq!(size_of::<SmallStr<7>>(), 16);
        assert_eq!(size_of::<SmallStr<12>>(), 16);
        assert_eq!(size_of::<Option<SmallStr<3>>>(), 16);
    }

    #[test]
    fn short_and_long_values_round_trip_verbatim() {
        for value in ["", "T", "EUSTON ", "CARLILE", "TEST-PUBSM-KGX-TP", "é"] {
            let small: SmallStr<7> = value.into();
            assert_eq!(small.as_str(), value);
            assert_eq!(small, value);
            assert_eq!(format!("{small:?}"), format!("{value:?}"));
            assert_eq!(matches!(small.0, Repr::Heap(_)), (value.len() > 7));
        }
    }

    #[test]
    fn serializes_byte_identically_to_string_and_deserializes_back() {
        for value in ["", "1", "EUSTON ", "TEST-PUBSM-KGX-TP"] {
            let small: SmallStr<7> = value.into();
            let json = serde_json::to_string(&small).unwrap();
            assert_eq!(json, serde_json::to_string(value).unwrap());
            let back: SmallStr<7> = serde_json::from_str(&json).unwrap();
            assert_eq!(back, small);
            let from_value: SmallStr<7> =
                serde_json::from_value(serde_json::Value::String(value.to_string())).unwrap();
            assert_eq!(from_value, small);
        }
        let none: Option<SmallStr<3>> = serde_json::from_str("null").unwrap();
        assert_eq!(none, None);
        assert_eq!(serde_json::to_string(&none).unwrap(), "null");
    }

    #[test]
    fn equality_and_hash_agree_with_the_underlying_str() {
        use std::collections::HashSet;
        let set: HashSet<SmallStr<7>> = ["A".into(), "TEST-PUBSM-KGX-TP".into()].into();
        assert!(set.contains("A"));
        assert!(set.contains("TEST-PUBSM-KGX-TP"));
        assert!(!set.contains("B"));
    }
}
