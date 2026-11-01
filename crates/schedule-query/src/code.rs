//! [`StopCode`] and [`TripCode`]: a stop's TIPLOC (or transit node) and a
//! train's UID (or transit trip), interned to a `u32` in a process-wide
//! table, so a [`crate::Connection`] carries three 4-byte ids instead of
//! three strings, and the planner indexes its per-search state by id
//! instead of hashing strings (DS-225).
//!
//! **Why interned.** A connection with three inline `CompactString`s was
//! 96 bytes; with these it is 32, and a day's graph (about 710,000
//! connections with `TfL`) shrinks with it. The searches used to hash each
//! connection's UID and both TIPLOCs into per-search maps several times
//! over; an id is a direct index into a vector.
//!
//! **Why process-wide, not per graph.** A code means the same everywhere:
//! a connection built for one service date, a live overlay's replacement
//! for a train, a test's hand-made array and a search's origin all compare
//! by id with no table to pass around, and every existing reader of
//! `connection.uid` keeps working through [`std::ops::Deref`] to `str`.
//! The cost is that a code, once seen, stays for the life of the process:
//! about 12,000 TIPLOCs and a few hundred thousand UIDs and transit trip
//! keys (`tfl:<n>`, bounded by a feed's trip count), tens of bytes each.
//! [`StopCode::count`] and [`TripCode::count`] report the sizes.
//!
//! **Order and equality.** Equality and hashing are by id (one code, one
//! id, so that matches string equality). Ordering is by the string, so
//! sorts (`build_connections`' `(departure, uid, from)` order above all)
//! come out exactly as they did with strings.
//!
//! **Normalized forms.** A TIPLOC may arrive still space-padded
//! (`"EUSTON "`); [`StopCode::normalized`] is the code of its trimmed form
//! ([`crate::normalize_tiploc`]), computed once when the code is interned.
//!
//! No `unsafe`: each code's text lives in a `OnceLock` slot of an
//! append-only, segmented table that is never freed, so a lookup is two
//! atomic loads and an index, with no lock.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{OnceLock, RwLock};

/// Codes in the first segment; segment `s` holds `FIRST << s`.
const FIRST_BITS: u32 = 12;
const FIRST: u32 = 1 << FIRST_BITS;
/// Enough segments for every `u32` id.
const SEGMENTS: usize = (33 - FIRST_BITS) as usize;

struct Entry {
    text: Box<str>,
    /// The id of `normalize_tiploc(text)` (this id when already trimmed).
    normalized: u32,
}

type Segment = Box<[OnceLock<Entry>]>;

/// `FxHash` (as rustc uses) for the text-to-id map: codes are short
/// timetable identifiers, not attacker-chosen keys.
#[derive(Default, Clone, Copy)]
struct FxHasher(u64);

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.0 = (self.0.rotate_left(5) ^ u64::from_le_bytes(word))
                .wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        }
    }

    fn write_u8(&mut self, byte: u8) {
        self.write(&[byte]);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type Ids = HashMap<&'static str, u32, std::hash::BuildHasherDefault<FxHasher>>;

/// One append-only table of codes. See the module doc.
struct Interner {
    segments: [OnceLock<Segment>; SEGMENTS],
    ids: RwLock<Ids>,
}

/// `(segment, offset)` of `id`.
fn locate(id: u32) -> (usize, usize) {
    let biased = u64::from(id) + u64::from(FIRST);
    let segment = biased.ilog2() - FIRST_BITS;
    let start = (u64::from(FIRST) << segment) - u64::from(FIRST);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "an offset within one segment is below 2^32"
    )]
    (segment as usize, (u64::from(id) - start) as usize)
}

impl Interner {
    const fn new() -> Self {
        Self {
            segments: [const { OnceLock::new() }; SEGMENTS],
            ids: RwLock::new(HashMap::with_hasher(std::hash::BuildHasherDefault::new())),
        }
    }

    fn entry(&'static self, id: u32) -> &'static Entry {
        let (segment, offset) = locate(id);
        self.segments[segment]
            .get()
            .and_then(|slots| slots[offset].get())
            .unwrap_or_else(|| unreachable!("code {id} was handed out before it was stored"))
    }

    fn find(&'static self, text: &str) -> Option<u32> {
        self.ids
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(text)
            .copied()
    }

    fn intern(&'static self, text: &str) -> u32 {
        if let Some(id) = self.find(text) {
            return id;
        }
        let trimmed = crate::normalize_tiploc(text);
        // The trimmed form first, so `normalized` can point at it.
        let normalized = if trimmed.len() == text.len() {
            None
        } else {
            Some(self.intern(trimmed))
        };
        let mut ids = self
            .ids
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(&id) = ids.get(text) {
            return id;
        }
        let id = u32::try_from(ids.len()).unwrap_or_else(|_| unreachable!("over 2^32 codes"));
        let (segment, offset) = locate(id);
        let slots = self.segments[segment].get_or_init(|| {
            (0..(1_usize << (FIRST_BITS as usize + segment)))
                .map(|_| OnceLock::new())
                .collect::<Vec<_>>()
                .into_boxed_slice()
        });
        let entry = slots[offset].get_or_init(|| Entry {
            text: text.into(),
            normalized: normalized.unwrap_or(id),
        });
        ids.insert(&entry.text, id);
        id
    }

    fn count(&'static self) -> usize {
        self.ids
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

static STOPS: Interner = Interner::new();
static TRIPS: Interner = Interner::new();

macro_rules! code_type {
    ($name:ident, $table:ident, $what:literal) => {
        #[doc = concat!("An interned ", $what, "; see the module doc.")]
        #[derive(Clone, Copy, PartialEq, Eq)]
        pub struct $name(u32);

        impl $name {
            /// The code for `text`, interning it on first sight.
            pub fn new(text: &str) -> Self {
                Self($table.intern(text))
            }

            /// The code for `text` if it has ever been interned: a lookup
            /// that never grows the table (for request input).
            pub fn find(text: &str) -> Option<Self> {
                $table.find(text).map(Self)
            }

            pub fn as_str(self) -> &'static str {
                &$table.entry(self.0).text
            }

            /// Dense, from 0: an index into a per-search vector of
            /// [`Self::count`] entries.
            pub fn index(self) -> usize {
                self.0 as usize
            }

            /// The code of this one's trimmed form (see the module doc).
            #[must_use]
            pub fn normalized(self) -> Self {
                Self($table.entry(self.0).normalized)
            }

            /// How many codes of this kind exist so far.
            pub fn count() -> usize {
                $table.count()
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;

            fn deref(&self) -> &str {
                self.as_str()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                if self.0 == other.0 {
                    std::cmp::Ordering::Equal
                } else {
                    self.as_str().cmp(other.as_str())
                }
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(self.as_str(), f)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl From<&str> for $name {
            fn from(text: &str) -> Self {
                Self::new(text)
            }
        }

        impl From<&String> for $name {
            fn from(text: &String) -> Self {
                Self::new(text)
            }
        }

        impl From<String> for $name {
            fn from(text: String) -> Self {
                Self::new(&text)
            }
        }

        impl From<compact_str::CompactString> for $name {
            fn from(text: compact_str::CompactString) -> Self {
                Self::new(&text)
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        impl PartialEq<String> for $name {
            fn eq(&self, other: &String) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<$name> for str {
            fn eq(&self, other: &$name) -> bool {
                self == other.as_str()
            }
        }

        impl PartialEq<$name> for &str {
            fn eq(&self, other: &$name) -> bool {
                *self == other.as_str()
            }
        }

        impl PartialEq<$name> for String {
            fn eq(&self, other: &$name) -> bool {
                self == other.as_str()
            }
        }
    };
}

code_type!(StopCode, STOPS, "TIPLOC or transit stop code");
code_type!(TripCode, TRIPS, "train UID or transit trip key");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_code_round_trips_and_is_one_id_per_text() {
        let a = StopCode::new("ZZCODEA");
        assert_eq!(a.as_str(), "ZZCODEA");
        assert_eq!(StopCode::new("ZZCODEA"), a);
        assert_eq!(StopCode::find("ZZCODEA"), Some(a));
        assert_ne!(StopCode::new("ZZCODEB"), a);
        assert_eq!(StopCode::find("ZZCODE-NEVER-SEEN"), None);
        assert_eq!(&*a, "ZZCODEA");
        assert_eq!(a, "ZZCODEA");
    }

    #[test]
    fn a_padded_code_knows_its_trimmed_form() {
        let padded = StopCode::new("ZZPAD  ");
        assert_eq!(padded.as_str(), "ZZPAD  ");
        assert_eq!(padded.normalized(), StopCode::new("ZZPAD"));
        assert_eq!(padded.normalized().normalized(), StopCode::new("ZZPAD"));
    }

    #[test]
    fn codes_order_by_their_text_not_their_id() {
        let later = TripCode::new("ZZORDER-B");
        let earlier = TripCode::new("ZZORDER-A");
        assert!(earlier < later);
        let mut sorted = vec![later, earlier];
        sorted.sort();
        assert_eq!(sorted, [earlier, later]);
    }

    #[test]
    fn segments_cover_every_id() {
        assert_eq!(locate(0), (0, 0));
        assert_eq!(locate(FIRST - 1), (0, (FIRST - 1) as usize));
        assert_eq!(locate(FIRST), (1, 0));
        assert_eq!(locate(3 * FIRST - 1), (1, (2 * FIRST - 1) as usize));
        assert_eq!(locate(3 * FIRST), (2, 0));
        assert_eq!(locate(u32::MAX).0, SEGMENTS - 1);
    }

    #[test]
    fn many_codes_intern_across_segments_and_threads() {
        let threads: Vec<_> = (0..4)
            .map(|t| {
                std::thread::spawn(move || {
                    (0..3000)
                        .map(|i| TripCode::new(&format!("ZZMANY-{t}-{i}")))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for (t, thread) in threads.into_iter().enumerate() {
            for (i, code) in thread.join().unwrap().into_iter().enumerate() {
                assert_eq!(code.as_str(), format!("ZZMANY-{t}-{i}"));
            }
        }
    }
}
