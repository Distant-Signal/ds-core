//! Authenticated encryption for secrets kept in Postgres (DS-415: the
//! webhook destinations' signing secrets and tokens).
//!
//! AES-256-GCM with a random 96-bit nonce per value, and a key id stored
//! beside each value so the key can rotate. The api encrypts on write and
//! the notifier decrypts on send; both build a [`KeyRing`] from the same
//! environment value:
//!
//! ```text
//! WEBHOOK_SECRET_KEYS=<id>:<base64 of 32 random bytes>[,<id>:<base64>...]
//! ```
//!
//! The first key encrypts; every key decrypts, so a retired key stays in the
//! list while any row still names its id. Generate a key with
//! `openssl rand -base64 32`. A key ring that does not parse is an error, so
//! a caller can refuse to start the feature rather than store or send
//! something unprotected. Nothing here logs or prints plaintext or keys
//! ([`KeyRing`]'s `Debug` lists only the ids).
//!
//! # Formats (DS-534; docs/secret-box-aad.md)
//!
//! The legacy format (**v1**) authenticates every value with one constant
//! AAD, so a writer of the database can move a sealed value to another row
//! or column and it still opens. **v2** binds the AAD to a [`Context`]: the
//! table, the column and the row id. The format is told by the length of the
//! stored nonce, never by trial decryption: v1 stores the 12-byte nonce, v2
//! stores `0x02` followed by the 12-byte nonce (13 bytes). No other length is
//! valid.
//!
//! [`KeyRing::open_in`] opens both. [`KeyRing::seal_in`] writes the ring's
//! write format, which is [`DEFAULT_WRITE_FORMAT`] unless
//! [`KeyRing::with_write_format`] says otherwise. **The default is
//! [`Format::V2`]** (stage 2): a binary from before the dual-read release
//! cannot open v2, so a consumer moves to this release only after every
//! reader opens v2.
//! [`KeyRing::seal`] and [`KeyRing::open`] keep their old signatures and
//! behaviour (`seal` always writes v1; `open` opens v1 only).

use std::fmt;

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine as _;
use rand::RngCore;
use rand::rngs::OsRng;

/// The additional authenticated data of every v1 value.
const AAD_V1: &[u8] = b"distant-signal secret-box v1";
/// The start of every v2 value's AAD; the context follows.
const AAD_V2: &[u8] = b"distant-signal secret-box v2";

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
/// The first byte of a v2 value's stored nonce.
const VERSION_V2: u8 = 2;
/// The stored nonce of a v2 value: the version byte and the AEAD nonce.
const NONCE_FIELD_LEN_V2: usize = 1 + NONCE_LEN;

/// The format a value is sealed in. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// v1: the constant AAD; the context is ignored.
    Legacy,
    /// v2: the AAD is bound to the [`Context`].
    V2,
}

/// The format [`KeyRing::seal_in`] writes unless told otherwise. Stage 1 of
/// DS-534 shipped `Legacy` so that a rolling deploy never has an old reader
/// meet a v2 value; stage 2 flipped it to `V2` once every reader opened v2
/// and the nonce check allowed 13 bytes (docs/secret-box-aad.md). Every
/// `seal_in` caller supplies a [`Context`], so none can write v2 without its
/// row id; [`KeyRing::seal`] takes no row and stays v1.
pub const DEFAULT_WRITE_FORMAT: Format = Format::V2;

/// A sealed column: the table and the column holding the ciphertext. Part of
/// a v2 value's AAD, and the `purpose` label of the legacy-open counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    table: &'static str,
    column: &'static str,
}

impl Column {
    pub const fn new(table: &'static str, column: &'static str) -> Column {
        Column { table, column }
    }

    /// `<table>.<column>`, the counter's `purpose` label.
    pub fn purpose(&self) -> String {
        format!("{}.{}", self.table, self.column)
    }
}

/// The webhook destinations' secret: `webhook_destinations.secret_ciphertext`
/// (with `secret_nonce` and `secret_key_id`), keyed by the row's `id`.
pub const WEBHOOK_DESTINATION_SECRET: Column =
    Column::new("webhook_destinations", "secret_ciphertext");

/// Every column that is sealed, so [`register_metrics`] can register each
/// one's counter at 0. A new sealed column is added here.
pub const KNOWN_COLUMNS: &[Column] = &[WEBHOOK_DESTINATION_SECRET];

/// A row's primary key, as bytes. The constructors tag the type, so the
/// integer `1` and the text `"1"` are different ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowId(Vec<u8>);

impl RowId {
    pub fn int(id: i64) -> RowId {
        let mut bytes = Vec::with_capacity(9);
        bytes.push(b'i');
        bytes.extend_from_slice(&id.to_be_bytes());
        RowId(bytes)
    }

    pub fn text(id: &str) -> RowId {
        let mut bytes = Vec::with_capacity(1 + id.len());
        bytes.push(b's');
        bytes.extend_from_slice(id.as_bytes());
        RowId(bytes)
    }

    pub fn bytes(id: &[u8]) -> RowId {
        let mut bytes = Vec::with_capacity(1 + id.len());
        bytes.push(b'b');
        bytes.extend_from_slice(id);
        RowId(bytes)
    }
}

impl From<i64> for RowId {
    fn from(id: i64) -> RowId {
        RowId::int(id)
    }
}

impl From<&str> for RowId {
    fn from(id: &str) -> RowId {
        RowId::text(id)
    }
}

/// Where a value is stored: what a v2 value is bound to. Seal and open with
/// the same context or the value does not open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    column: Column,
    row: RowId,
}

impl Context {
    pub fn new(column: Column, row: impl Into<RowId>) -> Context {
        Context {
            column,
            row: row.into(),
        }
    }

    /// The v2 AAD: the version tag, then the table, the column and the row
    /// id, each as a 4-byte big-endian length and its bytes (so no two
    /// contexts share an encoding).
    fn aad(&self) -> Vec<u8> {
        let mut aad = Vec::from(AAD_V2);
        for part in [
            self.column.table.as_bytes(),
            self.column.column.as_bytes(),
            self.row.0.as_slice(),
        ] {
            let len = u32::try_from(part.len()).unwrap_or(u32::MAX);
            aad.extend_from_slice(&len.to_be_bytes());
            aad.extend_from_slice(part);
        }
        aad
    }
}

/// The counter of values opened in the legacy format, labelled `purpose`
/// (`<table>.<column>`, or [`PURPOSE_UNSCOPED`] for [`KeyRing::open`]). When
/// it stops increasing and the backfill has run, the legacy fallback can go.
/// Emitted as `distant_signal_secret_box_legacy_opens_total`.
pub const LEGACY_OPENS_METRIC: &str = "secret_box_legacy_opens_total";

/// The `purpose` of an open that had no context (the old [`KeyRing::open`]).
pub const PURPOSE_UNSCOPED: &str = "unscoped";

/// Registers [`LEGACY_OPENS_METRIC`] at 0 for every [`KNOWN_COLUMNS`] purpose
/// and [`PURPOSE_UNSCOPED`], so an alert's `increase()` sees the first
/// increment. [`KeyRing::parse`] calls it.
pub fn register_metrics() {
    let name = crate::metrics::metric_name(LEGACY_OPENS_METRIC);
    for column in KNOWN_COLUMNS {
        metrics::counter!(name.clone(), "purpose" => column.purpose()).increment(0);
    }
    metrics::counter!(name, "purpose" => PURPOSE_UNSCOPED).increment(0);
}

fn count_legacy_open(purpose: String) {
    metrics::counter!(crate::metrics::metric_name(LEGACY_OPENS_METRIC), "purpose" => purpose)
        .increment(1);
}

/// One encrypted value, as stored: the ciphertext (with its tag), the nonce
/// and the id of the key that sealed it.
#[derive(Clone, PartialEq, Eq)]
pub struct Sealed {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub key_id: String,
}

impl fmt::Debug for Sealed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sealed")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum KeyRingError {
    Empty,
    /// An entry that is not `<id>:<base64>`, whose id is empty or not
    /// `[A-Za-z0-9_-]{1,32}`, or whose key is not 32 bytes. Never carries
    /// the entry itself.
    BadEntry(usize),
    DuplicateId,
}

impl fmt::Display for KeyRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyRingError::Empty => f.write_str("no keys given"),
            KeyRingError::BadEntry(n) => write!(
                f,
                "key {n} is not <id>:<base64 of 32 bytes> with an id of 1 to 32 letters, digits, - or _"
            ),
            KeyRingError::DuplicateId => f.write_str("a key id is used twice"),
        }
    }
}

impl std::error::Error for KeyRingError {}

#[derive(Debug, PartialEq, Eq)]
pub enum OpenError {
    /// No key with the value's id: the ring lacks a retired key.
    UnknownKey,
    /// Wrong key, a changed value, or a malformed nonce.
    Failed,
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::UnknownKey => f.write_str("the key that sealed it is not in the key ring"),
            OpenError::Failed => f.write_str("it could not be decrypted"),
        }
    }
}

impl std::error::Error for OpenError {}

struct Entry {
    id: String,
    cipher: Aes256Gcm,
}

/// The keys to encrypt with (the first) and to decrypt with (all).
pub struct KeyRing {
    entries: Vec<Entry>,
    write_format: Format,
}

impl fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyRing")
            .field(
                "ids",
                &self
                    .entries
                    .iter()
                    .map(|e| e.id.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("write_format", &self.write_format)
            .finish()
    }
}

fn valid_id(id: &str) -> bool {
    (1..=32).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl KeyRing {
    /// Parses `<id>:<base64>[,<id>:<base64>...]`.
    pub fn parse(value: &str) -> Result<KeyRing, KeyRingError> {
        let mut entries: Vec<Entry> = Vec::new();
        for (n, part) in value
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .enumerate()
        {
            let bad = KeyRingError::BadEntry(n + 1);
            let (id, key) = part.split_once(':').ok_or(KeyRingError::BadEntry(n + 1))?;
            if !valid_id(id) {
                return Err(bad);
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(key.trim())
                .map_err(|_| KeyRingError::BadEntry(n + 1))?;
            if bytes.len() != KEY_LEN {
                return Err(bad);
            }
            let cipher =
                Aes256Gcm::new_from_slice(&bytes).map_err(|_| KeyRingError::BadEntry(n + 1))?;
            if entries.iter().any(|e| e.id == id) {
                return Err(KeyRingError::DuplicateId);
            }
            entries.push(Entry {
                id: id.to_string(),
                cipher,
            });
        }
        if entries.is_empty() {
            return Err(KeyRingError::Empty);
        }
        register_metrics();
        Ok(KeyRing {
            entries,
            write_format: DEFAULT_WRITE_FORMAT,
        })
    }

    /// Sets the format [`KeyRing::seal_in`] writes.
    #[must_use]
    pub fn with_write_format(mut self, format: Format) -> KeyRing {
        self.write_format = format;
        self
    }

    /// The format [`KeyRing::seal_in`] writes.
    pub fn write_format(&self) -> Format {
        self.write_format
    }

    /// The id new values are sealed with.
    pub fn active_id(&self) -> &str {
        &self.entries[0].id
    }

    /// Encrypts `plaintext` with the active key and a fresh random nonce, in
    /// the legacy format (v1), whatever the write format. Kept for callers
    /// that have no row to bind to; prefer [`KeyRing::seal_in`].
    pub fn seal(&self, plaintext: &str) -> Result<Sealed, OpenError> {
        self.encrypt(plaintext, Format::Legacy, AAD_V1)
    }

    /// Encrypts `plaintext` for the row `context` names, in the ring's write
    /// format ([`DEFAULT_WRITE_FORMAT`] unless changed). A legacy write
    /// ignores the context.
    pub fn seal_in(&self, plaintext: &str, context: &Context) -> Result<Sealed, OpenError> {
        self.seal_as(plaintext, context, self.write_format)
    }

    /// Encrypts in an explicit `format`, for a backfill and for tests.
    pub fn seal_as(
        &self,
        plaintext: &str,
        context: &Context,
        format: Format,
    ) -> Result<Sealed, OpenError> {
        match format {
            Format::Legacy => self.encrypt(plaintext, Format::Legacy, AAD_V1),
            Format::V2 => self.encrypt(plaintext, Format::V2, &context.aad()),
        }
    }

    fn encrypt(&self, plaintext: &str, format: Format, aad: &[u8]) -> Result<Sealed, OpenError> {
        let entry = &self.entries[0];
        let mut nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::try_from(&nonce_bytes[..]).map_err(|_| OpenError::Failed)?;
        let ciphertext = entry
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext.as_bytes(),
                    aad,
                },
            )
            .map_err(|_| OpenError::Failed)?;
        let stored_nonce = match format {
            Format::Legacy => nonce_bytes.to_vec(),
            Format::V2 => {
                let mut stored = Vec::with_capacity(NONCE_FIELD_LEN_V2);
                stored.push(VERSION_V2);
                stored.extend_from_slice(&nonce_bytes);
                stored
            }
        };
        Ok(Sealed {
            ciphertext,
            nonce: stored_nonce,
            key_id: entry.id.clone(),
        })
    }

    /// Decrypts a legacy (v1) value with the key its id names. A v2 value
    /// does not open here, since it needs its row: use [`KeyRing::open_in`].
    /// Counted as an `unscoped` legacy open.
    pub fn open(&self, sealed: &Sealed) -> Result<String, OpenError> {
        let entry = self.entry(sealed)?;
        match format_of(&sealed.nonce)? {
            Format::Legacy => {
                let plaintext = decrypt(entry, sealed, &sealed.nonce, AAD_V1)?;
                count_legacy_open(PURPOSE_UNSCOPED.to_string());
                Ok(plaintext)
            }
            Format::V2 => Err(OpenError::Failed),
        }
    }

    /// Decrypts a value of either format. A v2 value opens only with the
    /// context it was sealed with. A v1 value ignores the context and counts
    /// in [`LEGACY_OPENS_METRIC`] under the context's column.
    pub fn open_in(&self, sealed: &Sealed, context: &Context) -> Result<String, OpenError> {
        let entry = self.entry(sealed)?;
        match format_of(&sealed.nonce)? {
            Format::Legacy => {
                let plaintext = decrypt(entry, sealed, &sealed.nonce, AAD_V1)?;
                count_legacy_open(context.column.purpose());
                Ok(plaintext)
            }
            Format::V2 => decrypt(entry, sealed, &sealed.nonce[1..], &context.aad()),
        }
    }

    fn entry(&self, sealed: &Sealed) -> Result<&Entry, OpenError> {
        self.entries
            .iter()
            .find(|e| e.id == sealed.key_id)
            .ok_or(OpenError::UnknownKey)
    }
}

/// The format a stored nonce says, by its length (and, for v2, its version
/// byte). Never by trying keys or AADs.
fn format_of(stored_nonce: &[u8]) -> Result<Format, OpenError> {
    match stored_nonce.len() {
        NONCE_LEN => Ok(Format::Legacy),
        NONCE_FIELD_LEN_V2 if stored_nonce[0] == VERSION_V2 => Ok(Format::V2),
        _ => Err(OpenError::Failed),
    }
}

/// Decrypts with the AEAD nonce `nonce` (the stored nonce minus any version
/// byte).
fn decrypt(entry: &Entry, sealed: &Sealed, nonce: &[u8], aad: &[u8]) -> Result<String, OpenError> {
    let nonce = Nonce::try_from(nonce).map_err(|_| OpenError::Failed)?;
    let plaintext = entry
        .cipher
        .decrypt(
            &nonce,
            Payload {
                msg: &sealed.ciphertext,
                aad,
            },
        )
        .map_err(|_| OpenError::Failed)?;
    String::from_utf8(plaintext).map_err(|_| OpenError::Failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> String {
        base64::engine::general_purpose::STANDARD.encode([byte; 32])
    }

    fn ring(spec: &str) -> KeyRing {
        KeyRing::parse(spec).expect("a valid ring")
    }

    #[test]
    fn a_value_round_trips_and_each_seal_has_its_own_nonce() {
        let ring = ring(&format!("k1:{}", key(1)));
        let a = ring.seal("whsec_secret").unwrap();
        let b = ring.seal("whsec_secret").unwrap();
        assert_eq!(a.key_id, "k1");
        assert_eq!(a.nonce.len(), 12);
        assert_ne!(a.nonce, b.nonce);
        assert_ne!(a.ciphertext, b.ciphertext);
        assert!(!a.ciphertext.windows(6).any(|w| w == b"secret"));
        assert_eq!(ring.open(&a).unwrap(), "whsec_secret");
    }

    #[test]
    fn a_changed_value_or_nonce_does_not_open() {
        let ring = ring(&format!("k1:{}", key(1)));
        let mut sealed = ring.seal("token").unwrap();
        let original = sealed.clone();
        sealed.ciphertext[0] ^= 1;
        assert_eq!(ring.open(&sealed), Err(OpenError::Failed));
        sealed = original.clone();
        sealed.nonce[0] ^= 1;
        assert_eq!(ring.open(&sealed), Err(OpenError::Failed));
        sealed = original;
        sealed.nonce.pop();
        assert_eq!(ring.open(&sealed), Err(OpenError::Failed));
    }

    #[test]
    fn rotation_seals_with_the_first_key_and_still_opens_the_old() {
        let old = ring(&format!("k1:{}", key(1)));
        let sealed_old = old.seal("token").unwrap();
        let rotated = ring(&format!("k2:{},k1:{}", key(2), key(1)));
        assert_eq!(rotated.active_id(), "k2");
        assert_eq!(rotated.open(&sealed_old).unwrap(), "token");
        assert_eq!(rotated.seal("token").unwrap().key_id, "k2");
        // A ring that dropped k1 says so, rather than failing the tag.
        let without = ring(&format!("k2:{}", key(2)));
        assert_eq!(without.open(&sealed_old), Err(OpenError::UnknownKey));
        // The same id with a different key fails authentication.
        let wrong = ring(&format!("k1:{}", key(9)));
        assert_eq!(wrong.open(&sealed_old), Err(OpenError::Failed));
    }

    #[test]
    fn bad_rings_are_refused_without_echoing_the_input() {
        assert_eq!(KeyRing::parse("").unwrap_err(), KeyRingError::Empty);
        assert_eq!(KeyRing::parse(" , ").unwrap_err(), KeyRingError::Empty);
        for bad in [
            "nocolon",
            ":AAAA",
            &format!("bad id:{}", key(1)),
            &format!("{}:{}", "x".repeat(33), key(1)),
            "k1:not base64!",
            &format!(
                "k1:{}",
                base64::engine::general_purpose::STANDARD.encode([1u8; 16])
            ),
        ] {
            let err = KeyRing::parse(bad).unwrap_err();
            assert!(matches!(err, KeyRingError::BadEntry(1)), "{bad}");
            assert!(!err.to_string().contains(bad), "{bad}");
        }
        assert_eq!(
            KeyRing::parse(&format!("k1:{},k1:{}", key(1), key(2))).unwrap_err(),
            KeyRingError::DuplicateId
        );
    }

    #[test]
    fn debug_never_shows_keys_or_plaintext() {
        let ring = ring(&format!("k1:{}", key(7)));
        let sealed = ring.seal("topsecret").unwrap();
        let shown = format!("{ring:?} {sealed:?}");
        assert!(shown.contains("k1"));
        assert!(!shown.contains(&key(7)) && !shown.contains("topsecret"));
    }

    fn ctx(id: i64) -> Context {
        Context::new(WEBHOOK_DESTINATION_SECRET, id)
    }

    // Vectors made outside this crate (AES-256-GCM, key = 32 bytes of 0x01,
    // nonce = 0..12). The key is a fake used only here.
    const V1_VECTOR: [u8; 28] = [
        0xcc, 0xce, 0xe6, 0x43, 0x43, 0xa1, 0x6a, 0x1c, 0x99, 0x0b, 0x84, 0xbd, 0xf5, 0x07, 0xc6,
        0x1c, 0xd1, 0x2f, 0x9b, 0x33, 0x54, 0xf2, 0x2f, 0x3c, 0x26, 0xb2, 0xd2, 0x12,
    ];
    // "whsec_v2" for (webhook_destinations, secret_ciphertext, row 42).
    const V2_VECTOR: [u8; 24] = [
        0xcc, 0xce, 0xe6, 0x43, 0x43, 0xa1, 0x70, 0x4b, 0x04, 0xea, 0x66, 0x54, 0xbd, 0xb1, 0x3d,
        0x3e, 0x5e, 0x51, 0xe6, 0x9a, 0xcf, 0xba, 0x8f, 0x1b,
    ];

    fn vector_nonce() -> Vec<u8> {
        (0u8..12).collect()
    }

    #[test]
    fn a_stored_v1_value_still_opens_by_either_call() {
        let ring = ring(&format!("k1:{}", key(1)));
        let sealed = Sealed {
            ciphertext: V1_VECTOR.to_vec(),
            nonce: vector_nonce(),
            key_id: "k1".to_string(),
        };
        assert_eq!(ring.open(&sealed).unwrap(), "whsec_legacy");
        // The context is ignored for v1, whatever row it names.
        assert_eq!(ring.open_in(&sealed, &ctx(1)).unwrap(), "whsec_legacy");
        assert_eq!(ring.open_in(&sealed, &ctx(2)).unwrap(), "whsec_legacy");
    }

    #[test]
    fn a_stored_v2_value_opens_only_with_its_row() {
        let ring = ring(&format!("k1:{}", key(1)));
        let mut nonce = vec![2u8];
        nonce.extend(vector_nonce());
        let sealed = Sealed {
            ciphertext: V2_VECTOR.to_vec(),
            nonce,
            key_id: "k1".to_string(),
        };
        assert_eq!(ring.open_in(&sealed, &ctx(42)).unwrap(), "whsec_v2");
        assert_eq!(ring.open_in(&sealed, &ctx(43)), Err(OpenError::Failed));
        // `open` has no context, so a v2 value never opens through it.
        assert_eq!(ring.open(&sealed), Err(OpenError::Failed));
    }

    #[test]
    fn the_default_write_format_is_v2_bound_to_the_row() {
        assert_eq!(DEFAULT_WRITE_FORMAT, Format::V2);
        let ring = ring(&format!("k1:{}", key(1)));
        assert_eq!(ring.write_format(), Format::V2);
        let sealed = ring.seal_in("token", &ctx(7)).unwrap();
        assert_eq!(sealed.nonce.len(), 13);
        assert_eq!(sealed.nonce[0], 2);
        assert_eq!(ring.open_in(&sealed, &ctx(7)).unwrap(), "token");
        assert_eq!(ring.open_in(&sealed, &ctx(8)), Err(OpenError::Failed));
        // `open` has no row, so it never opens a v2 value.
        assert_eq!(ring.open(&sealed), Err(OpenError::Failed));
    }

    #[test]
    fn a_legacy_write_format_and_seal_still_write_v1_that_opens_by_either_call() {
        let ring = ring(&format!("k1:{}", key(1))).with_write_format(Format::Legacy);
        let sealed = ring.seal_in("token", &ctx(7)).unwrap();
        assert_eq!(sealed.nonce.len(), 12);
        assert_eq!(ring.open(&sealed).unwrap(), "token");
        assert_eq!(ring.open_in(&sealed, &ctx(9)).unwrap(), "token");
        // `seal` is v1 even under the v2 default.
        let default_ring = KeyRing::parse(&format!("k1:{}", key(1))).unwrap();
        let legacy = default_ring.seal("token").unwrap();
        assert_eq!(legacy.nonce.len(), 12);
        assert_eq!(default_ring.open_in(&legacy, &ctx(1)).unwrap(), "token");
    }

    #[test]
    fn a_v2_value_round_trips_and_is_told_by_its_nonce_length() {
        let ring = ring(&format!("k1:{}", key(1))).with_write_format(Format::V2);
        let a = ring.seal_in("whsec_secret", &ctx(7)).unwrap();
        let b = ring.seal_in("whsec_secret", &ctx(7)).unwrap();
        assert_eq!(a.nonce.len(), 13);
        assert_eq!(a.nonce[0], 2);
        assert_ne!(a.nonce, b.nonce);
        assert_eq!(a.key_id, "k1");
        assert_eq!(ring.open_in(&a, &ctx(7)).unwrap(), "whsec_secret");
        // `seal` stays v1 whatever the write format.
        assert_eq!(ring.seal("x").unwrap().nonce.len(), 12);
    }

    #[test]
    fn a_v2_value_does_not_open_in_another_row_column_or_table() {
        let ring = ring(&format!("k1:{}", key(1)));
        let sealed = ring.seal_as("token", &ctx(7), Format::V2).unwrap();
        assert_eq!(ring.open_in(&sealed, &ctx(7)).unwrap(), "token");
        assert_eq!(ring.open_in(&sealed, &ctx(8)), Err(OpenError::Failed));
        for other in [
            Context::new(Column::new("webhook_destinations", "other"), 7i64),
            Context::new(Column::new("other", "secret_ciphertext"), 7i64),
            Context::new(WEBHOOK_DESTINATION_SECRET, "7"),
            Context::new(
                WEBHOOK_DESTINATION_SECRET,
                RowId::bytes(&7i64.to_be_bytes()),
            ),
        ] {
            assert_eq!(ring.open_in(&sealed, &other), Err(OpenError::Failed));
        }
    }

    #[test]
    fn context_encodings_do_not_collide_across_field_boundaries() {
        let a = Context::new(Column::new("ab", "c"), 1i64);
        let b = Context::new(Column::new("a", "bc"), 1i64);
        assert_ne!(a.aad(), b.aad());
    }

    #[test]
    fn a_changed_v2_value_does_not_open() {
        let ring = ring(&format!("k1:{}", key(1)));
        let original = ring.seal_as("token", &ctx(7), Format::V2).unwrap();
        let mut sealed = original.clone();
        sealed.ciphertext[0] ^= 1;
        assert_eq!(ring.open_in(&sealed, &ctx(7)), Err(OpenError::Failed));
        sealed = original.clone();
        sealed.nonce[1] ^= 1;
        assert_eq!(ring.open_in(&sealed, &ctx(7)), Err(OpenError::Failed));
        // An unknown version byte, and a nonce of neither length.
        sealed = original.clone();
        sealed.nonce[0] = 3;
        assert_eq!(ring.open_in(&sealed, &ctx(7)), Err(OpenError::Failed));
        sealed = original.clone();
        sealed.nonce.pop();
        sealed.nonce.pop();
        assert_eq!(ring.open_in(&sealed, &ctx(7)), Err(OpenError::Failed));
        sealed = original;
        sealed.nonce.push(0);
        assert_eq!(ring.open_in(&sealed, &ctx(7)), Err(OpenError::Failed));
    }

    #[test]
    fn a_v2_value_is_not_a_v1_value_nor_the_reverse() {
        let ring = ring(&format!("k1:{}", key(1)));
        // A v2 ciphertext under a 12-byte nonce is read as v1 and fails; a
        // v1 ciphertext under a version byte is read as v2 and fails.
        let v2 = ring.seal_as("token", &ctx(7), Format::V2).unwrap();
        let as_v1 = Sealed {
            nonce: v2.nonce[1..].to_vec(),
            ..v2
        };
        assert_eq!(ring.open_in(&as_v1, &ctx(7)), Err(OpenError::Failed));
        let v1 = ring.seal("token").unwrap();
        let mut nonce = vec![2u8];
        nonce.extend(&v1.nonce);
        let as_v2 = Sealed { nonce, ..v1 };
        assert_eq!(ring.open_in(&as_v2, &ctx(7)), Err(OpenError::Failed));
    }

    #[test]
    fn v2_follows_key_rotation() {
        let old = ring(&format!("k1:{}", key(1)));
        let sealed_old = old.seal_as("token", &ctx(7), Format::V2).unwrap();
        let rotated = ring(&format!("k2:{},k1:{}", key(2), key(1))).with_write_format(Format::V2);
        assert_eq!(rotated.open_in(&sealed_old, &ctx(7)).unwrap(), "token");
        assert_eq!(rotated.seal_in("token", &ctx(7)).unwrap().key_id, "k2");
        let without = ring(&format!("k2:{}", key(2)));
        assert_eq!(
            without.open_in(&sealed_old, &ctx(7)),
            Err(OpenError::UnknownKey)
        );
    }

    #[test]
    fn legacy_opens_are_counted_by_purpose_and_start_at_zero() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let ring = ring(&format!("k1:{}", key(1)));
            let rendered = handle.render();
            for series in [
                r#"distant_signal_secret_box_legacy_opens_total{purpose="webhook_destinations.secret_ciphertext"} 0"#,
                r#"distant_signal_secret_box_legacy_opens_total{purpose="unscoped"} 0"#,
            ] {
                assert!(
                    rendered.contains(series),
                    "{series} missing from {rendered}"
                );
            }
            let v1 = ring.seal("token").unwrap();
            let v2 = ring.seal_as("token", &ctx(7), Format::V2).unwrap();
            ring.open_in(&v1, &ctx(7)).unwrap();
            ring.open_in(&v1, &ctx(7)).unwrap();
            ring.open_in(&v2, &ctx(7)).unwrap();
            ring.open(&v1).unwrap();
            assert!(ring.open_in(&v1, &ctx(7)).is_ok());
        });
        let rendered = handle.render();
        // Three scoped opens of v1; the v2 open and any failure do not count.
        assert!(
            rendered.contains(
                r#"distant_signal_secret_box_legacy_opens_total{purpose="webhook_destinations.secret_ciphertext"} 3"#
            ),
            "{rendered}"
        );
        assert!(
            rendered
                .contains(r#"distant_signal_secret_box_legacy_opens_total{purpose="unscoped"} 1"#),
            "{rendered}"
        );
    }
}
