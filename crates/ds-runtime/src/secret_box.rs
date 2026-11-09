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

use std::fmt;

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine as _;
use rand::RngCore;
use rand::rngs::OsRng;

/// The additional authenticated data of every value: a value encrypted for
/// one purpose does not decrypt as another.
const AAD: &[u8] = b"distant-signal secret-box v1";

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

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
        Ok(KeyRing { entries })
    }

    /// The id new values are sealed with.
    pub fn active_id(&self) -> &str {
        &self.entries[0].id
    }

    /// Encrypts `plaintext` with the active key and a fresh random nonce.
    pub fn seal(&self, plaintext: &str) -> Result<Sealed, OpenError> {
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
                    aad: AAD,
                },
            )
            .map_err(|_| OpenError::Failed)?;
        Ok(Sealed {
            ciphertext,
            nonce: nonce_bytes.to_vec(),
            key_id: entry.id.clone(),
        })
    }

    /// Decrypts a value with the key its id names.
    pub fn open(&self, sealed: &Sealed) -> Result<String, OpenError> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.id == sealed.key_id)
            .ok_or(OpenError::UnknownKey)?;
        if sealed.nonce.len() != NONCE_LEN {
            return Err(OpenError::Failed);
        }
        let nonce = Nonce::try_from(&sealed.nonce[..]).map_err(|_| OpenError::Failed)?;
        let plaintext = entry
            .cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &sealed.ciphertext,
                    aad: AAD,
                },
            )
            .map_err(|_| OpenError::Failed)?;
        String::from_utf8(plaintext).map_err(|_| OpenError::Failed)
    }
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
}
