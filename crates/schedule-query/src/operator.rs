//! Operator ids for the trip planner's connections and fixed links
//! (docs/superpowers/specs/2026-10-31-planner-operator-filtering-design.md,
//! §5.1 and §6.2).
//!
//! An operator is either a two-character ATOC code (`"VT"`, the CIF `BX`
//! record's, via `schedule_services.operator_atoc`) or one of the DS
//! operators that run no CIF trains ([`DsOperator`]: the DLR, London Trams,
//! Metrolink, NET). [`OperatorId`] packs either into a `u16` that is the
//! same on every date and in every graph, so a search checks a filter with
//! one array read ([`OperatorMask`]) and no per-date table:
//!
//! - an ATOC code `ab` (each of `0-9A-Z`) is `index(a) * 36 + index(b)`,
//!   below [`ATOC_SPACE`];
//! - a DS operator is `ATOC_SPACE + its index`;
//! - [`OperatorId::UNKNOWN`] is `u16::MAX`: no operator DS can name. It
//!   passes every filter (spec §5.1).

/// Ids below this are ATOC codes (`36 * 36`).
pub const ATOC_SPACE: u16 = 36 * 36;

/// The operators DS attributes that have no ATOC code of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DsOperator {
    Dlr,
    LondonTrams,
    Metrolink,
    Net,
}

impl DsOperator {
    /// Every DS operator, in id order.
    pub const ALL: [Self; 4] = [Self::Dlr, Self::LondonTrams, Self::Metrolink, Self::Net];

    /// The API spelling, `ds:DLR`.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Dlr => "ds:DLR",
            Self::LondonTrams => "ds:LONDON_TRAMS",
            Self::Metrolink => "ds:METROLINK",
            Self::Net => "ds:NET",
        }
    }

    /// The display name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Dlr => "DLR",
            Self::LondonTrams => "London Trams",
            Self::Metrolink => "Metrolink",
            Self::Net => "NET",
        }
    }

    /// The DS operator spelled `code` (`ds:DLR`, case-insensitive).
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|op| op.code().eq_ignore_ascii_case(code.trim()))
    }

    const fn index(self) -> u16 {
        match self {
            Self::Dlr => 0,
            Self::LondonTrams => 1,
            Self::Metrolink => 2,
            Self::Net => 3,
        }
    }
}

/// Ids at or above this are not operators (only [`OperatorId::UNKNOWN`]).
const DS_OPERATORS: u16 = 4;
const _: () = assert!(DsOperator::ALL.len() == DS_OPERATORS as usize);
const ID_SPACE: u16 = ATOC_SPACE + DS_OPERATORS;

/// A connection's or fixed link's operator. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OperatorId(u16);

impl Default for OperatorId {
    fn default() -> Self {
        Self::UNKNOWN
    }
}

fn atoc_index(byte: u8) -> Option<u16> {
    match byte {
        b'0'..=b'9' => Some(u16::from(byte - b'0')),
        b'A'..=b'Z' => Some(u16::from(byte - b'A') + 10),
        b'a'..=b'z' => Some(u16::from(byte - b'a') + 10),
        _ => None,
    }
}

fn atoc_char(index: u16) -> char {
    // `index` < 36 by construction.
    let byte = u8::try_from(index).unwrap_or(0);
    char::from(if byte < 10 {
        b'0' + byte
    } else {
        b'A' + byte - 10
    })
}

impl OperatorId {
    /// No operator DS can name: passes every filter.
    pub const UNKNOWN: Self = Self(u16::MAX);

    /// London Underground (`LT`): its CIF trains, and every Tube link,
    /// access ride and transit trip (spec §5.1).
    pub const LONDON_UNDERGROUND: Self = Self(21 * 36 + 29);

    /// The id of a two-character ATOC code (case-insensitive, trimmed);
    /// `None` for anything else.
    #[must_use]
    pub fn from_atoc(code: &str) -> Option<Self> {
        let bytes = code.trim().as_bytes();
        let [a, b] = bytes else {
            return None;
        };
        Some(Self(atoc_index(*a)? * 36 + atoc_index(*b)?))
    }

    /// The id of an ATOC code, or [`Self::UNKNOWN`] for none or a malformed
    /// one.
    #[must_use]
    pub fn from_atoc_or_unknown(code: Option<&str>) -> Self {
        code.and_then(Self::from_atoc).unwrap_or(Self::UNKNOWN)
    }

    /// The id of a DS operator.
    #[must_use]
    pub const fn ds(op: DsOperator) -> Self {
        Self(ATOC_SPACE + op.index())
    }

    /// An ATOC code or a `ds:` code.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        DsOperator::from_code(code)
            .map(Self::ds)
            .or_else(|| Self::from_atoc(code))
    }

    #[must_use]
    pub const fn is_unknown(self) -> bool {
        self.0 >= ID_SPACE
    }

    /// Whether this is a DS operator (no CIF trains).
    #[must_use]
    pub fn ds_operator(self) -> Option<DsOperator> {
        if (ATOC_SPACE..ID_SPACE).contains(&self.0) {
            DsOperator::ALL
                .get(usize::from(self.0 - ATOC_SPACE))
                .copied()
        } else {
            None
        }
    }

    /// The code as the API spells it: `"VT"`, `"ds:DLR"`; `None` for
    /// [`Self::UNKNOWN`].
    #[must_use]
    pub fn code(self) -> Option<String> {
        if self.0 < ATOC_SPACE {
            Some(
                [atoc_char(self.0 / 36), atoc_char(self.0 % 36)]
                    .iter()
                    .collect(),
            )
        } else {
            self.ds_operator().map(|op| op.code().to_owned())
        }
    }

    /// The raw id, an index into an [`OperatorMask`].
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// A set of operators, one bit per [`OperatorId`] (about 160 bytes).
#[derive(Clone, PartialEq, Eq)]
pub struct OperatorMask([u64; (ID_SPACE as usize).div_ceil(64)]);

impl std::fmt::Debug for OperatorMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.codes()).finish()
    }
}

impl Default for OperatorMask {
    fn default() -> Self {
        Self([0; (ID_SPACE as usize).div_ceil(64)])
    }
}

impl OperatorMask {
    /// The mask holding `ids` ([`OperatorId::UNKNOWN`] is ignored).
    #[must_use]
    pub fn of(ids: impl IntoIterator<Item = OperatorId>) -> Self {
        let mut mask = Self::default();
        for id in ids {
            mask.insert(id);
        }
        mask
    }

    pub fn insert(&mut self, id: OperatorId) {
        if !id.is_unknown() {
            self.0[id.index() / 64] |= 1 << (id.index() % 64);
        }
    }

    /// Whether `id` is in the set ([`OperatorId::UNKNOWN`] never is).
    #[must_use]
    #[inline]
    pub fn contains(&self, id: OperatorId) -> bool {
        self.0
            .get(id.index() / 64)
            .is_some_and(|word| word & (1 << (id.index() % 64)) != 0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|word| *word == 0)
    }

    /// The codes in the set, in id order.
    #[must_use]
    pub fn codes(&self) -> Vec<String> {
        (0..ID_SPACE)
            .map(OperatorId)
            .filter(|id| self.contains(*id))
            .filter_map(OperatorId::code)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atoc_codes_round_trip() {
        for code in ["VT", "GR", "LT", "TW", "QC", "LF", "00", "ZZ", "A9"] {
            let id = OperatorId::from_atoc(code).unwrap();
            assert!(!id.is_unknown(), "{code}");
            assert_eq!(id.code().as_deref(), Some(code));
        }
        assert_eq!(OperatorId::from_atoc(" vt "), OperatorId::from_atoc("VT"));
        assert_eq!(
            OperatorId::from_atoc("LT"),
            Some(OperatorId::LONDON_UNDERGROUND)
        );
    }

    #[test]
    fn malformed_atoc_codes_are_none() {
        for code in ["", "V", "VTX", "V-", "é1"] {
            assert_eq!(OperatorId::from_atoc(code), None, "{code}");
        }
        assert_eq!(
            OperatorId::from_atoc_or_unknown(Some("V")),
            OperatorId::UNKNOWN
        );
        assert_eq!(OperatorId::from_atoc_or_unknown(None), OperatorId::UNKNOWN);
    }

    #[test]
    fn ds_operators_have_their_own_ids() {
        for op in DsOperator::ALL {
            let id = OperatorId::ds(op);
            assert!(!id.is_unknown());
            assert_eq!(id.ds_operator(), Some(op));
            assert_eq!(id.code().as_deref(), Some(op.code()));
            assert_eq!(OperatorId::from_code(op.code()), Some(id));
            assert_eq!(OperatorId::from_code(&op.code().to_lowercase()), Some(id));
        }
        assert_eq!(OperatorId::from_atoc("VT").unwrap().ds_operator(), None);
        assert_eq!(OperatorId::from_code("ds:NOPE"), None);
    }

    #[test]
    fn unknown_is_never_in_a_mask() {
        assert!(OperatorId::UNKNOWN.is_unknown());
        assert_eq!(OperatorId::UNKNOWN.code(), None);
        assert_eq!(OperatorId::default(), OperatorId::UNKNOWN);
        let mut mask = OperatorMask::of([OperatorId::UNKNOWN]);
        assert!(mask.is_empty());
        assert!(!mask.contains(OperatorId::UNKNOWN));
        mask.insert(OperatorId::from_atoc("VT").unwrap());
        assert!(mask.contains(OperatorId::from_atoc("VT").unwrap()));
        assert!(!mask.contains(OperatorId::from_atoc("GR").unwrap()));
    }

    #[test]
    fn mask_lists_its_codes_in_id_order() {
        let mask = OperatorMask::of(
            ["VT", "ds:DLR", "GR", "LT"]
                .into_iter()
                .map(|c| OperatorId::from_code(c).unwrap()),
        );
        assert_eq!(mask.codes(), ["GR", "LT", "VT", "ds:DLR"]);
    }
}
