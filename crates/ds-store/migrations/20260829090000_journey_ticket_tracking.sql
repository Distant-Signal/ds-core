-- -------------------------------------------------------------------------
-- Journey ticket tracking: a user-entered (or best-effort auto-filled,
-- always user-reviewed-before-save) record that they had a ticket for a
-- specific tracked train, plus the data needed to derive a Delay Repay
-- eligibility estimate against that train's own TRUST-sourced delay data.
-- See docs/superpowers/specs/2026-08-29-journey-ticket-tracking-design.md
-- and docs/superpowers/plans/2026-08-29-journey-ticket-tracking.md.
--
-- LEGAL/PRIVACY AUDIT (see this plan's Global Constraints): this table
-- deliberately stores ONLY operator, ticket_type, origin_crs,
-- destination_crs, source, and timestamps/ownership. It must NEVER gain a
-- column for payment/price data, any barcode payload (raw or decoded), any
-- ITSO data, passenger name, or the uploaded .pkpass/PDF file itself.
-- Diff any future migration touching this table against this list before
-- merging it.
--
-- source: provenance, extending DESIGN.md's dataQuality philosophy (see
-- DESIGN.md's Data quality section) of never collapsing inferred data into
-- an unlabelled value. 'manual' is the only trustworthy-by-construction
-- source; 'pkpass-semantics' / 'pkpass-heuristic' / 'pdf-heuristic' are all
-- pre-fills the user reviewed and explicitly confirmed via a manual-entry
-- POST before this row existed -- confirmation, not the parse itself, is
-- what makes the row trustworthy. See this plan's Task 2/3.
-- -------------------------------------------------------------------------

CREATE TABLE tracked_train_tickets (
    id BIGSERIAL PRIMARY KEY,
    tracked_train_id BIGINT NOT NULL REFERENCES tracked_trains(id) ON DELETE CASCADE,

    -- Redundant with tracked_trains.user_id by construction (a ticket's
    -- owner is always the same user who owns the tracked train it's
    -- attached to -- see Task 2's create_ticket, which only ever writes
    -- user_id from the caller after Task 3's ownership check on
    -- tracked_train_id passes). Kept explicit so every ownership check on
    -- this table filters directly (WHERE user_id = $n) without a join.
    user_id TEXT NOT NULL REFERENCES users(id),

    operator     TEXT,  -- free text or a known operator code; not
                         -- validated against a hard catalogue in v1.
    ticket_type  TEXT,  -- e.g. "single", "return", "season", "advance" --
                         -- user-entered or auto-filled, never parsed from
                         -- a barcode.
    origin_crs       TEXT,
    destination_crs  TEXT,

    source TEXT NOT NULL DEFAULT 'manual'
        CHECK (source IN ('manual', 'pkpass-semantics', 'pkpass-heuristic', 'pdf-heuristic')),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX tracked_train_tickets_tracked_train ON tracked_train_tickets (tracked_train_id);

-- Supports every ticket route's ownership-scoped query (Task 2) filtering
-- directly on user_id, per this table's own header comment above.
CREATE INDEX tracked_train_tickets_user_id ON tracked_train_tickets (user_id);
