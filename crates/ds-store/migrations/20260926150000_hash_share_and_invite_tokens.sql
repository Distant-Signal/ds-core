-- ---------------------------------------------------------------------
-- 2026-09-26 review, L14: unlisted_links.token and group_invite_links.token
-- stored bearer secrets in the clear, unlike sessions.id (SHA-256 at rest,
-- auth::hash_session_token). Anyone with read access to this database --
-- a backup, a replica, a leaked dump -- could use every live share and
-- invite link.
--
-- Both columns are renamed to token_hash and every existing row is
-- converted to SHA-256(token) as lowercase hex -- byte-for-byte what
-- auth::hash_session_token (and so unlisted_links::hash_link_token) produces
-- for the same token: Postgres's sha256() over the token's UTF-8 bytes,
-- encode(..., 'hex') being lowercase just like Rust's `{:x}`.
--
-- Migrated in place rather than invalidated: every link a user has
-- already sent someone keeps working (the recipient's plaintext token
-- hashes to exactly the stored value), and the plaintext is gone from the
-- table the moment this commits -- the security goal is met with zero
-- disruption to recipients. The only visible change is to OWNERS: an
-- already-existing link's URL can no longer be displayed again (only its
-- existence and expiry), so they regenerate to get a new copyable one.
-- Invalidating everything would have broken every outstanding link for no
-- additional security benefit.
--
-- The rename keeps each table's PRIMARY KEY (constraint and its index move
-- with the column), and no new index is built, so this is catalog-only
-- DDL plus a row rewrite of two small tables -- no
-- CREATE INDEX CONCURRENTLY/no-transaction concern (see
-- crates/api/tests/migration_index_locking.rs).
-- ---------------------------------------------------------------------

ALTER TABLE unlisted_links RENAME COLUMN token TO token_hash;
UPDATE unlisted_links SET token_hash = encode(sha256(convert_to(token_hash, 'UTF8')), 'hex');

ALTER TABLE group_invite_links RENAME COLUMN token TO token_hash;
UPDATE group_invite_links SET token_hash = encode(sha256(convert_to(token_hash, 'UTF8')), 'hex');
