-- Adds the (nullable, already-validated-before-write) return path a user
-- was on when they clicked "Log in", so /auth/callback can send them back
-- there instead of always to SSO_POST_LOGIN_REDIRECT_URL. See
-- docs/superpowers/specs/2026-08-31-dynamic-post-login-redirect-design.md.
-- NULL means "no return path captured, or the client-supplied one failed
-- validation at insert time" -- both fall back to the existing static
-- default, unchanged from before this column existed.
ALTER TABLE oidc_login_state ADD COLUMN return_to TEXT;
