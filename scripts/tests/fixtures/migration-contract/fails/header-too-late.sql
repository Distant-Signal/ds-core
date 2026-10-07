-- The contract header only counts in the leading comment block.
SET LOCAL lock_timeout = '5s';
-- contract: drop old_things (code stopped using it in 0123abc)
DROP TABLE old_things;
