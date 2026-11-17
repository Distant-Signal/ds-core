-- no-transaction
-- contract: rename the stations index (code stopped naming it in 0123abc; safe after core v0.2.0)
ALTER INDEX stations_crs RENAME TO stations_crs_code;
