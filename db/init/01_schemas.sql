-- Runs once, on the first start with an empty data volume.
CREATE SCHEMA IF NOT EXISTS raw;   -- API responses as loaded, one table per endpoint
CREATE SCHEMA IF NOT EXISTS core;  -- cleaned and joined tables (created by transformations)
CREATE SCHEMA IF NOT EXISTS meta;  -- pipeline bookkeeping (load log)
