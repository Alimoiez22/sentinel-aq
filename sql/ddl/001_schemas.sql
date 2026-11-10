-- Schemas (one per responsibility; see docs/ARCHITECTURE.md).
--   ops        operational metadata: manifest, checkpoints, incidents, audit
--   staging    one location-day of parsed rows per batch, replaced atomically
--   quarantine rejected rows with the rule that rejected them
--   wh         warehouse: dimensions, hourly fact, marts (published outputs)
--   golden     full rebuild from the latest raw revisions, for reconciliation
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS wh;
CREATE SCHEMA IF NOT EXISTS golden;
