-- Trading Automation Lab — read-only database role (Module 3)
--
-- Your owner role (screener) and the market_data database already exist: the
-- Postgres container created them from POSTGRES_USER / POSTGRES_DB back in 1.2.
-- This file adds a read-only role (claude_ro) that everything except your
-- loaders uses, so a wrong query in a read-only workflow cannot change data.
--
-- CHANGE the password below, then run this file as the screener (owner) role,
-- connected to the market_data database.

CREATE ROLE claude_ro LOGIN PASSWORD 'change-me-readonly-password';
GRANT CONNECT ON DATABASE market_data TO claude_ro;
GRANT USAGE ON SCHEMA public TO claude_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO claude_ro;

-- Make the read-only grant automatic for tables created later:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO claude_ro;
