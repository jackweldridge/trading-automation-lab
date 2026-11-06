-- Trading Automation Lab — database roles (Module 3)
-- Two roles: an owner that writes, and a read-only role everything else uses.
-- CHANGE both passwords before running. Run the first block as the superuser.

-- === Run as the default superuser ===
CREATE ROLE screener LOGIN PASSWORD 'change-me-owner-password';
CREATE DATABASE market_data OWNER screener;

-- === Now connect to the market_data database and run the rest ===
CREATE ROLE claude_ro LOGIN PASSWORD 'change-me-readonly-password';
GRANT CONNECT ON DATABASE market_data TO claude_ro;
GRANT USAGE ON SCHEMA public TO claude_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO claude_ro;

-- Make the read-only grant automatic for tables created later:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO claude_ro;
