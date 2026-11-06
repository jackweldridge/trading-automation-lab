-- Trading Automation Lab — database schema (Module 3)
-- Run once, as the owner role (screener), against the market_data database.
-- Every table has a unique key so its loader is safe to re-run (upsert).

-- Prices: one row per ticker per day.
CREATE TABLE IF NOT EXISTS daily_prices (
  ticker          text NOT NULL,
  date            date NOT NULL,
  open            double precision,
  high            double precision,
  low             double precision,
  close           double precision,
  adjusted_close  double precision,
  volume          bigint,
  PRIMARY KEY (ticker, date)
);

-- Tradable universe: the symbols everything else iterates over.
CREATE TABLE IF NOT EXISTS universe (
  ticker       text PRIMARY KEY,
  name         text,
  exchange     text,
  type         text,
  is_delisted  boolean DEFAULT false
);

-- Fundamentals: one row per figure, per quarter.
-- value is NULL when a company did not report it — never store 0 for "unknown".
CREATE TABLE IF NOT EXISTS fundamentals_facts (
  ticker      text NOT NULL,
  metric      text NOT NULL,
  period_end  date NOT NULL,
  value       double precision,
  PRIMARY KEY (ticker, metric, period_end)
);

-- FX: a daily rate per currency pair (you hold USD, report in GBP).
CREATE TABLE IF NOT EXISTS fx_rate_daily (
  pair  text NOT NULL,
  date  date NOT NULL,
  rate  double precision,
  PRIMARY KEY (pair, date)
);

-- Earnings calendar: upcoming report dates.
CREATE TABLE IF NOT EXISTS earnings_calendar (
  ticker       text NOT NULL,
  report_date  date NOT NULL,
  PRIMARY KEY (ticker, report_date)
);
