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

-- Trade journal (Module 5): one row per trade, written the moment it's placed.
-- Also used to record refused proposals (status = 'Rejected', with a reason).
CREATE TABLE IF NOT EXISTS trade_log (
  id            bigserial PRIMARY KEY,
  ticker        text NOT NULL,
  strategy      text,                    -- the reason / setup
  entry_price   double precision,
  initial_stop  double precision,        -- record where the stop STARTED
  target        double precision,
  shares        integer,
  risk_amount   double precision,        -- money at risk, in your account currency
  status        text DEFAULT 'Open',     -- Open / Closed / Rejected / Expired
  reject_reason text,                    -- set when a proposal is refused
  placed_at     timestamptz DEFAULT now()
);

-- 14-day ATR (average true range), used to size positions in Module 5.
-- This is the simple moving-average version; Wilder's smoothing is the
-- production refinement, but this is enough to size a trade from volatility.
CREATE OR REPLACE VIEW v_atr14 AS
WITH tr AS (
  SELECT
    ticker,
    date,
    greatest(
      high - low,
      abs(high - lag(close) OVER w),
      abs(low  - lag(close) OVER w)
    ) AS true_range
  FROM daily_prices
  WINDOW w AS (PARTITION BY ticker ORDER BY date)
)
SELECT
  ticker,
  date,
  avg(true_range) OVER (
    PARTITION BY ticker ORDER BY date
    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
  ) AS atr14
FROM tr;
