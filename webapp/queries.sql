-- Webapp queries (Module 7). Each section starts with "-- @@name"; build.py
-- runs each one with the READ-ONLY role and drops the result at the matching
-- @@name marker in template.html. Add your own sections as you grow the page.

-- @@positions
SELECT ticker, shares, entry_price, initial_stop, target
FROM trade_log
WHERE status = 'Open'
ORDER BY placed_at DESC;
