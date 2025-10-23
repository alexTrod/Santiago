/*
  This query first MERGES 'trades' and 'markets',
  then FIGURES OUT who the whales are relative
  to the market they traded in.
*/

-- Step 1: This subquery (CTE) runs first.
-- It finds the 90th percentile whale threshold
-- *for each market separately*.
WITH MarketThresholds AS (
    SELECT
        slug, -- The market ID from the 'trades' table
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY (size * price)) AS market_whale_threshold
    FROM
        trades -- This part only needs 'trades' to calculate trade values
    WHERE
        size IS NOT NULL
        AND price IS NOT NULL
        AND slug IS NOT NULL
    GROUP BY
        slug -- This makes it "market-relative"
)

/*
  Step 2: Now the main query.
  This is where we MERGE and FILTER.
*/
SELECT
    -- Data from 'trades' table:
    T.trade_time,
    T.proxy_wallet,
    T.side,
    (T.size * T.price) AS trade_value,
    -- Data from our subquery (for context):
    MT.market_whale_threshold,
    
    -- Data from 'markets' table (THE MERGE):
    M.question AS market_name,
    M.market_slug AS market_slug
FROM
    trades AS T
    
-- MERGE #1: trades -> markets
-- This links each trade to its market's details.
JOIN
    markets AS M ON T.slug = M.market_slug
    
-- MERGE #2: (trades+markets) -> thresholds
-- This links each trade to its market's WHALE THRESHOLD.
JOIN
    MarketThresholds AS MT ON T.slug = MT.slug
    
WHERE
    -- Filter for recent trades (Grafana macro)
    $__timeFilter(T.trade_time)
    
    -- Step 3: FIGURE OUT THE WHALES
    -- This compares the trade's value to its
    -- *own* market's specific threshold.
    AND (T.size * T.price) > MT.market_whale_threshold
    
ORDER BY
    T.trade_time DESC; -- Show newest whales first