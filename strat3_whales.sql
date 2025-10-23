/*
  QUERY FOR A GRAFANA "TABLE" PANEL (Strategy 3: Market Share by Position)
  
  Finds "whale positions" - users whose total volume on a
  *single side and outcome* (e.g., BUY on "Yes") is > 10%
  of the total market's volume.
*/

-- Step 1: Calculate the total volume for EACH USER,
-- per MARKET, per SIDE, and per OUTCOME.
WITH UserPositionVolume AS (
    SELECT
        proxy_wallet,
        slug, -- The market ID from 'trades'
        side,
        outcome,
        SUM(size * price) AS user_position_volume
    FROM
        trades
    WHERE
        $__timeFilter(trade_time) -- Filter for Grafana's time range
        AND size IS NOT NULL
        AND price IS NOT NULL
    GROUP BY
        proxy_wallet, slug, side, outcome
),

-- Step 2: Get the latest 'name' and 'pseudonym' for each user.
UserLatestDetails AS (
    SELECT DISTINCT ON (proxy_wallet)
        proxy_wallet,
        name,
        pseudonym
    FROM
        trades
    -- We must order by time DESC to get the *latest* info
    ORDER BY
        proxy_wallet, trade_time DESC
)

-- Step 3: Calculate percentage, filter for whales, and add all details
SELECT
    UPV.proxy_wallet,
    ULD.name,
    ULD.pseudonym,
    M.question AS market_name,
    UPV.slug AS market_slug,
    UPV.side,
    UPV.outcome,
    UPV.user_position_volume,
    M.volume_num AS total_market_volume, -- Using 'volume_num'
    
    -- This is the new market share calculation:
    (UPV.user_position_volume / M.volume_num) * 100.0 AS position_market_share_percent
FROM
    UserPositionVolume AS UPV
-- Join with 'markets' to get market name and total volume
JOIN
    markets AS M ON UPV.slug = M.market_slug
-- Join with 'UserLatestDetails' to get the user's name
JOIN
    UserLatestDetails AS ULD ON UPV.proxy_wallet = ULD.proxy_wallet
WHERE
    -- THIS IS YOUR WHALE THRESHOLD:
    (UPV.user_position_volume / M.volume_num) * 100.0 > 1.0 -- <-- SET YOUR THRESHOLD HERE (e.g., 10.0 = 10%)
    
    -- Avoid division by zero
    AND M.volume_num > 0 
ORDER BY
    position_market_share_percent DESC,
    user_position_volume DESC;