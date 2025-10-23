Here is a detailed, step-by-step explanation of how that SQL query works to find "whale positions."

### The Overall Goal 🎯

The query's goal is to find **concentrated bets** that are large relative to the *entire* market.

We are not just looking for a user who traded a lot. We are looking for a user who put a *significant* amount of money on a *single outcome* (e.g., "User 0x123 spent $50,000 on 'YES'"). We then compare that $50,000 to the *total volume of the entire market* (e.g., $200,000) to see if it's a "whale position" (in this case, 25% of the market).

The query is structured using **Common Table Expressions (CTEs)**, which are like temporary lookup tables. It builds two of them first, then runs the main query.

Here is the breakdown, following the order of execution.

-----

### Step 1: `WITH UserPositionVolume AS (...)`

This is the first and most important sub-query. Its job is to **aggregate all trades into specific user positions**.

```sql
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
)
```

  * **`FROM trades`**: It starts by looking at your `trades` table.
  * **`WHERE $__timeFilter(trade_time)`**: It immediately filters out all trades that are *not* in the time range you've selected on your Grafana dashboard.
  * **`GROUP BY proxy_wallet, slug, side, outcome`**: This is the key. It "buckets" all the trades. All trades by the same user, in the same market, on the same side, for the same outcome are grouped together.
  * **`SUM(size * price) AS user_position_volume`**: For each of these tiny buckets, it calculates the total value (`size * price`) and `SUM`s it up.

**Result:** This creates a temporary table `UserPositionVolume` that looks like this:

| proxy\_wallet | slug | side | outcome | user\_position\_volume |
| :--- | :--- | :--- | :--- | :--- |
| '0x123' | 'btc-price' | 'BUY' | 'Up' | 150000.00 |
| '0x123' | 'btc-price' | 'SELL' | 'Up' | 25000.00 |
| '0x456' | 'btc-price' | 'BUY' | 'Up' | 50000.00 |
| '0x123' | 'rain-market' | 'BUY' | 'Yes' | 1200.00 |

-----

### Step 2: `WITH UserLatestDetails AS (...)`

This is a helper query. Its only job is to **find the most recent `name` and `pseudonym` for every user**.

```sql
UserLatestDetails AS (
    SELECT DISTINCT ON (proxy_wallet)
        proxy_wallet,
        name,
        pseudonym
    FROM
        trades
    ORDER BY
        proxy_wallet, trade_time DESC
)
```

  * **`SELECT DISTINCT ON (proxy_wallet)`**: This is a PostgreSQL trick. It says "for every unique `proxy_wallet`, give me only the *first* row you find."
  * **`ORDER BY proxy_wallet, trade_time DESC`**: This is how we define "first." We sort the list so that a user's *newest* trades appear at the top.

**Result:** This creates a simple temporary lookup table `UserLatestDetails` for user info:

| proxy\_wallet | name | pseudonym |
| :--- | :--- | :--- |
| '0x123' | 'Bob' | 'CryptoBob' |
| '0x456' | 'Alice' | 'TraderAlice' |

-----

### Step 3: The Main Query (Joining and Filtering)

This is the final part that brings everything together.

```sql
SELECT
    UPV.proxy_wallet,
    ULD.name,
    ULD.pseudonym,
    M.question AS market_name,
    UPV.slug AS market_slug,
    UPV.side,
    UPV.outcome,
    UPV.user_position_volume,
    M.volume_num AS total_market_volume,
    (UPV.user_position_volume / M.volume_num) * 100.0 AS position_market_share_percent
FROM
    UserPositionVolume AS UPV
JOIN
    markets AS M ON UPV.slug = M.market_slug
JOIN
    UserLatestDetails AS ULD ON UPV.proxy_wallet = ULD.proxy_wallet
WHERE
    (UPV.user_position_volume / M.volume_num) * 100.0 > 10.0
    AND M.volume_num > 0 
ORDER BY
    position_market_share_percent DESC,
    user_position_volume DESC;
```

1.  **`FROM UserPositionVolume AS UPV`**: It starts with the "position" data we built in Step 1.
2.  **`JOIN markets AS M ...`**: It **merges** this data with the `markets` table. Now, each position row also has the market's `question` and (most importantly) its `volume_num` (total market volume).
3.  **`JOIN UserLatestDetails AS ULD ...`**: It **merges** again with the user info from Step 2. Now, each position row also has the user's `name` and `pseudonym`.

**At this point, just before the `WHERE` clause, our data looks like this:**

| proxy\_wallet | side | outcome | user\_position\_volume | market\_name | total\_market\_volume | name |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| '0x123' | 'BUY' | 'Up' | 150000.00 | "Bitcoin up?" | 500000.00 | 'Bob' |
| '0x123' | 'SELL' | 'Up' | 25000.00 | "Bitcoin up?" | 500000.00 | 'Bob' |
| '0x456' | 'BUY' | 'Up' | 50000.00 | "Bitcoin up?" | 500000.00 | 'Alice' |
| '0x123' | 'BUY' | 'Yes' | 1200.00 | "Will it rain?" | 2000.00 | 'Bob' |

-----

### Step 4: The `WHERE` Clause (This is how it finds the whale\!)

This is the final filter. It calculates the market share for each row and *discards* any that aren't big enough.

```sql
WHERE
    (UPV.user_position_volume / M.volume_num) * 100.0 > 10.0 -- Your 10% threshold
    AND M.volume_num > 0 -- Safety check
```

Let's test our rows:

  * **Row 1 (Bob's BUY):** `(150000.00 / 500000.00) * 100.0 = 30.0%`. **This is \> 10.0.** ✅ **(This is a whale position\!)**
  * **Row 2 (Bob's SELL):** `(25000.00 / 500000.00) * 100.0 = 5.0%`. This is not \> 10.0. ❌ (Filtered out)
  * **Row 3 (Alice's BUY):** `(50000.00 / 500000.00) * 100.0 = 10.0%`. This is not *strictly* \> 10.0. ❌ (Filtered out)
  * **Row 4 (Bob's rain bet):** `(1200.00 / 2000.00) * 100.0 = 60.0%`. **This is \> 10.0.** ✅ **(This is also a whale position\!)**

-----

### Step 5: Final `SELECT` and `ORDER BY`

The query now takes the two rows that passed the test (`✅`) and selects the columns you asked for, sorting them to put the highest percentage at the top.

**Final Output in your Grafana Table:**

| proxy\_wallet | name | pseudonym | market\_name | market\_slug | side | outcome | user\_position\_volume | total\_market\_volume | position\_market\_share\_percent |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| '0x123' | 'Bob' | 'CryptoBob' | "Will it rain?" | 'rain-market' | 'BUY' | 'Yes' | 1200.00 | 2000.00 | 60.0 |
| '0x123' | 'Bob' | 'CryptoBob' | "Bitcoin up?" | 'btc-price' | 'BUY' | 'Up' | 150000.00 | 500000.00 | 30.0 |