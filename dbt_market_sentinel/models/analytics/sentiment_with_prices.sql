-- Join daily sentiment with price data for correlation analysis

WITH daily_sentiment AS (
    -- Aggregate sentiment to daily level (multiple articles per day)
    SELECT
        ticker,
        DATE(published_at) as date,
        AVG(positive_score - negative_score) as avg_net_sentiment,
        STDDEV(positive_score - negative_score) as sentiment_volatility,
        COUNT(*) as article_count,
        AVG(positive_score) as avg_positive,
        AVG(negative_score) as avg_negative,
        AVG(neutral_score) as avg_neutral
    FROM {{ source('staging', 'sentiment_logs') }}
    WHERE published_at IS NOT NULL
    GROUP BY ticker, DATE(published_at)
),

price_changes AS (
    -- Calculate daily price changes
    SELECT
        ticker,
        date,
        open,
        high,
        low,
        close as close_price,
        volume,
        LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close,
        (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) as price_change,
        ((close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) 
         / NULLIF(LAG(close) OVER (PARTITION BY ticker ORDER BY date), 0) * 100) as price_change_pct
    FROM {{ source('staging', 'price_data') }}
)

-- Final join
SELECT
    s.ticker,
    s.date,
    s.avg_net_sentiment,
    s.sentiment_volatility,
    s.article_count,
    s.avg_positive,
    s.avg_negative,
    s.avg_neutral,
    p.open,
    p.high,
    p.low,
    p.close_price,
    p.volume,
    p.prev_close,
    p.price_change,
    p.price_change_pct
FROM daily_sentiment s
INNER JOIN price_changes p
    ON s.ticker = p.ticker
    AND s.date = p.date
WHERE p.price_change_pct IS NOT NULL  -- Filter out first day (no previous price)
ORDER BY s.ticker, s.date