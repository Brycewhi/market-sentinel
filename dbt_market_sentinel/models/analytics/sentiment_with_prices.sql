-- Join daily sentiment with price data for correlation analysis.
-- Weekend/holiday articles are mapped to the next available trading day
-- (e.g., Saturday/Sunday news lands on Monday's open).

WITH daily_sentiment AS (
    SELECT
        ticker,
        DATE(published_at) as sentiment_date,
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

-- Map each sentiment date to the next available trading day in price_data
sentiment_mapped AS (
    SELECT
        s.*,
        (
            SELECT MIN(p.date)
            FROM {{ source('staging', 'price_data') }} p
            WHERE p.ticker = s.ticker AND p.date >= s.sentiment_date
        ) as trading_date
    FROM daily_sentiment s
),

-- Re-aggregate to trading day (multiple calendar days may map to the same trading day)
sentiment_by_trading_day AS (
    SELECT
        ticker,
        trading_date as date,
        AVG(avg_net_sentiment)    as avg_net_sentiment,
        AVG(sentiment_volatility) as sentiment_volatility,
        SUM(article_count)        as article_count,
        AVG(avg_positive)         as avg_positive,
        AVG(avg_negative)         as avg_negative,
        AVG(avg_neutral)          as avg_neutral
    FROM sentiment_mapped
    WHERE trading_date IS NOT NULL
    GROUP BY ticker, trading_date
),

price_changes AS (
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
FROM sentiment_by_trading_day s
INNER JOIN price_changes p
    ON s.ticker = p.ticker
    AND s.date = p.date
WHERE p.price_change_pct IS NOT NULL
ORDER BY s.ticker, s.date
