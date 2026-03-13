-- Trading signal generator based on sentiment and volatility thresholds

WITH base AS (
    SELECT * FROM {{ ref('sentiment_with_prices') }}
),

signals AS (
    SELECT
        *,
        CASE
            WHEN avg_net_sentiment > 0.15 AND sentiment_volatility < 0.4 THEN 'BUY'
            WHEN avg_net_sentiment < -0.1 THEN 'SELL'
            ELSE 'HOLD'
        END AS signal,
        CASE
            WHEN avg_net_sentiment > 0.15 AND sentiment_volatility < 0.4
                THEN avg_net_sentiment - 0.15  -- distance above BUY threshold
            WHEN avg_net_sentiment < -0.1
                THEN ABS(avg_net_sentiment + 0.1)  -- distance below SELL threshold
            ELSE 0.0
        END AS signal_strength
    FROM base
),

with_next_day AS (
    SELECT
        *,
        LEAD(price_change_pct) OVER (PARTITION BY ticker ORDER BY date) AS next_day_return
    FROM signals
)

SELECT
    ticker,
    date,
    avg_net_sentiment,
    sentiment_volatility,
    article_count,
    avg_positive,
    avg_negative,
    avg_neutral,
    open,
    high,
    low,
    close_price,
    volume,
    prev_close,
    price_change,
    price_change_pct,
    signal,
    signal_strength,
    next_day_return
FROM with_next_day
ORDER BY ticker, date
