-- Volatility analysis: rolling metrics and z-score normalization per ticker

WITH base AS (
    SELECT *
    FROM {{ ref('sentiment_with_prices') }}
),

windowed AS (
    SELECT
        *,
        -- 7-day rolling price volatility
        STDDEV(price_change_pct) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_price_volatility_7d,

        -- 7-day rolling average sentiment volatility
        AVG(sentiment_volatility) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_sentiment_volatility_7d,

        -- Z-score inputs: ticker-level mean and stddev for price_change_pct
        AVG(price_change_pct) OVER (PARTITION BY ticker) AS ticker_mean_price_chg,
        STDDEV(price_change_pct) OVER (PARTITION BY ticker) AS ticker_stddev_price_chg,

        -- Z-score inputs: ticker-level mean and stddev for sentiment_volatility
        AVG(sentiment_volatility) OVER (PARTITION BY ticker) AS ticker_mean_sent_vol,
        STDDEV(sentiment_volatility) OVER (PARTITION BY ticker) AS ticker_stddev_sent_vol,

        -- Row number to filter out first 6 days per ticker
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn
    FROM base
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
    rolling_price_volatility_7d,
    rolling_sentiment_volatility_7d,
    -- Z-score: price_change_pct
    (price_change_pct - ticker_mean_price_chg)
        / NULLIF(ticker_stddev_price_chg, 0) AS price_change_pct_zscore,
    -- Z-score: sentiment_volatility
    (sentiment_volatility - ticker_mean_sent_vol)
        / NULLIF(ticker_stddev_sent_vol, 0) AS sentiment_volatility_zscore
FROM windowed
WHERE rn >= 7  -- Only include rows with a full 7-day window
ORDER BY ticker, date
