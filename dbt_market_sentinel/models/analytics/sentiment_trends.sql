-- Sentiment trend analysis with moving averages and momentum indicators

SELECT
    ticker,
    date,
    article_count,
    net_sentiment,
    sentiment_volatility,
    sentiment_confidence,
    bullish_count,
    bearish_count,
    neutral_count,
    sentiment_p25,
    sentiment_p50,
    sentiment_p75,

    -- Moving averages
    AVG(net_sentiment) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS sentiment_ma_3d,

    AVG(net_sentiment) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS sentiment_ma_7d,

    AVG(net_sentiment) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS sentiment_ma_30d,

    -- Momentum
    net_sentiment - LAG(net_sentiment, 1) OVER (PARTITION BY ticker ORDER BY date) AS sentiment_1d_change,
    net_sentiment - LAG(net_sentiment, 7) OVER (PARTITION BY ticker ORDER BY date) AS sentiment_7d_change,

    -- Bull/bear ratio
    CAST(bullish_count AS FLOAT) / NULLIF(bearish_count, 0) AS bull_bear_ratio,

    -- Volatility deviation from 30-day rolling average
    sentiment_volatility - AVG(sentiment_volatility) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS volatility_deviation

FROM {{ ref('daily_sentiment_summary') }}
