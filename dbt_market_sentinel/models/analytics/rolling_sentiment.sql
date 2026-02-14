-- Rolling statistics: Moving averages and z-scores for trend analysis

SELECT
    ticker,
    hour,
    article_count,
    avg_sentiment,
    sentiment_volatility,
    bullish_count,
    bearish_count,
    neutral_count,
    
    -- 7-hour moving average (smooths noise, reveals trends)
    AVG(avg_sentiment) OVER (
        PARTITION BY ticker 
        ORDER BY hour 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as sentiment_7hr_ma,
    
    -- 24-hour moving average (daily trend)
    AVG(avg_sentiment) OVER (
        PARTITION BY ticker 
        ORDER BY hour 
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) as sentiment_24hr_ma,
    
    -- Z-score (normalize across tickers for comparison)
    (avg_sentiment - AVG(avg_sentiment) OVER (PARTITION BY ticker)) 
    / NULLIF(STDDEV(avg_sentiment) OVER (PARTITION BY ticker), 0)
    as sentiment_zscore,
    
    -- Trend indicator (is sentiment improving or declining?)
    avg_sentiment - LAG(avg_sentiment, 1) OVER (
        PARTITION BY ticker 
        ORDER BY hour
    ) as sentiment_change_1hr,
    
    avg_sentiment - LAG(avg_sentiment, 7) OVER (
        PARTITION BY ticker 
        ORDER BY hour
    ) as sentiment_change_7hr

FROM {{ ref('hourly_sentiment_agg') }}
ORDER BY ticker, hour DESC
