-- Daily sentiment aggregation per ticker

SELECT
    ticker,
    DATE(created_at) AS date,
    COUNT(*) AS article_count,
    AVG(positive_score - negative_score) AS net_sentiment,
    STDDEV(positive_score - negative_score) AS sentiment_volatility,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY positive_score - negative_score) AS sentiment_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY positive_score - negative_score) AS sentiment_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY positive_score - negative_score) AS sentiment_p75,
    SUM(CASE WHEN positive_score > 0.6 THEN 1 ELSE 0 END) AS bullish_count,
    SUM(CASE WHEN negative_score > 0.6 THEN 1 ELSE 0 END) AS bearish_count,
    SUM(CASE WHEN neutral_score > 0.5 THEN 1 ELSE 0 END) AS neutral_count,
    (1 - STDDEV(positive_score - negative_score)) AS sentiment_confidence

FROM {{ ref('stg_sentiment') }}

GROUP BY ticker, DATE(created_at)
HAVING COUNT(*) >= 3
