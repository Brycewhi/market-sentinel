-- Hourly sentiment aggregation with volatility metrics
-- Groups by article publication time, not processing time

SELECT
    ticker,
    DATE_TRUNC('hour', published_at) as hour,
    
    -- Basic stats
    COUNT(*) as article_count,
    AVG(net_sentiment) as avg_sentiment,
    
    -- Volatility (standard deviation of sentiment)
    STDDEV(net_sentiment) as sentiment_volatility,
    
    -- Sentiment breakdown
    SUM(CASE WHEN sentiment_label = 'bullish' THEN 1 ELSE 0 END) as bullish_count,
    SUM(CASE WHEN sentiment_label = 'bearish' THEN 1 ELSE 0 END) as bearish_count,
    SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
    
    -- Extremes
    MAX(net_sentiment) as max_sentiment,
    MIN(net_sentiment) as min_sentiment

FROM {{ ref('stg_sentiment') }}
WHERE published_at IS NOT NULL
GROUP BY ticker, DATE_TRUNC('hour', published_at)
ORDER BY ticker, hour DESC
