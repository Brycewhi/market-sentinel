-- Staging model: Clean and enhance raw sentiment data

SELECT
    id,
    ticker,
    headline,
    positive_score,
    negative_score,
    neutral_score,
    published_at,
    created_at,
    
    -- Calculate net sentiment (ranges from -1 to +1)
    (positive_score - negative_score) as net_sentiment,
    
    -- Classify sentiment into categories
    CASE
        WHEN positive_score > 0.6 THEN 'bullish'
        WHEN negative_score > 0.6 THEN 'bearish'
        ELSE 'neutral'
    END as sentiment_label

FROM {{ source('staging', 'sentiment_logs') }}
