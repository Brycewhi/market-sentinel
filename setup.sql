-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw sentiment logs
CREATE TABLE IF NOT EXISTS staging.sentiment_logs (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    headline TEXT,
    url TEXT,
    source VARCHAR(50),
    published_at TIMESTAMP,
    positive_score FLOAT,
    negative_score FLOAT,
    neutral_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Price data
CREATE TABLE IF NOT EXISTS analytics.price_data (
    ticker VARCHAR(10),
    timestamp TIMESTAMP,
    close_price FLOAT,
    volume BIGINT,
    PRIMARY KEY (ticker, timestamp)
);

-- Aggregated hourly sentiment
CREATE TABLE IF NOT EXISTS analytics.hourly_sentiment (
    ticker VARCHAR(10),
    hour TIMESTAMP,
    avg_sentiment FLOAT,
    sentiment_volatility FLOAT,
    article_count INT,
    PRIMARY KEY (ticker, hour)
);
