-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw sentiment logs (populated by Airflow)
CREATE TABLE IF NOT EXISTS staging.sentiment_logs (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    headline TEXT NOT NULL,
    url TEXT,
    source VARCHAR(100),
    published_at TIMESTAMP,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    positive_score FLOAT,
    negative_score FLOAT,
    neutral_score FLOAT
);

-- Stock price data
CREATE TABLE IF NOT EXISTS analytics.price_data (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    close_price FLOAT NOT NULL,
    volume BIGINT,
    
    UNIQUE(ticker, timestamp)
);

-- Create indexes for performance
CREATE INDEX idx_sentiment_logs_ticker_time ON staging.sentiment_logs(ticker, published_at);
CREATE INDEX idx_price_data_ticker_time ON analytics.price_data(ticker, timestamp);
