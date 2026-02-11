-- Add unique constraint to prevent duplicate sentiment records
ALTER TABLE staging.sentiment_logs 
ADD CONSTRAINT unique_ticker_headline UNIQUE (ticker, headline);
