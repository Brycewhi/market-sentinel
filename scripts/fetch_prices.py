"""
Fetch daily stock price data using yfinance and store in PostgreSQL.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os

# Database connection - connects to the data postgres container
DB_USER = os.getenv('POSTGRES_USER', 'market_sentinel')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'airflow')
DB_HOST = 'postgres'  
DB_PORT = '5432'
DB_NAME = os.getenv('POSTGRES_DB', 'market_sentinel')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Tickers to track
TICKERS = ['AAPL', 'MSFT', 'GOOGL']


def fetch_price_data(ticker, days=30):
    """Fetch historical price data for a ticker."""
    print(f"Fetching price data for {ticker}...")
    
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=f"{days}d")
        
        if hist.empty:
            print(f"WARNING: No data returned for {ticker}")
            return None
        
        hist = hist.reset_index()
        hist['ticker'] = ticker
        
        price_df = hist[['Date', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
        price_df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        price_df['date'] = pd.to_datetime(price_df['date']).dt.date
        
        print(f"Successfully fetched {len(price_df)} days of data for {ticker}")
        return price_df
        
    except Exception as e:
        print(f"ERROR fetching data for {ticker}: {str(e)}")
        return None


def store_price_data(price_df, engine):
    """Store price data in PostgreSQL."""
    try:
        price_df.to_sql(
            name='price_data',
            schema='staging',
            con=engine,
            if_exists='append',
            index=False
        )
        print(f"Stored {len(price_df)} rows in database")
        
    except Exception as e:
        print(f"ERROR storing data: {str(e)}")
        raise


def main():
    """Main function."""
    print(f"\n{'='*50}")
    print(f"Starting price data fetch at {datetime.now()}")
    print(f"{'='*50}\n")
    
    engine = create_engine(DATABASE_URL)
    
    for ticker in TICKERS:
        price_df = fetch_price_data(ticker, days=30)
        
        if price_df is not None:
            store_price_data(price_df, engine)
        else:
            print(f"Skipping {ticker} due to fetch error")
        
        print()
    
    print(f"\n{'='*50}")
    print(f"Completed at {datetime.now()}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()