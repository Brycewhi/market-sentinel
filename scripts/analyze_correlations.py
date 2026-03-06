"""
analyze_correlations.py - Statistical analysis of sentiment-price correlations
"""

import pandas as pd
from sqlalchemy import create_engine
from scipy.stats import pearsonr
import numpy as np
from datetime import datetime

# Database connection
DB_CONNECTION_STRING = 'postgresql://market_sentinel:market_sentinel_password@postgres:5432/market_sentinel'

def analyze_correlations():
    """
    Analyze correlations between sentiment and price movements with statistical significance
    """
    print("="*80)
    print(f"Correlation Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    print()
    
    # Connect to database
    engine = create_engine(DB_CONNECTION_STRING)
    
    # Query the joined data
    query = """
    SELECT 
        ticker,
        date,
        avg_net_sentiment,
        sentiment_volatility,
        price_change_pct,
        article_count
    FROM analytics.sentiment_with_prices
    ORDER BY ticker, date
    """
    
    print("📊 Loading data from database...")
    df = pd.read_sql(query, engine)
    print(f"   Loaded {len(df)} rows of data")
    print()
    
    # Results storage
    results = []
    
    # Analyze each ticker
    for ticker in ['AAPL', 'MSFT', 'GOOGL']:
        print(f"\n{'='*80}")
        print(f"📈 {ticker} Analysis")
        print(f"{'='*80}")
        
        ticker_data = df[df['ticker'] == ticker].copy()
        n = len(ticker_data)
        
        if n < 3:
            print(f"   ⚠️  Insufficient data ({n} days) - need at least 3 days")
            continue
        
        print(f"   Sample size: {n} days")
        print(f"   Date range: {ticker_data['date'].min()} to {ticker_data['date'].max()}")
        print()
        
        # 1. Sentiment vs Price Change correlation
        print("   📊 Sentiment → Price Change Correlation:")
        r_sentiment_price, p_sentiment_price = pearsonr(
            ticker_data['avg_net_sentiment'], 
            ticker_data['price_change_pct']
        )
        
        print(f"      Correlation (r): {r_sentiment_price:.4f}")
        print(f"      P-value:         {p_sentiment_price:.4f}")
        print(f"      Significant:     {'✅ YES (p < 0.05)' if p_sentiment_price < 0.05 else '❌ NO (p >= 0.05)'}")
        
        # Interpret correlation strength
        strength = interpret_correlation(abs(r_sentiment_price))
        print(f"      Strength:        {strength}")
        print()
        
        # 2. Sentiment Volatility vs Absolute Price Change correlation
        print("   📊 Sentiment Volatility → Absolute Price Change Correlation:")
        r_vol_abs, p_vol_abs = pearsonr(
            ticker_data['sentiment_volatility'], 
            ticker_data['price_change_pct'].abs()
        )
        
        print(f"      Correlation (r): {r_vol_abs:.4f}")
        print(f"      P-value:         {p_vol_abs:.4f}")
        print(f"      Significant:     {'✅ YES (p < 0.05)' if p_vol_abs < 0.05 else '❌ NO (p >= 0.05)'}")
        
        strength_vol = interpret_correlation(abs(r_vol_abs))
        print(f"      Strength:        {strength_vol}")
        print()
        
        # 3. Summary statistics
        print("   📈 Sentiment Statistics:")
        print(f"      Mean sentiment:      {ticker_data['avg_net_sentiment'].mean():.4f}")
        print(f"      Sentiment std dev:   {ticker_data['avg_net_sentiment'].std():.4f}")
        print(f"      Mean volatility:     {ticker_data['sentiment_volatility'].mean():.4f}")
        print()
        
        print("   💰 Price Statistics:")
        print(f"      Mean price change:   {ticker_data['price_change_pct'].mean():.2f}%")
        print(f"      Price std dev:       {ticker_data['price_change_pct'].std():.2f}%")
        print(f"      Max gain:            {ticker_data['price_change_pct'].max():.2f}%")
        print(f"      Max loss:            {ticker_data['price_change_pct'].min():.2f}%")
        print()
        
        # Store results
        results.append({
            'ticker': ticker,
            'sample_size': n,
            'date_start': ticker_data['date'].min(),
            'date_end': ticker_data['date'].max(),
            'sentiment_price_correlation': r_sentiment_price,
            'sentiment_price_pvalue': p_sentiment_price,
            'sentiment_price_significant': p_sentiment_price < 0.05,
            'vol_abschange_correlation': r_vol_abs,
            'vol_abschange_pvalue': p_vol_abs,
            'vol_abschange_significant': p_vol_abs < 0.05,
            'mean_sentiment': ticker_data['avg_net_sentiment'].mean(),
            'mean_price_change_pct': ticker_data['price_change_pct'].mean(),
            'price_volatility': ticker_data['price_change_pct'].std()
        })
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    
    # Save to CSV
    output_file = 'correlation_analysis_results.csv'
    results_df.to_csv(output_file, index=False)
    print(f"\n{'='*80}")
    print(f"💾 Results saved to: {output_file}")
    print(f"{'='*80}")
    
    # Overall summary
    print(f"\n{'='*80}")
    print("📋 SUMMARY")
    print(f"{'='*80}")
    
    significant_correlations = results_df[results_df['sentiment_price_significant'] == True]
    
    if len(significant_correlations) > 0:
        print(f"\n✅ Statistically Significant Correlations Found:")
        for _, row in significant_correlations.iterrows():
            print(f"   • {row['ticker']}: r={row['sentiment_price_correlation']:.3f}, p={row['sentiment_price_pvalue']:.4f}")
    else:
        print(f"\n⚠️  No statistically significant correlations found (need p < 0.05)")
        print(f"   This could be due to small sample size - collect more data!")
    
    print(f"\n{'='*80}")
    print("✅ Analysis Complete!")
    print(f"{'='*80}\n")
    
    return results_df


def interpret_correlation(r):
    """Interpret correlation strength"""
    abs_r = abs(r)
    if abs_r >= 0.8:
        return "Very Strong"
    elif abs_r >= 0.6:
        return "Strong"
    elif abs_r >= 0.4:
        return "Moderate"
    elif abs_r >= 0.2:
        return "Weak"
    else:
        return "Very Weak"


if __name__ == "__main__":
    analyze_correlations()