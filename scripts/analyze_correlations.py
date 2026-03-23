"""
analyze_correlations.py - Statistical correlation analysis: sentiment vs price movements.
Joins analytics.sentiment_trends with staging.price_data, computes Pearson + Spearman
correlations with p-values to test whether sentiment predicts next-day price movements.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from scipy.stats import pearsonr, spearmanr
from datetime import datetime

DB_CONNECTION_STRING = 'postgresql://market_sentinel:market_sentinel_password@postgres:5432/market_sentinel'
TICKERS = ['AAPL', 'MSFT', 'GOOGL']
OUTPUT_DIR = 'analysis_results'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'correlation_analysis.csv')

CORRELATION_PAIRS = [
    ('net_sentiment',        'next_day_return',      'Net Sentiment vs Next-Day Return'),
    ('sentiment_volatility', 'price_volatility_7d',  'Sentiment Volatility vs Price Volatility 7D'),
    ('bull_bear_ratio',      'next_day_return',       'Bull/Bear Ratio vs Next-Day Return'),
    ('sentiment_ma_7d',      'next_day_return',       'Sentiment MA-7D vs Next-Day Return'),
]

SIG_LEVELS = [(0.001, '***'), (0.01, '**'), (0.05, '*'), (1.0, '')]


def sig_stars(p):
    for threshold, stars in SIG_LEVELS:
        if p < threshold:
            return stars
    return ''


def load_data(engine):
    sentiment_q = """
        SELECT ticker, date, net_sentiment, sentiment_volatility,
               bull_bear_ratio, sentiment_ma_7d
        FROM analytics.sentiment_trends
        ORDER BY ticker, date
    """
    price_q = """
        SELECT ticker, date, close
        FROM staging.price_data
        ORDER BY ticker, date
    """
    sentiment = pd.read_sql(sentiment_q, engine)
    prices = pd.read_sql(price_q, engine)
    return sentiment, prices


def build_analysis_df(sentiment, prices):
    # Compute daily return and next-day return
    prices = prices.copy()
    prices['daily_return'] = prices.groupby('ticker')['close'].pct_change() * 100
    prices['next_day_return'] = prices.groupby('ticker')['daily_return'].shift(-1)

    # Rolling 7-day price volatility (std of daily returns)
    prices['price_volatility_7d'] = (
        prices.groupby('ticker')['daily_return']
        .transform(lambda x: x.rolling(7, min_periods=3).std())
    )

    # Join sentiment (date) with price (date) on ticker + date
    df = pd.merge(
        sentiment,
        prices[['ticker', 'date', 'next_day_return', 'price_volatility_7d']],
        on=['ticker', 'date'],
        how='inner'
    )
    return df


def compute_correlations(data, label):
    """Return list of result dicts for all correlation pairs on the given DataFrame."""
    rows = []
    n_total = len(data)
    for x_col, y_col, pair_label in CORRELATION_PAIRS:
        subset = data[[x_col, y_col]].dropna()
        n = len(subset)
        if n < 5:
            rows.append({
                'group': label,
                'pair': pair_label,
                'pearson_r': np.nan,
                'pearson_p': np.nan,
                'spearman_r': np.nan,
                'spearman_p': np.nan,
                'n': n,
                'significance': 'insufficient data',
            })
            continue

        pr, pp = pearsonr(subset[x_col], subset[y_col])
        sr, sp = spearmanr(subset[x_col], subset[y_col])

        # Use the more conservative (higher) p-value for significance display
        min_p = min(pp, sp)
        rows.append({
            'group': label,
            'pair': pair_label,
            'pearson_r': pr,
            'pearson_p': pp,
            'spearman_r': sr,
            'spearman_p': sp,
            'n': n,
            'significance': sig_stars(min_p),
        })
    return rows


def print_table(results):
    col_widths = {
        'group':      12,
        'pair':       46,
        'pearson_r':   9,
        'pearson_p':   9,
        'spearman_r':  9,
        'spearman_p':  9,
        'n':           6,
        'sig':         5,
    }
    header = (
        f"{'Group':<{col_widths['group']}} "
        f"{'Metric Pair':<{col_widths['pair']}} "
        f"{'Pearson r':>{col_widths['pearson_r']}} "
        f"{'p-value':>{col_widths['pearson_p']}} "
        f"{'Spearman r':>{col_widths['spearman_r']}} "
        f"{'p-value':>{col_widths['spearman_p']}} "
        f"{'N':>{col_widths['n']}} "
        f"{'Sig':<{col_widths['sig']}}"
    )
    sep = '-' * len(header)
    print(sep)
    print(header)
    print(sep)
    for row in results:
        pr = f"{row['pearson_r']:.4f}"  if not np.isnan(row.get('pearson_r', float('nan'))) else 'N/A'
        pp = f"{row['pearson_p']:.4f}"  if not np.isnan(row.get('pearson_p', float('nan'))) else 'N/A'
        sr = f"{row['spearman_r']:.4f}" if not np.isnan(row.get('spearman_r', float('nan'))) else 'N/A'
        sp = f"{row['spearman_p']:.4f}" if not np.isnan(row.get('spearman_p', float('nan'))) else 'N/A'
        sig = row['significance']
        print(
            f"{row['group']:<{col_widths['group']}} "
            f"{row['pair']:<{col_widths['pair']}} "
            f"{pr:>{col_widths['pearson_r']}} "
            f"{pp:>{col_widths['pearson_p']}} "
            f"{sr:>{col_widths['spearman_r']}} "
            f"{sp:>{col_widths['spearman_p']}} "
            f"{row['n']:>{col_widths['n']}} "
            f"{sig:<{col_widths['sig']}}"
        )
    print(sep)
    print("Significance: * p<0.05   ** p<0.01   *** p<0.001")


def main():
    print('=' * 80)
    print(f"Correlation Analysis  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print('=' * 80)

    engine = create_engine(DB_CONNECTION_STRING)
    print("Loading data...")
    sentiment, prices = load_data(engine)
    print(f"  sentiment_trends: {len(sentiment)} rows | price_data: {len(prices)} rows")

    df = build_analysis_df(sentiment, prices)
    print(f"  Joined dataset:   {len(df)} rows after inner join")
    print(f"  Date range:       {df['date'].min()} to {df['date'].max()}")
    print()

    all_results = []

    # Per-ticker analysis
    for ticker in TICKERS:
        tdf = df[df['ticker'] == ticker]
        results = compute_correlations(tdf, ticker)
        all_results.extend(results)

    # Overall combined analysis
    overall = compute_correlations(df, 'OVERALL')
    all_results.extend(overall)

    # Print results table
    print_table(all_results)
    print()

    # Save CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_df = pd.DataFrame(all_results)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Results saved to: {OUTPUT_FILE}")

    # Quick interpretation
    sig_rows = [r for r in all_results if r['significance'] and r['group'] == 'OVERALL']
    print()
    if sig_rows:
        print("Statistically significant overall correlations:")
        for r in sig_rows:
            direction = 'positive' if r['pearson_r'] > 0 else 'negative'
            print(f"  {r['pair']}: r={r['pearson_r']:.3f} ({direction}), p={r['pearson_p']:.4f} {r['significance']}")
    else:
        print("No statistically significant overall correlations found.")
        print("Consider collecting more data to increase statistical power.")


if __name__ == '__main__':
    main()
