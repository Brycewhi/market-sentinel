"""
analyze_correlations.py - Statistical correlation analysis: sentiment vs price movements.
Joins analytics.sentiment_trends with staging.price_data, computes Pearson + Spearman
correlations with p-values to test whether sentiment predicts next-day price movements.

Enhanced with lag analysis (1d, 3d, 7d) and seaborn correlation heatmaps.
"""

import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
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

SENTIMENT_COLS = ['net_sentiment', 'sentiment_volatility', 'bull_bear_ratio', 'sentiment_ma_7d']
PRICE_COLS     = ['next_day_return', 'price_volatility_7d', 'volume_change_pct']

LAG_DAYS = [1, 3, 7]

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
        SELECT ticker, date, close, volume
        FROM staging.price_data
        ORDER BY ticker, date
    """
    sentiment = pd.read_sql(sentiment_q, engine)
    prices = pd.read_sql(price_q, engine)
    return sentiment, prices


def build_analysis_df(sentiment, prices):
    prices = prices.copy()
    prices['daily_return'] = prices.groupby('ticker')['close'].pct_change() * 100
    prices['next_day_return'] = prices.groupby('ticker')['daily_return'].shift(-1)

    prices['price_volatility_7d'] = (
        prices.groupby('ticker')['daily_return']
        .transform(lambda x: x.rolling(7, min_periods=3).std())
    )

    prices['volume_change_pct'] = (
        prices.groupby('ticker')['volume']
        .transform(lambda x: x.pct_change() * 100)
    )

    df = pd.merge(
        sentiment,
        prices[['ticker', 'date', 'next_day_return', 'price_volatility_7d', 'volume_change_pct']],
        on=['ticker', 'date'],
        how='inner'
    )
    return df


def compute_correlations(data, label):
    """Return list of result dicts for all correlation pairs on the given DataFrame."""
    rows = []
    for x_col, y_col, pair_label in CORRELATION_PAIRS:
        subset = data[[x_col, y_col]].dropna()
        n = len(subset)
        if n < 5:
            rows.append({
                'ticker': label, 'metric_x': x_col, 'metric_y': y_col,
                'lag_days': 0, 'pair': pair_label,
                'pearson_r': np.nan, 'pearson_p': np.nan,
                'spearman_r': np.nan, 'spearman_p': np.nan,
                'n': n, 'significance': 'insufficient data',
            })
            continue

        pr, pp = pearsonr(subset[x_col], subset[y_col])
        sr, sp = spearmanr(subset[x_col], subset[y_col])
        min_p = min(pp, sp)
        rows.append({
            'ticker': label, 'metric_x': x_col, 'metric_y': y_col,
            'lag_days': 0, 'pair': pair_label,
            'pearson_r': pr, 'pearson_p': pp,
            'spearman_r': sr, 'spearman_p': sp,
            'n': n, 'significance': sig_stars(min_p),
        })
    return rows


def analyze_lags(df):
    """
    For each lag period (1, 3, 7 days), compute sentiment[day_t] vs price_return[day_t+lag].
    Returns list of result dicts with ticker, metric_x, metric_y, lag_days, correlations.
    """
    rows = []
    sentiment_metrics = ['net_sentiment', 'sentiment_volatility', 'bull_bear_ratio', 'sentiment_ma_7d']

    print("\nLag Analysis")
    print("=" * 80)

    for lag in LAG_DAYS:
        lag_rows_all = []
        for ticker in TICKERS:
            tdf = df[df['ticker'] == ticker].copy().sort_values('date')
            # future return at t+lag relative to sentiment at t
            tdf[f'future_return_{lag}d'] = tdf['next_day_return'].shift(-lag + 1)

            for metric in sentiment_metrics:
                subset = tdf[[metric, f'future_return_{lag}d']].dropna()
                n = len(subset)
                if n < 5:
                    rows.append({
                        'ticker': ticker, 'metric_x': metric,
                        'metric_y': f'return_lag{lag}d', 'lag_days': lag,
                        'pearson_r': np.nan, 'pearson_p': np.nan,
                        'spearman_r': np.nan, 'spearman_p': np.nan,
                        'n': n, 'significance': 'insufficient data',
                    })
                    continue

                pr, pp = pearsonr(subset[metric], subset[f'future_return_{lag}d'])
                sr, sp = spearmanr(subset[metric], subset[f'future_return_{lag}d'])
                sig = sig_stars(min(pp, sp))
                rows.append({
                    'ticker': ticker, 'metric_x': metric,
                    'metric_y': f'return_lag{lag}d', 'lag_days': lag,
                    'pearson_r': pr, 'pearson_p': pp,
                    'spearman_r': sr, 'spearman_p': sp,
                    'n': n, 'significance': sig,
                })
                if sig:
                    lag_rows_all.append((ticker, metric, pr, pp, sr, sp, sig))

        # Overall aggregate for this lag
        agg = df.copy().sort_values(['ticker', 'date'])
        agg[f'future_return_{lag}d'] = agg.groupby('ticker')['next_day_return'].shift(-lag + 1)

        for metric in sentiment_metrics:
            subset = agg[[metric, f'future_return_{lag}d']].dropna()
            n = len(subset)
            if n < 5:
                rows.append({
                    'ticker': 'OVERALL', 'metric_x': metric,
                    'metric_y': f'return_lag{lag}d', 'lag_days': lag,
                    'pearson_r': np.nan, 'pearson_p': np.nan,
                    'spearman_r': np.nan, 'spearman_p': np.nan,
                    'n': n, 'significance': 'insufficient data',
                })
                continue

            pr, pp = pearsonr(subset[metric], subset[f'future_return_{lag}d'])
            sr, sp = spearmanr(subset[metric], subset[f'future_return_{lag}d'])
            sig = sig_stars(min(pp, sp))
            rows.append({
                'ticker': 'OVERALL', 'metric_x': metric,
                'metric_y': f'return_lag{lag}d', 'lag_days': lag,
                'pearson_r': pr, 'pearson_p': pp,
                'spearman_r': sr, 'spearman_p': sp,
                'n': n, 'significance': sig,
            })
            if sig:
                lag_rows_all.append(('OVERALL', metric, pr, pp, sr, sp, sig))

        # Print summary for this lag
        print(f"\n  Lag {lag}d — significant findings:")
        if lag_rows_all:
            for ticker, metric, pr, pp, sr, sp, sig in lag_rows_all:
                direction = 'positive' if pr > 0 else 'negative'
                print(f"    {ticker:8s}  {metric:<24s}  r={pr:+.4f} ({direction})  p={pp:.4f} {sig}")
        else:
            print(f"    None (all p > 0.05)")

    return rows


def build_heatmap_matrix(data, sentiment_cols, price_cols):
    """Build a (len(price_cols) x len(sentiment_cols)) correlation matrix."""
    matrix = pd.DataFrame(index=price_cols, columns=sentiment_cols, dtype=float)
    for s_col in sentiment_cols:
        for p_col in price_cols:
            subset = data[[s_col, p_col]].dropna()
            if len(subset) < 5:
                matrix.loc[p_col, s_col] = np.nan
            else:
                r, _ = pearsonr(subset[s_col], subset[p_col])
                matrix.loc[p_col, s_col] = r
    return matrix


def plot_heatmap(matrix, title, filepath):
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(
        matrix.astype(float),
        ax=ax,
        annot=True,
        fmt='.2f',
        cmap='RdYlGn_r',
        vmin=-1, vmax=1, center=0,
        linewidths=0.5,
        linecolor='#cccccc',
        cbar_kws={'label': 'Pearson r'},
    )
    ax.set_title(title, fontsize=14, fontweight='bold', pad=16)
    ax.set_xlabel('Sentiment Metrics', fontsize=11)
    ax.set_ylabel('Price Metrics', fontsize=11)
    ax.tick_params(axis='x', labelsize=9, rotation=20)
    ax.tick_params(axis='y', labelsize=9, rotation=0)
    plt.tight_layout()
    fig.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {filepath}")


def generate_heatmaps(df):
    print("\nGenerating Correlation Heatmaps")
    print("=" * 80)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Rename y-labels for readability in the chart
    col_display = {
        'net_sentiment':      'Net Sentiment',
        'sentiment_volatility':'Sent. Volatility',
        'bull_bear_ratio':    'Bull/Bear Ratio',
        'sentiment_ma_7d':    'Sentiment MA-7D',
        'next_day_return':    'Next-Day Return',
        'price_volatility_7d':'Price Vol 7D',
        'volume_change_pct':  'Volume Chg %',
    }

    def renamed(data):
        return data.rename(columns=col_display)

    heatmaps = [('OVERALL', df, 'analysis_results/heatmap_overall.png')]
    for ticker in TICKERS:
        heatmaps.append((ticker, df[df['ticker'] == ticker], f'analysis_results/heatmap_{ticker}.png'))

    for label, data, filepath in heatmaps:
        s_display = [col_display[c] for c in SENTIMENT_COLS]
        p_display = [col_display[c] for c in PRICE_COLS]
        matrix = build_heatmap_matrix(renamed(data), s_display, p_display)
        title = f"Sentiment vs Price Correlations — {label}"
        plot_heatmap(matrix, title, filepath)


def print_table(results):
    # Filter to lag=0 rows for the original-style table
    base = [r for r in results if r.get('lag_days', 0) == 0]
    col_widths = {
        'group': 12, 'pair': 46, 'pearson_r': 9,
        'pearson_p': 9, 'spearman_r': 9, 'spearman_p': 9,
        'n': 6, 'sig': 5,
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
    for row in base:
        pr = f"{row['pearson_r']:.4f}"  if not np.isnan(row.get('pearson_r', float('nan'))) else 'N/A'
        pp = f"{row['pearson_p']:.4f}"  if not np.isnan(row.get('pearson_p', float('nan'))) else 'N/A'
        sr = f"{row['spearman_r']:.4f}" if not np.isnan(row.get('spearman_r', float('nan'))) else 'N/A'
        sp = f"{row['spearman_p']:.4f}" if not np.isnan(row.get('spearman_p', float('nan'))) else 'N/A'
        sig = row['significance']
        print(
            f"{row['ticker']:<{col_widths['group']}} "
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

    # --- Baseline correlations (lag=0) ---
    for ticker in TICKERS:
        tdf = df[df['ticker'] == ticker]
        all_results.extend(compute_correlations(tdf, ticker))
    all_results.extend(compute_correlations(df, 'OVERALL'))

    print_table(all_results)

    # --- Lag analysis ---
    lag_results = analyze_lags(df)
    all_results.extend(lag_results)

    # --- Heatmaps ---
    generate_heatmaps(df)

    # --- Save CSV ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_df = pd.DataFrame(all_results)
    # Normalize columns: keep both 'pair' (legacy) and structured columns
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nResults saved to: {OUTPUT_FILE}")
    print(f"  Total rows: {len(out_df)}  (baseline: {len([r for r in all_results if r['lag_days']==0])}, lag: {len([r for r in all_results if r['lag_days']>0])})")

    # --- Summary ---
    sig_overall = [r for r in all_results if r.get('significance') and r.get('significance') not in ('', 'insufficient data') and r['ticker'] == 'OVERALL']
    print()
    if sig_overall:
        print("Statistically significant overall correlations (all lags):")
        for r in sig_overall:
            direction = 'positive' if r['pearson_r'] > 0 else 'negative'
            lag_str = f"lag={r['lag_days']}d" if r['lag_days'] > 0 else 'lag=0d'
            print(f"  [{lag_str}] {r['metric_x']} → {r['metric_y']}: r={r['pearson_r']:.3f} ({direction}), p={r['pearson_p']:.4f} {r['significance']}")
    else:
        print("No statistically significant overall correlations found.")
        print("Consider collecting more data to increase statistical power.")


if __name__ == '__main__':
    main()
