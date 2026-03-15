"""
backtest_strategy.py - Simulate trading based on analytics.trading_signals
"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

DB_CONNECTION_STRING = 'postgresql://market_sentinel:market_sentinel_password@postgres:5432/market_sentinel'

INITIAL_CASH = 10_000.0
TICKERS = ['AAPL', 'MSFT', 'GOOGL']


def load_signals(engine):
    query = """
        SELECT ticker, date, close_price, signal, signal_strength
        FROM analytics.trading_signals
        WHERE close_price IS NOT NULL
        ORDER BY ticker, date
    """
    return pd.read_sql(query, engine)


def backtest_ticker(ticker, data):
    cash = INITIAL_CASH
    shares = 0.0
    trades = []

    for _, row in data.iterrows():
        date = row['date']
        price = float(row['close_price'])
        signal = row['signal']

        if signal == 'BUY' and cash > 0:
            shares_bought = cash / price
            trade_value = cash
            profit_loss = None  # No P&L on buy; realized on sell
            trades.append({
                'date': date,
                'action': 'BUY',
                'shares': shares_bought,
                'price': price,
                'value': trade_value,
                'cash_after': 0.0,
                'portfolio_value': shares_bought * price,
                'profit_loss': None,
            })
            shares = shares_bought
            cash = 0.0

        elif signal == 'SELL' and shares > 0:
            sale_value = shares * price
            cost_basis = INITIAL_CASH if not trades else next(
                (t['value'] for t in reversed(trades) if t['action'] == 'BUY'), INITIAL_CASH
            )
            profit_loss = sale_value - cost_basis
            trades.append({
                'date': date,
                'action': 'SELL',
                'shares': shares,
                'price': price,
                'value': sale_value,
                'cash_after': cash + sale_value,
                'portfolio_value': cash + sale_value,
                'profit_loss': profit_loss,
            })
            cash += sale_value
            shares = 0.0

        else:
            # HOLD or no-op
            portfolio_value = cash + shares * price
            trades.append({
                'date': date,
                'action': 'HOLD',
                'shares': shares,
                'price': price,
                'value': shares * price,
                'cash_after': cash,
                'portfolio_value': portfolio_value,
                'profit_loss': None,
            })

    # Final portfolio value
    last_price = float(data.iloc[-1]['close_price'])
    final_value = cash + shares * last_price

    return trades, final_value


def calc_metrics(trades, final_value):
    total_return_pct = (final_value - INITIAL_CASH) / INITIAL_CASH * 100

    buy_sell_trades = [t for t in trades if t['action'] in ('BUY', 'SELL')]
    sell_trades = [t for t in trades if t['action'] == 'SELL' and t['profit_loss'] is not None]

    num_trades = len(buy_sell_trades)
    if sell_trades:
        winning = sum(1 for t in sell_trades if t['profit_loss'] > 0)
        win_rate = winning / len(sell_trades) * 100
    else:
        win_rate = 0.0

    return total_return_pct, num_trades, win_rate, sell_trades


def print_results(ticker, trades, final_value, total_return_pct, num_trades, win_rate, sell_trades):
    print(f"\n{'='*60}")
    print(f"  {ticker} Backtest Results")
    print(f"{'='*60}")
    print(f"  Starting capital: ${INITIAL_CASH:,.2f}")
    print(f"  Final value:      ${final_value:,.2f}")
    print(f"  Total return:     {total_return_pct:+.2f}%")
    print(f"  Number of trades: {num_trades}")
    print(f"  Win rate:         {win_rate:.1f}%")

    active_trades = [t for t in trades if t['action'] in ('BUY', 'SELL')]
    if active_trades:
        print(f"\n  --- Trade Log ---")
        for t in active_trades:
            pl = f"  P&L: ${t['profit_loss']:+,.2f}" if t['profit_loss'] is not None else ""
            print(f"  {t['date']}  {t['action']:<4}  {t['shares']:>8.3f} shares @ ${t['price']:>8.2f}  (${t['value']:>10,.2f}){pl}")

    if sell_trades:
        print(f"\n  --- Sell Summary ---")
        for t in sell_trades:
            result = "WIN" if t['profit_loss'] > 0 else "LOSS"
            print(f"  {t['date']}  {result}  P&L: ${t['profit_loss']:+,.2f}")


def main():
    print(f"\n{'='*60}")
    print(f"  Market Sentinel - Strategy Backtest")
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    engine = create_engine(DB_CONNECTION_STRING)

    print("\nLoading signals from analytics.trading_signals...")
    df = load_signals(engine)
    print(f"Loaded {len(df)} signal rows across {df['ticker'].nunique()} tickers.\n")

    summary = []

    for ticker in TICKERS:
        ticker_data = df[df['ticker'] == ticker].copy()

        if ticker_data.empty:
            print(f"No data for {ticker}, skipping.")
            continue

        trades, final_value = backtest_ticker(ticker, ticker_data)
        total_return_pct, num_trades, win_rate, sell_trades = calc_metrics(trades, final_value)

        print_results(ticker, trades, final_value, total_return_pct, num_trades, win_rate, sell_trades)

        summary.append({
            'ticker': ticker,
            'final_value': final_value,
            'total_return_pct': total_return_pct,
            'num_trades': num_trades,
            'win_rate': win_rate,
        })

    # Overall summary
    print(f"\n\n{'='*60}")
    print(f"  PORTFOLIO SUMMARY")
    print(f"{'='*60}")
    print(f"  {'Ticker':<8} {'Final Value':>12} {'Return':>10} {'Trades':>8} {'Win Rate':>10}")
    print(f"  {'-'*52}")
    for s in summary:
        print(f"  {s['ticker']:<8} ${s['final_value']:>11,.2f} {s['total_return_pct']:>+9.2f}% {s['num_trades']:>8} {s['win_rate']:>9.1f}%")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
