"""
backtest_strategy.py - Simulate trading based on analytics.trading_signals
Supports original and enhanced modes (transaction costs, position sizing, stop-loss, take-profit).
"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

DB_CONNECTION_STRING = 'postgresql://market_sentinel:market_sentinel_password@postgres:5432/market_sentinel'

INITIAL_CASH = 10_000.0
TICKERS = ['AAPL', 'MSFT', 'GOOGL']

# Enhanced mode parameters
TRANSACTION_FEE_PCT = 0.001   # 0.1% per trade
POSITION_SIZE_PCT   = 0.20    # invest 20% of cash per BUY
STOP_LOSS_PCT       = 0.95    # sell if price falls to 95% of entry
TAKE_PROFIT_PCT     = 1.10    # sell if price rises to 110% of entry


def load_signals(engine):
    query = """
        SELECT ticker, date, close_price, signal, signal_strength
        FROM analytics.trading_signals
        WHERE close_price IS NOT NULL
        ORDER BY ticker, date
    """
    return pd.read_sql(query, engine)


# ---------------------------------------------------------------------------
# Original strategy (unchanged logic, kept for comparison)
# ---------------------------------------------------------------------------

def backtest_ticker_original(ticker, data):
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
            trades.append({
                'date': date, 'action': 'BUY',
                'shares': shares_bought, 'price': price,
                'value': trade_value, 'cash_after': 0.0,
                'portfolio_value': shares_bought * price,
                'profit_loss': None,
            })
            shares = shares_bought
            cash = 0.0

        elif signal == 'SELL' and shares > 0:
            sale_value = shares * price
            cost_basis = next(
                (t['value'] for t in reversed(trades) if t['action'] == 'BUY'), INITIAL_CASH
            )
            profit_loss = sale_value - cost_basis
            trades.append({
                'date': date, 'action': 'SELL',
                'shares': shares, 'price': price,
                'value': sale_value, 'cash_after': cash + sale_value,
                'portfolio_value': cash + sale_value,
                'profit_loss': profit_loss,
            })
            cash += sale_value
            shares = 0.0

        else:
            portfolio_value = cash + shares * price
            trades.append({
                'date': date, 'action': 'HOLD',
                'shares': shares, 'price': price,
                'value': shares * price, 'cash_after': cash,
                'portfolio_value': portfolio_value,
                'profit_loss': None,
            })

    last_price = float(data.iloc[-1]['close_price'])
    final_value = cash + shares * last_price
    return trades, final_value


# ---------------------------------------------------------------------------
# Enhanced strategy
# ---------------------------------------------------------------------------

def _execute_sell(date, price, shares, cash, buy_value, action_label):
    """Sell shares with transaction fee; returns (trade_dict, new_cash, new_shares)."""
    gross = shares * price
    fee = gross * TRANSACTION_FEE_PCT
    net = gross - fee
    profit_loss = net - buy_value
    trade = {
        'date': date, 'action': action_label,
        'shares': shares, 'price': price,
        'value': net, 'fee': fee,
        'cash_after': cash + net,
        'portfolio_value': cash + net,
        'profit_loss': profit_loss,
    }
    return trade, cash + net, 0.0


def backtest_ticker_enhanced(ticker, data):
    cash = INITIAL_CASH
    shares = 0.0
    entry_price = None
    buy_value = 0.0   # cost basis of current position (after fee)

    total_fees = 0.0
    stop_losses = 0
    take_profits = 0
    trades = []

    for _, row in data.iterrows():
        date = row['date']
        price = float(row['close_price'])
        signal = row['signal']

        # --- Check stop-loss / take-profit on any day while holding ---
        if shares > 0 and entry_price is not None:
            if price <= entry_price * STOP_LOSS_PCT:
                trade, cash, shares = _execute_sell(date, price, shares, cash, buy_value, 'STOP-LOSS')
                total_fees += trade['fee']
                stop_losses += 1
                entry_price = None
                buy_value = 0.0
                trades.append(trade)
                continue

            if price >= entry_price * TAKE_PROFIT_PCT:
                trade, cash, shares = _execute_sell(date, price, shares, cash, buy_value, 'TAKE-PROFIT')
                total_fees += trade['fee']
                take_profits += 1
                entry_price = None
                buy_value = 0.0
                trades.append(trade)
                continue

        if signal == 'BUY' and cash > 0:
            invest_amount = cash * POSITION_SIZE_PCT
            fee = invest_amount * TRANSACTION_FEE_PCT
            net_invest = invest_amount - fee
            shares_bought = net_invest / price
            total_fees += fee
            # Accumulate position (average up cost basis if already holding)
            buy_value += net_invest
            entry_price = price if entry_price is None else (
                (entry_price * shares + price * shares_bought) / (shares + shares_bought)
            )
            shares += shares_bought
            cash -= invest_amount
            trades.append({
                'date': date, 'action': 'BUY',
                'shares': shares_bought, 'price': price,
                'value': net_invest, 'fee': fee,
                'cash_after': cash,
                'portfolio_value': cash + shares * price,
                'profit_loss': None,
            })

        elif signal == 'SELL' and shares > 0:
            trade, cash, shares = _execute_sell(date, price, shares, cash, buy_value, 'SELL')
            total_fees += trade['fee']
            entry_price = None
            buy_value = 0.0
            trades.append(trade)

        else:
            portfolio_value = cash + shares * price
            trades.append({
                'date': date, 'action': 'HOLD',
                'shares': shares, 'price': price,
                'value': shares * price, 'fee': 0.0,
                'cash_after': cash,
                'portfolio_value': portfolio_value,
                'profit_loss': None,
            })

    last_price = float(data.iloc[-1]['close_price'])
    final_value = cash + shares * last_price
    return trades, final_value, total_fees, stop_losses, take_profits


# ---------------------------------------------------------------------------
# Metrics & display
# ---------------------------------------------------------------------------

def calc_metrics(trades, final_value):
    sell_actions = {'SELL', 'STOP-LOSS', 'TAKE-PROFIT'}
    buy_sell_trades = [t for t in trades if t['action'] in sell_actions | {'BUY'}]
    sell_trades = [t for t in trades if t['action'] in sell_actions and t['profit_loss'] is not None]

    total_return_pct = (final_value - INITIAL_CASH) / INITIAL_CASH * 100
    num_trades = len(buy_sell_trades)
    win_rate = (
        sum(1 for t in sell_trades if t['profit_loss'] > 0) / len(sell_trades) * 100
        if sell_trades else 0.0
    )
    return total_return_pct, num_trades, win_rate, sell_trades


def print_results_original(ticker, trades, final_value, total_return_pct, num_trades, win_rate, sell_trades):
    print(f"\n{'='*62}")
    print(f"  {ticker} — Original Strategy")
    print(f"{'='*62}")
    print(f"  Starting capital : ${INITIAL_CASH:,.2f}")
    print(f"  Final value      : ${final_value:,.2f}")
    print(f"  Total return     : {total_return_pct:+.2f}%")
    print(f"  Number of trades : {num_trades}")
    print(f"  Win rate         : {win_rate:.1f}%")

    active = [t for t in trades if t['action'] in ('BUY', 'SELL')]
    if active:
        print(f"\n  --- Trade Log ---")
        for t in active:
            pl = f"  P&L: ${t['profit_loss']:+,.2f}" if t['profit_loss'] is not None else ""
            print(f"  {t['date']}  {t['action']:<4}  {t['shares']:>8.3f} sh @ ${t['price']:>8.2f}"
                  f"  (${t['value']:>10,.2f}){pl}")


def print_results_enhanced(ticker, trades, final_value, total_return_pct, num_trades, win_rate,
                           sell_trades, total_fees, stop_losses, take_profits):
    print(f"\n{'='*62}")
    print(f"  {ticker} — Enhanced Strategy")
    print(f"{'='*62}")
    print(f"  Starting capital : ${INITIAL_CASH:,.2f}")
    print(f"  Final value      : ${final_value:,.2f}")
    print(f"  Total return     : {total_return_pct:+.2f}%")
    print(f"  Number of trades : {num_trades}")
    print(f"  Win rate         : {win_rate:.1f}%")
    print(f"  --- Enhanced Features ---")
    print(f"  Total fees paid  : ${total_fees:,.2f}")
    print(f"  Stop-losses hit  : {stop_losses}")
    print(f"  Take-profits hit : {take_profits}")

    active_actions = {'BUY', 'SELL', 'STOP-LOSS', 'TAKE-PROFIT'}
    active = [t for t in trades if t['action'] in active_actions]
    if active:
        print(f"\n  --- Trade Log ---")
        for t in active:
            pl = f"  P&L: ${t['profit_loss']:+,.2f}" if t['profit_loss'] is not None else ""
            fee_str = f"  fee: ${t.get('fee', 0):,.2f}" if t.get('fee', 0) else ""
            print(f"  {t['date']}  {t['action']:<11}  {t['shares']:>8.3f} sh @ ${t['price']:>8.2f}"
                  f"  (${t['value']:>10,.2f}){fee_str}{pl}")


def print_comparison(summary_orig, summary_enh):
    print(f"\n\n{'='*70}")
    print(f"  HEAD-TO-HEAD COMPARISON")
    print(f"{'='*70}")
    print(f"  {'Ticker':<8} {'Mode':<12} {'Final Value':>12} {'Return':>10} {'Trades':>7} {'Win%':>7} {'Fees':>9}")
    print(f"  {'-'*66}")
    for orig, enh in zip(summary_orig, summary_enh):
        ticker = orig['ticker']
        print(f"  {ticker:<8} {'Original':<12} ${orig['final_value']:>11,.2f} "
              f"{orig['total_return_pct']:>+9.2f}% {orig['num_trades']:>7} "
              f"{orig['win_rate']:>6.1f}%        —")
        print(f"  {'':<8} {'Enhanced':<12} ${enh['final_value']:>11,.2f} "
              f"{enh['total_return_pct']:>+9.2f}% {enh['num_trades']:>7} "
              f"{enh['win_rate']:>6.1f}% ${enh['total_fees']:>7,.2f}")
        diff = enh['total_return_pct'] - orig['total_return_pct']
        arrow = "▲" if diff >= 0 else "▼"
        print(f"  {'':<8} {'  delta':<12} {'':>12} {arrow}{abs(diff):>9.2f}%   "
              f"SL:{enh['stop_losses']}  TP:{enh['take_profits']}")
        print()
    print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'='*62}")
    print(f"  Market Sentinel - Enhanced Strategy Backtest")
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*62}")

    engine = create_engine(DB_CONNECTION_STRING)

    print("\nLoading signals from analytics.trading_signals...")
    df = load_signals(engine)
    print(f"Loaded {len(df)} signal rows across {df['ticker'].nunique()} tickers.\n")

    summary_orig = []
    summary_enh = []

    for ticker in TICKERS:
        ticker_data = df[df['ticker'] == ticker].copy()
        if ticker_data.empty:
            print(f"No data for {ticker}, skipping.")
            continue

        # --- Original ---
        trades_o, final_o = backtest_ticker_original(ticker, ticker_data)
        ret_o, n_o, wr_o, sells_o = calc_metrics(trades_o, final_o)
        print_results_original(ticker, trades_o, final_o, ret_o, n_o, wr_o, sells_o)
        summary_orig.append({
            'ticker': ticker, 'final_value': final_o,
            'total_return_pct': ret_o, 'num_trades': n_o, 'win_rate': wr_o,
        })

        # --- Enhanced ---
        trades_e, final_e, fees_e, sl_e, tp_e = backtest_ticker_enhanced(ticker, ticker_data)
        ret_e, n_e, wr_e, sells_e = calc_metrics(trades_e, final_e)
        print_results_enhanced(ticker, trades_e, final_e, ret_e, n_e, wr_e, sells_e, fees_e, sl_e, tp_e)
        summary_enh.append({
            'ticker': ticker, 'final_value': final_e,
            'total_return_pct': ret_e, 'num_trades': n_e, 'win_rate': wr_e,
            'total_fees': fees_e, 'stop_losses': sl_e, 'take_profits': tp_e,
        })

    print_comparison(summary_orig, summary_enh)


if __name__ == "__main__":
    main()
