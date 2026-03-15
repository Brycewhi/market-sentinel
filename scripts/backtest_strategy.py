"""
backtest_strategy.py - Simulate trading based on analytics.trading_signals
Supports original and enhanced modes (transaction costs, position sizing, stop-loss, take-profit).
Week 6 Day 4: Professional performance metrics + buy-and-hold comparison.
"""

import math
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

TRADING_DAYS_PER_YEAR = 252


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
# Buy-and-Hold benchmark
# ---------------------------------------------------------------------------

def calc_buy_and_hold(ticker_data):
    """Buy on day 1, hold to the end. Returns (final_value, return_pct, daily_returns)."""
    first_price = float(ticker_data.iloc[0]['close_price'])
    last_price  = float(ticker_data.iloc[-1]['close_price'])
    shares = INITIAL_CASH / first_price
    final_value = shares * last_price
    return_pct = (final_value - INITIAL_CASH) / INITIAL_CASH * 100

    prices = ticker_data['close_price'].astype(float).values
    daily_returns = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices))]
    return final_value, return_pct, daily_returns


# ---------------------------------------------------------------------------
# Performance metrics
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


def calc_sharpe(trades):
    """Annualised Sharpe ratio from daily portfolio values (risk-free rate = 0)."""
    pvs = [t['portfolio_value'] for t in trades]
    if len(pvs) < 2:
        return 0.0
    daily_returns = [(pvs[i] - pvs[i - 1]) / pvs[i - 1] for i in range(1, len(pvs))]
    n = len(daily_returns)
    mean_r = sum(daily_returns) / n
    if n < 2:
        return 0.0
    variance = sum((r - mean_r) ** 2 for r in daily_returns) / (n - 1)
    std_r = math.sqrt(variance)
    if std_r == 0:
        return 0.0
    return (mean_r / std_r) * math.sqrt(TRADING_DAYS_PER_YEAR)


def calc_max_drawdown(trades):
    """Maximum peak-to-trough decline of portfolio value (as positive percentage)."""
    pvs = [t['portfolio_value'] for t in trades]
    if not pvs:
        return 0.0
    peak = pvs[0]
    max_dd = 0.0
    for pv in pvs:
        if pv > peak:
            peak = pv
        dd = (peak - pv) / peak * 100
        if dd > max_dd:
            max_dd = dd
    return max_dd


def calc_profit_factor(sell_trades):
    """Sum of winning P&Ls / abs(sum of losing P&Ls). Returns None if no losses."""
    wins   = sum(t['profit_loss'] for t in sell_trades if t['profit_loss'] > 0)
    losses = sum(t['profit_loss'] for t in sell_trades if t['profit_loss'] < 0)
    if losses == 0:
        return None
    return wins / abs(losses)


def calc_win_loss_stats(sell_trades):
    """Returns (avg_win, avg_loss, win_loss_ratio)."""
    win_pls  = [t['profit_loss'] for t in sell_trades if t['profit_loss'] > 0]
    loss_pls = [t['profit_loss'] for t in sell_trades if t['profit_loss'] < 0]

    avg_win  = sum(win_pls)  / len(win_pls)  if win_pls  else 0.0
    avg_loss = sum(loss_pls) / len(loss_pls) if loss_pls else 0.0
    ratio    = avg_win / abs(avg_loss)        if avg_loss else None
    return avg_win, avg_loss, ratio


def calc_max_consecutive_losses(sell_trades):
    """Longest streak of consecutive losing trades."""
    max_streak = 0
    streak = 0
    for t in sell_trades:
        if t['profit_loss'] < 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def calc_calmar(total_return_pct, num_days, max_drawdown_pct):
    """Calmar = annualised return / max drawdown. Returns None if drawdown is 0."""
    if num_days == 0 or max_drawdown_pct == 0:
        return None
    annual_return_pct = total_return_pct * (TRADING_DAYS_PER_YEAR / num_days)
    return annual_return_pct / max_drawdown_pct


def calc_all_advanced(trades, final_value):
    """Compute all advanced metrics; returns a dict."""
    total_return_pct, num_trades, win_rate, sell_trades = calc_metrics(trades, final_value)
    sharpe    = calc_sharpe(trades)
    max_dd    = calc_max_drawdown(trades)
    pf        = calc_profit_factor(sell_trades)
    avg_win, avg_loss, wl_ratio = calc_win_loss_stats(sell_trades)
    max_cons_loss = calc_max_consecutive_losses(sell_trades)
    num_days  = len(trades)
    calmar    = calc_calmar(total_return_pct, num_days, max_dd)

    return {
        'total_return_pct': total_return_pct,
        'num_trades':       num_trades,
        'win_rate':         win_rate,
        'sell_trades':      sell_trades,
        'sharpe':           sharpe,
        'max_drawdown':     max_dd,
        'profit_factor':    pf,
        'avg_win':          avg_win,
        'avg_loss':         avg_loss,
        'win_loss_ratio':   wl_ratio,
        'max_cons_losses':  max_cons_loss,
        'calmar':           calmar,
    }


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _fmt_optional(val, fmt, prefix='', suffix=''):
    return f"{prefix}{val:{fmt}}{suffix}" if val is not None else 'N/A'


def print_results_original(ticker, trades, final_value, m):
    print(f"\n{'='*62}")
    print(f"  {ticker} — Original Strategy")
    print(f"{'='*62}")
    print(f"  Starting capital     : ${INITIAL_CASH:,.2f}")
    print(f"  Final value          : ${final_value:,.2f}")
    print(f"  Total return         : {m['total_return_pct']:+.2f}%")
    print(f"  Number of trades     : {m['num_trades']}")
    print(f"  Win rate             : {m['win_rate']:.1f}%")
    print(f"  --- Advanced Metrics ---")
    print(f"  Sharpe ratio         : {m['sharpe']:.3f}")
    print(f"  Max drawdown         : {m['max_drawdown']:.2f}%")
    pf_str = _fmt_optional(m['profit_factor'], '.3f')
    print(f"  Profit factor        : {pf_str}")
    print(f"  Avg win              : ${m['avg_win']:,.2f}")
    print(f"  Avg loss             : ${m['avg_loss']:,.2f}")
    wl_str = _fmt_optional(m['win_loss_ratio'], '.3f')
    print(f"  Win/loss ratio       : {wl_str}")
    print(f"  Max consec. losses   : {m['max_cons_losses']}")
    calmar_str = _fmt_optional(m['calmar'], '.3f')
    print(f"  Calmar ratio         : {calmar_str}")

    active = [t for t in trades if t['action'] in ('BUY', 'SELL')]
    if active:
        print(f"\n  --- Trade Log ---")
        for t in active:
            pl = f"  P&L: ${t['profit_loss']:+,.2f}" if t['profit_loss'] is not None else ""
            print(f"  {t['date']}  {t['action']:<4}  {t['shares']:>8.3f} sh @ ${t['price']:>8.2f}"
                  f"  (${t['value']:>10,.2f}){pl}")


def print_results_enhanced(ticker, trades, final_value, m, total_fees, stop_losses, take_profits):
    print(f"\n{'='*62}")
    print(f"  {ticker} — Enhanced Strategy")
    print(f"{'='*62}")
    print(f"  Starting capital     : ${INITIAL_CASH:,.2f}")
    print(f"  Final value          : ${final_value:,.2f}")
    print(f"  Total return         : {m['total_return_pct']:+.2f}%")
    print(f"  Number of trades     : {m['num_trades']}")
    print(f"  Win rate             : {m['win_rate']:.1f}%")
    print(f"  --- Enhanced Features ---")
    print(f"  Total fees paid      : ${total_fees:,.2f}")
    print(f"  Stop-losses hit      : {stop_losses}")
    print(f"  Take-profits hit     : {take_profits}")
    print(f"  --- Advanced Metrics ---")
    print(f"  Sharpe ratio         : {m['sharpe']:.3f}")
    print(f"  Max drawdown         : {m['max_drawdown']:.2f}%")
    pf_str = _fmt_optional(m['profit_factor'], '.3f')
    print(f"  Profit factor        : {pf_str}")
    print(f"  Avg win              : ${m['avg_win']:,.2f}")
    print(f"  Avg loss             : ${m['avg_loss']:,.2f}")
    wl_str = _fmt_optional(m['win_loss_ratio'], '.3f')
    print(f"  Win/loss ratio       : {wl_str}")
    print(f"  Max consec. losses   : {m['max_cons_losses']}")
    calmar_str = _fmt_optional(m['calmar'], '.3f')
    print(f"  Calmar ratio         : {calmar_str}")

    active_actions = {'BUY', 'SELL', 'STOP-LOSS', 'TAKE-PROFIT'}
    active = [t for t in trades if t['action'] in active_actions]
    if active:
        print(f"\n  --- Trade Log ---")
        for t in active:
            pl = f"  P&L: ${t['profit_loss']:+,.2f}" if t['profit_loss'] is not None else ""
            fee_str = f"  fee: ${t.get('fee', 0):,.2f}" if t.get('fee', 0) else ""
            print(f"  {t['date']}  {t['action']:<11}  {t['shares']:>8.3f} sh @ ${t['price']:>8.2f}"
                  f"  (${t['value']:>10,.2f}){fee_str}{pl}")


def print_comparison(summary_orig, summary_enh, summary_bah):
    # ---- Strategy vs Buy-and-Hold table ----
    print(f"\n\n{'='*80}")
    print(f"  STRATEGY vs BUY-AND-HOLD")
    print(f"{'='*80}")
    hdr = f"  {'Ticker':<7} {'Mode':<14} {'Final $':>11} {'Return':>9} {'Alpha':>8} {'Sharpe':>8} {'MaxDD':>8} {'Calmar':>8}"
    print(hdr)
    print(f"  {'-'*76}")
    for orig, enh, bah in zip(summary_orig, summary_enh, summary_bah):
        ticker = orig['ticker']
        bah_ret = bah['return_pct']

        def alpha(strat_ret):
            return strat_ret - bah_ret

        for label, s in [('Original', orig), ('Enhanced', enh), ('Buy&Hold', bah)]:
            ret   = s.get('total_return_pct', s.get('return_pct', 0))
            alp   = f"{alpha(ret):+.2f}%" if label != 'Buy&Hold' else '   —   '
            sharpe = f"{s['sharpe']:.2f}" if s.get('sharpe') is not None else '  N/A '
            maxdd  = f"{s['max_drawdown']:.1f}%" if s.get('max_drawdown') is not None else '  N/A '
            calmar = f"{s['calmar']:.2f}" if s.get('calmar') is not None else '  N/A '
            fv    = s.get('final_value', INITIAL_CASH * (1 + ret / 100))
            print(f"  {ticker if label == 'Original' else '':<7} {label:<14} ${fv:>10,.2f} "
                  f"{ret:>+8.2f}% {alp:>8} {sharpe:>8} {maxdd:>8} {calmar:>8}")
        print()

    # ---- Head-to-head (original vs enhanced) ----
    print(f"\n{'='*80}")
    print(f"  ORIGINAL vs ENHANCED HEAD-TO-HEAD")
    print(f"{'='*80}")
    print(f"  {'Ticker':<8} {'Mode':<12} {'Final Value':>12} {'Return':>10} {'Trades':>7} {'Win%':>7} {'Fees':>9} {'PF':>7}")
    print(f"  {'-'*76}")
    for orig, enh in zip(summary_orig, summary_enh):
        ticker = orig['ticker']
        pf_orig = _fmt_optional(orig.get('profit_factor'), '.2f')
        pf_enh  = _fmt_optional(enh.get('profit_factor'), '.2f')
        print(f"  {ticker:<8} {'Original':<12} ${orig['final_value']:>11,.2f} "
              f"{orig['total_return_pct']:>+9.2f}% {orig['num_trades']:>7} "
              f"{orig['win_rate']:>6.1f}%       — {pf_orig:>7}")
        print(f"  {'':<8} {'Enhanced':<12} ${enh['final_value']:>11,.2f} "
              f"{enh['total_return_pct']:>+9.2f}% {enh['num_trades']:>7} "
              f"{enh['win_rate']:>6.1f}% ${enh['total_fees']:>7,.2f} {pf_enh:>7}")
        diff = enh['total_return_pct'] - orig['total_return_pct']
        arrow = "▲" if diff >= 0 else "▼"
        print(f"  {'':<8} {'  delta':<12} {'':>12} {arrow}{abs(diff):>9.2f}%   "
              f"SL:{enh['stop_losses']}  TP:{enh['take_profits']}")
        print()
    print(f"{'='*80}\n")


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
    summary_enh  = []
    summary_bah  = []

    for ticker in TICKERS:
        ticker_data = df[df['ticker'] == ticker].copy()
        if ticker_data.empty:
            print(f"No data for {ticker}, skipping.")
            continue

        # --- Buy-and-Hold benchmark ---
        bah_final, bah_ret, bah_daily_returns = calc_buy_and_hold(ticker_data)
        num_days = len(ticker_data)
        bah_mean = sum(bah_daily_returns) / len(bah_daily_returns) if bah_daily_returns else 0
        bah_std  = math.sqrt(
            sum((r - bah_mean) ** 2 for r in bah_daily_returns) / max(len(bah_daily_returns) - 1, 1)
        ) if bah_daily_returns else 0
        bah_sharpe = (bah_mean / bah_std) * math.sqrt(TRADING_DAYS_PER_YEAR) if bah_std else 0

        # Max drawdown for buy-and-hold using price series
        prices = ticker_data['close_price'].astype(float).values
        bah_pvs = [INITIAL_CASH / prices[0] * p for p in prices]
        bah_peak = bah_pvs[0]
        bah_max_dd = 0.0
        for pv in bah_pvs:
            bah_peak = max(bah_peak, pv)
            bah_max_dd = max(bah_max_dd, (bah_peak - pv) / bah_peak * 100)

        bah_calmar = calc_calmar(bah_ret, num_days, bah_max_dd)
        summary_bah.append({
            'ticker':       ticker,
            'final_value':  bah_final,
            'return_pct':   bah_ret,
            'sharpe':       bah_sharpe,
            'max_drawdown': bah_max_dd,
            'calmar':       bah_calmar,
        })

        # --- Original ---
        trades_o, final_o = backtest_ticker_original(ticker, ticker_data)
        m_o = calc_all_advanced(trades_o, final_o)
        print_results_original(ticker, trades_o, final_o, m_o)
        summary_orig.append({
            'ticker':           ticker,
            'final_value':      final_o,
            'total_return_pct': m_o['total_return_pct'],
            'num_trades':       m_o['num_trades'],
            'win_rate':         m_o['win_rate'],
            'sharpe':           m_o['sharpe'],
            'max_drawdown':     m_o['max_drawdown'],
            'profit_factor':    m_o['profit_factor'],
            'calmar':           m_o['calmar'],
        })

        # --- Enhanced ---
        trades_e, final_e, fees_e, sl_e, tp_e = backtest_ticker_enhanced(ticker, ticker_data)
        m_e = calc_all_advanced(trades_e, final_e)
        print_results_enhanced(ticker, trades_e, final_e, m_e, fees_e, sl_e, tp_e)
        summary_enh.append({
            'ticker':           ticker,
            'final_value':      final_e,
            'total_return_pct': m_e['total_return_pct'],
            'num_trades':       m_e['num_trades'],
            'win_rate':         m_e['win_rate'],
            'total_fees':       fees_e,
            'stop_losses':      sl_e,
            'take_profits':     tp_e,
            'sharpe':           m_e['sharpe'],
            'max_drawdown':     m_e['max_drawdown'],
            'profit_factor':    m_e['profit_factor'],
            'calmar':           m_e['calmar'],
        })

    print_comparison(summary_orig, summary_enh, summary_bah)


if __name__ == "__main__":
    main()
