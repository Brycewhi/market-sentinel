"""
Market Sentinel - Financial Sentiment Analytics Dashboard
Bloomberg Terminal dark theme, multi-page Streamlit app.
"""

import os
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text

# ── Page config (must be first Streamlit call) ───────────────────────────────
st.set_page_config(
    page_title="Market Sentinel",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ────────────────────────────────────────────────────────────────
# Connects to Postgres exposed on localhost from Docker Compose port mapping.
# The container uses host 'postgres'; from the host machine use localhost:5434.
DB_CONNECTION_STRING = "postgresql://market_sentinel:market_sentinel_password@localhost:5432/market_sentinel"

TICKERS = ["AAPL", "MSFT", "GOOGL"]
ANALYSIS_DIR = os.path.join(os.path.dirname(__file__), "..", "scripts", "analysis_results")

# ── Bloomberg Terminal CSS ────────────────────────────────────────────────────
BLOOMBERG_CSS = """
<style>
/* ── Root palette ── */
:root {
    --bg-main:    #0a1929;
    --bg-card:    #162c43;
    --bg-sidebar: #0d1d2e;
    --accent:     #00d4ff;
    --positive:   #00ff88;
    --negative:   #ff4444;
    --text-pri:   #ffffff;
    --text-sec:   #b8c5d6;
    --border:     #1e3a52;
}

/* ── Main background ── */
.stApp { background-color: var(--bg-main); color: var(--text-pri); }
.main .block-container { padding-top: 1.5rem; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: var(--bg-sidebar);
    border-right: 1px solid var(--border);
}
[data-testid="stSidebar"] * { color: var(--text-sec) !important; }
[data-testid="stSidebar"] .stRadio label { color: var(--text-pri) !important; }
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 { color: var(--accent) !important; }

/* ── KPI Cards ── */
.kpi-card {
    background-color: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 0.5rem;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: "";
    position: absolute;
    top: 0; left: 0;
    width: 3px; height: 100%;
    background: var(--accent);
}
.kpi-icon  { font-size: 1.6rem; margin-bottom: 0.3rem; }
.kpi-label { font-size: 0.72rem; color: var(--text-sec); text-transform: uppercase;
             letter-spacing: 0.08em; margin-bottom: 0.2rem; }
.kpi-value { font-size: 2rem; font-weight: 700; color: var(--text-pri);
             font-family: "Courier New", monospace; line-height: 1.1; }
.kpi-delta { font-size: 0.8rem; margin-top: 0.3rem; }
.kpi-delta.positive { color: var(--positive); }
.kpi-delta.negative { color: var(--negative); }
.kpi-delta.neutral  { color: var(--text-sec); }

/* ── Section headers ── */
.section-title {
    color: var(--accent);
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem;
    margin: 1.2rem 0 0.8rem 0;
}

/* ── Dashboard header ── */
.dash-header {
    background: linear-gradient(135deg, #0d2137 0%, #162c43 60%, #0a3d62 100%);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1.4rem 2rem;
    margin-bottom: 1.5rem;
    border-left: 4px solid var(--accent);
}
.dash-title {
    font-size: 1.8rem;
    font-weight: 800;
    color: var(--text-pri);
    letter-spacing: -0.02em;
    margin: 0 0 0.2rem 0;
}
.dash-title span { color: var(--accent); }
.dash-subtitle { color: var(--text-sec); font-size: 0.85rem; margin: 0; }

/* ── Chart containers ── */
.chart-container {
    background-color: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
    margin-bottom: 1rem;
}
.chart-title {
    color: var(--text-pri);
    font-size: 0.9rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
    padding-bottom: 0.4rem;
    border-bottom: 1px solid var(--border);
}

/* ── Coming-soon placeholder ── */
.placeholder-card {
    background-color: var(--bg-card);
    border: 1px dashed var(--border);
    border-radius: 10px;
    padding: 3rem 2rem;
    text-align: center;
    color: var(--text-sec);
}
.placeholder-card h2 { color: var(--accent); font-size: 1.4rem; margin-bottom: 0.5rem; }
.placeholder-card ul { text-align: left; display: inline-block; margin-top: 1rem; }

/* ── Streamlit widget overrides ── */
[data-testid="stMetricValue"] { color: var(--text-pri) !important; }
div[data-testid="stSelectbox"] label,
div[data-testid="stRadio"] label { color: var(--text-sec) !important; }
.stDownloadButton button {
    background-color: var(--accent) !important;
    color: #000 !important;
    font-weight: 600 !important;
    border: none !important;
    width: 100%;
}
.stDownloadButton button:hover { opacity: 0.85 !important; }

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }

/* ── Dataframe / table ── */
[data-testid="stDataFrame"] {
    background-color: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: var(--bg-main); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
"""

# ── DB helpers ───────────────────────────────────────────────────────────────

@st.cache_resource
def get_db_connection():
    return create_engine(DB_CONNECTION_STRING, pool_pre_ping=True)


@st.cache_data(ttl=300)
def load_sentiment_data(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    engine = get_db_connection()
    ticker_filter = "AND ticker = :ticker" if ticker != "All Tickers" else ""
    query = f"""
        SELECT ticker, date, article_count, net_sentiment, sentiment_volatility,
               sentiment_ma_3d, sentiment_ma_7d, sentiment_ma_30d,
               bull_bear_ratio, bullish_count, bearish_count,
               neutral_count, sentiment_confidence
        FROM analytics.sentiment_trends
        WHERE date BETWEEN :start AND :end
        {ticker_filter}
        ORDER BY ticker, date
    """
    params = {"start": start_date, "end": end_date}
    if ticker != "All Tickers":
        params["ticker"] = ticker
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


@st.cache_data(ttl=300)
def load_price_data(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    engine = get_db_connection()
    ticker_filter = "AND ticker = :ticker" if ticker != "All Tickers" else ""
    query = f"""
        SELECT ticker, date, close, volume
        FROM staging.price_data
        WHERE date BETWEEN :start AND :end
        {ticker_filter}
        ORDER BY ticker, date
    """
    params = {"start": start_date, "end": end_date}
    if ticker != "All Tickers":
        params["ticker"] = ticker
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


@st.cache_data(ttl=300)
def load_trading_signals(ticker: str) -> pd.DataFrame:
    engine = get_db_connection()
    where = "WHERE ticker = :ticker" if ticker != "All Tickers" else ""
    query = f"""
        SELECT ticker, date, signal, signal_strength, avg_net_sentiment, next_day_return
        FROM analytics.trading_signals
        {where}
        ORDER BY date
    """
    params = {"ticker": ticker} if ticker != "All Tickers" else {}
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params if params else None)


@st.cache_data(ttl=3600)
def load_correlation_data() -> pd.DataFrame:
    path = os.path.join(ANALYSIS_DIR, "correlation_analysis.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_about_stats() -> dict:
    engine = get_db_connection()
    query = """
        SELECT COUNT(*) as total_articles,
               MIN(published_at) as first_date,
               MAX(published_at) as last_updated
        FROM staging.sentiment_logs
    """
    with engine.connect() as conn:
        row = conn.execute(text(query)).fetchone()
    return {
        "total_articles": int(row[0]),
        "first_date": row[1],
        "last_updated": row[2],
    }

# ── Date-range resolver ───────────────────────────────────────────────────────

def resolve_dates(range_label: str, custom_start=None, custom_end=None):
    today = date.today()
    if range_label == "Last 7 Days":
        return today - timedelta(days=7), today
    if range_label == "Last 30 Days":
        return today - timedelta(days=30), today
    if range_label == "Custom Range":
        return custom_start or today - timedelta(days=30), custom_end or today
    # "All Time"
    return date(2026, 1, 1), today

# ── Sidebar ──────────────────────────────────────────────────────────────────

def render_sidebar() -> tuple:
    st.sidebar.markdown("## 📡 Market Sentinel")
    st.sidebar.markdown("---")

    page = st.sidebar.radio(
        "Navigation",
        ["Overview Dashboard", "Trading Signals & Backtesting",
         "Statistical Analysis", "Multi-Ticker Comparison"],
        label_visibility="collapsed",
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Filters")

    ticker = st.sidebar.radio(
        "Ticker",
        ["AAPL", "MSFT", "GOOGL", "All Tickers"],
    )

    date_range = st.sidebar.selectbox(
        "Date Range",
        ["Last 7 Days", "Last 30 Days", "All Time", "Custom Range"],
        index=1,
    )

    custom_start = custom_end = None
    if date_range == "Custom Range":
        custom_start = st.sidebar.date_input("Start Date", value=date.today() - timedelta(days=30))
        custom_end   = st.sidebar.date_input("End Date",   value=date.today())

    start_date, end_date = resolve_dates(date_range, custom_start, custom_end)

    # Export button — load data then offer download
    st.sidebar.markdown("---")
    if st.sidebar.button("⬇ Prepare CSV Export"):
        df = load_sentiment_data(ticker, start_date, end_date)
        st.sidebar.download_button(
            label="Download CSV",
            data=df.to_csv(index=False).encode(),
            file_name=f"market_sentinel_{ticker}_{start_date}_{end_date}.csv",
            mime="text/csv",
        )

    # About section
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    try:
        stats = load_about_stats()
        st.sidebar.markdown(f"""
<small style='color:#b8c5d6'>
📰 <strong>{stats['total_articles']:,}</strong> total articles<br>
📅 Since <strong>{stats['first_date'].strftime('%b %d, %Y') if stats['first_date'] else 'N/A'}</strong><br>
🕐 Updated <strong>{stats['last_updated'].strftime('%b %d %H:%M') if stats['last_updated'] else 'N/A'}</strong>
</small>
""", unsafe_allow_html=True)
    except Exception as e:
        st.sidebar.warning(f"Stats unavailable: {e}")

    return page, ticker, start_date, end_date

# ── KPI card helper ───────────────────────────────────────────────────────────

def kpi_card(icon: str, label: str, value: str, delta: str = "", delta_positive: bool | None = None):
    if delta:
        if delta_positive is True:
            delta_class, arrow = "positive", "▲"
        elif delta_positive is False:
            delta_class, arrow = "negative", "▼"
        else:
            delta_class, arrow = "neutral", "●"
        delta_html = f'<div class="kpi-delta {delta_class}">{arrow} {delta}</div>'
    else:
        delta_html = ""

    st.markdown(f"""
<div class="kpi-card">
  <div class="kpi-icon">{icon}</div>
  <div class="kpi-label">{label}</div>
  <div class="kpi-value">{value}</div>
  {delta_html}
</div>
""", unsafe_allow_html=True)

# ── Page 1: Overview Dashboard ────────────────────────────────────────────────

def page_overview(ticker: str, start_date: date, end_date: date):
    import plotly.graph_objects as go
    import plotly.express as px

    PLOTLY_LAYOUT = dict(
        paper_bgcolor="#162c43",
        plot_bgcolor="#0a1929",
        font=dict(color="#b8c5d6", family="Courier New"),
        xaxis=dict(gridcolor="#1e3a52", showgrid=True, zeroline=False),
        yaxis=dict(gridcolor="#1e3a52", showgrid=True, zeroline=False),
        margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
    )

    # Header
    last_updated = datetime.now().strftime("%B %d, %Y  %H:%M")
    st.markdown(f"""
<div class="dash-header">
  <p class="dash-title">Market <span>Sentinel</span> — Financial Sentiment Analytics</p>
  <p class="dash-subtitle">Real-time sentiment tracking & price correlation analysis &nbsp;|&nbsp; Last updated: {last_updated}</p>
</div>
""", unsafe_allow_html=True)

    # ── Load data ──
    try:
        df_sent  = load_sentiment_data(ticker, start_date, end_date)
        df_price = load_price_data(ticker, start_date, end_date)
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if df_sent.empty:
        st.warning("No sentiment data found for the selected filters.")
        return

    # Previous period for delta comparison
    period_days = (end_date - start_date).days or 1
    prev_start  = start_date - timedelta(days=period_days)
    prev_end    = start_date - timedelta(days=1)
    try:
        df_prev = load_sentiment_data(ticker, prev_start, prev_end)
    except Exception:
        df_prev = pd.DataFrame()

    # ── KPI Calculations ──
    total_articles   = int(df_sent["article_count"].sum())
    unique_days      = df_sent["date"].nunique()
    avg_daily        = total_articles / unique_days if unique_days else 0
    latest_ma7       = df_sent["sentiment_ma_7d"].dropna().iloc[-1] if not df_sent["sentiment_ma_7d"].dropna().empty else 0
    latest_bb_ratio  = df_sent["bull_bear_ratio"].dropna().iloc[-1]  if not df_sent["bull_bear_ratio"].dropna().empty  else 0

    prev_articles    = int(df_prev["article_count"].sum()) if not df_prev.empty else 0
    prev_avg_daily   = prev_articles / max(df_prev["date"].nunique(), 1) if not df_prev.empty else 0

    def pct_delta(curr, prev):
        if prev == 0:
            return None
        return (curr - prev) / abs(prev) * 100

    art_delta     = pct_delta(total_articles, prev_articles)
    avg_delta     = pct_delta(avg_daily, prev_avg_daily)

    # MA7 change vs 7 days prior
    if len(df_sent["sentiment_ma_7d"].dropna()) >= 2:
        ma7_prev     = df_sent["sentiment_ma_7d"].dropna().iloc[-8] if len(df_sent["sentiment_ma_7d"].dropna()) > 7 else df_sent["sentiment_ma_7d"].dropna().iloc[0]
        ma7_delta    = latest_ma7 - ma7_prev
    else:
        ma7_delta = 0

    # ── KPI Cards ──
    st.markdown('<p class="section-title">Key Performance Indicators</p>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        delta_str = f"{art_delta:+.1f}% vs prior period" if art_delta is not None else "No prior data"
        kpi_card("📰", "Total Articles", f"{total_articles:,}",
                 delta_str, delta_positive=art_delta > 0 if art_delta is not None else None)

    with c2:
        delta_str = f"{avg_delta:+.1f}% vs prior period" if avg_delta is not None else "No prior data"
        kpi_card("📊", "Avg Daily Articles", f"{avg_daily:.1f}",
                 delta_str, delta_positive=avg_delta > 0 if avg_delta is not None else None)

    with c3:
        ma7_color = ma7_delta >= 0
        kpi_card("💭", "Sentiment (7-Day MA)", f"{latest_ma7:+.3f}",
                 f"{ma7_delta:+.3f} vs 7d ago", delta_positive=ma7_color)

    with c4:
        bb_pos = latest_bb_ratio >= 1.0
        bb_str = f"{'Bullish' if bb_pos else 'Bearish'} bias"
        kpi_card("⚖️", "Bull/Bear Ratio", f"{latest_bb_ratio:.2f}",
                 bb_str, delta_positive=bb_pos)

    # ── Sentiment Timeline ──
    st.markdown('<p class="section-title">Sentiment Timeline</p>', unsafe_allow_html=True)

    if not df_sent.empty:
        # Aggregate across tickers if "All Tickers"
        if ticker == "All Tickers":
            chart_df = (
                df_sent.groupby("date")
                .agg(
                    net_sentiment=("net_sentiment", "mean"),
                    sentiment_ma_3d=("sentiment_ma_3d", "mean"),
                    sentiment_ma_7d=("sentiment_ma_7d", "mean"),
                    sentiment_ma_30d=("sentiment_ma_30d", "mean"),
                )
                .reset_index()
                .sort_values("date")
            )
        else:
            chart_df = df_sent.sort_values("date")

        fig_sent = go.Figure()

        # ── Fill areas: green above zero, red below ──
        pos = chart_df["net_sentiment"].clip(lower=0)
        neg = chart_df["net_sentiment"].clip(upper=0)

        fig_sent.add_trace(go.Scatter(
            x=chart_df["date"], y=pos,
            fill="tozeroy", mode="none",
            fillcolor="rgba(0,255,136,0.10)",
            name="Positive zone", showlegend=False,
            hoverinfo="skip",
        ))
        fig_sent.add_trace(go.Scatter(
            x=chart_df["date"], y=neg,
            fill="tozeroy", mode="none",
            fillcolor="rgba(255,68,68,0.10)",
            name="Negative zone", showlegend=False,
            hoverinfo="skip",
        ))

        # ── Trace 1: Daily net sentiment ──
        fig_sent.add_trace(go.Scatter(
            x=chart_df["date"],
            y=chart_df["net_sentiment"],
            mode="lines",
            name="Daily Sentiment",
            line=dict(color="#00d4ff", width=3),
            customdata=np.stack([
                chart_df["sentiment_ma_3d"].fillna(float("nan")),
                chart_df["sentiment_ma_7d"].fillna(float("nan")),
                chart_df["sentiment_ma_30d"].fillna(float("nan")),
            ], axis=1),
            hovertemplate=(
                "<b>%{x|%b %d, %Y}</b><br>"
                "Daily:  %{y:.3f}<br>"
                "MA-3D:  %{customdata[0]:.3f}<br>"
                "MA-7D:  %{customdata[1]:.3f}<br>"
                "MA-30D: %{customdata[2]:.3f}<extra></extra>"
            ),
        ))

        # ── Trace 2: 3-Day MA ──
        fig_sent.add_trace(go.Scatter(
            x=chart_df["date"],
            y=chart_df["sentiment_ma_3d"],
            mode="lines",
            name="3-Day MA",
            line=dict(color="#ffeb3b", width=2),
            hoverinfo="skip",
        ))

        # ── Trace 3: 7-Day MA ──
        fig_sent.add_trace(go.Scatter(
            x=chart_df["date"],
            y=chart_df["sentiment_ma_7d"],
            mode="lines",
            name="7-Day MA",
            line=dict(color="#ff9800", width=2.5),
            hoverinfo="skip",
        ))

        # ── Trace 4: 30-Day MA ──
        fig_sent.add_trace(go.Scatter(
            x=chart_df["date"],
            y=chart_df["sentiment_ma_30d"],
            mode="lines",
            name="30-Day MA",
            line=dict(color="#f44336", width=3, dash="dash"),
            hoverinfo="skip",
        ))

        fig_sent.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#b8c5d6", family="Courier New"),
            title=dict(
                text="Sentiment Moving Averages — Daily / 3D / 7D / 30D",
                font=dict(color="#ffffff", size=14),
            ),
            xaxis=dict(
                gridcolor="#243a52",
                griddash="dot",
                showgrid=True,
                zeroline=False,
                rangeslider=dict(visible=True, bgcolor="#0d1d2e", thickness=0.06),
            ),
            yaxis=dict(
                gridcolor="#243a52",
                griddash="dot",
                showgrid=True,
                zeroline=False,
                range=[-1, 1],
                title="Sentiment Score",
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom", y=1.02,
                xanchor="right", x=1,
                font=dict(color="#b8c5d6"),
                bgcolor="rgba(0,0,0,0)",
            ),
            shapes=[dict(
                type="line", xref="paper", x0=0, x1=1,
                yref="y", y0=0, y1=0,
                line=dict(color="white", dash="dot", width=1),
                opacity=0.3,
            )],
            margin=dict(l=40, r=20, t=50, b=20),
            height=400,
            hovermode="x unified",
        )

        st.plotly_chart(fig_sent, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("No sentiment data for chart.")

    # ── Sentiment vs Price Dual-Axis Overlay ──
    st.markdown('<p class="section-title">Sentiment vs Price Overlay</p>', unsafe_allow_html=True)

    if ticker == "All Tickers":
        st.info("Please select a specific ticker to view dual-axis chart.")
    else:
        sent_df = df_sent[df_sent["ticker"] == ticker].sort_values("date")[["date", "net_sentiment"]].dropna()
        prc_df  = df_price[df_price["ticker"] == ticker].sort_values("date")[["date", "close"]].dropna() if not df_price.empty else pd.DataFrame()

        if sent_df.empty or prc_df.empty:
            st.info("Insufficient data to render dual-axis chart for the selected period.")
        else:
            merged = pd.merge(sent_df, prc_df, on="date", how="inner").sort_values("date")

            if merged.empty:
                st.info("No overlapping dates between sentiment and price data.")
            else:
                fig_overlay = go.Figure()

                # Background shading: positive sentiment = green fill, negative = red fill
                pos_sent = merged["net_sentiment"].clip(lower=0)
                neg_sent = merged["net_sentiment"].clip(upper=0)

                fig_overlay.add_trace(go.Scatter(
                    x=merged["date"], y=pos_sent,
                    fill="tozeroy", mode="none",
                    fillcolor="rgba(0,255,136,0.12)",
                    showlegend=False, hoverinfo="skip",
                    yaxis="y",
                ))
                fig_overlay.add_trace(go.Scatter(
                    x=merged["date"], y=neg_sent,
                    fill="tozeroy", mode="none",
                    fillcolor="rgba(255,68,68,0.12)",
                    showlegend=False, hoverinfo="skip",
                    yaxis="y",
                ))

                # Sentiment line (left axis)
                fig_overlay.add_trace(go.Scatter(
                    x=merged["date"],
                    y=merged["net_sentiment"],
                    mode="lines",
                    name="Sentiment",
                    line=dict(color="#00d4ff", width=2.5),
                    yaxis="y",
                    hovertemplate="<b>%{x|%b %d}</b><br>Sentiment: %{y:.3f}<extra></extra>",
                ))

                # Price line (right axis)
                fig_overlay.add_trace(go.Scatter(
                    x=merged["date"],
                    y=merged["close"],
                    mode="lines",
                    name="Price",
                    line=dict(color="#00ff88", width=2.5),
                    yaxis="y2",
                    hovertemplate="<b>%{x|%b %d}</b><br>Price: $%{y:.2f}<extra></extra>",
                ))

                fig_overlay.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#b8c5d6", family="Courier New"),
                    title=dict(
                        text=f"Sentiment vs Price — Dual-Axis View ({ticker})",
                        font=dict(color="#ffffff", size=14),
                    ),
                    xaxis=dict(
                        gridcolor="#243a52",
                        griddash="dot",
                        showgrid=True,
                        zeroline=False,
                    ),
                    yaxis=dict(
                        title=dict(text="Sentiment", font=dict(color="#00d4ff")),
                        tickfont=dict(color="#00d4ff"),
                        gridcolor="#243a52",
                        griddash="dot",
                        showgrid=True,
                        zeroline=False,
                        range=[-1, 1],
                        side="left",
                    ),
                    yaxis2=dict(
                        title=dict(text="Price (USD)", font=dict(color="#00ff88")),
                        tickfont=dict(color="#00ff88"),
                        gridcolor="rgba(0,0,0,0)",
                        showgrid=False,
                        zeroline=False,
                        overlaying="y",
                        side="right",
                    ),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom", y=1.02,
                        xanchor="right", x=1,
                        bgcolor="rgba(0,0,0,0)",
                        font=dict(color="#b8c5d6"),
                    ),
                    shapes=[dict(
                        type="line", xref="paper", x0=0, x1=1,
                        yref="y", y0=0, y1=0,
                        line=dict(color="white", dash="dot", width=1),
                        opacity=0.3,
                    )],
                    margin=dict(l=60, r=60, t=50, b=40),
                    height=350,
                    hovermode="x unified",
                )

                st.plotly_chart(fig_overlay, use_container_width=True, config={"displayModeBar": False})

    # ── Bull/Bear Ratio Timeline ──
    st.markdown('<p class="section-title">Bull/Bear Ratio Timeline</p>', unsafe_allow_html=True)

    if not df_sent.empty:
        if ticker == "All Tickers":
            bb_df = (
                df_sent.groupby("date")
                .agg(bullish_count=("bullish_count", "sum"), bearish_count=("bearish_count", "sum"))
                .reset_index()
            )
            bb_df["bull_bear_ratio"] = bb_df.apply(
                lambda r: r["bullish_count"] / r["bearish_count"] if r["bearish_count"] > 0
                else (r["bullish_count"] if r["bullish_count"] > 0 else None),
                axis=1,
            )
        else:
            bb_df = df_sent[df_sent["ticker"] == ticker][["date", "bull_bear_ratio", "bullish_count", "bearish_count"]].copy()
            bb_df["bull_bear_ratio"] = bb_df.apply(
                lambda r: r["bull_bear_ratio"] if r["bearish_count"] > 0
                else (r["bullish_count"] if r["bullish_count"] > 0 else None),
                axis=1,
            )

        bb_df = bb_df.dropna(subset=["bull_bear_ratio"]).sort_values("date")

        if not bb_df.empty:
            def bar_color(ratio):
                if ratio > 1.1:
                    return "#00ff88"
                elif ratio < 0.9:
                    return "#ff4444"
                else:
                    return "#ffeb3b"

            colors = [bar_color(r) for r in bb_df["bull_bear_ratio"]]

            fig_bb = go.Figure()
            fig_bb.add_trace(go.Bar(
                x=bb_df["date"],
                y=bb_df["bull_bear_ratio"],
                marker_color=colors,
                customdata=bb_df[["bullish_count", "bearish_count"]].values,
                hovertemplate=(
                    "<b>%{x|%b %d, %Y}</b><br>"
                    "Ratio: %{y:.2f}:1<br>"
                    "Bullish: %{customdata[0]}<br>"
                    "Bearish: %{customdata[1]}<extra></extra>"
                ),
                name="Bull/Bear Ratio",
            ))

            fig_bb.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#b8c5d6", family="Courier New"),
                title=dict(text="Bull/Bear Ratio Timeline", font=dict(color="#ffffff", size=14)),
                xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False),
                yaxis=dict(
                    gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False,
                    title="Ratio",
                ),
                shapes=[dict(
                    type="line", xref="paper", x0=0, x1=1,
                    yref="y", y0=1.0, y1=1.0,
                    line=dict(color="white", dash="dash", width=1),
                    opacity=0.5,
                )],
                annotations=[
                    dict(
                        xref="paper", x=0.01, yref="y", y=1.06,
                        text="Bullish >", showarrow=False,
                        font=dict(color="#00ff88", size=10),
                    ),
                    dict(
                        xref="paper", x=0.01, yref="y", y=0.94,
                        text="< Bearish", showarrow=False,
                        font=dict(color="#ff4444", size=10),
                    ),
                ],
                legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
                margin=dict(l=40, r=20, t=50, b=40),
                height=300,
                showlegend=False,
            )

            st.plotly_chart(fig_bb, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No bull/bear data for the selected period.")

    # ── Article Volume ──
    st.markdown('<p class="section-title">Article Volume</p>', unsafe_allow_html=True)

    if not df_sent.empty:
        if ticker == "All Tickers":
            vol_df = df_sent.groupby(["date", "ticker"])["article_count"].sum().reset_index()
        else:
            vol_df = df_sent[["date", "ticker", "article_count"]].copy()

        fig_vol = go.Figure()
        ticker_colors = {"AAPL": "#00d4ff", "MSFT": "#00ff88", "GOOGL": "#ff9800"}
        for t in vol_df["ticker"].unique():
            t_df = vol_df[vol_df["ticker"] == t].sort_values("date")
            fig_vol.add_trace(go.Bar(
                x=t_df["date"], y=t_df["article_count"],
                name=t, marker_color=ticker_colors.get(t, "#00d4ff"),
            ))

        fig_vol.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text="Daily Article Volume by Ticker", font=dict(color="#ffffff", size=14)),
            yaxis_title="Articles",
            barmode="stack",
            height=260,
        )
        st.plotly_chart(fig_vol, use_container_width=True)

    # ── Price Timeline ──
    st.markdown('<p class="section-title">Price Data</p>', unsafe_allow_html=True)

    if not df_price.empty:
        if ticker == "All Tickers":
            price_df = df_price.copy()
        else:
            price_df = df_price[df_price["ticker"] == ticker].copy() if "ticker" in df_price.columns else df_price.copy()

        fig_price = go.Figure()
        for t in price_df["ticker"].unique():
            t_df = price_df[price_df["ticker"] == t].sort_values("date")
            fig_price.add_trace(go.Scatter(
                x=t_df["date"], y=t_df["close"],
                mode="lines", name=t,
                line=dict(color=ticker_colors.get(t, "#00d4ff"), width=2),
            ))

        fig_price.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text="Close Price", font=dict(color="#ffffff", size=14)),
            yaxis_title="Price (USD)",
            height=300,
        )
        st.plotly_chart(fig_price, use_container_width=True)
    else:
        st.info("No price data for the selected period.")

    # ── Sentiment Breakdown table ──
    st.markdown('<p class="section-title">Sentiment Breakdown (Latest 10 Days)</p>', unsafe_allow_html=True)
    display_cols = ["date", "ticker", "article_count", "net_sentiment", "sentiment_ma_7d",
                    "bull_bear_ratio", "bullish_count", "bearish_count", "neutral_count"]
    display_df = df_sent[display_cols].sort_values("date", ascending=False).head(10)
    display_df = display_df.rename(columns={
        "date": "Date", "ticker": "Ticker", "article_count": "Articles",
        "net_sentiment": "Net Sentiment", "sentiment_ma_7d": "MA-7D",
        "bull_bear_ratio": "Bull/Bear", "bullish_count": "Bullish",
        "bearish_count": "Bearish", "neutral_count": "Neutral",
    })
    for col in ["Net Sentiment", "MA-7D", "Bull/Bear"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ── Page 2: Trading Signals & Backtesting ─────────────────────────────────────

def page_trading_signals(ticker: str):
    import plotly.graph_objects as go

    SIGNAL_COLORS = {"BUY": "#00ff88", "SELL": "#ff4444", "HOLD": "#ffeb3b"}

    CHART_LAYOUT = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#b8c5d6", family="Courier New"),
        xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False),
        yaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
        margin=dict(l=50, r=20, t=50, b=40),
    )

    st.markdown("""
<div class="dash-header">
  <p class="dash-title">Trading <span>Signals</span> & Backtesting</p>
  <p class="dash-subtitle">Sentiment-driven signal generation and next-day return analysis</p>
</div>
""", unsafe_allow_html=True)

    # ── Data note ──
    st.markdown("""
<div style="background:#162c43;border:1px solid #1e3a52;border-left:4px solid #00d4ff;
            border-radius:8px;padding:0.8rem 1.2rem;margin-bottom:1rem;
            color:#b8c5d6;font-size:0.85rem;">
  📊 Backtest covers 25 signals across 3 tickers (AAPL, MSFT, GOOGL) from March 5 – April 4, 2026.
  HOLD-heavy distribution (80%) reflects cautious market sentiment during this period.
  Returns shown are actual next-day price change (%). Signals without next-day data are excluded from return metrics.
</div>
""", unsafe_allow_html=True)

    # ── Load data ──
    try:
        df = load_trading_signals(ticker)
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if df.empty:
        st.warning("No trading signals found for the selected ticker.")
        return

    df["date"] = pd.to_datetime(df["date"])
    df["signal_strength"] = pd.to_numeric(df["signal_strength"], errors="coerce").fillna(0)
    df["next_day_return"] = pd.to_numeric(df["next_day_return"], errors="coerce")

    df_ret = df.dropna(subset=["next_day_return"])  # rows with known returns

    # ── KPI Calculations ──
    total_signals = len(df)
    counts = df["signal"].value_counts()
    buy_ct  = int(counts.get("BUY",  0))
    sell_ct = int(counts.get("SELL", 0))
    hold_ct = int(counts.get("HOLD", 0))

    win_rate = (df_ret["next_day_return"] > 0).mean() * 100 if not df_ret.empty else float("nan")

    avg_by_signal = df_ret.groupby("signal")["next_day_return"].mean() if not df_ret.empty else pd.Series(dtype=float)
    overall_avg   = df_ret["next_day_return"].mean() if not df_ret.empty else float("nan")

    best_row  = df_ret.loc[df_ret["next_day_return"].idxmax()] if not df_ret.empty else None
    worst_row = df_ret.loc[df_ret["next_day_return"].idxmin()] if not df_ret.empty else None

    # Best performing signal type
    best_signal = avg_by_signal.idxmax() if not avg_by_signal.empty else "N/A"
    best_signal_ret = avg_by_signal.max()   if not avg_by_signal.empty else float("nan")

    # ── KPI Cards ──
    st.markdown('<p class="section-title">Performance Overview</p>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        kpi_card(
            "📡", "Total Signals", str(total_signals),
            f"{buy_ct} BUY / {hold_ct} HOLD / {sell_ct} SELL",
            delta_positive=None,
        )

    with c2:
        wr_str = f"{win_rate:.1f}%" if not np.isnan(win_rate) else "N/A"
        kpi_card(
            "🎯", "Win Rate",  wr_str,
            f"{len(df_ret)} trades with known return",
            delta_positive=win_rate > 50 if not np.isnan(win_rate) else None,
        )

    with c3:
        avg_str = f"{best_signal_ret:+.2f}%" if not np.isnan(best_signal_ret) else "N/A"
        kpi_card(
            "📈", f"Best Signal ({best_signal})", avg_str,
            f"Overall avg: {overall_avg:+.2f}%" if not np.isnan(overall_avg) else "No data",
            delta_positive=best_signal_ret > 0 if not np.isnan(best_signal_ret) else None,
        )

    with c4:
        if best_row is not None and worst_row is not None:
            best_str  = f"+{best_row['next_day_return']:.2f}%"
            worst_str = f"{worst_row['next_day_return']:.2f}%"
            kpi_card(
                "⚡", "Best / Worst Trade",
                best_str,
                f"Worst: {worst_str} ({worst_row['ticker']} {worst_row['date'].strftime('%b %d')})",
                delta_positive=True,
            )
        else:
            kpi_card("⚡", "Best / Worst Trade", "N/A", "No return data", delta_positive=None)

    # ── Scatter: Signals over time ──
    st.markdown('<p class="section-title">Signals Over Time</p>', unsafe_allow_html=True)

    fig_scatter = go.Figure()

    for sig, color in SIGNAL_COLORS.items():
        mask = df["signal"] == sig
        sub  = df[mask]
        if sub.empty:
            continue
        # Scale marker size: HOLD has strength=0, give it a fixed small size
        sizes = (sub["signal_strength"] * 400 + 10).clip(lower=10, upper=40)
        ret_vals = sub["next_day_return"].fillna(float("nan"))

        fig_scatter.add_trace(go.Scatter(
            x=sub["date"],
            y=ret_vals,
            mode="markers",
            name=sig,
            marker=dict(
                color=color,
                size=sizes,
                opacity=0.85,
                line=dict(color="rgba(255,255,255,0.2)", width=1),
            ),
            customdata=np.column_stack([
                sub["ticker"].values,
                sub["signal_strength"].values,
                sub["avg_net_sentiment"].values,
            ]),
            hovertemplate=(
                "<b>%{x|%b %d, %Y}</b><br>"
                "Ticker:    %{customdata[0]}<br>"
                "Signal:    " + sig + "<br>"
                "Next-Day Return: %{y:.2f}%<br>"
                "Signal Strength: %{customdata[1]:.4f}<br>"
                "Avg Sentiment:   %{customdata[2]:.4f}"
                "<extra></extra>"
            ),
        ))

    # Zero-return reference line
    fig_scatter.add_hline(
        y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)", line_width=1,
    )

    fig_scatter.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#b8c5d6", family="Courier New"),
        title=dict(text="Trading Signals Over Time", font=dict(color="#ffffff", size=14)),
        xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False, title="Date"),
        yaxis=dict(
            gridcolor="#243a52", griddash="dot", showgrid=True,
            title="Next-Day Return (%)",
            zeroline=True, zerolinecolor="rgba(255,255,255,0.2)",
        ),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
        margin=dict(l=50, r=20, t=50, b=40),
        height=400,
        hovermode="closest",
    )

    st.plotly_chart(fig_scatter, use_container_width=True, config={"displayModeBar": False})

    # ── Two charts side by side ──
    st.markdown('<p class="section-title">Signal Distribution & Return Analysis</p>', unsafe_allow_html=True)
    col_left, col_right = st.columns(2)

    with col_left:
        # Bar chart: signal distribution
        sig_order = ["BUY", "HOLD", "SELL"]
        sig_counts = [int(counts.get(s, 0)) for s in sig_order]
        sig_pcts   = [c / total_signals * 100 for c in sig_counts]
        bar_colors = [SIGNAL_COLORS[s] for s in sig_order]

        fig_bar = go.Figure(go.Bar(
            x=sig_order,
            y=sig_counts,
            marker_color=bar_colors,
            text=[f"{c}<br>({p:.0f}%)" for c, p in zip(sig_counts, sig_pcts)],
            textposition="outside",
            textfont=dict(color="#ffffff", size=11),
            hovertemplate="<b>%{x}</b><br>Count: %{y}<extra></extra>",
        ))

        fig_bar.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#b8c5d6", family="Courier New"),
            title=dict(text="Signal Type Distribution", font=dict(color="#ffffff", size=14)),
            xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False, title=""),
            yaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False, title="Count"),
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
            margin=dict(l=50, r=20, t=50, b=40),
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

    with col_right:
        # Bar chart: avg return by signal type (only signals with return data)
        if not avg_by_signal.empty:
            ret_sigs   = [s for s in sig_order if s in avg_by_signal.index]
            ret_vals_b = [float(avg_by_signal[s]) for s in ret_sigs]
            ret_colors = [SIGNAL_COLORS[s] for s in ret_sigs]

            # Error bars: std dev
            std_by_signal = df_ret.groupby("signal")["next_day_return"].std()
            ret_errs = [float(std_by_signal.get(s, 0)) for s in ret_sigs]

            fig_ret = go.Figure(go.Bar(
                x=ret_sigs,
                y=ret_vals_b,
                marker_color=ret_colors,
                error_y=dict(type="data", array=ret_errs, color="#ffffff", thickness=1.5, width=4),
                text=[f"{v:+.2f}%" for v in ret_vals_b],
                textposition="outside",
                textfont=dict(color="#ffffff", size=11),
                hovertemplate="<b>%{x}</b><br>Avg Return: %{y:.2f}%<extra></extra>",
            ))

            fig_ret.add_hline(
                y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)", line_width=1,
            )

            fig_ret.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#b8c5d6", family="Courier New"),
                title=dict(text="Avg Next-Day Return by Signal Type", font=dict(color="#ffffff", size=14)),
                xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False, title=""),
                yaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True,
                           title="Avg Return (%)", zeroline=True, zerolinecolor="rgba(255,255,255,0.2)"),
                legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
                margin=dict(l=50, r=20, t=50, b=40),
                height=300,
                showlegend=False,
            )
            st.plotly_chart(fig_ret, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("Insufficient return data to calculate averages.")

    # ── Signal log table ──
    st.markdown('<p class="section-title">Signal Log</p>', unsafe_allow_html=True)
    display_df = df.sort_values("date", ascending=False).copy()
    display_df["date"] = display_df["date"].dt.strftime("%b %d, %Y")
    display_df["signal_strength"] = display_df["signal_strength"].apply(lambda x: f"{x:.4f}")
    display_df["avg_net_sentiment"] = display_df["avg_net_sentiment"].apply(
        lambda x: f"{x:+.4f}" if pd.notna(x) else "—"
    )
    display_df["next_day_return"] = display_df["next_day_return"].apply(
        lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"
    )
    display_df = display_df.rename(columns={
        "date": "Date", "ticker": "Ticker", "signal": "Signal",
        "signal_strength": "Signal Strength", "avg_net_sentiment": "Avg Sentiment",
        "next_day_return": "Next-Day Return",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ── Page 3: Statistical Analysis ─────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_sentiment_with_prices(ticker: str) -> pd.DataFrame:
    engine = get_db_connection()
    where = "WHERE ticker = :ticker" if ticker != "All Tickers" else ""
    query = f"""
        SELECT ticker, date, avg_net_sentiment, sentiment_volatility,
               price_change_pct, volume
        FROM analytics.sentiment_with_prices
        {where}
        ORDER BY ticker, date
    """
    params = {"ticker": ticker} if ticker != "All Tickers" else {}
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params if params else None)


def _corr_heatmap(df: pd.DataFrame, title: str, height: int = 350):
    """Return a Plotly heatmap figure for the 4 correlation variables."""
    import plotly.graph_objects as go

    cols = ["avg_net_sentiment", "sentiment_volatility", "price_change_pct", "volume"]
    labels = ["Sentiment", "Sent. Volatility", "Price Chg %", "Volume"]
    numeric = df[cols].apply(pd.to_numeric, errors="coerce").dropna()

    if len(numeric) < 3:
        return None

    corr = numeric.corr()
    z = corr.values
    text = [[f"{v:.2f}" for v in row] for row in z]

    fig = go.Figure(go.Heatmap(
        z=z,
        x=labels,
        y=labels,
        text=text,
        texttemplate="%{text}",
        textfont=dict(size=11, color="#ffffff"),
        colorscale="RdBu_r",
        zmin=-1, zmax=1,
        showscale=True,
        colorbar=dict(thickness=10, tickfont=dict(color="#b8c5d6", size=9)),
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#b8c5d6", family="Courier New"),
        title=dict(text=title, font=dict(color="#ffffff", size=13)),
        margin=dict(l=10, r=10, t=40, b=10),
        height=height,
        xaxis=dict(side="bottom", tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=10)),
    )
    return fig


def page_statistical_analysis(ticker: str):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    from scipy import stats

    CHART_LAYOUT = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#b8c5d6", family="Courier New"),
        xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False),
        yaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
        margin=dict(l=50, r=20, t=50, b=40),
    )

    st.markdown("""
<div class="dash-header">
  <p class="dash-title">Statistical <span>Analysis</span></p>
  <p class="dash-subtitle">Correlation matrices and statistical significance testing</p>
</div>
""", unsafe_allow_html=True)

    # ── Load data ──
    try:
        df_all  = load_sentiment_with_prices("All Tickers")
        df_filt = load_sentiment_with_prices(ticker)
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if df_all.empty:
        st.warning("No data available.")
        return

    for col in ["avg_net_sentiment", "sentiment_volatility", "price_change_pct", "volume"]:
        df_all[col]  = pd.to_numeric(df_all[col],  errors="coerce")
        df_filt[col] = pd.to_numeric(df_filt[col], errors="coerce")

    # ── Section 1: Overall heatmap + by-ticker heatmaps ──
    st.markdown('<p class="section-title">Correlation Heatmaps</p>', unsafe_allow_html=True)
    col_left, col_right = st.columns([1, 1])

    with col_left:
        label = ticker if ticker != "All Tickers" else "All Tickers"
        fig_overall = _corr_heatmap(df_filt, f"Overall Correlation — {label}", height=360)
        if fig_overall:
            st.plotly_chart(fig_overall, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("Insufficient data for correlation heatmap.")

    with col_right:
        st.markdown(
            '<div style="font-size:13px;font-weight:600;color:#ffffff;'
            'margin-bottom:0.5rem">By-Ticker Comparison</div>',
            unsafe_allow_html=True,
        )
        ticker_colors_map = {"AAPL": "#00d4ff", "MSFT": "#00ff88", "GOOGL": "#ff9800"}
        cols_data = ["avg_net_sentiment", "sentiment_volatility", "price_change_pct", "volume"]
        labels = ["Sentiment", "Sent. Vol.", "Price Chg", "Volume"]

        fig_sub = make_subplots(
            rows=1, cols=3,
            subplot_titles=["AAPL", "MSFT", "GOOGL"],
            horizontal_spacing=0.08,
        )

        for i, t in enumerate(["AAPL", "MSFT", "GOOGL"], 1):
            sub = df_all[df_all["ticker"] == t][cols_data].dropna()
            if len(sub) >= 3:
                z = sub.corr().values
                text = [[f"{v:.2f}" for v in row] for row in z]
            else:
                z = [[0] * 4] * 4
                text = [["N/A"] * 4] * 4

            fig_sub.add_trace(
                go.Heatmap(
                    z=z, x=labels, y=labels,
                    text=text, texttemplate="%{text}",
                    textfont=dict(size=8, color="#ffffff"),
                    colorscale="RdBu_r", zmin=-1, zmax=1,
                    showscale=(i == 3),
                    colorbar=dict(thickness=8, tickfont=dict(color="#b8c5d6", size=8), x=1.02),
                ),
                row=1, col=i,
            )

        fig_sub.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#b8c5d6", family="Courier New", size=9),
            height=360,
            margin=dict(l=5, r=40, t=30, b=5),
        )
        for ax in ["xaxis", "xaxis2", "xaxis3", "yaxis", "yaxis2", "yaxis3"]:
            fig_sub.update_layout(**{ax: dict(tickfont=dict(size=8))})

        st.plotly_chart(fig_sub, use_container_width=True, config={"displayModeBar": False})

    # ── Section 2: Significance table ──
    st.markdown('<p class="section-title">Statistical Significance</p>', unsafe_allow_html=True)

    src = df_filt.dropna(subset=["avg_net_sentiment", "sentiment_volatility",
                                  "price_change_pct", "volume"])

    relationships = [
        ("Sentiment → Price Change",        "avg_net_sentiment",    "price_change_pct"),
        ("Sent. Volatility → Price Volatility", "sentiment_volatility", "price_change_pct"),
        ("Sentiment → Volume",              "avg_net_sentiment",    "volume"),
        ("Sent. Volatility → Volume",       "sentiment_volatility", "volume"),
    ]

    rows = []
    for name, col_a, col_b in relationships:
        a = pd.to_numeric(src[col_a], errors="coerce")
        b = pd.to_numeric(src[col_b], errors="coerce")
        mask = a.notna() & b.notna()
        if mask.sum() >= 3:
            r, p = stats.pearsonr(a[mask], b[mask])
        else:
            r, p = float("nan"), float("nan")
        if not np.isnan(p):
            sig = "Yes (p<0.05)" if p < 0.05 else ("Marginal" if p < 0.10 else "No")
        else:
            sig = "N/A"
        rows.append({"Relationship": name, "Correlation (r)": round(r, 3),
                     "P-value": round(p, 4) if not np.isnan(p) else None, "Significant?": sig})

    sig_df = pd.DataFrame(rows)

    def _color_sig(row):
        if row["Significant?"] == "Yes (p<0.05)":
            bg = "background-color: rgba(0,200,100,0.18)"
        elif row["Significant?"] == "Marginal":
            bg = "background-color: rgba(255,235,59,0.18)"
        else:
            bg = "background-color: rgba(255,68,68,0.12)"
        return [bg] * len(row)

    styled = sig_df.style.apply(_color_sig, axis=1).format(
        {"Correlation (r)": "{:.3f}", "P-value": lambda x: f"{x:.4f}" if x is not None else "N/A"}
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # ── Section 3: Key Insights ──
    st.markdown('<p class="section-title">Key Insights</p>', unsafe_allow_html=True)

    insights = []
    for row in rows:
        r_val = row["Correlation (r)"]
        p_val = row["P-value"]
        if np.isnan(r_val):
            continue
        strength = "strong" if abs(r_val) > 0.5 else ("moderate" if abs(r_val) > 0.3 else "weak")
        direction = "positive" if r_val > 0 else "negative"
        sig_str = f"p={p_val:.4f}" if p_val is not None else "p=N/A"
        insights.append(
            f"**{row['Relationship']}** shows a {strength} {direction} correlation "
            f"(r={r_val:.3f}, {sig_str}) — "
            + ("statistically significant." if row["Significant?"] == "Yes (p<0.05)"
               else "not statistically significant." if row["Significant?"] == "No"
               else "marginally significant.")
        )

    # Which ticker has strongest sentiment-price correlation
    best_t, best_r = None, 0.0
    for t in ["AAPL", "MSFT", "GOOGL"]:
        sub = df_all[df_all["ticker"] == t][["avg_net_sentiment", "price_change_pct"]].dropna()
        if len(sub) >= 3:
            r_t, _ = stats.pearsonr(sub["avg_net_sentiment"], sub["price_change_pct"])
            if abs(r_t) > abs(best_r):
                best_r, best_t = r_t, t
    if best_t:
        insights.append(
            f"**{best_t}** exhibits the strongest sentiment-price relationship "
            f"(r={best_r:.3f}) among the three tickers."
        )

    st.markdown("\n\n".join(f"- {i}" for i in insights), unsafe_allow_html=False)

# ── Page 4: Multi-Ticker Comparison ──────────────────────────────────────────

@st.cache_data(ttl=300)
def load_signals_all_tickers() -> pd.DataFrame:
    engine = get_db_connection()
    query = """
        SELECT ticker, date, signal, signal_strength, avg_net_sentiment,
               next_day_return, avg_net_sentiment AS sentiment,
               price_change_pct, volume
        FROM analytics.trading_signals
        ORDER BY ticker, date
    """
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def page_multi_ticker():
    import plotly.graph_objects as go
    from scipy import stats

    TICKER_COLORS = {"AAPL": "#00d4ff", "MSFT": "#00ff88", "GOOGL": "#ff9800"}
    SIGNAL_COLORS = {"BUY": "#00ff88", "SELL": "#ff4444", "HOLD": "#ffeb3b"}

    st.markdown("""
<div class="dash-header">
  <p class="dash-title">Multi-Ticker <span>Comparison</span></p>
  <p class="dash-subtitle">Comparative performance and sentiment analysis</p>
</div>
""", unsafe_allow_html=True)

    st.markdown(
        '<div style="background:#162c43;border:1px solid #1e3a52;border-left:4px solid #00d4ff;'
        'border-radius:8px;padding:0.6rem 1.2rem;margin-bottom:1rem;'
        'color:#b8c5d6;font-size:0.85rem;">'
        'Showing all tickers for comparison — ticker filter is disabled on this page.'
        '</div>',
        unsafe_allow_html=True,
    )

    # ── Load data ──
    try:
        df = load_signals_all_tickers()
        df_prices = load_sentiment_with_prices("All Tickers")
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if df.empty:
        st.warning("No trading signals data available.")
        return

    for col in ["next_day_return", "price_change_pct", "avg_net_sentiment"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df_prices["price_change_pct"] = pd.to_numeric(df_prices["price_change_pct"], errors="coerce")
    df_prices["avg_net_sentiment"] = pd.to_numeric(df_prices["avg_net_sentiment"], errors="coerce")

    # ── Section 1: Ticker Performance Cards ──
    st.markdown('<p class="section-title">Ticker Performance</p>', unsafe_allow_html=True)
    card_cols = st.columns(3)

    stats_by_ticker = {}
    for t in ["AAPL", "MSFT", "GOOGL"]:
        sub = df[df["ticker"] == t]
        sub_ret = sub.dropna(subset=["next_day_return"])
        total = len(sub)
        wins = int((sub_ret["next_day_return"] > 0).sum())
        win_rate = wins / len(sub_ret) * 100 if len(sub_ret) > 0 else float("nan")
        avg_ret = sub_ret["next_day_return"].mean() * 100 if not sub_ret.empty else float("nan")
        best_ret = sub_ret["next_day_return"].max() * 100 if not sub_ret.empty else float("nan")
        best_date = sub_ret.loc[sub_ret["next_day_return"].idxmax(), "date"] if not sub_ret.empty else None
        stats_by_ticker[t] = dict(total=total, win_rate=win_rate, avg_ret=avg_ret,
                                   best_ret=best_ret, best_date=best_date)

    for i, t in enumerate(["AAPL", "MSFT", "GOOGL"]):
        s = stats_by_ticker[t]
        wr = s["win_rate"]
        border_color = ("#00ff88" if (not np.isnan(wr) and wr > 55)
                        else "#ffeb3b" if (not np.isnan(wr) and wr >= 45)
                        else "#ff4444")
        wr_str   = f"{wr:.1f}%" if not np.isnan(wr) else "N/A"
        avg_str  = f"{s['avg_ret']:+.2f}%" if not np.isnan(s["avg_ret"]) else "N/A"
        best_str = f"{s['best_ret']:+.2f}%" if not np.isnan(s["best_ret"]) else "N/A"
        date_str = s["best_date"].strftime("%b %d") if s["best_date"] is not None else ""

        with card_cols[i]:
            st.markdown(f"""
<div style="background:rgba(22,44,67,0.8);border:2px solid {border_color};
            border-radius:10px;padding:1.2rem 1.4rem;margin-bottom:0.5rem;">
  <div style="font-size:1.3rem;font-weight:800;color:#ffffff;
              font-family:Courier New;margin-bottom:0.8rem">{t}</div>
  <div style="font-size:0.72rem;color:#b8c5d6;text-transform:uppercase;
              letter-spacing:0.08em">Total Signals</div>
  <div style="font-size:1.6rem;font-weight:700;color:#ffffff;
              font-family:Courier New">{s['total']}</div>
  <div style="margin-top:0.6rem;font-size:0.72rem;color:#b8c5d6;
              text-transform:uppercase;letter-spacing:0.08em">Win Rate</div>
  <div style="font-size:1.4rem;font-weight:700;color:{border_color};
              font-family:Courier New">{wr_str}</div>
  <div style="margin-top:0.6rem;font-size:0.72rem;color:#b8c5d6;
              text-transform:uppercase;letter-spacing:0.08em">Avg Return</div>
  <div style="font-size:1.1rem;font-weight:600;color:#ffffff;
              font-family:Courier New">{avg_str}</div>
  <div style="margin-top:0.6rem;font-size:0.72rem;color:#b8c5d6;
              text-transform:uppercase;letter-spacing:0.08em">Best Trade</div>
  <div style="font-size:1.1rem;font-weight:600;color:#00ff88;
              font-family:Courier New">{best_str}{f" ({date_str})" if date_str else ""}</div>
</div>
""", unsafe_allow_html=True)

    # ── Section 2: Signal performance + Correlation comparison (side by side) ──
    st.markdown('<p class="section-title">Signal Performance & Correlation</p>', unsafe_allow_html=True)
    col_sig, col_corr = st.columns(2)

    with col_sig:
        fig_sig = go.Figure()
        sig_order = ["BUY", "HOLD", "SELL"]
        for t, color in TICKER_COLORS.items():
            sub = df[df["ticker"] == t].dropna(subset=["next_day_return"])
            win_rates = []
            for sig in sig_order:
                sg = sub[sub["signal"] == sig]
                wr = (sg["next_day_return"] > 0).mean() * 100 if len(sg) > 0 else 0
                win_rates.append(wr)
            fig_sig.add_trace(go.Bar(
                name=t, x=sig_order, y=win_rates,
                marker_color=color,
                text=[f"{v:.0f}%" for v in win_rates],
                textposition="outside",
                textfont=dict(color="#ffffff", size=10),
            ))

        fig_sig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#b8c5d6", family="Courier New"),
            title=dict(text="Win Rate by Signal & Ticker", font=dict(color="#ffffff", size=13)),
            xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=False, zeroline=False),
            yaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True, zeroline=False,
                       title="Win Rate (%)", range=[0, 130]),
            barmode="group",
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#b8c5d6")),
            margin=dict(l=50, r=20, t=50, b=40),
            height=340,
        )
        st.plotly_chart(fig_sig, use_container_width=True, config={"displayModeBar": False})

    with col_corr:
        corr_vals, corr_tickers = [], []
        for t in ["AAPL", "MSFT", "GOOGL"]:
            sub = df_prices[df_prices["ticker"] == t][["avg_net_sentiment", "price_change_pct"]].dropna()
            if len(sub) >= 3:
                r, _ = stats.pearsonr(sub["avg_net_sentiment"], sub["price_change_pct"])
            else:
                r = 0.0
            corr_vals.append(round(r, 3))
            corr_tickers.append(t)

        bar_colors = [TICKER_COLORS[t] for t in corr_tickers]
        fig_corr = go.Figure(go.Bar(
            x=corr_tickers,
            y=corr_vals,
            marker_color=bar_colors,
            text=[f"{v:.3f}" for v in corr_vals],
            textposition="outside",
            textfont=dict(color="#ffffff", size=11),
        ))
        fig_corr.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)", line_width=1)
        fig_corr.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#b8c5d6", family="Courier New"),
            title=dict(text="Sentiment-Price Correlation by Ticker",
                       font=dict(color="#ffffff", size=13)),
            xaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=False, zeroline=False),
            yaxis=dict(gridcolor="#243a52", griddash="dot", showgrid=True,
                       title="Pearson r", zeroline=True,
                       zerolinecolor="rgba(255,255,255,0.2)",
                       range=[-1.2, 1.2]),
            margin=dict(l=50, r=20, t=50, b=40),
            height=340,
            showlegend=False,
        )
        st.plotly_chart(fig_corr, use_container_width=True, config={"displayModeBar": False})

    # ── Section 3: Comparative Stats Table ──
    st.markdown('<p class="section-title">Comparative Stats</p>', unsafe_allow_html=True)

    table_rows = {}
    for t in ["AAPL", "MSFT", "GOOGL"]:
        s = stats_by_ticker[t]
        sub = df[df["ticker"] == t].dropna(subset=["next_day_return"])
        worst = sub["next_day_return"].min() * 100 if not sub.empty else float("nan")

        # correlation
        sp = df_prices[df_prices["ticker"] == t][["avg_net_sentiment", "price_change_pct"]].dropna()
        r_val = stats.pearsonr(sp["avg_net_sentiment"], sp["price_change_pct"])[0] if len(sp) >= 3 else float("nan")

        table_rows[t] = {
            "Total Signals": s["total"],
            "Win Rate (%)": round(s["win_rate"], 1) if not np.isnan(s["win_rate"]) else None,
            "Avg Return (%)": round(s["avg_ret"], 2) if not np.isnan(s["avg_ret"]) else None,
            "Best Trade (%)": round(s["best_ret"], 2) if not np.isnan(s["best_ret"]) else None,
            "Worst Trade (%)": round(worst, 2) if not np.isnan(worst) else None,
            "Sent-Price Corr": round(r_val, 3) if not np.isnan(r_val) else None,
        }

    stats_df = pd.DataFrame(table_rows).T  # tickers as rows, metrics as cols
    stats_df.index.name = "Ticker"
    stats_df = stats_df.reset_index()

    def _bold_best(col):
        vals = pd.to_numeric(col, errors="coerce")
        if vals.isna().all():
            return [""] * len(col)
        best_idx = vals.abs().idxmax()
        return ["font-weight: bold; color: #00ff88" if i == best_idx else "" for i in col.index]

    numeric_cols = [c for c in stats_df.columns if c != "Ticker"]
    styled_table = stats_df.style.apply(_bold_best, subset=numeric_cols)
    st.dataframe(styled_table, use_container_width=True, hide_index=True)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    st.markdown(BLOOMBERG_CSS, unsafe_allow_html=True)
    page, ticker, start_date, end_date = render_sidebar()

    if page == "Overview Dashboard":
        page_overview(ticker, start_date, end_date)
    elif page == "Trading Signals & Backtesting":
        page_trading_signals(ticker)
    elif page == "Statistical Analysis":
        page_statistical_analysis(ticker)
    elif page == "Multi-Ticker Comparison":
        page_multi_ticker()


if __name__ == "__main__":
    main()
