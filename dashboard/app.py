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

def page_trading_signals():
    st.markdown("""
<div class="dash-header">
  <p class="dash-title">Trading <span>Signals</span> & Backtesting</p>
  <p class="dash-subtitle">Automated signal generation and strategy performance analysis</p>
</div>
""", unsafe_allow_html=True)

    st.markdown("""
<div class="placeholder-card">
  <h2>🚧 Coming Soon</h2>
  <p>This page is under construction. Planned features:</p>
  <ul>
    <li>📈 Real-time buy/sell signal generation from <code>analytics.trading_signals</code></li>
    <li>💹 Backtest P&L curve vs buy-and-hold benchmark</li>
    <li>📊 Sharpe ratio, max drawdown, win rate metrics</li>
    <li>🔔 Signal alert configuration (threshold-based)</li>
    <li>📉 Sentiment-triggered entry/exit visualization</li>
  </ul>
</div>
""", unsafe_allow_html=True)

# ── Page 3: Statistical Analysis ─────────────────────────────────────────────

def page_statistical_analysis():
    st.markdown("""
<div class="dash-header">
  <p class="dash-title">Statistical <span>Analysis</span></p>
  <p class="dash-subtitle">Pearson & Spearman correlations, lag analysis, significance testing</p>
</div>
""", unsafe_allow_html=True)

    st.markdown("""
<div class="placeholder-card">
  <h2>🚧 Coming Soon</h2>
  <p>This page is under construction. Planned features:</p>
  <ul>
    <li>🔥 Interactive correlation heatmaps (AAPL, MSFT, GOOGL, Overall)</li>
    <li>📐 Lag analysis: 1D, 3D, 7D sentiment → price lead/lag relationships</li>
    <li>🧪 Pearson & Spearman correlations with p-value significance stars</li>
    <li>📊 Distribution plots: sentiment vs return scatter charts</li>
    <li>📋 Full correlation table with CSV export</li>
  </ul>
</div>
""", unsafe_allow_html=True)

# ── Page 4: Multi-Ticker Comparison ──────────────────────────────────────────

def page_multi_ticker():
    st.markdown("""
<div class="dash-header">
  <p class="dash-title">Multi-Ticker <span>Comparison</span></p>
  <p class="dash-subtitle">Side-by-side sentiment, price, and correlation comparison across tickers</p>
</div>
""", unsafe_allow_html=True)

    st.markdown("""
<div class="placeholder-card">
  <h2>🚧 Coming Soon</h2>
  <p>This page is under construction. Planned features:</p>
  <ul>
    <li>🔄 Normalized price & sentiment comparison: AAPL vs MSFT vs GOOGL</li>
    <li>📊 Side-by-side KPI comparison grid</li>
    <li>🏆 Ticker ranking by sentiment momentum and coverage</li>
    <li>📈 Relative strength index built on sentiment MA divergence</li>
    <li>🗓️ Event timeline: high-volatility sentiment days across all tickers</li>
  </ul>
</div>
""", unsafe_allow_html=True)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    st.markdown(BLOOMBERG_CSS, unsafe_allow_html=True)
    page, ticker, start_date, end_date = render_sidebar()

    if page == "Overview Dashboard":
        page_overview(ticker, start_date, end_date)
    elif page == "Trading Signals & Backtesting":
        page_trading_signals()
    elif page == "Statistical Analysis":
        page_statistical_analysis()
    elif page == "Multi-Ticker Comparison":
        page_multi_ticker()


if __name__ == "__main__":
    main()
