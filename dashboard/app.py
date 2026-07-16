"""
app.py — Dev World Radar
=========================
A Streamlit dashboard over the Gold Iceberg tables (demo.gold.*), read
directly via pyiceberg's REST catalog client — no Spark required.

Tabs:
  Trending Repos        — virality rankings per day/week/month window
  Tech Stack            — language market share for the latest analysis date
  Dev World Orientation — momentum (rising/cooling languages), fastest-growing
                          repos, and an optional Groq-powered AI briefing
"""

import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

# Allow running both via `streamlit run dashboard/app.py` and as a package import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import data_loader
import insights

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Dev World Radar", layout="wide")

st.markdown(
    """
    <style>
        .block-container { padding-top: 1.5rem; }
        .demo-banner {
            background: linear-gradient(90deg, #0f172a 0%, #1e293b 100%);
            color: #e2e8f0;
            padding: 0.75rem 1rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
            font-size: 0.9rem;
        }
        .demo-banner code {
            background: #334155;
            color: #f8fafc;
            padding: 0.15rem 0.35rem;
            border-radius: 0.25rem;
        }
        div[data-testid="stMetricValue"] { font-size: 1.6rem; font-weight: 700; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _md_table(df: pd.DataFrame, cols: list, max_rows: int = 15) -> str:
    """Render a small markdown table (avoids the optional `tabulate` dependency)."""
    if df.empty:
        return "(no data)"
    df = df[cols].head(max_rows)
    lines = ["| " + " | ".join(cols) + " |", "|" + " --- |" * len(cols)]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in cols) + " |")
    return "\n".join(lines)


def _repo_link_column(label: str = "Repo"):
    """LinkColumn config: cell holds a github.com URL, text shows owner/repo."""
    return st.column_config.LinkColumn(label, display_text=r"https://github\.com/(.+)")


def _macro_dict(macro: pd.DataFrame) -> dict:
    """Latest macro_stats row as a compact, JSON-friendly dict for the AI prompt."""
    if macro.empty:
        return {}
    row = macro.iloc[-1]
    keys = [
        "analysis_date", "total_events", "total_stars", "total_forks",
        "total_prs_opened", "total_prs_merged", "pr_merge_rate",
        "distinct_active_contributors", "star_to_fork_ratio",
        "avg_commits_per_push", "top_event_type", "top_language",
    ]
    out = {}
    for k in keys:
        if k in row and pd.notna(row[k]):
            v = row[k]
            if isinstance(v, pd.Timestamp):
                out[k] = v.strftime("%Y-%m-%d")
            elif hasattr(v, "item"):  # numpy scalar -> python scalar
                out[k] = v.item()
            else:
                out[k] = v
    return out


# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Dev World Radar")
    st.caption("Where the dev world is heading.")

    windows = data_loader.available_windows()
    window_type = st.selectbox(
        "Trend window", windows, index=windows.index("week") if "week" in windows else 0
    )
    top_n = st.slider("Top N repos", min_value=5, max_value=30, value=10)

    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    if os.environ.get("GROQ_API_KEY"):
        st.success("Groq key detected — AI briefing unlocked.")
    else:
        groq_key = st.text_input(
            "Groq API key (optional — unlocks the AI briefing)", type="password"
        )
        if groq_key:
            os.environ["GROQ_API_KEY"] = groq_key


# ---------------------------------------------------------------------------
# Empty state — stack unreachable and demo data disabled
# ---------------------------------------------------------------------------

if not data_loader.data_available():
    st.title("Dev World Radar")
    st.warning(
        "Can't reach the analytics stack and no demo data is available.\n\n"
        "The dashboard reads the Iceberg Gold tables through the REST catalog "
        "(default `http://localhost:8181`) backed by LocalStack S3."
    )
    st.markdown("**Start the stack, then run the pipeline:**")
    st.code(
        "cd docker && docker-compose up -d\n"
        "python main.py --action run-analytics --start-date 2024-01-15 --end-date 2024-01-15",
        language="bash",
    )
    if st.button("Retry", type="primary"):
        st.rerun()
    st.stop()


# ---------------------------------------------------------------------------
# Load Gold tables (cached)
# ---------------------------------------------------------------------------

macro = data_loader.load_macro_stats()
viral = data_loader.load_viral_repos(window_type)
trends = data_loader.load_tech_trends()
is_demo = data_loader.demo_mode()

st.title("Dev World Radar")
if is_demo:
    st.markdown(
        '<div class="demo-banner">'
        'Demo mode: showing realistic sample data. '
        'Run the pipeline to replace this with live GitHub Archive results.'
        '</div>',
        unsafe_allow_html=True,
    )
else:
    st.caption("Live from the Gold Iceberg tables via pyiceberg — no Spark involved.")


# ---------------------------------------------------------------------------
# KPI strip — latest macro_stats row, deltas vs previous analysis_date
# ---------------------------------------------------------------------------

if not macro.empty:
    latest = macro.iloc[-1]
    prev = macro.iloc[-2] if len(macro) > 1 else None

    def _delta(row, prev_row, col, suffix=""):
        if prev_row is None:
            return None
        diff = row[col] - prev_row[col]
        return f"{diff:+,.1f}{suffix}" if suffix else f"{diff:+,.0f}"

    kpi = st.columns(6)
    kpi[0].metric("Total events", f"{int(latest['total_events']):,}", _delta(latest, prev, "total_events"))
    kpi[1].metric("Stars", f"{int(latest['total_stars']):,}", _delta(latest, prev, "total_stars"))
    kpi[2].metric("Forks", f"{int(latest['total_forks']):,}", _delta(latest, prev, "total_forks"))
    kpi[3].metric("PR merge rate", f"{latest['pr_merge_rate']:.1f}%", _delta(latest, prev, "pr_merge_rate", " pp"))
    kpi[4].metric("Active contributors", f"{int(latest['distinct_active_contributors']):,}", _delta(latest, prev, "distinct_active_contributors"))
    kpi[5].metric("Top language", str(latest["top_language"]))
    st.caption(
        f"Analysis date {latest['analysis_date']:%Y-%m-%d} · "
        f"period {latest['period_start']:%Y-%m-%d} → {latest['period_end']:%Y-%m-%d} · "
        f"top event type: {latest['top_event_type']}"
    )


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_repos, tab_stack, tab_orientation = st.tabs(
    ["Trending Repos", "Tech Stack", "Dev World Orientation"]
)


# ---- Tab A: Trending Repos ------------------------------------------------

with tab_repos:
    if viral.empty:
        st.info(f"No viral repo data for the '{window_type}' window yet.")
    else:
        latest_start = viral["window_start"].max()
        current = (
            viral[viral["window_start"] == latest_start]
            .sort_values("rank_in_window")
            .head(top_n)
            .copy()
        )
        current["repo_url"] = "https://github.com/" + current["repo_name"]

        st.subheader(f"Top {len(current)} repos — '{window_type}' window starting {latest_start:%Y-%m-%d}")

        fig = px.bar(
            current.sort_values("virality_score"),
            x="virality_score", y="repo_name", orientation="h",
            color="virality_score", color_continuous_scale="Viridis",
            labels={"virality_score": "Virality score", "repo_name": ""},
            hover_data={"star_count": True, "fork_count": True, "star_velocity": ":.1f"},
        )
        fig.update_layout(coloraxis_showscale=False, height=max(360, 34 * len(current)))
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            current[[
                "repo_url", "virality_score", "star_count", "fork_count",
                "pr_opened_count", "star_velocity", "active_contributors", "rank_in_window",
            ]],
            column_config={
                "repo_url": _repo_link_column(),
                "virality_score": st.column_config.NumberColumn("Virality", format="%.0f"),
                "star_count": st.column_config.NumberColumn("Stars", format="%d"),
                "fork_count": st.column_config.NumberColumn("Forks", format="%d"),
                "pr_opened_count": st.column_config.NumberColumn("PRs", format="%d"),
                "star_velocity": st.column_config.NumberColumn("Stars/day", format="%.1f"),
                "active_contributors": st.column_config.NumberColumn("Contributors", format="%d"),
                "rank_in_window": st.column_config.NumberColumn("Rank", format="%d"),
            },
            hide_index=True,
            use_container_width=True,
        )


# ---- Tab B: Tech Stack ----------------------------------------------------

with tab_stack:
    if trends.empty:
        st.info("No tech stack trend data yet.")
    else:
        latest_date = trends["analysis_date"].max()
        current_trends = trends[trends["analysis_date"] == latest_date].sort_values("language_rank")
        top15 = current_trends.head(15).copy()

        st.subheader(f"Language market share — {latest_date:%Y-%m-%d}")

        chart_col, pie_col = st.columns([3, 2])
        with chart_col:
            fig = px.bar(
                top15.sort_values("event_share_pct"),
                x="event_share_pct", y="repo_language", orientation="h",
                color="event_share_pct", color_continuous_scale="Blues",
                labels={"event_share_pct": "Share of events (%)", "repo_language": ""},
            )
            fig.update_layout(coloraxis_showscale=False, height=520)
            st.plotly_chart(fig, use_container_width=True)
        with pie_col:
            fig = px.pie(top15, names="repo_language", values="event_share_pct", hole=0.5)
            fig.update_traces(textposition="inside", textinfo="percent")
            fig.update_layout(height=520)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Per-language activity")
        st.dataframe(
            current_trends[[
                "repo_language", "total_stars", "total_forks", "total_prs",
                "distinct_repos", "distinct_contributors", "event_share_pct", "language_rank",
            ]],
            column_config={
                "repo_language": "Language",
                "total_stars": st.column_config.NumberColumn("Stars", format="%d"),
                "total_forks": st.column_config.NumberColumn("Forks", format="%d"),
                "total_prs": st.column_config.NumberColumn("PRs", format="%d"),
                "distinct_repos": st.column_config.NumberColumn("Repos", format="%d"),
                "distinct_contributors": st.column_config.NumberColumn("Contributors", format="%d"),
                "event_share_pct": st.column_config.NumberColumn("Share (%)", format="%.2f"),
                "language_rank": st.column_config.NumberColumn("Rank", format="%d"),
            },
            hide_index=True,
            use_container_width=True,
        )


# ---- Tab C: Dev World Orientation -----------------------------------------

with tab_orientation:
    # ---- Language momentum: latest vs previous analysis_date ----
    dates = sorted(trends["analysis_date"].drop_duplicates()) if not trends.empty else []
    delta_df = pd.DataFrame()

    if len(dates) < 2:
        st.info(
            "Momentum analysis needs at least two analysis dates in "
            "`demo.gold.tech_stack_trends` — run the pipeline for more days."
        )
    else:
        prev_date, latest_date = dates[-2], dates[-1]
        prev = (
            trends[trends["analysis_date"] == prev_date][["repo_language", "event_share_pct"]]
            .rename(columns={"event_share_pct": "prev_share"})
        )
        curr = (
            trends[trends["analysis_date"] == latest_date][["repo_language", "event_share_pct"]]
            .rename(columns={"event_share_pct": "latest_share"})
        )
        delta_df = prev.merge(curr, on="repo_language", how="outer").fillna(0.0)
        delta_df["share_delta"] = delta_df["latest_share"] - delta_df["prev_share"]

        st.subheader(f"Language momentum · {prev_date:%Y-%m-%d} → {latest_date:%Y-%m-%d}")
        rising = delta_df[delta_df["share_delta"] > 0].nlargest(10, "share_delta")
        cooling = delta_df[delta_df["share_delta"] < 0].nsmallest(10, "share_delta")

        rise_col, cool_col = st.columns(2)
        with rise_col:
            st.markdown("##### Rising")
            if rising.empty:
                st.caption("No languages gained share in this period.")
            else:
                fig = px.bar(
                    rising.sort_values("share_delta"),
                    x="share_delta", y="repo_language", orientation="h",
                    color_discrete_sequence=["#26a69a"],
                    labels={"share_delta": "Δ event share (pp)", "repo_language": ""},
                )
                fig.update_layout(height=max(300, 32 * len(rising)))
                st.plotly_chart(fig, use_container_width=True)
        with cool_col:
            st.markdown("##### Cooling")
            if cooling.empty:
                st.caption("No languages lost share in this period.")
            else:
                fig = px.bar(
                    cooling.sort_values("share_delta", ascending=False),
                    x="share_delta", y="repo_language", orientation="h",
                    color_discrete_sequence=["#ef5350"],
                    labels={"share_delta": "Δ event share (pp)", "repo_language": ""},
                )
                fig.update_layout(height=max(300, 32 * len(cooling)))
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ---- Fastest growing repos (star velocity in the latest viral window) ----
    st.subheader(f"Fastest growing repos — latest '{window_type}' window")
    if viral.empty:
        st.info(f"No viral repo data for the '{window_type}' window yet.")
    else:
        latest_start = viral["window_start"].max()
        fastest = (
            viral[viral["window_start"] == latest_start]
            .nlargest(top_n, "star_velocity")
            .copy()
        )
        fastest["repo_url"] = "https://github.com/" + fastest["repo_name"]
        st.dataframe(
            fastest[[
                "repo_url", "star_velocity", "fork_velocity",
                "star_count", "virality_score", "rank_in_window",
            ]],
            column_config={
                "repo_url": _repo_link_column(),
                "star_velocity": st.column_config.NumberColumn("Stars/day", format="%.1f"),
                "fork_velocity": st.column_config.NumberColumn("Forks/day", format="%.1f"),
                "star_count": st.column_config.NumberColumn("Stars", format="%d"),
                "virality_score": st.column_config.NumberColumn("Virality", format="%.0f"),
                "rank_in_window": st.column_config.NumberColumn("Rank", format="%d"),
            },
            hide_index=True,
            use_container_width=True,
        )

    st.divider()

    # ---- AI Analyst briefing (Groq, optional) ----
    st.subheader("AI Analyst briefing")
    if insights.groq_available():
        if st.button("Generate AI briefing", type="primary"):
            with st.spinner("The AI analyst is reading the radar..."):
                if viral.empty:
                    top_repos_md = "(no data)"
                else:
                    top_repos_md = _md_table(
                        viral[viral["window_start"] == viral["window_start"].max()]
                        .sort_values("rank_in_window"),
                        ["repo_name", "virality_score", "star_count", "fork_count",
                         "star_velocity", "active_contributors"],
                    )
                if delta_df.empty:
                    delta_md = "(no momentum data — only one analysis date available)"
                else:
                    delta_md = _md_table(
                        delta_df.sort_values("share_delta", ascending=False).round(2),
                        ["repo_language", "prev_share", "latest_share", "share_delta"],
                    )
                briefing = insights.generate_orientation_summary(
                    top_repos_md, delta_md, _macro_dict(macro)
                )
            st.markdown(briefing)
    else:
        st.info(
            "Set `GROQ_API_KEY` (environment variable or the sidebar input) to unlock "
            "the AI briefing — everything else works without it."
        )
