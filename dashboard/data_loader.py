"""
data_loader.py — Gold-layer data access for the Dev World Radar dashboard.
============================================================================
Reads the three Gold Iceberg tables (demo.gold.*) directly through pyiceberg's
REST catalog client — no Spark session required.

Connection settings come from environment variables, with defaults matching
the host-machine endpoints in config/config.yaml:

    CATALOG_URI            (default http://localhost:8181)
    S3_ENDPOINT            (default http://localhost:4566)
    AWS_ACCESS_KEY_ID      (default "test" — LocalStack)
    AWS_SECRET_ACCESS_KEY  (default "test" — LocalStack)
    AWS_REGION             (default us-east-1)

Every loader degrades gracefully: if the catalog is unreachable or a table is
missing, an empty DataFrame is returned instead of raising.
"""

import logging
import os

import pandas as pd
import streamlit as st
from pyiceberg.catalog import load_catalog

logger = logging.getLogger(__name__)

# Fallback sample data when the Iceberg stack has no Gold tables yet.
from demo_data import (
    data_available as _demo_data_available,
    available_windows as _demo_available_windows,
    load_viral_repos as _demo_load_viral_repos,
    load_tech_trends as _demo_load_tech_trends,
    load_macro_stats as _demo_load_macro_stats,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG_NAME = "demo"

GOLD_VIRAL_REPOS = "gold.viral_repos"
GOLD_TECH_TRENDS = "gold.tech_stack_trends"
GOLD_MACRO_STATS = "gold.macro_stats"

WINDOW_TYPES = ["day", "week", "month"]

# DATE columns across the Gold tables — converted to pandas timestamps on load
_DATE_COLUMNS = {"window_start", "window_end", "analysis_date", "period_start", "period_end"}

CACHE_TTL_SECONDS = 300


# ---------------------------------------------------------------------------
# Catalog + raw table loading (no Spark)
# ---------------------------------------------------------------------------

def _catalog():
    """Build a pyiceberg REST catalog client from env-var configuration."""
    return load_catalog(
        CATALOG_NAME,
        **{
            "type": "rest",
            "uri": os.environ.get("CATALOG_URI", "http://localhost:8181"),
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:4566"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )


def _load(table_name: str) -> pd.DataFrame:
    """Scan a full Iceberg table into a DataFrame; empty DataFrame on any failure."""
    try:
        table = _catalog().load_table(table_name)
        df = table.scan().to_pandas()
    except Exception as exc:  # catalog down, table missing, S3 unreachable, ...
        logger.warning("Could not load %s: %s", table_name, exc)
        return pd.DataFrame()

    for col in df.columns:
        if col in _DATE_COLUMNS:
            df[col] = pd.to_datetime(df[col])
    return df


# ---------------------------------------------------------------------------
# Demo-mode detection
# ---------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def _iceberg_has_data() -> bool:
    """True when at least one real Gold Iceberg table exists and is non-empty."""
    if not _load(GOLD_TECH_TRENDS).empty or not _load(GOLD_MACRO_STATS).empty:
        return True
    viral_df = _load(GOLD_VIRAL_REPOS)
    if viral_df.empty or "window_type" not in viral_df.columns:
        return False
    return any(not viral_df[viral_df["window_type"] == w].empty for w in WINDOW_TYPES)


def demo_mode() -> bool:
    """True when the dashboard is rendering generated sample data."""
    return not _iceberg_has_data()


# ---------------------------------------------------------------------------
# Public cached loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_viral_repos(window_type: str = "week") -> pd.DataFrame:
    """Viral repo rankings for one window type, latest window first."""
    if demo_mode():
        return _demo_load_viral_repos(window_type)
    df = _load(GOLD_VIRAL_REPOS)
    if df.empty:
        return df
    df = df[df["window_type"] == window_type]
    return df.sort_values(
        ["window_start", "rank_in_window"], ascending=[False, True]
    ).reset_index(drop=True)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_tech_trends() -> pd.DataFrame:
    """Tech stack trends, latest analysis date first, ranked by language."""
    if demo_mode():
        return _demo_load_tech_trends()
    df = _load(GOLD_TECH_TRENDS)
    if df.empty:
        return df
    return df.sort_values(
        ["analysis_date", "language_rank"], ascending=[False, True]
    ).reset_index(drop=True)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_macro_stats() -> pd.DataFrame:
    """Macro platform stats, ascending by analysis date (latest row last)."""
    if demo_mode():
        return _demo_load_macro_stats()
    df = _load(GOLD_MACRO_STATS)
    if df.empty:
        return df
    return df.sort_values("analysis_date").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=CACHE_TTL_SECONDS)
def available_windows() -> list:
    """Window types actually present in the viral table (falls back to all three)."""
    if demo_mode():
        return _demo_available_windows()
    df = _load(GOLD_VIRAL_REPOS)
    if df.empty or "window_type" not in df.columns:
        return list(WINDOW_TYPES)
    present = [w for w in WINDOW_TYPES if w in set(df["window_type"].unique())]
    return present or list(WINDOW_TYPES)


def data_available() -> bool:
    """True when at least one Gold table is reachable, non-empty, or demo data is enabled."""
    if _demo_data_available():
        return True
    if not load_tech_trends().empty or not load_macro_stats().empty:
        return True
    return any(not load_viral_repos(w).empty for w in WINDOW_TYPES)
