"""
demo_data.py — Realistic sample data for the Dev World Radar dashboard.
=======================================================================
Used as a fallback when the Iceberg Gold tables are empty or unreachable,
so the dashboard is immediately usable without running the full Spark stack.
"""

from datetime import datetime, timedelta

import pandas as pd
import numpy as np


WINDOW_TYPES = ["day", "week", "month"]

_LANGUAGES = [
    ("Python", 28.5),
    ("JavaScript", 19.2),
    ("TypeScript", 14.7),
    ("Go", 8.3),
    ("Rust", 6.8),
    ("Java", 5.4),
    ("C++", 4.1),
    ("Ruby", 2.9),
    ("Swift", 2.4),
    ("Kotlin", 2.2),
    ("PHP", 1.9),
    ("C#", 1.6),
    ("Dart", 1.0),
    ("Elixir", 0.7),
    ("Zig", 0.3),
]

_REPOS = [
    ("maybe-finance/maybe", "Python", 887, 213, 11),
    ("lewagon/dotfiles", "Ruby", 804, 48, 204),
    ("VikParuchuri/surya", "Python", 545, 128, 11),
    ("vanna-ai/vanna", "Python", 448, 112, 0),
    ("danny-avila/LibreChat", "TypeScript", 363, 85, 7),
    ("TencentARC/PhotoMaker", "Python", 288, 69, 4),
    ("EpicGames/raddebugger", "C", 269, 65, 3),
    ("janhq/jan", "TypeScript", 231, 56, 2),
    ("krahets/hello-algo", "Java", 215, 50, 5),
    ("mlabonne/llm-course", "Python", 198, 42, 3),
    ("xenova/transformers.js", "JavaScript", 184, 38, 6),
    ("shadcn-ui/ui", "TypeScript", 176, 34, 9),
    ("rustdesk/rustdesk", "Rust", 165, 31, 4),
    ("codecrafters-io/build-your-own-x", "Markdown", 152, 29, 12),
    (" Practical-Tutorials/project-based-learning", "Markdown", 141, 27, 8),
    ("sindresorhus/awesome", "Markdown", 132, 25, 15),
    ("facebook/react", "JavaScript", 128, 24, 22),
    ("vercel/next.js", "TypeScript", 121, 23, 18),
    ("oven-sh/bun", "Zig", 115, 22, 5),
    ("nuxt/nuxt", "TypeScript", 108, 20, 7),
    ("dotnet/maui", "C#", 98, 18, 4),
    ("google/flutter", "Dart", 92, 17, 6),
    ("tokio-rs/tokio", "Rust", 87, 16, 3),
    ("spring-projects/spring-boot", "Java", 81, 15, 9),
    ("laravel/framework", "PHP", 76, 14, 5),
    ("apple/swift-algorithms", "Swift", 71, 13, 2),
    ("JetBrains/kotlin", "Kotlin", 66, 12, 4),
    ("opencv/opencv", "C++", 62, 11, 6),
    ("godotengine/godot", "C++", 58, 10, 8),
    ("ziglang/zig", "Zig", 54, 9, 1),
]


def _base_date() -> datetime:
    return datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)


def load_viral_repos(window_type: str = "week") -> pd.DataFrame:
    """Return a realistic viral-repo ranking for the requested window."""
    base = _base_date()
    if window_type == "day":
        start, end = base - timedelta(days=1), base
    elif window_type == "month":
        start, end = base - timedelta(days=30), base
    else:
        start, end = base - timedelta(days=7), base

    rows = []
    for rank, (repo, lang, vi, stars, forks) in enumerate(_REPOS, start=1):
        days = max(1, (end - start).days)
        rows.append(
            {
                "repo_name": repo,
                "repo_language": lang,
                "window_type": window_type,
                "window_start": start,
                "window_end": end,
                "rank_in_window": rank,
                "virality_score": float(vi) * (1.0 - rank * 0.015),
                "star_count": stars,
                "fork_count": forks,
                "pr_opened_count": max(1, int(stars * 0.08)),
                "star_velocity": stars / days,
                "fork_velocity": forks / days,
                "active_contributors": max(3, int(stars * 0.12)),
            }
        )
    return pd.DataFrame(rows)


def load_tech_trends() -> pd.DataFrame:
    """Return two analysis dates of language market-share data."""
    base = _base_date()
    rows = []
    for offset, shift in [(7, 0.0), (0, 0.6)]:
        date = base - timedelta(days=offset)
        shares = np.array([s for _, s in _LANGUAGES])
        # add a small realistic drift between the two dates
        drift = np.random.RandomState(offset).normal(0, 0.4, len(shares))
        if offset == 0:
            drift = np.clip(drift + shift * 0.05, -1.2, 1.5)
        shares = np.clip(shares + drift, 0.1, None)
        shares = shares / shares.sum() * 100.0
        for rank, ((lang, _), share) in enumerate(zip(_LANGUAGES, shares), start=1):
            rows.append(
                {
                    "analysis_date": date,
                    "repo_language": lang,
                    "language_rank": rank,
                    "total_stars": int(share * 4200),
                    "total_forks": int(share * 950),
                    "total_prs": int(share * 310),
                    "distinct_repos": int(share * 180),
                    "distinct_contributors": int(share * 75),
                    "event_share_pct": round(share, 2),
                }
            )
    return pd.DataFrame(rows)


def load_macro_stats() -> pd.DataFrame:
    """Return two macro-stat rows for delta comparisons."""
    base = _base_date()
    rows = [
        {
            "analysis_date": base - timedelta(days=7),
            "period_start": base - timedelta(days=14),
            "period_end": base - timedelta(days=7),
            "total_events": 4_820_000,
            "total_stars": 312_000,
            "total_forks": 71_000,
            "total_prs_opened": 48_000,
            "total_prs_merged": 31_000,
            "pr_merge_rate": 64.6,
            "distinct_active_contributors": 189_000,
            "star_to_fork_ratio": 4.4,
            "avg_commits_per_push": 3.2,
            "top_event_type": "WatchEvent",
            "top_language": "Python",
        },
        {
            "analysis_date": base,
            "period_start": base - timedelta(days=7),
            "period_end": base,
            "total_events": 5_140_000,
            "total_stars": 338_000,
            "total_forks": 76_000,
            "total_prs_opened": 51_000,
            "total_prs_merged": 34_000,
            "pr_merge_rate": 66.7,
            "distinct_active_contributors": 201_000,
            "star_to_fork_ratio": 4.45,
            "avg_commits_per_push": 3.3,
            "top_event_type": "WatchEvent",
            "top_language": "Python",
        },
    ]
    return pd.DataFrame(rows)


def available_windows() -> list:
    return list(WINDOW_TYPES)


def data_available() -> bool:
    return True
