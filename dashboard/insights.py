"""
insights.py — Groq-powered "AI Analyst" for the Dev World Radar dashboard.
==========================================================================
Generates a short narrative briefing (4–6 bullet insights) about where the
developer world is heading, from Gold-table markdown snippets + macro KPIs.

Configuration via environment variables:
    GROQ_API_KEY   — primary key
    GROQ_API_KEYS  — optional comma-separated extra keys for rotation
    GROQ_MODEL     — default "llama-3.3-70b-versatile"

The module never raises into the dashboard: any failure returns a friendly
fallback string instead.
"""

import logging
import os
import time
from typing import List

import streamlit as st

logger = logging.getLogger(__name__)

GROQ_MODEL_DEFAULT = "llama-3.3-70b-versatile"
TEMPERATURE = 0.3
MAX_INPUT_CHARS = 4000  # safety cap per input section (rows are pre-capped at 15)

FALLBACK_MESSAGE = (
    "The AI analyst is unavailable right now — check that `GROQ_API_KEY` "
    "is set and the Groq API is reachable, then try again."
)

_SYSTEM_PROMPT = (
    "You are a sharp tech-industry analyst covering the open-source ecosystem. "
    "You receive raw GitHub activity analytics and explain where the developer "
    "world is heading. Be concrete, opinionated and concise — no fluff, no "
    "disclaimers, no caveats."
)

_clients: dict = {}
_current_key_index: int = 0


# ---------------------------------------------------------------------------
# Key pool
# ---------------------------------------------------------------------------

def _all_keys() -> List[str]:
    """Return the primary key plus any comma-separated extra keys."""
    keys = []
    primary = os.environ.get("GROQ_API_KEY", "").strip()
    if primary:
        keys.append(primary)
    extras = os.environ.get("GROQ_API_KEYS", "")
    if extras:
        keys.extend([k.strip() for k in extras.split(",") if k.strip()])
    return keys


def _next_key() -> str | None:
    """Round-robin across the Groq key pool."""
    global _current_key_index
    keys = _all_keys()
    if not keys:
        return None
    key = keys[_current_key_index % len(keys)]
    _current_key_index = (_current_key_index + 1) % len(keys)
    return key


def _get_client(key: str):
    """Create (and cache) a Groq client for a specific key."""
    if key not in _clients:
        from groq import Groq
        _clients[key] = Groq(api_key=key)
    return _clients[key]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def groq_available() -> bool:
    """True when at least one Groq API key is set and the package is importable."""
    if not _all_keys():
        return False
    try:
        import groq  # noqa: F401
    except ImportError:
        return False
    return True


# ---------------------------------------------------------------------------
# Briefing generation
# ---------------------------------------------------------------------------

def _build_prompt(top_repos_md: str, language_delta_md: str, macro_dict: dict) -> str:
    macro_lines = "\n".join(f"- {k}: {v}" for k, v in macro_dict.items()) or "(no macro stats)"
    return f"""Here is the latest GitHub activity radar:

## Trending repositories (by virality score)
{top_repos_md[:MAX_INPUT_CHARS]}

## Language momentum (event-share change between the two most recent analysis dates)
{language_delta_md[:MAX_INPUT_CHARS]}

## Platform macro KPIs
{macro_lines[:MAX_INPUT_CHARS]}

Write 4-6 punchy bullet-point insights about where the dev world is heading:
- which ecosystems/languages are rising or cooling, and why that matters
- what the trending repositories have in common
- what developers are collectively betting on right now

Output markdown bullets only — no headings, no preamble, no closing paragraph."""


@st.cache_data(ttl=3600, show_spinner=False)
def generate_orientation_summary(top_repos_md: str, language_delta_md: str, macro_dict: dict) -> str:
    """Generate the AI briefing; cached on the exact input strings. Never raises."""
    if not groq_available():
        return FALLBACK_MESSAGE

    keys = _all_keys()
    for attempt, key in enumerate(keys):
        try:
            client = _get_client(key)
            response = client.chat.completions.create(
                model=os.environ.get("GROQ_MODEL", GROQ_MODEL_DEFAULT),
                temperature=TEMPERATURE,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": _build_prompt(top_repos_md, language_delta_md, macro_dict)},
                ],
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            err_msg = str(exc).lower()
            logger.warning("Groq briefing failed with key %d/%d: %s", attempt + 1, len(keys), exc)
            # If rate-limited, try next key; otherwise abort.
            if "rate_limit" in err_msg or "rate limit" in err_msg:
                # Honor Retry-After if present.
                retry_after = getattr(exc, "headers", {})
                retry_after = retry_after.get("retry-after") if retry_after else None
                if retry_after:
                    try:
                        time.sleep(max(float(retry_after), 0))
                    except (TypeError, ValueError):
                        pass
                continue
            break

    return FALLBACK_MESSAGE
