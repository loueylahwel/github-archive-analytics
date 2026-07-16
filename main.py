#!/usr/bin/env python3
"""
main.py  —  GitHub Archive Trend & Virality Analytics Platform
===============================================================
CLI entry point for orchestrating the full Bronze → Silver → Gold pipeline.

Usage examples
--------------
  # Full analytics run (ingest + transform + aggregate)
  python main.py --action run-analytics --start-date 2024-01-01 --end-date 2024-01-07

  # Individual stages
  python main.py --action ingest    --start-date 2024-01-01 --end-date 2024-01-03
  python main.py --action transform --start-date 2024-01-01 --end-date 2024-01-03
  python main.py --action aggregate --start-date 2024-01-01 --end-date 2024-01-03

  # Maintenance
  python main.py --action optimize

  # Dry-run (simulate ingest without uploading)
  python main.py --action ingest --start-date 2024-01-01 --end-date 2024-01-01 --dry-run

  # Docker mode (use Docker-internal service hostnames)
  python main.py --action run-analytics --start-date 2024-01-01 --end-date 2024-01-03 --docker

  # Dev World Radar dashboard (Streamlit over the Gold tables, no Spark)
  python main.py --action dashboard
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime

import yaml

# ---------------------------------------------------------------------------
# Logging setup — do this before any other imports so module-level loggers
# inherit the configuration.
# ---------------------------------------------------------------------------
def _setup_logging(level: str = "INFO") -> None:
    try:
        import colorlog
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
            )
        )
    except ImportError:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.addHandler(handler)

    # Suppress noisy third-party loggers
    for noisy in ["py4j", "pyspark", "boto3", "botocore", "urllib3", "s3transfer"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config(config_path: str = None) -> dict:
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "config", "config.yaml")
    with open(config_path) as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Date parsing helper
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}'. Expected YYYY-MM-DD.")


# ---------------------------------------------------------------------------
# Action handlers
# ---------------------------------------------------------------------------

def action_ingest(args, cfg) -> None:
    """Bronze: download GH Archive files to S3."""
    from src.pipeline.ingest import BronzeIngester

    logger.info("=== BRONZE INGEST ===")
    ingester = BronzeIngester(dry_run=args.dry_run)
    stats = ingester.run(args.start_date, args.end_date)
    logger.info("Ingest stats: %s", stats)


def action_transform(args, cfg) -> None:
    """Silver: parse & flatten JSON into Iceberg."""
    from src.pipeline.transform import SilverTransformer
    from src.utils.spark_session import create_spark_session

    logger.info("=== SILVER TRANSFORM ===")
    spark = create_spark_session(use_docker_endpoints=args.docker)
    try:
        transformer = SilverTransformer(spark)
        stats = transformer.transform(args.start_date, args.end_date)
        logger.info("Transform stats: %s", stats)
    finally:
        spark.stop()


def action_aggregate(args, cfg) -> None:
    """Gold: compute virality, tech trends, macro stats."""
    from src.pipeline.aggregate import GoldAggregator
    from src.utils.spark_session import create_spark_session

    logger.info("=== GOLD AGGREGATE ===")
    spark = create_spark_session(use_docker_endpoints=args.docker)
    try:
        aggregator = GoldAggregator(spark)
        stats = aggregator.run(args.start_date, args.end_date)
        logger.info("Aggregate stats: %s", stats)
    finally:
        spark.stop()


def action_run_analytics(args, cfg) -> None:
    """Full pipeline: Bronze → Silver → Gold."""
    logger.info("=== FULL ANALYTICS RUN ===")
    logger.info("Date range: %s → %s", args.start_date, args.end_date)
    t0 = time.time()

    # ---- Stage 1: Bronze ----
    action_ingest(args, cfg)

    # ---- Stage 2: Silver ----
    action_transform(args, cfg)

    # ---- Stage 3: Gold ----
    action_aggregate(args, cfg)

    elapsed = time.time() - t0
    logger.info("Full analytics run completed in %.1f seconds.", elapsed)


def action_optimize(args, cfg) -> None:
    """Run Iceberg table maintenance on all layers."""
    from src.pipeline.transform import SilverTransformer
    from src.pipeline.aggregate import GoldAggregator
    from src.utils.spark_session import create_spark_session

    logger.info("=== TABLE OPTIMIZATION ===")
    spark = create_spark_session(use_docker_endpoints=args.docker)
    try:
        SilverTransformer(spark).optimize()
        GoldAggregator(spark).optimize()
        logger.info("Optimization complete.")
    finally:
        spark.stop()


def action_list_bronze(args, cfg) -> None:
    """List all files in S3 Bronze zone."""
    from src.pipeline.ingest import BronzeIngester

    ingester = BronzeIngester()
    files = ingester.list_bronze_files()
    if files:
        logger.info("Bronze files (%d):", len(files))
        for f in sorted(files):
            print(f"  {f}")
    else:
        logger.info("No Bronze files found.")


def action_show_viral(args, cfg) -> None:
    """Quick-print top viral repos from the Gold table."""
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session(use_docker_endpoints=args.docker)
    try:
        window_type = getattr(args, "window", "week")
        table = cfg["tables"]["gold_viral_repos"]
        spark.sql(f"""
            SELECT
                window_start, window_end, repo_name,
                virality_score, star_count, fork_count, rank_in_window
            FROM {table}
            WHERE window_type = '{window_type}'
            ORDER BY window_start DESC, rank_in_window ASC
            LIMIT 20
        """).show(truncate=False)
    finally:
        spark.stop()


def action_dashboard(args, cfg) -> None:
    """Launch the Dev World Radar Streamlit dashboard."""
    logger.info("=== DEV WORLD RADAR DASHBOARD ===")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", os.path.join("dashboard", "app.py")]
    )


# ---------------------------------------------------------------------------
# CLI parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="GitHub Archive Trend & Virality Analytics Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--action",
        required=True,
        choices=[
            "run-analytics",
            "ingest",
            "transform",
            "aggregate",
            "optimize",
            "list-bronze",
            "show-viral",
            "dashboard",
        ],
        help="Pipeline action to execute.",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Required for ingest/transform/aggregate.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Required for ingest/transform/aggregate.",
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        default=False,
        help="Use Docker-internal service hostnames (iceberg-rest, localstack).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Simulate ingest without uploading to S3.",
    )
    parser.add_argument(
        "--window",
        choices=["day", "week", "month"],
        default="week",
        help="Time window for show-viral action (default: week).",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: config/config.yaml).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )

    return parser


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    _setup_logging(args.log_level)

    cfg = _load_config(args.config)

    # Validate date args for date-range actions
    date_required = {"run-analytics", "ingest", "transform", "aggregate"}
    if args.action in date_required:
        if args.start_date is None or args.end_date is None:
            parser.error(f"--start-date and --end-date are required for --action {args.action}")
        if args.start_date > args.end_date:
            parser.error("--start-date must be ≤ --end-date")

    dispatch = {
        "run-analytics": action_run_analytics,
        "ingest": action_ingest,
        "transform": action_transform,
        "aggregate": action_aggregate,
        "optimize": action_optimize,
        "list-bronze": action_list_bronze,
        "show-viral": action_show_viral,
        "dashboard": action_dashboard,
    }

    try:
        dispatch[args.action](args, cfg)
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        return 130
    except Exception as exc:
        logger.error("Fatal error: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
