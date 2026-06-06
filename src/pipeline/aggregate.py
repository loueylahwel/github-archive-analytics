"""
aggregate.py  —  Gold Layer
============================
Reads from Silver Iceberg table and produces three Gold analytical tables:
  1. demo.gold.viral_repos       — Virality Index per repo x time window
  2. demo.gold.tech_stack_trends — Language/ecosystem activity metrics
  3. demo.gold.macro_stats       — Platform-wide macro statistics

Virality Index:
    VI = (stars x 4) + (forks x 3) + (pr_opened x 2) + (issues_opened x 1)
"""

import logging
import os
from datetime import date, timedelta
from typing import Optional

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

logger = logging.getLogger(__name__)


def _load_config(config_path=None):
    if config_path is None:
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base, "config", "config.yaml")
    with open(config_path) as fh:
        return yaml.safe_load(fh)


class GoldAggregator:
    """Produces all three Gold analytical tables from the Silver events table."""

    def __init__(self, spark: SparkSession, config_path=None):
        self.spark = spark
        self.cfg = _load_config(config_path)
        self.tables = self.cfg["tables"]
        self.virality_cfg = self.cfg["virality"]

    def run(self, start_date: date, end_date: date) -> dict:
        """Execute all three Gold aggregations for the given date range."""
        logger.info("Starting Gold aggregations for %s -> %s", start_date, end_date)
        self._create_gold_tables()

        silver_df = self._load_silver(start_date, end_date)
        silver_df.cache()
        total_silver = silver_df.count()
        logger.info("Silver records loaded: %d", total_silver)

        stats = {}
        stats["viral_repos"] = self._run_virality_engine(silver_df, start_date, end_date)
        stats["tech_stack_trends"] = self._run_tech_stack_trends(silver_df, start_date, end_date)
        stats["macro_stats"] = self._run_macro_stats(silver_df, start_date, end_date)

        silver_df.unpersist()
        logger.info("Gold aggregations complete. Stats: %s", stats)
        return stats

    def _create_gold_tables(self):
        """Create all three Gold Iceberg tables if they do not exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['gold_viral_repos']} (
                window_start        DATE,
                window_end          DATE,
                window_type         STRING,
                repo_name           STRING,
                virality_score      DOUBLE,
                star_count          BIGINT,
                fork_count          BIGINT,
                pr_opened_count     BIGINT,
                issue_opened_count  BIGINT,
                star_velocity       DOUBLE,
                fork_velocity       DOUBLE,
                active_contributors BIGINT,
                rank_in_window      INT
            )
            USING iceberg
            PARTITIONED BY (window_type, window_start)
            TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['gold_tech_stack_trends']} (
                analysis_date           DATE,
                repo_language           STRING,
                total_events            BIGINT,
                total_stars             BIGINT,
                total_forks             BIGINT,
                total_prs               BIGINT,
                total_pushes            BIGINT,
                distinct_repos          BIGINT,
                distinct_contributors   BIGINT,
                event_share_pct         DOUBLE,
                language_rank           INT
            )
            USING iceberg
            PARTITIONED BY (analysis_date)
            TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['gold_macro_stats']} (
                analysis_date               DATE,
                period_start                DATE,
                period_end                  DATE,
                total_events                BIGINT,
                total_stars                 BIGINT,
                total_forks                 BIGINT,
                total_prs_opened            BIGINT,
                total_prs_merged            BIGINT,
                total_issues_opened         BIGINT,
                total_push_events           BIGINT,
                total_commits               BIGINT,
                distinct_active_repos       BIGINT,
                distinct_active_contributors BIGINT,
                star_to_fork_ratio          DOUBLE,
                pr_merge_rate               DOUBLE,
                avg_commits_per_push        DOUBLE,
                top_event_type              STRING,
                top_language                STRING
            )
            USING iceberg
            PARTITIONED BY (analysis_date)
            TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
        """)

        logger.info("Gold tables ensured.")

    # =========================================================================
    # Module 1: Dynamic Virality Engine
    # =========================================================================

    def _run_virality_engine(self, silver_df, start_date: date, end_date: date) -> int:
        weights = self.virality_cfg["weights"]
        window_defs = self.virality_cfg["windows"]
        min_events = self.virality_cfg["min_events_threshold"]
        total_rows = 0

        for window_name, window_days in window_defs.items():
            logger.info("Virality Engine: computing '%s' window (%d days) ...", window_name, window_days)
            current = start_date
            while current <= end_date:
                w_start = current
                w_end = min(current + timedelta(days=window_days - 1), end_date)

                window_df = silver_df.filter(
                    (F.col("event_date") >= F.lit(w_start))
                    & (F.col("event_date") <= F.lit(w_end))
                )

                agg_df = window_df.groupBy("repo_name").agg(
                    F.sum(F.when(F.col("is_star") == True, 1).otherwise(0)).alias("star_count"),
                    F.sum(F.when(F.col("event_type") == "ForkEvent", 1).otherwise(0)).alias("fork_count"),
                    F.sum(F.when((F.col("event_type") == "PullRequestEvent") & (F.col("pr_action") == "opened"), 1).otherwise(0)).alias("pr_opened_count"),
                    F.sum(F.when((F.col("event_type") == "IssuesEvent") & (F.col("issue_action") == "opened"), 1).otherwise(0)).alias("issue_opened_count"),
                    F.countDistinct("actor_login").alias("active_contributors"),
                    F.count("*").alias("total_events"),
                )

                days_in_window = max((w_end - w_start).days + 1, 1)
                w_star = weights["star"]
                w_fork = weights["fork"]
                w_pr = weights["pull_request"]
                w_issue = weights["issue"]

                scored_df = agg_df.filter(F.col("total_events") >= min_events).withColumn(
                    "virality_score",
                    (F.col("star_count") * w_star) + (F.col("fork_count") * w_fork)
                    + (F.col("pr_opened_count") * w_pr) + (F.col("issue_opened_count") * w_issue),
                ).withColumn("star_velocity", F.col("star_count") / F.lit(float(days_in_window))
                ).withColumn("fork_velocity", F.col("fork_count") / F.lit(float(days_in_window))
                ).withColumn("window_start", F.lit(w_start)
                ).withColumn("window_end", F.lit(w_end)
                ).withColumn("window_type", F.lit(window_name))

                rank_window = Window.partitionBy("window_type", "window_start").orderBy(F.col("virality_score").desc())
                final_df = scored_df.withColumn("rank_in_window", F.row_number().over(rank_window)).select(
                    "window_start", "window_end", "window_type", "repo_name",
                    "virality_score", "star_count", "fork_count", "pr_opened_count",
                    "issue_opened_count", "star_velocity", "fork_velocity",
                    "active_contributors", "rank_in_window",
                )

                rows = final_df.count()
                if rows > 0:
                    final_df.writeTo(self.tables["gold_viral_repos"]).append()
                    total_rows += rows
                    logger.info("  %s window [%s -> %s]: %d repos written", window_name, w_start, w_end, rows)

                current += timedelta(days=window_days)

        logger.info("Virality Engine done. Total rows: %d", total_rows)
        return total_rows

    # =========================================================================
    # Module 2: Tech Stack Ecosystem Analysis
    # =========================================================================

    def _run_tech_stack_trends(self, silver_df, start_date: date, end_date: date) -> int:
        logger.info("Tech Stack Trends: aggregating ...")
        lang_df = silver_df.filter(F.col("repo_language").isNotNull())

        agg = lang_df.groupBy("repo_language").agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("is_star") == True, 1).otherwise(0)).alias("total_stars"),
            F.sum(F.when(F.col("event_type") == "ForkEvent", 1).otherwise(0)).alias("total_forks"),
            F.sum(F.when((F.col("event_type") == "PullRequestEvent") & (F.col("pr_action") == "opened"), 1).otherwise(0)).alias("total_prs"),
            F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0)).alias("total_pushes"),
            F.countDistinct("repo_name").alias("distinct_repos"),
            F.countDistinct("actor_login").alias("distinct_contributors"),
        )

        total_events_sum = agg.agg(F.sum("total_events")).collect()[0][0] or 1

        ranked_df = agg.withColumn(
            "event_share_pct", F.round(F.col("total_events") / F.lit(float(total_events_sum)) * 100.0, 4),
        ).withColumn(
            "language_rank", F.row_number().over(Window.orderBy(F.col("total_events").desc())),
        ).withColumn("analysis_date", F.lit(end_date)).select(
            "analysis_date", "repo_language", "total_events", "total_stars",
            "total_forks", "total_prs", "total_pushes", "distinct_repos",
            "distinct_contributors", "event_share_pct", "language_rank",
        )

        rows = ranked_df.count()
        if rows > 0:
            ranked_df.writeTo(self.tables["gold_tech_stack_trends"]).append()
        logger.info("Tech Stack Trends: %d language rows written.", rows)
        return rows

    # =========================================================================
    # Module 3: Platform Macro Statistics
    # =========================================================================

    def _run_macro_stats(self, silver_df, start_date, end_date):
        """Compute platform-wide aggregate metrics using pure Spark SQL."""
        logger.info("Macro Stats: computing ...")

        silver_df.createOrReplaceTempView("silver_events_tmp")

        agg_df = self.spark.sql("""
            SELECT
                COUNT(*)                                                                AS total_events,
                SUM(CASE WHEN is_star = true THEN 1 ELSE 0 END)                        AS total_stars,
                SUM(CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END)              AS total_forks,
                SUM(CASE WHEN event_type = 'PullRequestEvent'
                          AND pr_action = 'opened' THEN 1 ELSE 0 END)                  AS total_prs_opened,
                SUM(CASE WHEN event_type = 'PullRequestEvent'
                          AND pr_merged = true THEN 1 ELSE 0 END)                      AS total_prs_merged,
                SUM(CASE WHEN event_type = 'IssuesEvent'
                          AND issue_action = 'opened' THEN 1 ELSE 0 END)               AS total_issues_opened,
                SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END)              AS total_push_events,
                SUM(CASE WHEN event_type = 'PushEvent'
                          THEN COALESCE(push_commit_count, 0) ELSE 0 END)              AS total_commits,
                COUNT(DISTINCT repo_name)                                               AS distinct_active_repos,
                COUNT(DISTINCT actor_login)                                             AS distinct_active_contributors
            FROM silver_events_tmp
        """)

        top_event = self.spark.sql("""
            SELECT event_type FROM silver_events_tmp
            GROUP BY event_type ORDER BY COUNT(*) DESC LIMIT 1
        """).collect()[0][0]

        top_lang_rows = self.spark.sql("""
            SELECT repo_language FROM silver_events_tmp
            WHERE repo_language IS NOT NULL
            GROUP BY repo_language ORDER BY COUNT(*) DESC LIMIT 1
        """).collect()
        top_lang = top_lang_rows[0][0] if top_lang_rows else "unknown"

        final_df = agg_df \
            .withColumn("analysis_date", F.lit(str(end_date)).cast("date")) \
            .withColumn("period_start", F.lit(str(start_date)).cast("date")) \
            .withColumn("period_end", F.lit(str(end_date)).cast("date")) \
            .withColumn("star_to_fork_ratio",
                F.round(F.col("total_stars") / F.greatest(F.col("total_forks"), F.lit(1)), 4)) \
            .withColumn("pr_merge_rate",
                F.round(F.col("total_prs_merged") * 100.0 / F.greatest(F.col("total_prs_opened"), F.lit(1)), 4)) \
            .withColumn("avg_commits_per_push",
                F.round(F.col("total_commits") / F.greatest(F.col("total_push_events"), F.lit(1)), 4)) \
            .withColumn("top_event_type", F.lit(top_event)) \
            .withColumn("top_language", F.lit(top_lang)) \
            .select(
                "analysis_date", "period_start", "period_end",
                "total_events", "total_stars", "total_forks",
                "total_prs_opened", "total_prs_merged", "total_issues_opened",
                "total_push_events", "total_commits",
                "distinct_active_repos", "distinct_active_contributors",
                "star_to_fork_ratio", "pr_merge_rate", "avg_commits_per_push",
                "top_event_type", "top_language",
            )

        final_df.writeTo(self.tables["gold_macro_stats"]).append()
        logger.info("Macro Stats row written for period %s -> %s.", start_date, end_date)
        return 1

    # =========================================================================
    # Silver loader
    # =========================================================================

    def _load_silver(self, start_date: date, end_date: date):
        return self.spark.table(self.tables["silver_events"]).filter(
            (F.col("event_date") >= F.lit(start_date))
            & (F.col("event_date") <= F.lit(end_date))
        )

    def optimize(self):
        """Run Iceberg maintenance on all Gold tables."""
        for tbl in [self.tables["gold_viral_repos"], self.tables["gold_tech_stack_trends"], self.tables["gold_macro_stats"]]:
            logger.info("Optimising Gold table: %s", tbl)
            try:
                self.spark.sql(f"CALL demo.system.rewrite_data_files(table => '{tbl}', strategy => 'binpack', options => map('min-input-files', '2'))")
                self.spark.sql(f"CALL demo.system.expire_snapshots(table => '{tbl}', retain_last => 3)")
            except Exception as exc:
                logger.warning("Optimisation failed for %s: %s", tbl, exc)
        logger.info("Gold optimisation complete.")
