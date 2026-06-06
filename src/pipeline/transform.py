"""
transform.py  —  Silver Layer
==============================
Reads raw .json.gz files from S3 Bronze, flattens the complex GH Archive
JSON schema, and writes clean, partitioned records into the Iceberg table
  demo.silver.events

GH Archive event schema (relevant fields):
  {
    "id": "...",
    "type": "WatchEvent | ForkEvent | PushEvent | PullRequestEvent | ...",
    "actor": { "login": "...", "id": ... },
    "repo": { "id": ..., "name": "owner/repo", "url": "..." },
    "payload": { ...type-specific... },
    "created_at": "2024-01-01T15:30:00Z",
    "org": { "login": "..." }   -- optional
  }

The silver layer extracts a normalised flat record for each event.
"""

import logging
import os
from datetime import date, timedelta
from typing import Optional

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Config helpers
# =============================================================================

def _load_config(config_path: Optional[str] = None) -> dict:
    if config_path is None:
        base = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        config_path = os.path.join(base, "config", "config.yaml")
    with open(config_path) as fh:
        return yaml.safe_load(fh)


# =============================================================================
# GH Archive JSON Schema
# =============================================================================

# Only the fields we actually need — Spark will ignore everything else.
GH_ARCHIVE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created_at", StringType(), True),

    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("display_login", StringType(), True),
    ]), True),

    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]), True),

    StructField("org", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
    ]), True),

    StructField("payload", StructType([
        # PushEvent
        StructField("push_id", LongType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("ref", StringType(), True),

        # PullRequestEvent / IssuesEvent
        StructField("action", StringType(), True),
        StructField("number", IntegerType(), True),

        # PullRequestEvent nested
        StructField("pull_request", StructType([
            StructField("state", StringType(), True),
            StructField("title", StringType(), True),
            StructField("merged", BooleanType(), True),
            StructField("additions", IntegerType(), True),
            StructField("deletions", IntegerType(), True),
            StructField("changed_files", IntegerType(), True),
            StructField("base", StructType([
                StructField("repo", StructType([
                    StructField("language", StringType(), True),
                    StructField("stargazers_count", IntegerType(), True),
                    StructField("forks_count", IntegerType(), True),
                ]), True),
            ]), True),
        ]), True),

        # IssuesEvent nested
        StructField("issue", StructType([
            StructField("state", StringType(), True),
            StructField("title", StringType(), True),
        ]), True),

        # ForkEvent
        StructField("forkee", StructType([
            StructField("full_name", StringType(), True),
            StructField("language", StringType(), True),
            StructField("stargazers_count", IntegerType(), True),
        ]), True),

        # WatchEvent — payload.action == "started" means a star
        # CreateEvent
        StructField("description", StringType(), True),
        StructField("master_branch", StringType(), True),
        StructField("ref_type", StringType(), True),
    ]), True),
])


# =============================================================================
# Silver transformer
# =============================================================================

class SilverTransformer:
    """
    Reads Bronze raw JSON.GZ files from S3 and writes cleaned Silver Iceberg
    table.

    Parameters
    ----------
    spark : SparkSession
        Pre-configured SparkSession (Iceberg + S3 enabled).
    config_path : str, optional
        Path to config.yaml.
    """

    def __init__(self, spark: SparkSession, config_path: Optional[str] = None):
        self.spark = spark
        self.cfg = _load_config(config_path)
        self.silver_table = self.cfg["tables"]["silver_events"]
        self.bucket = self.cfg["aws"]["bucket"]

    # -------------------------------------------------------------------------
    # Table DDL
    # -------------------------------------------------------------------------

    def create_table_if_not_exists(self) -> None:
        """Create the Silver Iceberg table if it does not already exist."""
        logger.info("Ensuring Silver table exists: %s", self.silver_table)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.silver_table} (
                event_id        STRING        COMMENT 'GH Archive event ID',
                event_type      STRING        COMMENT 'WatchEvent, ForkEvent, PushEvent, etc.',
                created_at      TIMESTAMP     COMMENT 'Event creation timestamp (UTC)',
                event_date      DATE          COMMENT 'Partition key — date of event',

                actor_id        BIGINT        COMMENT 'GitHub actor (user) ID',
                actor_login     STRING        COMMENT 'GitHub username',

                repo_id         BIGINT        COMMENT 'GitHub repository ID',
                repo_name       STRING        COMMENT 'owner/repo full name',
                repo_language   STRING        COMMENT 'Primary programming language',

                org_login       STRING        COMMENT 'Organisation login (if applicable)',

                -- Push details
                push_commit_count    INT      COMMENT 'Commits in PushEvent',

                -- PR details
                pr_action       STRING        COMMENT 'opened/closed/merged',
                pr_merged       BOOLEAN       COMMENT 'Was the PR merged?',
                pr_additions    INT           COMMENT 'Lines added',
                pr_deletions    INT           COMMENT 'Lines deleted',

                -- Issue details
                issue_action    STRING        COMMENT 'opened/closed/reopened',

                -- Fork details
                fork_repo_name  STRING        COMMENT 'Name of the forked repo',

                -- Star (Watch) details: event_type = WatchEvent + payload.action = started
                is_star         BOOLEAN       COMMENT 'True if this is a Star event',

                -- Create event
                create_ref_type STRING        COMMENT 'branch/tag/repository'
            )
            USING iceberg
            PARTITIONED BY (event_date)
            TBLPROPERTIES (
                'write.sort-order' = 'event_type ASC NULLS LAST, actor_login ASC NULLS LAST',
                'write.metadata.compression-codec' = 'gzip',
                'write.parquet.compression-codec' = 'zstd'
            )
        """)
        logger.info("Silver table ready.")

    # -------------------------------------------------------------------------
    # ETL
    # -------------------------------------------------------------------------

    def transform(self, start_date: date, end_date: date) -> dict:
        """
        Process Bronze files for the given date range and append to Silver.

        Parameters
        ----------
        start_date, end_date : date
            Inclusive date range to process.

        Returns
        -------
        dict
            Stats: files_processed, rows_written.
        """
        self.create_table_if_not_exists()

        # Build list of S3 paths for date range
        paths = self._bronze_paths(start_date, end_date)
        if not paths:
            logger.warning("No Bronze files found for %s → %s", start_date, end_date)
            return {"files_processed": 0, "rows_written": 0}

        logger.info("Reading %d Bronze path patterns", len(paths))

        # ---- Read raw JSON ----
        raw_df = (
            self.spark.read
            .schema(GH_ARCHIVE_SCHEMA)
            .option("multiLine", "false")
            .option("mode", "PERMISSIVE")
            .json(paths)
        )

        # ---- Flatten + transform ----
        silver_df = self._flatten(raw_df)

        row_count = silver_df.count()
        logger.info("Transformed %d Silver records, writing to %s …", row_count, self.silver_table)

        # ---- Write to Iceberg (append by day partitions) ----
        silver_df.writeTo(self.silver_table).append()

        logger.info("Silver write complete.")
        return {"files_processed": len(paths), "rows_written": row_count}

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _bronze_paths(self, start_date: date, end_date: date) -> list:
        """Construct S3 glob paths for Bronze files in the date range."""
        paths = []
        current = start_date
        while current <= end_date:
            path = (
                f"s3a://{self.bucket}/bronze/"
                f"{current.strftime('%Y/%m/%d')}/*.json.gz"
            )
            paths.append(path)
            current += timedelta(days=1)
        return paths

    def _flatten(self, raw_df):
        """
        Flatten the nested GH Archive schema into a wide, clean Silver record.
        """
        return raw_df.select(
            # ---- Core ----
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_date("created_at").alias("event_date"),

            # ---- Actor ----
            F.col("actor.id").alias("actor_id"),
            F.coalesce(
                F.col("actor.display_login"), F.col("actor.login")
            ).alias("actor_login"),

            # ---- Repo ----
            F.col("repo.id").alias("repo_id"),
            F.col("repo.name").alias("repo_name"),

            # Language: try PR base repo first, then forkee
            F.coalesce(
                F.col("payload.pull_request.base.repo.language"),
                F.col("payload.forkee.language"),
            ).alias("repo_language"),

            # ---- Org ----
            F.col("org.login").alias("org_login"),

            # ---- Push ----
            F.when(F.col("type") == "PushEvent", F.col("payload.size"))
             .alias("push_commit_count"),

            # ---- PR ----
            F.when(F.col("type") == "PullRequestEvent", F.col("payload.action"))
             .alias("pr_action"),
            F.when(F.col("type") == "PullRequestEvent", F.col("payload.pull_request.merged"))
             .alias("pr_merged"),
            F.when(F.col("type") == "PullRequestEvent", F.col("payload.pull_request.additions"))
             .alias("pr_additions"),
            F.when(F.col("type") == "PullRequestEvent", F.col("payload.pull_request.deletions"))
             .alias("pr_deletions"),

            # ---- Issues ----
            F.when(F.col("type") == "IssuesEvent", F.col("payload.action"))
             .alias("issue_action"),

            # ---- Fork ----
            F.when(F.col("type") == "ForkEvent", F.col("payload.forkee.full_name"))
             .alias("fork_repo_name"),

            # ---- Star (WatchEvent + action=started) ----
            F.when(
                (F.col("type") == "WatchEvent") & (F.col("payload.action") == "started"),
                F.lit(True),
            ).otherwise(F.lit(False)).alias("is_star"),

            # ---- Create ----
            F.when(F.col("type") == "CreateEvent", F.col("payload.ref_type"))
             .alias("create_ref_type"),
        ).filter(
            # Drop records with no event ID or date (malformed rows)
            F.col("event_id").isNotNull() & F.col("created_at").isNotNull()
        )

    def optimize(self) -> None:
        """
        Run Iceberg table maintenance:
        - REWRITE DATA FILES (bin-packing small files)
        - EXPIRE SNAPSHOTS (keep last 5)
        - REWRITE MANIFESTS
        """
        logger.info("Optimising Silver table: %s", self.silver_table)
        self.spark.sql(f"""
            CALL demo.system.rewrite_data_files(
                table => '{self.silver_table}',
                strategy => 'binpack',
                options => map('min-input-files', '2')
            )
        """)
        self.spark.sql(f"""
            CALL demo.system.expire_snapshots(
                table => '{self.silver_table}',
                retain_last => 5
            )
        """)
        self.spark.sql(f"""
            CALL demo.system.rewrite_manifests('{self.silver_table}')
        """)
        logger.info("Silver optimisation complete.")
