"""
ingest.py  —  Bronze Layer
==========================
Downloads raw GH Archive hourly .json.gz files for a given date range and
uploads them into the S3 Bronze zone (s3a://github-archive-bucket/bronze/).

GH Archive URL pattern:
  https://data.gharchive.org/YYYY-MM-DD-H.json.gz   (H = 0..23)

This module does NOT use Spark for the download step; it uses requests + boto3
so it can run lightweight without spinning up the Spark cluster.  Spark is only
used for reading/writing the actual Iceberg tables downstream.
"""

import gzip
import io
import logging
import os
import time
from datetime import date, timedelta
from typing import Iterator, Optional

import boto3
import requests
import yaml
from botocore.config import Config
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)
from tqdm import tqdm

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


def _s3_client(cfg: dict):
    """Return a boto3 S3 client pointed at LocalStack."""
    aws = cfg["aws"]
    return boto3.client(
        "s3",
        endpoint_url=aws["endpoint_url"],
        aws_access_key_id=aws["access_key_id"],
        aws_secret_access_key=aws["secret_access_key"],
        region_name=aws["region"],
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


# =============================================================================
# Date range generator
# =============================================================================

def _date_range(start: date, end: date) -> Iterator[date]:
    """Yields every date from start (inclusive) to end (inclusive)."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# =============================================================================
# Download & upload helpers
# =============================================================================

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type((requests.RequestException, IOError)),
    reraise=True,
)
def _download_hour(url: str, timeout: int) -> Optional[bytes]:
    """
    Download a single GH Archive hourly file.
    Returns raw gzip bytes, or None if the file does not exist (404).
    """
    logger.debug("Downloading %s", url)
    resp = requests.get(url, timeout=timeout, stream=True)
    if resp.status_code == 404:
        logger.warning("File not found (404): %s — skipping.", url)
        return None
    resp.raise_for_status()

    chunks = []
    for chunk in resp.iter_content(chunk_size=65536):
        if chunk:
            chunks.append(chunk)
    return b"".join(chunks)


def _s3_key_for(day: date, hour: int) -> str:
    """Return the S3 key for a given day/hour."""
    return f"bronze/{day.strftime('%Y/%m/%d')}/{day.strftime('%Y-%m-%d')}-{hour}.json.gz"


def _already_uploaded(s3, bucket: str, key: str) -> bool:
    """Check if a file already exists in S3 to enable idempotent re-runs."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False
    except Exception:
        return False


def _count_events_in_gz(data: bytes) -> int:
    """Count JSON lines (events) in a gzip byte payload."""
    try:
        with gzip.open(io.BytesIO(data)) as gz:
            return sum(1 for _ in gz)
    except Exception:
        return -1


# =============================================================================
# Public API
# =============================================================================

class BronzeIngester:
    """
    Orchestrates downloading GH Archive files and uploading them to S3 Bronze.

    Parameters
    ----------
    config_path : str, optional
        Path to config.yaml.
    dry_run : bool
        If True, simulates the download without uploading to S3.
    """

    def __init__(self, config_path: Optional[str] = None, dry_run: bool = False):
        self.cfg = _load_config(config_path)
        self.dry_run = dry_run
        self.s3 = _s3_client(self.cfg)
        self.bucket = self.cfg["aws"]["bucket"]
        self.base_url = self.cfg["gharchive"]["base_url"]
        self.hours = self.cfg["gharchive"]["hours_per_day"]
        self.timeout = self.cfg["gharchive"]["download_timeout_seconds"]

        logger.info(
            "BronzeIngester initialised. Bucket=%s, DryRun=%s", self.bucket, dry_run
        )

    def run(self, start_date: date, end_date: date) -> dict:
        """
        Download GH Archive files for every hour in [start_date, end_date] and
        upload to S3 Bronze.

        Returns
        -------
        dict
            Summary statistics: downloaded, skipped, failed counts.
        """
        logger.info(
            "Starting Bronze ingest: %s → %s (%d hours/day)",
            start_date,
            end_date,
            len(self.hours),
        )

        stats = {"downloaded": 0, "skipped": 0, "failed": 0, "total_events": 0}
        dates = list(_date_range(start_date, end_date))
        total_tasks = len(dates) * len(self.hours)

        with tqdm(total=total_tasks, desc="Bronze Ingest", unit="file") as pbar:
            for day in dates:
                for hour in self.hours:
                    key = _s3_key_for(day, hour)
                    url = f"{self.base_url}/{day.strftime('%Y-%m-%d')}-{hour}.json.gz"

                    # ---- Idempotency check ----
                    if not self.dry_run and _already_uploaded(self.s3, self.bucket, key):
                        logger.debug("Already in S3, skipping: %s", key)
                        stats["skipped"] += 1
                        pbar.update(1)
                        continue

                    # ---- Download ----
                    try:
                        data = _download_hour(url, self.timeout)
                    except Exception as exc:
                        logger.error("Failed to download %s: %s", url, exc)
                        stats["failed"] += 1
                        pbar.update(1)
                        continue

                    if data is None:
                        # 404 — file simply doesn't exist yet
                        stats["skipped"] += 1
                        pbar.update(1)
                        continue

                    event_count = _count_events_in_gz(data)
                    stats["total_events"] += max(event_count, 0)

                    # ---- Upload ----
                    if not self.dry_run:
                        try:
                            self.s3.put_object(
                                Bucket=self.bucket,
                                Key=key,
                                Body=data,
                                ContentType="application/gzip",
                            )
                            logger.debug(
                                "Uploaded s3://%s/%s (%d events)", self.bucket, key, event_count
                            )
                        except Exception as exc:
                            logger.error("Upload failed for %s: %s", key, exc)
                            stats["failed"] += 1
                            pbar.update(1)
                            continue
                    else:
                        logger.info("[DRY RUN] Would upload %s (%d events)", key, event_count)

                    stats["downloaded"] += 1
                    pbar.set_postfix(
                        downloaded=stats["downloaded"],
                        failed=stats["failed"],
                        events=stats["total_events"],
                    )
                    pbar.update(1)

        logger.info(
            "Bronze ingest complete. Downloaded=%d, Skipped=%d, Failed=%d, TotalEvents=%d",
            stats["downloaded"],
            stats["skipped"],
            stats["failed"],
            stats["total_events"],
        )
        return stats

    def list_bronze_files(self, prefix: str = "bronze/") -> list:
        """List all files in the Bronze S3 zone."""
        paginator = self.s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
