"""
spark_session.py
================
Factory for creating a fully-configured PySpark SparkSession with:
  - Apache Iceberg support (REST catalog → LocalStack S3)
  - Hadoop AWS / S3A file system
  - Iceberg SQL extensions
"""

import logging
import os
from typing import Optional

import yaml
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _load_config(config_path: Optional[str] = None) -> dict:
    if config_path is None:
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base, "config", "config.yaml")
    with open(config_path, "r") as fh:
        return yaml.safe_load(fh)


def create_spark_session(
    app_name: Optional[str] = None,
    config_path: Optional[str] = None,
    use_docker_endpoints: bool = False,
) -> SparkSession:
    """
    Build and return a SparkSession configured for Iceberg + LocalStack S3.
    Uses Iceberg 1.5.2 with AWS SDK v2 bundle for S3FileIO compatibility.
    """
    cfg = _load_config(config_path)

    spark_cfg = cfg["spark"]
    aws_cfg = cfg["aws"]
    catalog_cfg = cfg["catalog"]

    s3_endpoint = (
        aws_cfg["endpoint_url_docker"] if use_docker_endpoints else aws_cfg["endpoint_url"]
    )
    catalog_uri = (
        catalog_cfg["uri_docker"] if use_docker_endpoints else catalog_cfg["uri"]
    )
    spark_master = (
        spark_cfg["master_docker"] if use_docker_endpoints else spark_cfg["master"]
    )

    resolved_app_name = app_name or spark_cfg["app_name"]

    logger.info("Building SparkSession '%s' (master=%s)", resolved_app_name, spark_master)
    logger.info("S3 endpoint : %s", s3_endpoint)
    logger.info("Catalog URI : %s", catalog_uri)

    # -------------------------------------------------------------------------
    # JAR packages:
    # - iceberg-spark-runtime-3.5_2.12:1.5.2  (Iceberg core + Spark integration)
    # - iceberg-aws-bundle:1.5.2               (AWS SDK v2 + S3FileIO — required!)
    # - hadoop-aws:3.3.4                       (S3A filesystem for reading raw files)
    # - aws-java-sdk-bundle:1.12.262           (AWS SDK v1, needed by hadoop-aws)
    # -------------------------------------------------------------------------
    jars_packages = ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    builder = (
        SparkSession.builder
        .master(spark_master)
        .appName(resolved_app_name)

        # ---- Memory ----
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.executor.memory", spark_cfg["executor_memory"])
        .config("spark.executor.cores", str(spark_cfg["executor_cores"]))

        # ---- Iceberg SQL extensions ----
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        # ---- Iceberg REST Catalog ----
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "rest")
        .config("spark.sql.catalog.demo.uri", catalog_uri)
        .config("spark.sql.catalog.demo.warehouse", catalog_cfg["warehouse"])
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.demo.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.demo.s3.path-style-access", "true")
        .config("spark.sql.catalog.demo.s3.access-key-id", aws_cfg["access_key_id"])
        .config("spark.sql.catalog.demo.s3.secret-access-key", aws_cfg["secret_access_key"])
        # Force AWS SDK v2 to use path-style and custom endpoint
        .config("spark.sql.catalog.demo.s3.region", aws_cfg["region"])

        # ---- Hadoop / S3A FileSystem (for reading Bronze .json.gz files) ----
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_cfg["access_key_id"])
        .config("spark.hadoop.fs.s3a.secret.key", aws_cfg["secret_access_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_cfg["region"])

        # ---- JAR packages ----
        .config("spark.jars.packages", jars_packages)

        # ---- Performance ----
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "50")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))

    _ensure_namespaces(spark)

    logger.info("SparkSession ready.")
    return spark


def _ensure_namespaces(spark: SparkSession) -> None:
    """Create Iceberg namespaces (databases) if they don't exist."""
    for ns in ["demo.silver", "demo.gold"]:
        try:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
            logger.debug("Namespace ensured: %s", ns)
        except Exception as exc:
            logger.warning("Could not ensure namespace %s: %s", ns, exc)
