# GitHub Archive Trend & Virality Analytics Platform

> A fully open-source, locally runnable analytics platform that ingests raw [GH Archive](https://www.gharchive.org/) data, processes it through a **Bronze → Silver → Gold** Medallion Architecture built on **Apache Iceberg** and **Apache Spark**, and surfaces viral repository rankings, tech-stack ecosystem trends, and macro GitHub platform statistics.

---

## Results Preview

```
Top Viral Repositories — Week of Jan 15, 2024
+-----------------------------+--------------+----------+----------+
| repo_name                   | virality     | stars    | forks    |
+-----------------------------+--------------+----------+----------+
| maybe-finance/maybe         | 887.0        | 213      | 11       |
| lewagon/dotfiles            | 804.0        | 48       | 204      |
| VikParuchuri/surya          | 545.0        | 128      | 11       |
| vanna-ai/vanna              | 448.0        | 112      | 0        |
| danny-avila/LibreChat       | 363.0        | 85       | 7        |
| TencentARC/PhotoMaker       | 288.0        | 69       | 4        |
| EpicGames/raddebugger       | 269.0        | 65       | 3        |
| janhq/jan                   | 231.0        | 56       | 2        |
| krahets/hello-algo          | 215.0        | 50       | 5        |
+-----------------------------+--------------+----------+----------+
```

---

## Features

| Feature | Details |
|---|---|
| **Virality Engine** | `(Stars×4) + (Forks×3) + (PRs×2) + (Issues×1)` |
| **Time Windows** | Day / Week / Month sliding aggregations |
| **Tech Stack Trends** | Language distribution, distinct contributors, event share % |
| **Macro Stats** | Star:Fork ratio, PR merge rate, avg commits/push |
| **Iceberg Time Travel** | Query any historical snapshot |
| **100% Local / Free** | LocalStack S3 + Iceberg REST Catalog + Spark in Docker |
| **CLI Orchestrator** | Single `main.py` entry point with argparse |
| **Jupyter Notebook** | Interactive trend analysis with charts |

---

## Architecture

```
GH Archive (.json.gz)
        │
        ▼
┌─────────────────┐
│  Bronze Layer   │  Raw .json.gz stored in
│  (LocalStack S3)│  s3a://github-archive-bucket/bronze/
└────────┬────────┘
         │  Spark reads + parses JSON
         ▼
┌─────────────────┐
│  Silver Layer   │  Flattened, typed Iceberg table
│  (Iceberg)      │  demo.silver.events  (partitioned by date)
└────────┬────────┘
         │  Spark aggregations
         ▼
┌──────────────────────────────────────────────────┐
│  Gold Layer (Iceberg)                            │
│  ├── demo.gold.viral_repos       (virality index)│
│  ├── demo.gold.tech_stack_trends (language stats)│
│  └── demo.gold.macro_stats       (platform KPIs) │
└──────────────────────────────────────────────────┘
```

---

## Project Structure

```
github-archive-analytics/
├── docker/
│   ├── docker-compose.yml          # Full stack: LocalStack + Iceberg + Spark
│   └── spark/
│       └── Dockerfile              # Spark 3.5 + Iceberg jars + Python deps
├── config/
│   └── config.yaml                 # All settings (endpoints, weights, hours, etc.)
├── src/
│   ├── pipeline/
│   │   ├── ingest.py               # Bronze: download GH Archive → S3
│   │   ├── transform.py            # Silver: flatten JSON → Iceberg
│   │   └── aggregate.py            # Gold: virality, tech trends, macro stats
│   └── utils/
│       └── spark_session.py        # SparkSession factory (Iceberg + S3)
├── notebooks/
│   └── viral_analysis.ipynb        # Interactive analysis + time-travel queries
├── main.py                         # CLI orchestrator
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | Latest | Allocate ≥ 6 GB RAM in Settings → Resources |
| Python | 3.10+ | 3.13 works fine |
| Java (JDK) | 11 | [Adoptium Temurin 11](https://adoptium.net/temurin/releases/?version=11) |
| winutils (Windows only) | hadoop-3.3.5 | See Windows setup below |

---

### Step 1 — Clone & Install

```bash
git clone https://github.com/YOUR_USERNAME/github-archive-analytics.git
cd github-archive-analytics

python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install pyspark==3.5.1 boto3 botocore requests urllib3 PyYAML pyarrow \
    tenacity tqdm colorlog python-dateutil jupyter notebook ipykernel
pip install "pyiceberg[s3fs,pyarrow]"
```

---

### Step 2 — Windows Only: Install winutils

Spark on Windows requires `winutils.exe` to emulate Hadoop file operations.

```powershell
# Download winutils
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"

# Set permanently (run as Administrator)
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "Machine")
[System.Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";C:\hadoop\bin", "Machine")
```

Or set it temporarily for your current session before each run:
```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

---

### Step 3 — Start the Docker Stack

```bash
cd docker
docker-compose up -d
```

This starts 5 services. First run takes **5–10 minutes** to pull images.

```bash
# Verify everything is running
docker-compose ps
```

Expected output — all services up:
```
NAME                IMAGE                           STATUS
iceberg-rest        tabulario/iceberg-rest:0.10.0   Up
localstack          localstack/localstack:3.4        Up (healthy)
spark-master        github-analytics-spark:3.5      Up
spark-worker        github-analytics-spark:3.5      Up
```

Verify services manually:
```bash
curl http://localhost:4566/_localstack/health   # LocalStack S3
curl http://localhost:8181/v1/config            # Iceberg REST Catalog
```

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| LocalStack S3 | http://localhost:4566 |
| Iceberg REST | http://localhost:8181 |

---

### Step 4 — Run the Pipeline

```bash
cd ..  # back to project root

# Quick test — 3 hours of data (~750K events, ~3 min)
python main.py --action run-analytics --start-date 2024-01-15 --end-date 2024-01-15

# Full day — 24 hours (~5.5M events, ~15 min)
# First edit config/config.yaml and set hours_per_day to all 24 hours
python main.py --action run-analytics --start-date 2024-01-15 --end-date 2024-01-15

# Multi-day run
python main.py --action run-analytics --start-date 2024-01-01 --end-date 2024-01-07
```

---

### Step 5 — View Results

```bash
# Top viral repos (weekly ranking)
python main.py --action show-viral --window week

# Daily ranking
python main.py --action show-viral --window day

# Monthly ranking
python main.py --action show-viral --window month
```

---

## Configuration

All settings live in `config/config.yaml`:

```yaml
# Speed up testing — download only 3 hours instead of 24
gharchive:
  hours_per_day: [10, 11, 12]

# Virality formula weights
virality:
  weights:
    star: 4          # Stars are most viral
    fork: 3          # Forks show deep interest
    pull_request: 2  # PRs = active contribution
    issue: 1         # Issues = engagement
```

---

## CLI Reference

```
python main.py --action <ACTION> [OPTIONS]

Actions:
  run-analytics   Full Bronze → Silver → Gold pipeline
  ingest          Bronze only: download GH Archive files to S3
  transform       Silver only: parse JSON → Iceberg table
  aggregate       Gold only: compute all analytics
  optimize        Run Iceberg table maintenance
  list-bronze     List files currently in S3 Bronze zone
  show-viral      Print top viral repos to console

Options:
  --start-date    YYYY-MM-DD  (required for data actions)
  --end-date      YYYY-MM-DD  (required for data actions)
  --docker        Use Docker-internal hostnames (when running inside container)
  --dry-run       Simulate ingest without uploading to S3
  --window        day|week|month  (for show-viral, default: week)
  --log-level     DEBUG|INFO|WARNING|ERROR
```

**Examples:**

```bash
# Dry run — see what would be downloaded without actually downloading
python main.py --action ingest --start-date 2024-01-15 --end-date 2024-01-15 --dry-run

# Run only Silver transform (Bronze files already in S3)
python main.py --action transform --start-date 2024-01-15 --end-date 2024-01-15

# Run only Gold aggregations (Silver table already exists)
python main.py --action aggregate --start-date 2024-01-15 --end-date 2024-01-15

# Optimize Iceberg tables (bin-packing + snapshot expiry)
python main.py --action optimize

# List all Bronze files in S3
python main.py --action list-bronze
```

---

## 📓 Jupyter Notebook

```bash
cd notebooks
jupyter notebook viral_analysis.ipynb
```

The notebook covers:

1. **Top 10 viral repos per window** — horizontal bar charts
2. **Language ecosystem comparison** — Python vs TypeScript vs Rust vs Go
3. **Iceberg time-travel queries** — `VERSION AS OF` and `TIMESTAMP AS OF`
4. **Event type distribution** — stacked area chart over time
5. **Macro platform stats** — formatted summary card

---

## Iceberg SQL Reference

Connect via the notebook or query directly in Python:

```python
from src.utils.spark_session import create_spark_session
spark = create_spark_session()

# Top viral repos
spark.sql("""
    SELECT repo_name, virality_score, star_count, fork_count
    FROM demo.gold.viral_repos
    WHERE window_type = 'week' AND rank_in_window <= 10
    ORDER BY virality_score DESC
""").show(truncate=False)

# Language market share
spark.sql("""
    SELECT repo_language, event_share_pct, distinct_contributors
    FROM demo.gold.tech_stack_trends
    ORDER BY language_rank LIMIT 15
""").show()

# Platform macro stats
spark.sql("SELECT * FROM demo.gold.macro_stats").show()

# Iceberg snapshots (for time travel)
spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM demo.silver.events.snapshots
    ORDER BY committed_at DESC
""").show()

# Time travel — query as of a specific snapshot
spark.sql("""
    SELECT COUNT(*) FROM demo.silver.events
    VERSION AS OF <snapshot_id>
""").show()

# Time travel — query as of a timestamp
spark.sql("""
    SELECT COUNT(*) FROM demo.silver.events
    TIMESTAMP AS OF '2024-01-15 12:00:00'
""").show()

# Partition summary
spark.sql("""
    SELECT event_date, COUNT(*) AS records
    FROM demo.silver.events
    GROUP BY event_date ORDER BY event_date
""").show()

# Manual table optimization
spark.sql("CALL demo.system.rewrite_data_files(table => 'demo.silver.events')")
spark.sql("CALL demo.system.expire_snapshots(table => 'demo.silver.events', retain_last => 5)")
```

---

## Troubleshooting

### Docker services not starting
```bash
# Check logs for a specific service
docker logs iceberg-rest
docker logs localstack

# Full reset — wipes all data and restarts clean
cd docker
docker-compose down -v
docker-compose up -d
```

> `docker-compose down -v` deletes all stored data including Bronze S3 files. After a full reset, the pipeline will re-download GH Archive files on the next run.

### `HADOOP_HOME` error on Windows
```
HADOOP_HOME and hadoop.home.dir are unset
```
Set it in your current PowerShell session:
```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

### Java not found
Install [Adoptium Temurin JDK 11](https://adoptium.net/temurin/releases/?version=11). Make sure to check **"Set JAVA_HOME variable"** during installation, then open a new terminal.

### Iceberg table has stale metadata after `down -v`
The Iceberg REST catalog stores table metadata in memory (SQLite). After a full volume reset, old table references become stale. Fix:
```bash
# Full reset clears everything
docker-compose down -v && docker-compose up -d
# Then rerun the pipeline — tables will be recreated fresh
python main.py --action run-analytics --start-date 2024-01-15 --end-date 2024-01-15
```

### Out of memory / Python worker crashed
Reduce the number of hours processed per day in `config/config.yaml`:
```yaml
gharchive:
  hours_per_day: [10, 11, 12]  # 3 hours ~750K events (recommended for 8GB RAM)
```
Or increase Spark memory in `config.yaml`:
```yaml
spark:
  driver_memory: "4g"
  executor_memory: "4g"
```

### Spark temp file deletion warnings on Windows
```
IOException: Failed to delete ... wildfly-openssl.jar
```
These are harmless Windows file-locking warnings on Spark shutdown. They do not affect results — ignore them.

---

## Technology Stack

| Component | Technology |
|---|---|
| Processing Engine | Apache Spark 3.5.1 (PySpark) |
| Table Format | Apache Iceberg 1.5.2 |
| Catalog | Iceberg REST Catalog (tabulario) |
| Object Storage | LocalStack 3.4 (S3-compatible) |
| Data Source | GH Archive (gharchive.org) |
| Language | Python 3.10+ |
| Infrastructure | Docker Compose |

---

## Dependencies

```
pyspark==3.5.1          # Distributed processing
pyiceberg[s3fs,pyarrow] # Iceberg Python client
boto3 / botocore        # S3 (LocalStack) access
requests                # GH Archive download
PyYAML                  # Config management
pyarrow                 # Columnar data
tenacity                # Retry logic
tqdm                    # Progress bars
colorlog                # Coloured logging
jupyter / notebook      # Analysis notebook
```

---

## License

Apache 2.0 — free for commercial and personal use.

---

## Data Source

All data comes from [GH Archive](https://www.gharchive.org/) — a project that records the public GitHub timeline and makes it available for analysis. Data is available from 2011 onwards, updated hourly.
