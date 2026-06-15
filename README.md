# ❄️ Snowflake ELT Pipeline — Data Engineering Portfolio

> A hands-on data engineering project demonstrating end-to-end ELT pipeline development in Snowflake, covering staging architecture, core data modeling, automation with Streams & Tasks, Dynamic Tables, pipeline monitoring, and data quality checks.

---

## 📌 Project Overview

This project implements a **production-style ELT (Extract, Load, Transform) pipeline** in Snowflake using a **Medallion Architecture** (Bronze → Silver → Gold layers). It covers the full lifecycle of data engineering in Snowflake — from raw data ingestion and staging, through core model transformation, to automated pipeline execution and observability.

Built as part of an advanced Snowflake data engineering study path, these scripts reflect real-world patterns used in enterprise data platforms.

---

## 🗂️ Repository Structure

```
Snowflake/
│
├── module2_clip2_staging_demo.sql        # Bronze layer — raw data staging setup
├── module2_clip3_core_model_demo.sql     # Silver layer — core data model & transformations
├── module3_clip2_streams_tasks_demo.sql  # Automation — Streams & Tasks for incremental loads
├── module3_clip3_dynamic_tables_demo.sql # Dynamic Tables — declarative pipeline automation
├── module4_clip2_monitoring_demo.sql     # Observability — pipeline monitoring & event tables
└── module4_clip3_data_quality_demo.sql   # Data quality — validation checks & error handling
```

---

## 🏗️ Architecture

```
Raw Source Data
      │
      ▼
┌─────────────────┐
│   BRONZE Layer  │  ← Raw ingestion, Parquet/CSV staging, COPY INTO
│   (Staging)     │    module2_clip2_staging_demo.sql
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SILVER Layer  │  ← Cleaned, typed, validated records
│  (Core Model)   │    module2_clip3_core_model_demo.sql
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AUTOMATION     │  ← Streams + Tasks + Dynamic Tables
│  (Incremental)  │    module3_clip2, module3_clip3
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  OBSERVABILITY  │  ← Monitoring, data quality, alerting
│  & QUALITY      │    module4_clip2, module4_clip3
└─────────────────┘
```

---

## 📄 Script Breakdown

### `module2_clip2_staging_demo.sql` — Staging Layer (Bronze)
Sets up the raw ingestion layer for incoming data files.
- Creates external and internal stages for file landing
- Configures `COPY INTO` commands for bulk data loading
- Defines file formats (CSV, Parquet, JSON)
- Sets up Bronze layer tables for raw, unmodified records

### `module2_clip3_core_model_demo.sql` — Core Data Model (Silver)
Transforms raw staged data into clean, queryable models.
- Creates Silver layer tables with typed columns and constraints
- Applies transformations: type casting, JSON parsing, field normalization
- Uses `TRY_TO_NUMBER`, `PARSE_JSON`, and `LATERAL FLATTEN` for safe transformations
- Implements `INSERT INTO ... SELECT` patterns for Bronze → Silver promotion

### `module3_clip2_streams_tasks_demo.sql` — Streams & Tasks (Automation)
Automates incremental data processing without full table scans.
- Creates **Snowflake Streams** to capture CDC (Change Data Capture) on source tables
- Defines **Tasks** with CRON scheduling for automated pipeline execution
- Implements `SYSTEM$STREAM_HAS_DATA` checks before task execution
- Chains tasks into a dependency tree for ordered pipeline stages

### `module3_clip3_dynamic_tables_demo.sql` — Dynamic Tables
Declarative, self-maintaining pipeline automation.
- Defines **Dynamic Tables** that automatically refresh based on upstream changes
- Sets target lag thresholds for near-real-time data freshness
- Compares Dynamic Tables vs. Streams+Tasks approach for different use cases
- Demonstrates simplified pipeline code with automatic dependency management

### `module4_clip2_monitoring_demo.sql` — Pipeline Monitoring & Observability
Implements full observability for the data pipeline.
- Configures **Event Tables** (`ALTER DATABASE ... SET EVENT_TABLE`) for telemetry capture
- Queries `SNOWFLAKE.ACCOUNT_USAGE` views for pipeline health metrics
- Monitors task history, warehouse usage, and query performance
- Sets up **Resource Monitors** and usage alerts

### `module4_clip3_data_quality_demo.sql` — Data Quality Checks
Validates data integrity throughout the pipeline.
- Implements row-level validation checks (null checks, range checks, referential integrity)
- Uses Snowflake **Data Metric Functions (DMFs)** for automated quality scoring
- Creates quality audit tables to log validation results over time
- Handles bad records with reject tables and error logging patterns

---

## 🛠️ Tech Stack

| Tool | Usage |
|------|-------|
| **Snowflake** | Primary data platform — warehousing, compute, storage |
| **SQL / SnowSQL** | All pipeline logic and transformations |
| **Snowsight** | UI for development, monitoring, and query profiling |
| **Snowflake Streams** | CDC-based incremental change tracking |
| **Snowflake Tasks** | Scheduled, dependency-based pipeline automation |
| **Dynamic Tables** | Declarative pipeline refresh automation |
| **Event Tables** | Telemetry and pipeline observability |
| **Data Metric Functions** | Automated data quality scoring |

---

## 💡 Key Concepts Demonstrated

- **Medallion Architecture** — Bronze (raw) → Silver (clean) → Gold (aggregated) layering
- **ELT vs ETL** — loading raw data first, then transforming inside Snowflake
- **Incremental Loading** — Streams + Tasks to process only new/changed records
- **Pipeline Observability** — Event Tables and ACCOUNT_USAGE monitoring
- **Data Quality Engineering** — automated validation with reject handling
- **Declarative Pipelines** — Dynamic Tables as a simpler alternative to task graphs

---

## 🚀 How to Run

1. **Set up a Snowflake account** — [Free trial at snowflake.com](https://signup.snowflake.com/)

2. **Create a database and schema:**
```sql
CREATE DATABASE ELT_DEMO;
CREATE SCHEMA ELT_DEMO.BRONZE;
CREATE SCHEMA ELT_DEMO.SILVER;
```

3. **Run scripts in order:**
```
1. module2_clip2_staging_demo.sql     -- Set up staging
2. module2_clip3_core_model_demo.sql  -- Build core model
3. module3_clip2_streams_tasks_demo.sql  -- Add automation
4. module3_clip3_dynamic_tables_demo.sql -- Add dynamic tables
5. module4_clip2_monitoring_demo.sql  -- Enable monitoring
6. module4_clip3_data_quality_demo.sql -- Add quality checks
```

4. **Use Snowsight** to monitor task runs, query history, and pipeline health.

---

## 📈 What I Learned

- Designing scalable multi-layer data pipelines in Snowflake
- Using Streams and Tasks for production-grade incremental processing
- Monitoring pipeline health with Event Tables and ACCOUNT_USAGE
- Implementing data quality checks as first-class pipeline components
- Comparing Dynamic Tables vs. Streams+Tasks for different latency requirements

---

## 🔗 Related Work

- 📘 [Pluralsight: Build ELT Pipelines in Snowflake](https://www.pluralsight.com) — Completed Feb 2026
- 📘 [Pluralsight: Warehouse Design and Architecture in Snowflake](https://www.pluralsight.com) — Completed Feb 2026
- 📘 [Pluralsight: Test and Validate Data in Snowflake Pipelines](https://www.pluralsight.com) — In Progress

---

## 👤 Author

**Preetham Samatham**
MS in Artificial Intelligence — University of Central Missouri
Data Engineering → ML & AI

- 🔗 [LinkedIn](https://www.linkedin.com/in/preetham-samatham-02597321/)
- 🐙 [GitHub](https://github.com/preethamsamatham)

---

*Part of an ongoing data engineering portfolio. More projects coming as I progress through GCP BigQuery, Dataflow, and Vertex AI.*
