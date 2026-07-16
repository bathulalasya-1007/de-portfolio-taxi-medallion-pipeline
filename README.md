# NYC Taxi Medallion Pipeline

A batch ELT pipeline built on Databricks that ingests raw NYC Yellow Taxi trip data, cleans and validates it, and produces business-ready daily summary metrics — following the Bronze / Silver / Gold Medallion architecture.

## Problem Statement

Raw trip-level data from public sources like the NYC Taxi & Limousine Commission (TLC) is messy — duplicate records, invalid fares, missing passenger counts — and isn't directly usable for reporting or analytics. This pipeline demonstrates a repeatable, production-style pattern for turning that raw data into clean, trustworthy, business-ready tables that answer real questions (e.g. daily trip volume, revenue, average fare).

## Architecture

```
NYC TLC public dataset (parquet)
        │
        ▼
  Databricks Volume (raw landing zone)
        │
        ▼
  Auto Loader (incremental ingestion, schema evolution)
        │
        ▼
  BRONZE  →  raw data as-is + ingestion metadata (_ingested_at, _source_file)
        │
        ▼
  SILVER  →  deduplicated, null-filtered, invalid-record-filtered
        │
        ▼
  GOLD    →  daily aggregated business metrics
        │
        ▼
  Lakeflow Job (scheduled orchestration of the full pipeline)
```

## Tools Used & Why

| Tool | Why |
|---|---|
| **Databricks Auto Loader** | Handles incremental file ingestion automatically, with built-in schema evolution — avoids manually tracking which files have already been processed |
| **Delta Lake** | ACID transactions and reliable `MERGE`/upsert support, which a plain Parquet table doesn't give you |
| **PySpark** | Scales cleanly from this dataset size to much larger volumes without rewriting logic |
| **Lakeflow Jobs** | Turns the manual notebook run into a scheduled, monitored, repeatable pipeline |
| **Databricks Volumes** | Used as the raw storage layer for this phase of the project (see note below) |

**Note on storage:** this version of the pipeline uses a Databricks-managed Volume as the raw landing zone instead of Amazon S3. The ingestion logic (Auto Loader, schema handling, Delta writes) is identical either way — only the source path changes. A future iteration will point the same pipeline at an S3 bucket to reflect a more typical cloud-native raw-storage pattern.

## Pipeline Results

| Layer | Row Count | What Happened |
|---|---|---|
| Bronze | 2,964,624 | Raw January 2024 NYC Yellow Taxi trips ingested as-is via Auto Loader |
| Silver | 2,721,765 | Deduplicated on (VendorID, pickup time, dropoff time); dropped null passenger counts, non-positive fares/distances |
| Gold | 34 | Aggregated to one row per trip date: total trips, total revenue, average trip distance, average fare |

**Data quality note:** the Silver layer removed roughly 8% of Bronze records (~243K rows) as duplicates or invalid entries — showing the raw source data has a meaningful noise rate worth filtering before it reaches reporting tables. The Gold table also surfaced a couple of corrupted-timestamp records (dates far outside January 2024, e.g. 2009 and December 2023) that passed the current Silver filters — a known gap, and a good example of why a data quality pass usually needs a second iteration once you see what the data actually contains. A production version would add an explicit date-range filter to catch this.

## Orchestration

The full pipeline (Bronze → Silver → Gold) runs as a single scheduled Lakeflow Job. See `screenshots/job_run_success.png` for a successful run (52s total duration, serverless compute).

**Tradeoff:** for this version, all three layers run as one task in one notebook rather than three separate dependent tasks. This was a simplicity tradeoff for a first iteration — a production setup would split Bronze/Silver/Gold into separate tasks with explicit dependencies, so each layer can be retried, monitored, and scaled independently without re-running the whole pipeline on a partial failure.

## How to Run It

1. Clone this repo
2. Import `bronze_layer_ingestion.py` into a Databricks workspace (Free Edition or higher)
3. Run the notebook top to bottom, or attach it as a task in a Lakeflow Job for scheduled execution
4. Tables are created under the `portfolio.raw_data` catalog/schema: `bronze_taxi_trips`, `silver_taxi_trips`, `gold_daily_taxi_summary`

## Dataset

[NYC Yellow Taxi Trip Records, January 2024](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — publicly released monthly by the NYC Taxi & Limousine Commission.
