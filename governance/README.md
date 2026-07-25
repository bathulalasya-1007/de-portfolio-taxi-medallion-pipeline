# Governance & Cost-Aware Architecture — Unity Catalog on the Medallion Pipeline

A governance layer built on top of the [NYC Taxi Medallion Pipeline](../de-portfolio-taxi-medallion-pipeline), adding access control, column and row-level security, lineage tracking, audit logging, and a cost/optimization analysis using Databricks Unity Catalog.

## Overview

A working Bronze/Silver/Gold pipeline is only half the story in a real data platform — the other half is controlling who can see what, proving where data came from, and knowing what it costs to run. This project takes the existing medallion pipeline and wraps it in a proper governance model: personas with scoped access, PII-style column masking, row-level filtering, automatic lineage capture, and queryable audit logs — the layer that separates a pipeline demo from platform-level thinking.

## Architecture

```
portfolio_catalog
├── bronze.taxi_trips      (raw, ingested via Auto Loader from S3)
├── silver.taxi_trips      (cleaned, deduplicated)
│     └── silver.taxi_trips_masked   (dynamic view — column masking)
└── gold.daily_taxi_summary (business aggregates)

Personas (Account Groups)
├── data_engineers   → full read/write across bronze/silver/gold, sees unmasked columns, bypasses row filter
├── data_analysts    → read-only on gold; masked view on silver; row-filtered to VendorID = 1
└── pii_restricted   → reserved for stricter future column sets

Governance controls applied directly on portfolio_catalog:
├── GRANT / REVOKE          → schema-level access per persona
├── Dynamic view + CASE     → column-level masking (PULocationID, DOLocationID)
├── ROW FILTER function     → row-level security (is_account_group_member-based)
├── Unity Catalog Lineage   → automatic, from Lakeflow Job runs
└── system.access.audit     → queryable audit trail
```

## Why a Separate Catalog

The original pipeline wrote to `portfolio.raw_data` — a single schema holding all three layers together. For this project, a new `portfolio_catalog` was created with **separate schemas per medallion layer** (`bronze`, `silver`, `gold`), and the existing tables were copied in via `CREATE TABLE ... AS SELECT`. The Bronze/Silver/Gold notebooks and the Lakeflow Job were then updated to read and write against `portfolio_catalog` directly, so governance, lineage, and the pipeline all operate on the same catalog rather than being split across two.

## Personas & Access Control

Three account-level groups were created to represent realistic roles:

| Group | Access |
|---|---|
| `data_engineers` | Full privileges across `portfolio_catalog` (bronze → gold), unmasked columns, unrestricted rows |
| `data_analysts` | `SELECT` on the `gold` schema only; explicitly revoked from `bronze` and `silver`; reads silver only through the masked view |
| `pii_restricted` | Created as a reserved group for stricter column-level policies as the dataset grows |

```sql
GRANT SELECT ON SCHEMA portfolio_catalog.gold TO `data_analysts`;

REVOKE ALL PRIVILEGES ON SCHEMA portfolio_catalog.bronze FROM `data_analysts`;
REVOKE ALL PRIVILEGES ON SCHEMA portfolio_catalog.silver FROM `data_analysts`;

GRANT ALL PRIVILEGES ON CATALOG portfolio_catalog TO `data_engineers`;
```

## Column-Level Masking

`PULocationID` and `DOLocationID` (pickup/dropoff location identifiers — treated here as quasi-PII, since they can reveal a rider's home or work location) are masked for any group other than `data_engineers`, via a dynamic view:

```sql
CREATE VIEW portfolio_catalog.silver.taxi_trips_masked AS
SELECT
  VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
  passenger_count, trip_distance,
  CASE WHEN is_account_group_member('data_engineers') THEN PULocationID ELSE NULL END AS PULocationID,
  CASE WHEN is_account_group_member('data_engineers') THEN DOLocationID ELSE NULL END AS DOLocationID,
  payment_type, fare_amount, tip_amount, tolls_amount
FROM portfolio_catalog.silver.taxi_trips;
```

**Verified both ways:** querying as a `data_engineers` member returned real location IDs (e.g. `262`, `236`); after temporarily removing that membership and querying as `data_analysts` only, the same columns returned `NULL` for every row — confirming the mask activates correctly based on live group membership rather than being a static transform.

## Row-Level Security

A row filter restricts `data_analysts` to a single `VendorID`, simulating a scenario where an analyst should only see their own vendor's trips, while engineers see everything:

```sql
CREATE OR REPLACE FUNCTION portfolio_catalog.silver.vendor_row_filter(vendor_id INT)
RETURN is_account_group_member('data_engineers') OR vendor_id = 1;

ALTER TABLE portfolio_catalog.silver.taxi_trips
SET ROW FILTER portfolio_catalog.silver.vendor_row_filter ON (VendorID);
```

**Verified both ways:** as `data_analysts` only, querying `taxi_trips` returned exclusively `VendorID = 1` rows (669,237 of them). As a `data_engineers` member, both `VendorID = 1` and `VendorID = 2` were visible (matching the table's full row count of 2,721,765) — the filter correctly widens access based on group membership.

## Lineage Tracking

Running the Lakeflow Job against `portfolio_catalog` lets Unity Catalog automatically capture lineage end to end.

![Lineage graph for portfolio_catalog.gold.daily_taxi_summary](./screenshots/lineage_graph.png)

*`portfolio_catalog.silver.taxi_trips` → `portfolio_catalog.gold.daily_taxi_summary`, with column-level detail and downstream consumers, captured automatically from the Job run — no manual lineage documentation required.*

**Why this matters in an incident:** if the Gold `total_revenue` numbers looked wrong, lineage lets you trace directly back to the exact Silver transformation and, from there, the Bronze source file that produced it — instead of manually reconstructing the pipeline from notebook code.

## Audit Logs

Unity Catalog activity is queryable directly from system tables:

```sql
SELECT event_time, user_identity.email AS user_email, action_name, request_params
FROM system.access.audit
WHERE service_name = 'unityCatalog'
ORDER BY event_time DESC
LIMIT 20;
```

Sample output captured real governance activity as it happened — `updateTables`, `getTable`, and `metadataAndPermissionsSnapshot` events tied to the account performing the GRANT/REVOKE and masking work in this project, each timestamped and attributed to a specific user.

## Cost & Optimization Analysis

**Pipeline run cost:** all three Lakeflow Job tasks ran on Serverless compute, total job duration 1m 38s (bronze_task 1m 6s, silver_task 17s, gold_layer 13s).

![Lakeflow Job DAG on portfolio_catalog](./screenshots/job_run_success.png)

*`bronze_task → silver_task → gold_layer`, all succeeded, running end-to-end against the governed `portfolio_catalog`.*

**Optimization tested:** `OPTIMIZE portfolio_catalog.silver.taxi_trips ZORDER BY (VendorID)`.

**Result:** a `VendorID`-filtered aggregation query ran in 2s both before and after the optimization — no measurable improvement at this dataset size (~2.7M rows). This is an expected, honestly-reported result rather than a fabricated win: Z-ordering's benefit comes from file-level data skipping, and it scales with data volume and file count. On a Delta table this size running on serverless compute, the query planner already has little to gain. At a larger scale (10M+ rows, multiple concurrent filters across partitions), the same optimization would be expected to show a measurable reduction in runtime — this is documented here as a tested tradeoff, not a claimed one.

## Tradeoffs & Decisions

**New catalog instead of governing `portfolio.raw_data` in place.** A fresh `portfolio_catalog` with per-layer schemas was created rather than retrofitting governance onto the original flat schema, so the schema structure itself (`bronze` / `silver` / `gold`) reflects the access boundaries, rather than relying on table naming conventions alone.

**Column masking chosen over full table duplication.** Rather than maintaining separate masked and unmasked copies of the Silver table, a single dynamic view handles both cases based on live group membership — one source of truth, correct for any group without redeployment.

**Row-level security scoped to `VendorID` rather than a geographic region.** The dataset doesn't carry an explicit region/store column, so `VendorID` was used as the closest realistic analogue to a "each analyst sees only their own book of business" boundary. A production system with a true region column would apply the same pattern there instead.

**Optimization result reported as-is.** The Z-ordering test showed no measurable gain at this data volume. Rather than omit it or claim an improvement that didn't happen, it's documented with the reasoning for why it didn't move the needle here and where it would be expected to.

## How to Explore This

1. Query `portfolio_catalog.silver.taxi_trips_masked` as different group memberships to see column masking in action.
2. Query `portfolio_catalog.silver.taxi_trips` directly to see row-level filtering based on group membership.
3. View lineage on any table under `portfolio_catalog` via Catalog Explorer → table → **Lineage** tab.
4. Run `SELECT * FROM system.access.audit WHERE service_name = 'unityCatalog' ORDER BY event_time DESC` to see recent governance activity.

## Repository Structure

```
├── Governance_layer.py       -- catalog/schema setup, grants, masking, row filter, audit queries
├── screenshots/
│   └── lineage_graph.png
└── README.md
```
