# Enterprise Data Warehouse & ELT Pipeline

A multi-source ELT pipeline that ingests three independent, differently-structured
data sources (SQL Server-style, ERP/CSV-style, and Oracle-style exports), lands
them in cloud storage, loads them into a cloud data warehouse, and transforms
them into three separate star-schema data marts using dbt — with automated
testing and row-level reconciliation proving data integrity end to end.

## The problem

Companies routinely inherit sales/order data spread across systems that were
never designed to talk to each other — a legacy transactional database, ERP
CSV exports, a separate supply chain system with its own naming conventions.
Without an automated pipeline, answering a simple question like "what did we
sell last month" requires manually pulling and reconciling data by hand, with
no guarantee nothing was dropped or duplicated along the way.

This project builds that pipeline for three genuinely different, publicly
available datasets, chosen specifically because their schemas, naming
conventions, and data quality issues do not match — to prove the ingestion
and cleaning logic generalizes rather than being hand-fit to one clean source.

## Architecture

```
[SQL Server-style: AdventureWorks]   ─┐
[ERP-style CSV: Olist e-commerce]    ─┼──> S3 (raw landing) ──> Snowflake (raw)
[Oracle-style: DataCo Supply Chain]  ─┘                              │
                                                                      ▼
                                                    dbt: staging → dimensional marts
                                                                      │
                                        ┌─────────────────┬──────────┴───────┐
                                        ▼                 ▼                  ▼
                              AdventureWorks mart    Olist mart        DataCo mart
                              (dim_customer_aw,      (dim_customer_    (dim_customer_
                               dim_product_aw,        olist,            dataco,
                               fact_orders_aw)        dim_product_      dim_product_
                                                       olist,            dataco,
                                                       fact_orders_      fact_orders_
                                                       olist)            dataco)
                                        │                 │                  │
                                        └─────────────────┴──────────────────┘
                                                  dim_date (shared)
                                                          │
                                              dbt tests + reconciliation_check
```

See `lineage_graph.png` for the full auto-generated dbt lineage diagram.

## Design decision: three marts, not one merged model

AdventureWorks sells to B2B resellers, Olist sells to Brazilian e-commerce
consumers, and DataCo serves a separate, unrelated global consumer base. These
are not the same customers or products, and merging them into one fake
"unified" customer/product dimension would misrepresent the data. Instead,
each source gets its own complete star schema (dimensions + fact table), and
all three share only what is genuinely universal: a single `dim_date`
calendar dimension. This mirrors how real companies handle data from
acquisitions or unrelated business units — separate, honest analysis per
source, with shared infrastructure where it actually makes sense.

## The three sources

| Source | Style | Tables loaded | Order-line rows |
|---|---|---|---|
| AdventureWorks | SQL Server export (tab-delimited) | 7 | 57,851 |
| Olist Brazilian E-Commerce | ERP/CSV export (comma-delimited) | 9 | 112,650 |
| DataCo Smart Supply Chain | Reshaped to Oracle-style export (`ALL_CAPS` columns, `DD-MON-YY` dates) | 1 | 180,519 |

The DataCo file is a real Kaggle dataset programmatically reshaped (column
names and date format only — no underlying data changed) to mimic a legacy
Oracle export, since a genuine Oracle-source dataset with this scope wasn't
publicly available.

## Pipeline stages

1. **Ingestion** — raw files landed in S3 (`raw/<source>/`), untouched.
2. **Raw load** — loaded into Snowflake via `COPY INTO`, one raw table per
   source file, no transformation.
3. **Staging (dbt)** — one model per raw table: standardizes column names to
   snake_case, casts to correct types, and fixes real data quality issues
   found in the raw files (see below).
4. **Dimensional modeling (dbt)** — three independent star schemas
   (dimension + fact tables), plus one shared `dim_date` calendar dimension
   spanning 1,978 days (Jan 2015–May 2020, covering all three sources).
5. **Testing & reconciliation (dbt)** — 18 automated tests (uniqueness,
   not-null, referential integrity) plus a custom reconciliation model.
6. **Incremental loading** — `fact_orders_aw` is built as an incremental
   model, only processing new rows on repeat runs instead of reprocessing the
   full table.

## Real data quality issues found and fixed

- **Currency stored as text**: AdventureWorks money fields arrived as
  `"$2,024.99"` — stripped and cast to `NUMBER(18,2)` in staging.
- **Spelled-out dates**: `"Friday, August 25, 2017"` parsed into real `DATE`
  values using Snowflake's lenient format matching.
- **Wrong delimiter assumptions**: AdventureWorks files are tab-delimited,
  not comma-delimited, despite the `.csv` extension — caught by inspecting
  raw file content directly rather than trusting the extension.
- **Two-digit year bug**: the DataCo Oracle-style reshaping introduced a
  genuine bug — Snowflake's date inference interpreted a 2-digit year (`18`)
  as year 18 AD instead of 2018. Caught by checking actual output data after
  a "successful" load, not just trusting that the load ran without error.
  Fixed with `DATEADD(year, 2000, ...)` in staging.

## Reconciliation results

A custom model compares row counts between each raw source table and its
corresponding final fact table:

| Source | Raw rows | Fact table rows | Status |
|---|---|---|---|
| AdventureWorks | 57,851 | 57,851 | MATCH |
| Olist | 112,650 | 112,650 | MATCH |
| DataCo | 180,519 | 180,519 | MATCH |

Zero rows lost or duplicated across 351,020 combined order-line records
flowing through staging and dimensional modeling.

## Tech stack

- **Storage**: AWS S3
- **Warehouse**: Snowflake
- **Transformation**: dbt (dbt-core + dbt-snowflake)
- **Testing**: dbt schema tests + custom reconciliation model
- **Documentation**: dbt docs (auto-generated lineage graph)

## Project structure

```
models/
├── staging/
│   ├── adventureworks/   (7 models + sources.yml)
│   ├── olist/             (9 models + sources.yml)
│   └── dataco/            (1 model + sources.yml)
└── marts/
    ├── dim_date.sql
    ├── dim_customer_aw.sql / dim_product_aw.sql / fact_orders_aw.sql
    ├── dim_customer_olist.sql / dim_product_olist.sql / fact_orders_olist.sql
    ├── dim_customer_dataco.sql / dim_product_dataco.sql / fact_orders_dataco.sql
    ├── reconciliation_check.sql
    └── _marts_schema.yml
```

## Running this project

1. Load the three raw datasets into S3 under `raw/<source_name>/`.
2. Configure `~/.dbt/profiles.yml` with your Snowflake credentials.
3. `dbt run` — builds all 28 models (17 staging + 11 marts).
4. `dbt test` — runs all 18 data tests.
5. `dbt docs generate && dbt docs serve` — view the interactive lineage graph.
