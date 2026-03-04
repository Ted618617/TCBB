# TCBB
# TCBB_Ted
# Bank Data Integration & Quality Pipeline (Postgres) — Demo Project
# Demo Result ： 2026-03-03 17:49:39+0000 | success | rows_raw=100000 | 83aac446-0cd3-44ad-b355-cca24e4224c8

A small but production-minded data pipeline demo for a bank-like transaction domain:
- Raw → Staging → Mart layering
- Data Quality rules + Quarantine isolation
- Incremental processing via watermark
- Run monitoring dashboard (Streamlit)

## Why this project
Bank data integration often involves:
- multiple source systems (ATM / Internet Banking / Card / Branch)
- inconsistent data quality (duplicates, missing FK, invalid currency, abnormal amount, time anomalies)
- strict auditability and traceability requirements

This project demonstrates an end-to-end pipeline that is **traceable**, **auditable**, and **operationally observable**.

---

## Architecture (high-level)

Raw ingestion (Postgres)
→ Staging standardization (schema normalization, currency standardization, basic cleanup)
→ Data Quality checks (rule-based)
→ Quarantine (bad rows isolated)
→ Mart Fact/Dim (clean rows upserted)
→ Monitoring Dashboard (Run Log / DQ / KPI)

Schemas:
- `raw`  : immutable ingestion tables
- `stg`  : standardized/cleaned tables for processing
- `mart` : dimensions + fact tables (analytics-ready)
- `meta` : pipeline metadata (run log, watermark state, dq results, quarantine)

---

## Tech Stack
- Python 3.11+
- PostgreSQL 16 (Docker)
- SQLAlchemy + psycopg2
- pandas / numpy
- Streamlit dashboard

---

## Quick Start

1) Start Postgres
docker compose up -d

2) Create schemas & tables
# PowerShell: use cmd redirection
cmd /c "docker exec -i bank_pg psql -U bank -d bank_demo < schema_postgres.sql"

3) Set env
# Create .env:
DATABASE_URL=postgresql+psycopg2://bank:bankpass@localhost:5432/bank_demo

4) Generate raw data (with dirty injection)
python generate_data.py --reset --n-transactions 100000

5) Run ETL (full)
python analyze_data.py --mode full --reset-state

6) Run dashboard
streamlit run dashboard.py

# 【Incremental demo (watermark)】
# generate new transactions in a newer time window than watermark
python generate_data.py --n-transactions 20000 --start-date 2026-04-02 --months 1
python analyze_data.py --mode incremental

# 【Data Quality Rules (examples)】

	R001 Required fields not null

	R002 Amount within valid range

	R003 Currency in allowlist

	R004 Transaction time not in future

	R005 Account FK exists

	R006 Channel FK exists

	R007 Merchant FK exists for card purchase

	R008 No duplicate txn_id + source_system in batch

PS：Bad rows are written to meta.quarantine_transactions and excluded from Mart Fact.

# 【Demo Result (example run)】

	raw rows: 100,000

	quarantine rows: 2,811

	mart fact upserted: 97,189

	watermark recorded in meta.etl_state

	run logs stored in meta.etl_run_log
	
# 【Notes / Design choices】

	Raw layer is kept append-only for auditability.

	Dimensions are upserted with de-duplication (latest ingestion_time wins).

	Fact table uses upsert (idempotent reruns).

	Dashboard provides operational visibility (duration, pass rate, run_id traceability).

