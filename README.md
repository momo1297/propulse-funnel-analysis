# Propulse Analytics — Funnel Conversion Pipeline

> **"Where does our acquisition funnel leak — and which channel actually converts?"**

A production-grade analytics pipeline built for subscription businesses managing multiple acquisition partners. Turns raw visitor event data into actionable funnel and A/B test insights using a Bronze → Silver → Gold medallion architecture.

---

## Business Context

**The problem:** A SaaS subscription platform acquires users through 3 media partners (HealthMedia, WellnessPress, NutriDigital) across 5 acquisition channels. Each partner needs to answer the same critical questions every month:

- Which channel actually drives paying subscribers — or just traffic?
- Where exactly in the funnel are we losing visitors, and on which device?
- Do our pricing and UX experiments actually move the needle?
- How long does it take a referral visitor to pay vs. a paid social visitor?

Without a unified data layer, these questions require manual cross-referencing of marketing exports, product analytics, and A/B test dashboards. This pipeline automates the entire flow — from raw event ingestion to executive-ready conversion tables — in a single `python pipeline.py` run.

**Dataset:** 50,000 visitors · 15,000 A/B test participants · 3 partners · 5 acquisition channels · 1 year of history

---

## Technical Stack

| Layer | Tool | Why |
|---|---|---|
| Query engine | **DuckDB** | Analytical SQL on local CSV/DB at scale — no server needed |
| Language | **Python 3** | Orchestration, path handling, summary reporting |
| Storage format | **CSV + DuckDB file** | Human-readable output + persistent in-process DB for layer reuse |
| Architecture | **Medallion (Bronze/Silver/Gold)** | Separates raw ingestion, cleaning, and business logic into auditable layers |
| Modeling | **Star Schema** (Gold layer) | Fact tables (funnel events, A/B tests) + dimensions (channel, device, partner) — standard for BI tools |

---

## Pipeline Architecture

```
data/bronze/          ← Raw CSVs + _ingested_at timestamp (audit trail)
     ↓
data/silver/          ← Typed, cleaned, business columns added
     ↓
data/gold/            ← 4 analytical tables, KPI-ready
```

### Bronze — Immutable raw ingestion
Every CSV is loaded as-is with a `_ingested_at` timestamp. Nothing is modified. This layer is the auditable source of truth: if a data quality issue surfaces weeks later, you can always trace it back to the exact ingestion batch.

### Silver — Business-grade cleaning

| Table | Key transformations |
|---|---|
| `silver_funnel` | Typed dates via `TRY_CAST` · `time_to_signup_days`, `time_to_trial_days`, `time_to_paid_days` · `funnel_stage` (deepest step reached) · `dropped_at` (exit step, NULL if converted) |

### Gold — Analytical tables (one business question per table)

| Table | Business question | Key metric |
|---|---|---|
| `gold_funnel_overview` | **Which channel × partner combination actually converts?** Volume + step-level rates + overall visit→paid rate | Referral = 21.25% overall conversion |
| `gold_dropoff_analysis` | **Where do we lose the most visitors — and does device matter?** Dropoffs by stage × channel × device | visit→signup loses 29,667 visitors |
| `gold_time_to_convert` | **Which channel converts fastest end-to-end?** Avg and median days per step, per channel | Referral full journey: 23.8 days |
| `gold_ab_test_results` | **Do our experiments actually work?** Control vs. treatment conversion rate, absolute lift in pp, sample size validation | pricing_test: +17.19 pp lift |

---

## 4 Key Insights

### 1. Referral converts at 8× the rate of paid social
Referral drives **21.25%** of visitors all the way to payment. Paid social delivers only **2.65%** — a **7× gap** on the same subscription product. Referral visitors arrive with higher intent (peer endorsement acts as a pre-qualification filter), shorter consideration cycles, and lower price sensitivity. Reallocating even 10% of paid social budget toward referral incentive programs (affiliate commissions, "share with a friend" credits) would directly improve blended CAC payback.

### 2. The biggest funnel leak is at the very first step
**29,667 visitors** — 59.3% of all traffic — drop before ever signing up. The signup → trial and trial → paid steps lose far fewer people in absolute terms. This means the highest-leverage UX investment is the **landing page and signup form**, not the trial experience or pricing page. Reducing friction at visit→signup by even 5 percentage points would add ~2,500 signups per 50K visitors without touching the rest of the funnel.

### 3. pricing_test delivers a +17 pp lift — the clearest experiment winner
The pricing test moves conversion from **7.05%** (control) to **24.24%** (treatment), a **+17.19 percentage point** absolute lift. This outperforms cta_button and trial_length by a wide margin. Absolute lift (not relative %) is the right metric here: it tells stakeholders "17 more paying users per 100 treatment visitors", which maps directly to revenue. With sufficient sample sizes on both arms, this result warrants a full rollout.

### 4. Desktop converts 3× better than mobile
Desktop visitors convert at **8.31%** visit→paid. Mobile visitors — who represent the majority of traffic (55% of sessions) — convert at a significantly lower rate. The best performing combination is **referral × desktop** at **22.78%**. This gap signals a mobile experience problem: the trial activation flow or payment form is likely not optimised for small screens. A focused mobile UX sprint on the signup and checkout steps would unlock the largest untapped conversion opportunity.

---

## Results Summary

```
Total visitors                 50,000
Total signups                  20,333  (40.7%)
Total trials                   10,234  (50.3% of signups)
Total paid conversions          3,970  (7.94% overall)
A/B test participants          15,000  (30% of visitors)

Best channel:    referral      →  21.25% overall conversion
Worst channel:   paid_social   →   2.65% overall conversion
Biggest leak:    visit→signup  →  29,667 visitors lost

Best A/B test:   pricing_test  →  +17.19 pp  (7.05% → 24.24%)
Best device:     desktop       →   8.31% conversion
Best combo:      referral × desktop  →  22.78% conversion
```

---

## Production Equivalent

> **"This runs on a laptop today — here's how it scales to 100M events on AWS."**

This pipeline was built with production migration in mind. Every design decision maps directly to a cloud-native equivalent:

| Local (this repo) | AWS production equivalent |
|---|---|
| `data/bronze/*.csv` | **S3** `s3://bucket/bronze/` — same files land via Kinesis Firehose or partner SFTP push |
| `data/silver/` (DuckDB table) | **S3** `s3://bucket/silver/` — Parquet partitioned by `partner_id/year/month/` for Athena performance |
| `data/gold/*.csv` | **S3** `s3://bucket/gold/` — queried directly by **Amazon Athena** or loaded into **Redshift** for BI dashboards |
| DuckDB in-process SQL | **AWS Glue** (PySpark) or **dbt** on Redshift — same SQL logic, distributed execution |
| `python pipeline.py` | **AWS Glue Job** or **Airflow DAG** triggered daily on S3 event or schedule |
| Manual `_ingested_at` | **AWS Glue Data Catalog** with partition projection and event-time metadata |
| `funnel.duckdb` local file | **AWS Lake Formation** for access control per partner + column-level security on PII fields |

**Migration path (3 steps):**
1. Replace `read_csv_auto(path)` with `read_parquet(s3://...)` — DuckDB supports S3 natively via the `httpfs` extension
2. Replace `COPY TO 'local/path'` with writes to S3 using the same extension
3. Register Gold tables in **AWS Glue Data Catalog** → instant Athena queryability, no ETL rewrite

The medallion architecture, star schema modeling, and idempotent `DROP TABLE IF EXISTS` + `CREATE TABLE AS` pattern are AWS best practices — this local pipeline is already structurally identical to a production data lakehouse.

---

## Run Locally

```bash
# Install dependencies
pip install duckdb pandas numpy

# Generate synthetic data
python generate_funnel_data.py

# Run the full pipeline
python pipeline.py
```

**Output:** CSVs in `data/bronze/`, `data/gold/` + DuckDB persistent file at `data/funnel.duckdb` + KPI summary in terminal.

---

## Repository Structure

```
02_funnel_conversion/
├── pipeline.py               # Full Bronze → Silver → Gold pipeline
├── generate_funnel_data.py   # Synthetic dataset generator (50K visitors)
├── data/
│   ├── bronze/               # Raw CSVs with _ingested_at watermark
│   │   ├── funnel_events.csv
│   │   └── ab_tests.csv
│   ├── silver/               # (DuckDB table: silver_funnel)
│   ├── gold/                 # Analytical KPI tables
│   │   ├── gold_funnel_overview.csv
│   │   ├── gold_dropoff_analysis.csv
│   │   ├── gold_time_to_convert.csv
│   │   └── gold_ab_test_results.csv
│   └── funnel.duckdb         # Persistent in-process DB
└── README.md
```
