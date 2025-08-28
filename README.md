# SaaS Analytics Pipeline

## 🚀 Project Overview

This repo contains a **production-style analytics engineering warehouse** for a fictional SaaS/Marketplace company. All data is **synthetic but realistic** — generated to mimic real-world scale, edge cases, and dirty data.  
The pipeline is built solo to simulate the experience of a *founding Analytics Engineer*, covering the entire modern stack:  
- **Data generation** with messy edge cases (late data, schema drift, skew, PII, etc)
- **End-to-end warehouse modeling**: raw → staging → core → marts (with dbt)
- **Business logic**: revenue, funnel, operations, attribution marts
- **Orchestration & CI/CD**: dbt Cloud jobs (or Airflow/Prefect), GitHub Actions for tests/deploy
- **Data quality**: custom dbt tests for edge-case resilience
- **Performance tuning**: partitioning, clustering, and cost-awareness

---

## 🏗️ Folder Structure

/data_gen/ # Scripts to generate synthetic raw data (CSV, Parquet, JSON)
generate.py
rules.yaml
seeds/
iso_countries.csv
fx_rates.csv
/raw/ # Generated raw data partitions, by source and date
/dbt/ # dbt project (models, seeds, tests, docs)
/orchestration/ # Orchestration configs/scripts (dbt Cloud, Prefect, Airflow)
/.github/workflows/ # GitHub Actions for CI/CD


---

## 🗺️ Project Kanban (Backlog & Progress)

| Sprint | Epic                           | Tickets/Features                                           | Status   |
|--------|--------------------------------|------------------------------------------------------------|----------|
| 1      | Data Gen + Raw/Staging         | Synthetic data gen, raw models, staging, rejects           | [ ] TODO |
| 2      | Core Models + DQ Tests         | SCD2 dims, facts, custom dbt tests                         | [ ] TODO |
| 3      | Marts + Orchestration          | Revenue/funnel marts, orchestration, monitoring            | [ ] TODO |
| 4      | CI/CD + Perf                   | GitHub Actions, perf tuning, rollups                       | [ ] TODO |
| 5      | (Stretch) API + Attribution    | API ingestion, marketing marts, anomaly detection          | [ ] TODO |
| 6      | (Polish) Docs + Walkthrough    | Blog/Medium writeup, Loom video, data gen packaging        | [ ] TODO |

---

## 📊 ERD & Architecture

- *Add ERD and stack diagram here once models are built.*
- *dbt lineage screenshot after Sprint 2/3.*

---

## 🧪 Data Quality & Testing

- dbt generic + custom tests:
    - No negative unit_price/quantity
    - Payment reconciliation (orders ≈ payments)
    - Late-arrival % < threshold
    - SCD2 window overlap prevention
    - PII leakage checks in JSON

---

## ⚙️ Orchestration & CI/CD

- dbt Cloud jobs / Prefect / Airflow (choose one)
- GitHub Actions: PR (compile + test), Main (build + docs)
- Freshness checks and recon alerting

---

## 🏆 Learning Outcomes

- Build, document, and ship a production-style analytics warehouse from scratch
- Simulate “founding AE” responsibilities: data gen, modeling, ops, testing, perf
- Stress-test models with dirty data and edge cases

---

## 📋 Setup & Usage

1. Clone repo  
2. Run `/data_gen/generate.py --day YYYY-MM-DD` to emit raw partitions  
3. Configure warehouse & dbt profile in `/dbt`  
4. Run `dbt run` and `dbt test`  
5. (Optional) Set up orchestrator for scheduled builds  
6. (Optional) PR for CI/CD run

## Version Update:

Will be using BigQuery instead of Snowflake for this project to design partitioning and clustering 
with pedagogical clarity + a mental model transferrable to any stack in future.

---

## 🙋‍♀️ Author

[Rachael Ogungbose](https://github.com/ray-dataworks1) — aspiring Analytics Engineer / Data Engineer

---
