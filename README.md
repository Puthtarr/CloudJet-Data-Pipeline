# ✈️ CloudJet Data Pipeline

A hands-on **Data Engineering Portfolio Project** that simulates a real-world pipeline for tracking and analyzing flight data from Bangkok (BKK/DMK) to major Asian cities, combined with weather data to generate insights. The project is fully automated, modular, and runs on free & open-source tools.

---

## 🧱 Tech Stack

| Layer | Tools Used |
|-------|------------|
| **Extract** | Python + Requests + Public APIs |
| **Transform** | PySpark |
| **Storage** | Local Disk (raw), MinIO (Clean Parquet) |
| **Warehouse** | PostgreSQL / DuckDB |
| **Orchestration** | Airflow / Prefect |
| **Dashboard** | Metabase / Looker Studio |
| **DevOps** | Docker Compose |

---

## 🎯 Project Goals

- Extract flight data from [AviationStack API](https://aviationstack.com/)
- Extract weather data from [OpenWeatherMap API](https://openweathermap.org/api)
- Clean & join datasets using PySpark
- Store raw data (JSON) locally, and clean data (Parquet) in MinIO
- Load data to PostgreSQL (or DuckDB) for reporting
- Visualize daily summaries on a dashboard
- Automate everything using DAGs

---

## 🧭 Scope

- Origin: Don Mueang (DMK) / Suvarnabhumi (BKK)
- Destination: Tokyo, Seoul, Singapore, Kuala Lumpur, Taipei, HCMC, Hong Kong
- Daily flight data extraction and weather data from destination cities

---


