[![Airflow](https://img.shields.io/badge/Orchestration-Apache_Airflow_2.x-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Containerization-Docker_&_Compose-2496ED?logo=docker)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/Storage-PostgreSQL-316192?logo=postgresql)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Language-Python_3.10+-3776AB?logo=python)](https://www.python.org/)

---

# 🏗️ Immo Eliza  
### Orchestrated Real Estate Data Pipeline for Belgian Market Analytics

**Immo Eliza** is a containerized, Airflow-orchestrated data pipeline designed to extract, validate, and structure real estate market data from Belgian property platforms.  
The system implements a modular ETL architecture with PostgreSQL-backed storage and a layered data model to support analytics and machine learning workflows.

## 📌 Project Scope

This project implements a **scheduled, reproducible ETL pipeline** for real estate market data ingestion and processing, fully containerized with **Docker** and orchestrated with **Apache Airflow**.

The platform is designed to:

- Collect structured and semi-structured property data  
- Ingest property listings for sale across **all 11 Belgian provinces**  
- Crawl the **first 50 result pages per province**  
- Support incremental ingestion through scheduled execution  
- Update datasets **automatically every Friday at 00:00**  
- Implement layered data organization  
- Produce ML-ready datasets  
- Provide operational observability via Apache Airflow  
- Ensure deterministic and reproducible pipeline runs  

The pipeline is designed as a **data platform foundation**, not a single-use scraper, and is executed automatically on a scheduled basis.



---

## 🏛️ Architecture Overview

Immo Eliza is composed of the following core components:

- **Orchestration:** Apache Airflow 2.x  
- **Compute:** Python-based processing services  
- **Metadata:** PostgreSQL (Airflow backend only)  
- **Storage:** Local file-based data lake structure  

PostgreSQL is used exclusively for Airflow metadata and workflow state management.  
All pipeline datasets are persisted in the local filesystem.


---

## 🗂️ Data Layers

| Layer | Purpose | Location |
|------|--------|----------|
| Bronze | Raw scraped data | `data/raw/` |
| Silver | Cleaned datasets | `data/processed/` |
| Metadata | URL discovery | `data/urls/` |

---

## 📐 Design Principles

- Modular task structure  
- Deterministic execution  
- Idempotent processing  
- Reproducible deployment  

## 📂 Repository Structure

```bash
immo-eliza-airflow-ml/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env
├── dags/                      # Airflow DAG definitions
│   └── immo_eliza_etl_dag.py
├── scripts/                   # ETL logic
│   ├── url_collector.py
│   ├── property_scraper.py
│   └── data_cleaner.py
├── data/                      # Data lake structure
│   ├── raw/                   # Raw scraped data(Bronze)
│   ├── processed/             # Cleaned data(Silver)
│   └── urls/                  # URL discovery metadata
├── logs/                      # Airflow logs
├── plugins/                   # Airflow plugins
├── tests/                     # Unit tests
│   ├── test_clean_urls.py
│   └── test_local_scraper.py
└── README.md
```
## 🔄 ETL Pipeline

The pipeline consists of three logical stages:

1. **URL Discovery (`collect_urls`)**  
2. **Property Extraction (`scrape_properties`)**  
3. **Data Transformation (`clean_data`)**
---

## 📁 Data Flow

```text
Source Website(https://immovlan.be/en/)
   ↓
data/urls/property_urls.csv
   ↓
data/raw/properties_raw.csv
   ↓
data/processed/properties_cleaned.csv
```
## 🚀 Deployment

### Environment Setup

```bash
# Set Airflow user ID
echo "AIRFLOW_UID=$(id -u)" > .env

# Create required directories
mkdir -p dags logs plugins data/raw data/processed data/urls
```
### Build & Start
```bash
docker-compose build
docker-compose up airflow-init
docker-compose up -d

```

## 🌐 Access Airflow

- URL: `http://localhost:8080`  
- Authentication is managed by the Airflow configuration


## ⚙️ Configuration

Runtime parameters are managed via the `.env` file:

```env 

MAX_PAGES_PER_PROVINCE=50
SCRAPING_DELAY_MIN=0.1
SCRAPING_DELAY_MAX=0.25
```

---

## 📝 Monitoring


- Pipeline execution and task state are monitored via the Airflow UI  
- Logs are accessible through Airflow or Docker:

```bash
docker-compose logs -f
```
## 🛠️ Troubleshooting

- Check container status and logs:
```bash
docker-compose ps
docker-compose logs
```
## 🧭 Roadmap

- Database-backed storage  
- Structured data validation  
- Incremental ingestion optimization  
- Monitoring and alerting  

This project was developed as part of the **AI & Data Science Bootcamp**, specializing in **Data Engineering**, at **`</becode>`**.

**Author:**  
- Welederufeal Tadege [LinkedIn](https://www.linkedin.com/in/) | [Github](https://github.com/welde2001-bot) 

**Supervision:**  
- Vanessa Rivera Quinones — AI & Data Science Coach