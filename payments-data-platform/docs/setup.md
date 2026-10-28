# Setup Guide

## Prerequisites

- Python 3.11+
- Docker Desktop
- Java 17 (for PySpark)
- AWS account + AWS CLI
- Git

## 1. Local Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## 2. AWS Resources

1. **S3 bucket**, e.g. `payments-dp-<yourname>` (region `eu-central-1`)
2. **RDS PostgreSQL**: smallest free-tier instance. Allow inbound access from your IP only.
3. **IAM user** for the pipeline with least-privilege access (S3 on this bucket only)
4. Put the credentials in `.env`. **Never commit `.env`.**

> 💸 **Cost tip:** Stop the RDS instance when you're not using it, and set an AWS budget alert.

## 3. Airflow (Docker)

- Use the official `docker-compose.yaml` from the Apache Airflow docs ("Running Airflow in Docker").
- Mount `./dags` and `./src` into the containers.
- Run `docker compose up airflow-init`, then `docker compose up -d`.
- The UI is at `http://localhost:8080`.

## 4. dbt

```bash
cd dbt
dbt debug      # check connection
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## 5. Dashboard

```bash
streamlit run dashboard/app.py
```

## 6. Tests

```bash
ruff check .
pytest -v
```
