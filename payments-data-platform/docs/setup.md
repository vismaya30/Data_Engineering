# Setup

## Prerequisites

- Python 3.11
- Java 17 (needed by PySpark)
- Docker, for PostgreSQL and Airflow
- An AWS account, only if you want the lake on S3

## 1. Local Python environment

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env
```

Check everything works:

```bash
make lint
make test
```

## 2. Start PostgreSQL

```bash
docker compose up -d warehouse
```

This starts PostgreSQL 16 on `localhost:15432` (set by `PG_PORT` in `.env`) with the credentials from `.env`. Port 15432 avoids clashing with a PostgreSQL already installed on the machine.

## 3. Run the pipeline without Airflow

`src/pipeline.py` runs every step for a range of days in one Spark session. Run days in order from `PIPELINE_START_DATE`.

```bash
make run START=2026-10-01 END=2026-10-14
make dbt
```

Data lands under `./data` and the warehouse tables under the `raw`, `staging`, `marts` and `reference` schemas.

To run a single step for a single day:

```bash
python -m src.generator.generate --run-date 2026-10-15
python -m src.transforms.silver --run-date 2026-10-15
python -m src.transforms.reconciliation --run-date 2026-10-15
python -m src.transforms.load_warehouse --run-date 2026-10-15
```

## 4. Dashboard

```bash
make dashboard
```

Opens on http://localhost:8501.

## 5. Airflow

```bash
docker compose up -d --build
```

The image extends `apache/airflow:2.10.5-python3.11` with Java, the pipeline dependencies and a separate dbt install. Airflow runs in standalone mode on http://localhost:8080. The admin password is printed in the container logs:

```bash
docker compose logs airflow | grep -i password
```

Enable the `payments_daily` DAG. With `catchup=True` it backfills from `PIPELINE_START_DATE`, one day at a time.

## 6. Moving the lake to S3

1. Create a bucket, for example `my-payments-lake`, in your region.
2. Give the IAM user or role `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` and `s3:DeleteObject` on it.
3. Set these in `.env`:

   ```
   DATA_ROOT=s3a://my-payments-lake
   AWS_REGION=eu-west-2
   AWS_PROFILE=your-profile
   ```

4. The compose file mounts `~/.aws` read-only into the Airflow container so the default credential chain picks up your profile.

The Spark session pulls `hadoop-aws` automatically when `DATA_ROOT` points at S3.

## 7. Moving the warehouse to RDS

1. Create a PostgreSQL 16 instance and a database called `payments`.
2. Allow inbound 5432 from wherever the pipeline runs.
3. Point `PG_HOST`, `PG_USER` and `PG_PASSWORD` in `.env` at the instance.

The loader creates the `raw` schema and tables on first run. dbt creates the rest.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `JAVA_HOME is not set` | Install Java 17 and export `JAVA_HOME` |
| `PYTHON_VERSION_MISMATCH` from Spark | Set `PYSPARK_PYTHON` to the same interpreter you run the job with |
| `DataQualityError: ... reject ratio` | Look in `data/quarantine/` and `data/reports/data_quality/` for that day |
| Reconciliation reads no settlements | Days must run in order. Rerun from `PIPELINE_START_DATE` |
