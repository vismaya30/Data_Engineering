# NASA HTTP Log Analytics — ETL Pipeline

End-to-end data engineering pipeline processing 3.5 million HTTP requests 
from NASA Kennedy Space Center web server logs (1995).

## Tech Stack
- Apache Airflow — orchestration
- Amazon S3 — raw data lake
- Amazon RDS (PostgreSQL) — data warehouse
- Python, pandas — transformation
- Streamlit, Plotly — dashboard
- Docker — containerization

## Pipeline
Raw Logs → S3 → Parse & Transform → RDS → Streamlit Dashboard

## Dataset
NASA Kennedy Space Center HTTP logs, Jul–Aug 1995, ~3.5M requests.
Source: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

## How to Run
1. Upload .gz files to S3
2. Run schema.sql on RDS
3. Start Airflow: docker-compose up
4. Trigger DAG with {"month": "Jul"}
5. Run dashboard: streamlit run dashboard/dashboard.py
