FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN python -m venv /opt/dbt \
    && /opt/dbt/bin/pip install --no-cache-dir dbt-core==1.8.9 dbt-postgres==1.8.2
ENV DBT_BIN=/opt/dbt/bin/dbt

USER airflow
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-pipeline.txt
