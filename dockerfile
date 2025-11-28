FROM apache/airflow:3.1.0

RUN pip install --upgrade pip && \
    pip install requests huggingface_hub boto3 apache-airflow-providers-mongo apache-airflow-providers-postgres airflow-clickhouse-plugin beautifulsoup4