import json
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from services.benchmark import BenchmarkModelsLoader
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

DATA_FILE = Path(__file__).parent / "utils" / "res.json"

def parse_datetime_field(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    elif isinstance(value, datetime):
        return value
    else:
        return None

with DAG(
    'benchmark_dbs_performance',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    def run_benchmark_fn(ti):
        try:
            if not DATA_FILE.exists():
                raise FileNotFoundError(f"Data file not found: {DATA_FILE}")

            models = []
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        model = json.loads(line)
                        if 'created_at' in model:
                            model['created_at'] = parse_datetime_field(model['created_at'])
                        models.append(model)
                        if len(models) >= 10000:
                            break
                        
            if not models:
                logging.warning("No models to benchmark")
                return

            loaded_at = datetime.now(timezone.utc)
            temp_suffix = loaded_at.strftime("%Y%m%dT%H%M%S")

            pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
            mongo_hook = MongoHook(mongo_conn_id='mongo_hf_conn')
            clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_hf_conn')

            loader = BenchmarkModelsLoader(pg_hook, mongo_hook, clickhouse_hook, temp_suffix)

            loader.create_temp_tables()
            logging.info(f"Temporary tables created with suffix: {temp_suffix}")

            try:
                pg_time = loader.load_postgresql(models, loaded_at)
                mongo_time = loader.load_mongodb(models, loaded_at)
                ch_time = loader.load_clickhouse(models, loaded_at)

                read_results = loader.run_benchmarks()

                loader.create_dimension_tables()
                join_results = loader.run_join_benchmarks()

                logging.info("=== WRITE PERFORMANCE (10k records) ===")
                logging.info(f"PostgreSQL : {pg_time:.3f}s")
                logging.info(f"MongoDB    : {mongo_time:.3f}s")
                logging.info(f"ClickHouse : {ch_time:.3f}s")

                logging.info("=== READ PERFORMANCE (queries) ===")
                for db, queries in read_results.items():
                    logging.info(f"[{db.upper()}]")
                    for q_name, q_time in queries.items():
                        logging.info(f"  {q_name}: {q_time:.4f}s")

                logging.info("=== READ PERFORMANCE (JOIN queries) ===")
                for db, queries in join_results.items():
                    logging.info(f"[{db.upper()}]")
                    for q_name, q_time in queries.items():
                        logging.info(f"  {q_name}: {q_time:.4f}s")

            finally:
                loader.drop_temp_objects()
                logging.info("Temporary objects cleaned up")

        except Exception as e:
            logging.error(f"Benchmark failed: {str(e)}")
            raise

    benchmark_task = PythonOperator(
        task_id='run_benchmark_task',
        python_callable=run_benchmark_fn,
    )