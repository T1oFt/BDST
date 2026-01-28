from datetime import datetime, timedelta, timezone
import logging

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from services.dds.papers import (
    PapersDataExtractor,
    PapersDataTransformer,
    PapersDataLoader,
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='papers_dds',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:


    def dds_load_fn(dag_run, **_):
        loaded_at_iso = dag_run.conf.get("loaded_at")
        if not loaded_at_iso:
            raise ValueError('papers_etl_loaded_at is not set in XCom')

        loaded_at = datetime.fromisoformat(loaded_at_iso)

        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')

        extractor = PapersDataExtractor(pg_hook=pg_hook)
        df = extractor.get_papers_by_loaded_at(
            loaded_at.strftime('%Y-%m-%d %H:%M:%S.%f')
        )

        if df.empty:
            logging.warning(f"No papers found for {loaded_at.isoformat()}")
            return

        logging.info(f"Found {len(df)} papers for {loaded_at.isoformat()}")

        transformer = PapersDataTransformer()
        (
            dim_papers_raw,
            unique_dates,
            unique_times,
            unique_authors,
            unique_keywords,
            unique_parties,
            source_authors,
            source_keywords,
        ) = transformer.transform_papers_df(df)

        logging.info(f"Transformed {len(dim_papers_raw)} papers")

        loader = PapersDataLoader(postgres_conn_id='pg_hf_conn')
        loader.load_all(
            dim_papers_raw=dim_papers_raw,
            unique_dates=unique_dates,
            unique_times=unique_times,
            unique_authors=unique_authors,
            unique_keywords=unique_keywords,
            unique_parties=unique_parties,
            source_authors=source_authors,
            source_keywords=source_keywords,
            loaded_at=loaded_at,
        )

        logging.info(f"Loaded {len(dim_papers_raw)} papers")

    dds_load_task = PythonOperator(
        task_id='dds_load_task',
        python_callable=dds_load_fn,
    )

    dds_load_task
