import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from services.dds.posts import (
    PostsDataExtractor,
    PostsDataTransformer,
    PostsDataLoader,
)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='posts_dds',
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
        extractor = PostsDataExtractor(pg_hook=pg_hook)
        df = extractor.get_posts_by_loaded_at(
            loaded_at.strftime('%Y-%m-%d %H:%M:%S.%f')
        )

        if df.empty:
            logging.warning(f"No posts found for {loaded_at.isoformat()}")
            return
        
        logging.info(f"Found {len(df)} posts for {loaded_at.isoformat()}")

        transformer = PostsDataTransformer()
        (
            dim_posts_raw,
            fact_posts_metrics_raw,
            fact_posts_reactions_raw,
            unique_parties,
            unique_dates,
            unique_times,
        ) = transformer.transform_posts_df(df, loaded_at)

        logging.info(f"Transformed {len(dim_posts_raw)} posts")

        loader = PostsDataLoader(postgres_conn_id='pg_hf_conn')
        loader.load_all(
            dim_posts_raw=dim_posts_raw,
            fact_posts_metrics_raw=fact_posts_metrics_raw,
            fact_posts_reactions_raw=fact_posts_reactions_raw,
            unique_parties=unique_parties,
            unique_dates=unique_dates,
            unique_times=unique_times,
            loaded_at=loaded_at,
        )

        logging.info(f"Loaded {len(dim_posts_raw)} posts")

    dds_load_task = PythonOperator(
        task_id='dds_load_task',
        python_callable=dds_load_fn,
    )
