from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable
from datetime import datetime, timedelta

from services.minio_client import MinioHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 14),
    'retries': 0,
}

with DAG(
        'db_init_dag',
        default_args=default_args,
        schedule=None,
        catchup=False,
) as dag:

    def init_postgres():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Models
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS models_ods (
            id TEXT,
            owner TEXT,
            author TEXT,
            created_at TIMESTAMP WITH TIME ZONE,
            downloads BIGINT,
            likes INTEGER,
            library_name TEXT,
            pipeline_tag TEXT,
            trending_score DOUBLE PRECISION,
            language TEXT[],
            library TEXT[],
            task TEXT[],
            license TEXT,
            base_models TEXT[],
            modification TEXT,
            region TEXT,
            diffusers_pipeline TEXT,
            deploy TEXT[],
            dataset TEXT[],
            arxiv TEXT[],
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)

        # Papers
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS papers_ods (
            id TEXT,
            authors TEXT[],
            published_at TIMESTAMP,
            title TEXT,
            summary TEXT,
            upvotes INTEGER,
            discussion_id TEXT,
            source TEXT,
            comments INTEGER,
            submitted_at TIMESTAMP,
            submitted_by TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # Posts
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts_ods (
            slug TEXT,
            author_name TEXT,
            author_id TEXT,
            content_raw TEXT,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            total_unique_impressions INTEGER,
            num_comments INTEGER,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
        """)

        conn.commit()
        cursor.close()


    def init_clickhouse():
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_hf_conn')
        conn = ch_hook.get_conn()

        # models
        conn.execute("""
        CREATE TABLE IF NOT EXISTS models_ods (
            id String,
            owner Nullable(String),
            author Nullable(String),
            created_at DateTime,
            downloads UInt64,
            likes UInt32,
            library_name Nullable(String),
            pipeline_tag Nullable(String),
            trending_score Float64,
            language Array(String),
            library Array(String),
            task Array(String),
            license Nullable(String),
            base_models Array(String),
            modification Nullable(String),
            region Nullable(String),
            diffusers_pipeline Nullable(String),
            deploy Array(String),
            dataset Array(String),
            arxiv Array(String),
            loaded_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY loaded_at;
        """)

        # papers
        conn.execute("""
        CREATE TABLE IF NOT EXISTS papers_ods (
            id String,
            authors Array(String),
            published_at DateTime,
            title Nullable(String),
            summary Nullable(String),
            upvotes UInt32,
            discussion_id Nullable(String),
            source Nullable(String),
            comments UInt32,
            submitted_at DateTime,
            submitted_by Nullable(String),
            loaded_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY loaded_at;
        """)

        # posts
        conn.execute("""
        CREATE TABLE IF NOT EXISTS posts_ods (
            slug String,
            author_name Nullable(String),
            author_id Nullable(String),
            content_raw Nullable(String),
            published_at DateTime,
            updated_at DateTime,
            total_unique_impressions UInt32,
            num_comments UInt32,
            loaded_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY loaded_at;
        """)


    def init_mongodb():
        mongo_hook = MongoHook(mongo_conn_id='mongo_hf_conn')
        client = mongo_hook.get_conn()
        db = client['huggingface']
        for coll_name in ['models_ods', 'papers_ods', 'posts_ods']:
                    if coll_name not in db.list_collection_names():
                        db.create_collection(coll_name)


    def init_minio():
        hook = MinioHook(minio_conn_id='minio_hf_conn')
        client = hook.get_conn()
        bucket_name = Variable.get("minio_bucket", default_var="etl-data")

        try:
            client.head_bucket(Bucket=bucket_name)
        except Exception as e:
             client.create_bucket(Bucket=bucket_name)


    create_postgres = PythonOperator(task_id='init_postgres', python_callable=init_postgres)
    create_clickhouse = PythonOperator(task_id='init_clickhouse', python_callable=init_clickhouse)
    create_mongo = PythonOperator(task_id='init_mongodb', python_callable=init_mongodb)
    create_minio = PythonOperator(task_id='init_minio', python_callable=init_minio)

    [create_postgres, create_clickhouse, create_mongo, create_minio]
