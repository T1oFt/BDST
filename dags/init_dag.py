from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 14),
    'retries': 0,
}

with DAG(
        'db_init_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:

    def init_postgres():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Models
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS models (
            id TEXT PRIMARY KEY,
            owner TEXT,
            author TEXT,
            sha TEXT,
            created_at TIMESTAMP,
            last_modified TIMESTAMP,
            private BOOLEAN,
            disabled BOOLEAN,
            downloads INTEGER,
            downloads_all_time INTEGER,
            likes INTEGER,
            library_name TEXT,
            tags TEXT,
            pipeline_tag TEXT,
            trending_score INTEGER
        );
        """)

        # Papers
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            id TEXT PRIMARY KEY,
            published_at TIMESTAMP,
            title TEXT,
            summary TEXT,
            upvotes INTEGER,
            discussion_id TEXT,
            source TEXT,
            comments INTEGER,
            submitted_at TIMESTAMP,
            submitted_by TEXT
        );
        """)

        # Posts
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            slug TEXT PRIMARY KEY,
            author_name TEXT,
            author_id TEXT,
            content_raw TEXT,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            total_unique_impressions INTEGER,
            num_comments INTEGER
        );
        """)

        conn.commit()
        cursor.close()

    def init_clickhouse():
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_hf_conn')
        
        #models
        ch_hook.run("""
        CREATE TABLE IF NOT EXISTS models (
            id String,
            owner String,
            author String,
            sha String,
            created_at DateTime,
            last_modified DateTime,
            private UInt8,
            disabled UInt8,
            downloads UInt32,
            downloads_all_time UInt32,
            likes UInt32,
            library_name String,
            tags String,
            pipeline_tag String,
            trending_score UInt32
        ) ENGINE = MergeTree()
        ORDER BY id;
        """)
        
        #papers
        ch_hook.run("""
        CREATE TABLE IF NOT EXISTS papers (
            id String,
            published_at DateTime,
            title String,
            summary String,
            upvotes UInt32,
            discussion_id String,
            source String,
            comments UInt32,
            submitted_at DateTime,
            submitted_by String
        ) ENGINE = MergeTree()
        ORDER BY id;
        """)

        #posts
        ch_hook.run("""
        CREATE TABLE IF NOT EXISTS posts (
            slug String,
            author_name String,
            author_id String,
            content_raw String,
            published_at DateTime,
            updated_at DateTime,
            total_unique_impressions UInt32,
            num_comments UInt32
        ) ENGINE = MergeTree()
        ORDER BY slug;
        """)

    def init_mongodb():
        mongo_hook = MongoHook(conn_id='mongo_hf_conn')
        db = mongo_hook.get_db()

        if 'models' not in db.list_collection_names():
            db.create_collection('models')
        db.models.create_index('id', unique=True)

        if 'papers' not in db.list_collection_names():
            db.create_collection('papers')
        db.papers.create_index('id', unique=True)

        if 'posts' not in db.list_collection_names():
            db.create_collection('posts')
        db.posts.create_index('slug', unique=True)

    create_postgres = PythonOperator(task_id='init_postgres', python_callable=init_postgres)
    create_clickhouse = PythonOperator(task_id='init_clickhouse', python_callable=init_clickhouse)
    create_mongo = PythonOperator(task_id='init_mongodb', python_callable=init_mongodb)

    create_postgres >> create_clickhouse >> create_mongo
