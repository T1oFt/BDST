from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable
from datetime import datetime

from services.minio_client import MinioHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 14),
    'retries': 0,
}

with DAG(
        'db_reset_dag',
        default_args=default_args,
        schedule=None,
        catchup=False,
) as dag:

    def reset_postgres_ods():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS ods.models CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS ods.papers CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS ods.posts CASCADE;")
        cursor.execute("DROP SCHEMA IF EXISTS ods CASCADE;")
        
        conn.commit()
        cursor.close()

    def reset_postgres_dds():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Сначала дропаем типы (после таблиц)
        cursor.execute('DROP TYPE IF EXISTS "dds.party_type" CASCADE;')
        
        # Дропаем все DDS таблицы через схему
        cursor.execute("""
        DROP TABLE IF EXISTS dds.bridge_models_languages CASCADE;
        DROP TABLE IF EXISTS dds.bridge_models_libraries CASCADE;
        DROP TABLE IF EXISTS dds.bridge_models_tasks CASCADE;
        DROP TABLE IF EXISTS dds.bridge_models_datasets CASCADE;
        DROP TABLE IF EXISTS dds.bridge_models_papers CASCADE;
        DROP TABLE IF EXISTS dds.bridge_papers_keywords CASCADE;
        DROP TABLE IF EXISTS dds.bridge_papers_authors CASCADE;
        DROP TABLE IF EXISTS dds.dim_libraries CASCADE;
        DROP TABLE IF EXISTS dds.dim_datasets CASCADE;
        DROP TABLE IF EXISTS dds.dim_languages CASCADE;
        DROP TABLE IF EXISTS dds.dim_tasks CASCADE;
        DROP TABLE IF EXISTS dds.dim_authors CASCADE;
        DROP TABLE IF EXISTS dds.dim_keywords CASCADE;
        DROP TABLE IF EXISTS dds.dim_reactions CASCADE;
        DROP TABLE IF EXISTS dds.dim_models CASCADE;
        DROP TABLE IF EXISTS dds.dim_papers CASCADE;
        DROP TABLE IF EXISTS dds.dim_posts CASCADE;
        DROP TABLE IF EXISTS dds.parties CASCADE;
        DROP TABLE IF EXISTS dds.dim_date CASCADE;
        DROP TABLE IF EXISTS dds.dim_time CASCADE;
        DROP TABLE IF EXISTS dds.fact_models_metrics CASCADE;
        DROP TABLE IF EXISTS dds.fact_posts_metrics CASCADE;
        DROP TABLE IF EXISTS dds.fact_posts_reactions CASCADE;
        DROP SCHEMA IF EXISTS dds CASCADE;
        """)
        
        conn.commit()
        cursor.close()

    def reset_clickhouse():
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_hf_conn')
        conn = ch_hook.get_conn()
        
        conn.execute("DROP TABLE IF EXISTS models_ods;")
        conn.execute("DROP TABLE IF EXISTS papers_ods;")
        conn.execute("DROP TABLE IF EXISTS posts_ods;")

    def reset_mongodb():
        mongo_hook = MongoHook(mongo_conn_id='mongo_hf_conn')
        client = mongo_hook.get_conn()
        db = client['huggingface']
        
        for coll_name in ['models_ods', 'papers_ods', 'posts_ods']:
            db[coll_name].drop()

    def reset_minio():
        hook = MinioHook(minio_conn_id='minio_hf_conn')
        client = hook.get_conn()
        bucket_name = Variable.get("minio_bucket", default_var="etl-data")
        
        try:
            buckets = client.list_buckets()
            for bucket in buckets:
                if bucket.name == bucket_name:
                    # Удаляем все объекты в бакете
                    objects = client.list_objects(bucket_name, recursive=True)
                    for obj in objects:
                        client.remove_object(bucket_name, obj.object_name)
                    break
        except Exception:
            pass  # Бакет не существует - ничего не делаем

    # Создание задач
    reset_postgres_ods_task = PythonOperator(task_id='reset_postgres_ods', python_callable=reset_postgres_ods)
    reset_postgres_dds_task = PythonOperator(task_id='reset_postgres_dds', python_callable=reset_postgres_dds)
    reset_clickhouse_task = PythonOperator(task_id='reset_clickhouse', python_callable=reset_clickhouse)
    reset_mongodb_task = PythonOperator(task_id='reset_mongodb', python_callable=reset_mongodb)
    reset_minio_task = PythonOperator(task_id='reset_minio', python_callable=reset_minio)

    # Параллельное выполнение всех reset задач
    [reset_postgres_ods_task, reset_postgres_dds_task, reset_clickhouse_task, 
     reset_mongodb_task, reset_minio_task]
