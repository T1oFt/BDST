from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable
from datetime import datetime, timedelta, date
import pandas as pd

from services.minio_client import MinioHook
from services.categories import (
    multimodal_tasks, computer_vision_tasks, nlp_tasks, audio_tasks, 
    tabular_tasks, rl_tasks, tasks, libraries, reactoins, languages_2d, languages_3d
)

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

    def init_postgres_ods():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # ODS Models
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS ods;
        CREATE TABLE IF NOT EXISTS ods.models (
            id TEXT,
            owner TEXT,
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
            deploy TEXT,
            dataset TEXT[],
            arxiv INTEGER,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)

        # ODS Papers
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ods.papers (
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
            ai_summary TEXT,
            ai_keywords TEXT[],
            organization TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # ODS Posts
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ods.posts (
            slug TEXT,
            author TEXT,
            content_raw TEXT,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            total_unique_impressions INTEGER,
            num_comments INTEGER,
            reactions JSONB,
            mentions TEXT[],
            attachments TEXT[],
            loaded_at TIMESTAMP DEFAULT NOW()
        );
        """)

        conn.commit()
        cursor.close()

    def init_postgres_dds():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # schema creation
        cursor.execute('CREATE SCHEMA IF NOT EXISTS dds;')
        cursor.execute('CREATE SCHEMA IF NOT EXISTS public;')

        # DDS tables
        tables_sql = """
        -- Bridge tables
        CREATE TABLE IF NOT EXISTS dds.bridge_models_languages (
            model_id bigint NOT NULL,
            language_id bigint NOT NULL,
            CONSTRAINT "pk_models_languages_model_id_language_id" PRIMARY KEY (model_id, language_id)
        );

        CREATE TABLE IF NOT EXISTS dds.bridge_models_libraries (
            model_id bigint NOT NULL,
            library_id bigint NOT NULL,
            CONSTRAINT "pk_models_libraries_model_id_library_id" PRIMARY KEY (model_id, library_id)
        );

        CREATE TABLE IF NOT EXISTS dds.bridge_models_tasks (
            model_id bigint NOT NULL,
            task_id bigint NOT NULL,
            CONSTRAINT "pk_models_tasks_model_id_task_id" PRIMARY KEY (model_id, task_id)
        );

        CREATE TABLE IF NOT EXISTS dds.bridge_models_datasets (
            model_id bigint NOT NULL,
            dataset_id bigint NOT NULL,
            CONSTRAINT "pk_models_datasets_model_id_dataset_id" PRIMARY KEY (model_id, dataset_id)
        );

        CREATE TABLE IF NOT EXISTS dds.bridge_papers_keywords (
            paper_id bigint NOT NULL,
            keyword_id bigint NOT NULL,
            PRIMARY KEY (paper_id, keyword_id)
        );

        CREATE TABLE IF NOT EXISTS dds.bridge_papers_authors (
            paper_id bigint NOT NULL,
            author_id bigint NOT NULL,
            PRIMARY KEY (paper_id, author_id)
        );

        -- Dimension tables
        CREATE TABLE IF NOT EXISTS dds.dim_libraries (
            id BIGSERIAL,
            name varchar(255) NOT NULL,
            CONSTRAINT "pk_libraries_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_datasets (
            id BIGSERIAL,
            name varchar(255) NOT NULL,
            CONSTRAINT "pk_datasets_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_languages (
            id BIGSERIAL,
            code varchar(255) NOT NULL,
            name varchar(255) NOT NULL,
            CONSTRAINT "pk_languages_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_tasks (
            id BIGSERIAL,
            name varchar(255) NOT NULL,
            type varchar(255) NOT NULL,
            CONSTRAINT "pk_tasks_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_authors (
            id BIGSERIAL,
            name varchar(255) NOT NULL,
            CONSTRAINT "pk_authors_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_keywords (
            id BIGSERIAL,
            name varchar NOT NULL,
            CONSTRAINT "pk_keywords_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_reactions (
            id BIGSERIAL,
            reaction varchar(255) NOT NULL,
            CONSTRAINT "pk_reactions_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_models (
            id BIGSERIAL,
            name text NOT NULL,
            owner_id bigint NOT NULL,
            library_name text,
            pipeline_tag text,
            license text,
            modification text,
            region text,
            diffusers_pipeline text,
            deploy text,
            valid_from timestamp NOT NULL,
            valid_to timestamp NOT NULL,
            is_current boolean NOT NULL,
            date_id bigint NOT NULL,
            time_id bigint NOT NULL,
            hashdiff text NOT NULL,
            CONSTRAINT "pk_models_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_papers (
            id BIGSERIAL,
            source_id text NOT NULL,
            title text NOT NULL,
            summary text,
            upvotes integer NOT NULL,
            comments integer NOT NULL,
            submitted_by_id bigint NOT NULL,
            organization_id bigint,
            ai_summary text,
            publish_date_id bigint,
            publish_time_id bigint,
            submitted_date_id bigint NOT NULL,
            submitted_time_id bigint NOT NULL,
            CONSTRAINT "pk_papers_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_posts (
            id BIGSERIAL,
            slug text NOT NULL,
            author_id bigint NOT NULL,
            content_raw text NOT NULL,
            valid_from timestamp NOT NULL,
            valid_to timestamp NOT NULL,
            is_current boolean NOT NULL,
            publish_date_id bigint NOT NULL,
            publish_time_id bigint NOT NULL,
            update_date_id bigint,
            update_time_id bigint,
            hashdiff text NOT NULL,
            CONSTRAINT "pk_posts_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.parties (
            id BIGSERIAL,
            name varchar(255) NOT NULL,
            valid_from timestamp NOT NULL,
            valid_to timestamp NOT NULL,
            is_current boolean NOT NULL,
            type varchar NOT NULL,
            hashdiff text NOT NULL,
            CONSTRAINT "pk_parties_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_date (
            id BIGSERIAL,
            date date NOT NULL,
            year integer NOT NULL,
            month integer NOT NULL,
            month_name varchar NOT NULL,
            day integer NOT NULL,
            day_name varchar NOT NULL,
            week_day integer NOT NULL,
            is_weekend boolean NOT NULL,
            quarter integer NOT NULL,
            CONSTRAINT "pk_dim_date_id" PRIMARY KEY (id)
        );

        CREATE TABLE IF NOT EXISTS dds.dim_time (
            id BIGSERIAL,
            time time NOT NULL,
            hour integer NOT NULL,
            time_of_day varchar NOT NULL,
            is_business_hour boolean NOT NULL,
            CONSTRAINT "pk_dim_time_id" PRIMARY KEY (id)
        );

        -- Fact tables
        CREATE TABLE IF NOT EXISTS dds.fact_models_metrics (
            model_id bigint NOT NULL,
            downloads bigint NOT NULL,
            likes bigint NOT NULL,
            trending_score double precision NOT NULL,
            date_id bigint NOT NULL,
            time_id bigint NOT NULL,
            CONSTRAINT "pk_fact_models_id" PRIMARY KEY (model_id, date_id, time_id)
        );

        CREATE TABLE IF NOT EXISTS dds.fact_posts_metrics (
            post_id bigint NOT NULL,
            total_unique_impressions bigint NOT NULL,
            num_comments bigint NOT NULL,
            num_reactions bigint NOT NULL,
            date_id bigint NOT NULL,
            time_id bigint NOT NULL,
            CONSTRAINT "pk_fact_posts_id" PRIMARY KEY (post_id, date_id, time_id)
        );

        CREATE TABLE IF NOT EXISTS dds.fact_posts_reactions (
            post_id bigint NOT NULL,
            reaction_id bigint NOT NULL,
            user_id bigint NOT NULL,
            date_id bigint NOT NULL,
            time_id bigint NOT NULL,
            CONSTRAINT "pk_fact_posts_reactions_post_id" PRIMARY KEY (post_id, reaction_id, user_id, date_id, time_id)
        );
        """
        cursor.execute(tables_sql)
        
        # Foreign Key constraints
        fk_sql = """
        -- Bridge FKs (DROP + ADD)
        ALTER TABLE dds.bridge_models_tasks 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_tasks_model_id_dim_models_id,
        ADD CONSTRAINT fk_bridge_models_tasks_model_id_dim_models_id 
        FOREIGN KEY(model_id) REFERENCES dds.dim_models(id);

        ALTER TABLE dds.bridge_models_tasks 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_tasks_task_id_dim_tasks_id,
        ADD CONSTRAINT fk_bridge_models_tasks_task_id_dim_tasks_id 
        FOREIGN KEY(task_id) REFERENCES dds.dim_tasks(id);

        ALTER TABLE dds.bridge_models_languages 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_languages_model_id_dim_models_id,
        ADD CONSTRAINT fk_bridge_models_languages_model_id_dim_models_id 
        FOREIGN KEY(model_id) REFERENCES dds.dim_models(id);

        ALTER TABLE dds.bridge_models_languages 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_languages_language_id_dim_languages_id,
        ADD CONSTRAINT fk_bridge_models_languages_language_id_dim_languages_id 
        FOREIGN KEY(language_id) REFERENCES dds.dim_languages(id);

        ALTER TABLE dds.bridge_models_libraries 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_libraries_model_id_dim_models_id,
        ADD CONSTRAINT fk_bridge_models_libraries_model_id_dim_models_id 
        FOREIGN KEY(model_id) REFERENCES dds.dim_models(id);

        ALTER TABLE dds.bridge_models_libraries 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_libraries_library_id_dim_libraries_id,
        ADD CONSTRAINT fk_bridge_models_libraries_library_id_dim_libraries_id 
        FOREIGN KEY(library_id) REFERENCES dds.dim_libraries(id);

        ALTER TABLE dds.bridge_models_datasets 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_datasets_model_id_dim_models_id,
        ADD CONSTRAINT fk_bridge_models_datasets_model_id_dim_models_id 
        FOREIGN KEY(model_id) REFERENCES dds.dim_models(id);

        ALTER TABLE dds.bridge_models_datasets 
        DROP CONSTRAINT IF EXISTS fk_bridge_models_datasets_dataset_id_dim_datasets_id,
        ADD CONSTRAINT fk_bridge_models_datasets_dataset_id_dim_datasets_id 
        FOREIGN KEY(dataset_id) REFERENCES dds.dim_datasets(id);

        ALTER TABLE dds.bridge_papers_keywords 
        DROP CONSTRAINT IF EXISTS fk_bridge_papers_keywords_paper_id_dim_papers_id,
        ADD CONSTRAINT fk_bridge_papers_keywords_paper_id_dim_papers_id 
        FOREIGN KEY(paper_id) REFERENCES dds.dim_papers(id);

        ALTER TABLE dds.bridge_papers_keywords 
        DROP CONSTRAINT IF EXISTS fk_bridge_papers_keywords_keyword_id_dim_keywords_id,
        ADD CONSTRAINT fk_bridge_papers_keywords_keyword_id_dim_keywords_id 
        FOREIGN KEY(keyword_id) REFERENCES dds.dim_keywords(id);

        ALTER TABLE dds.bridge_papers_authors 
        DROP CONSTRAINT IF EXISTS fk_bridge_papers_authors_paper_id_dim_papers_id,
        ADD CONSTRAINT fk_bridge_papers_authors_paper_id_dim_papers_id 
        FOREIGN KEY(paper_id) REFERENCES dds.dim_papers(id);

        ALTER TABLE dds.bridge_papers_authors 
        DROP CONSTRAINT IF EXISTS fk_bridge_papers_authors_author_id_dim_authors_id,
        ADD CONSTRAINT fk_bridge_papers_authors_author_id_dim_authors_id 
        FOREIGN KEY(author_id) REFERENCES dds.dim_authors(id);

        -- Dimension FKs
        ALTER TABLE dds.dim_models 
        DROP CONSTRAINT IF EXISTS fk_dim_models_owner_id_parties_id,
        ADD CONSTRAINT fk_dim_models_owner_id_parties_id 
        FOREIGN KEY(owner_id) REFERENCES dds.parties(id);

        ALTER TABLE dds.dim_models 
        DROP CONSTRAINT IF EXISTS fk_dim_models_date_id_dim_date_id,
        ADD CONSTRAINT fk_dim_models_date_id_dim_date_id 
        FOREIGN KEY(date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.dim_models 
        DROP CONSTRAINT IF EXISTS fk_dim_models_time_id_dim_time_id,
        ADD CONSTRAINT fk_dim_models_time_id_dim_time_id 
        FOREIGN KEY(time_id) REFERENCES dds.dim_time(id);

        ALTER TABLE dds.dim_papers 
        DROP CONSTRAINT IF EXISTS fk_dim_papers_submitted_by_id_parties_id,
        ADD CONSTRAINT fk_dim_papers_submitted_by_id_parties_id 
        FOREIGN KEY(submitted_by_id) REFERENCES dds.parties(id);

        ALTER TABLE dds.dim_papers 
        DROP CONSTRAINT IF EXISTS fk_dim_papers_organization_id_parties_id,
        ADD CONSTRAINT fk_dim_papers_organization_id_parties_id 
        FOREIGN KEY(organization_id) REFERENCES dds.parties(id);

        ALTER TABLE dds.dim_papers 
        DROP CONSTRAINT IF EXISTS fk_dim_papers_publish_date_id_dim_date_id,
        ADD CONSTRAINT fk_dim_papers_publish_date_id_dim_date_id 
        FOREIGN KEY(publish_date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.dim_papers 
        DROP CONSTRAINT IF EXISTS fk_dim_papers_publish_time_id_dim_time_id,
        ADD CONSTRAINT fk_dim_papers_publish_time_id_dim_time_id 
        FOREIGN KEY(publish_time_id) REFERENCES dds.dim_time(id);

        ALTER TABLE dds.dim_papers 
        DROP CONSTRAINT IF EXISTS fk_dim_papers_submitted_date_id_dim_date_id,
        ADD CONSTRAINT fk_dim_papers_submitted_date_id_dim_date_id 
        FOREIGN KEY(submitted_date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.dim_papers 
        DROP CONSTRAINT IF EXISTS fk_dim_papers_submitted_time_id_dim_time_id,
        ADD CONSTRAINT fk_dim_papers_submitted_time_id_dim_time_id 
        FOREIGN KEY(submitted_time_id) REFERENCES dds.dim_time(id);

        ALTER TABLE dds.dim_posts 
        DROP CONSTRAINT IF EXISTS fk_dim_posts_author_parties_id,
        ADD CONSTRAINT fk_dim_posts_author_parties_id 
        FOREIGN KEY(author_id) REFERENCES dds.parties(id);

        ALTER TABLE dds.dim_posts 
        DROP CONSTRAINT IF EXISTS fk_dim_posts_publish_date_id_dim_date_id,
        ADD CONSTRAINT fk_dim_posts_publish_date_id_dim_date_id 
        FOREIGN KEY(publish_date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.dim_posts 
        DROP CONSTRAINT IF EXISTS fk_dim_posts_publish_time_id_dim_time_id,
        ADD CONSTRAINT fk_dim_posts_publish_time_id_dim_time_id 
        FOREIGN KEY(publish_time_id) REFERENCES dds.dim_time(id);

        ALTER TABLE dds.dim_posts 
        DROP CONSTRAINT IF EXISTS fk_dim_posts_update_date_id_dim_date_id,
        ADD CONSTRAINT fk_dim_posts_update_date_id_dim_date_id 
        FOREIGN KEY(update_date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.dim_posts 
        DROP CONSTRAINT IF EXISTS fk_dim_posts_update_time_id_dim_time_id,
        ADD CONSTRAINT fk_dim_posts_update_time_id_dim_time_id 
        FOREIGN KEY(update_time_id) REFERENCES dds.dim_time(id);

        -- Fact FKs
        ALTER TABLE dds.fact_models_metrics 
        DROP CONSTRAINT IF EXISTS fk_fact_models_metrics_model_id_dim_models_id,
        ADD CONSTRAINT fk_fact_models_metrics_model_id_dim_models_id 
        FOREIGN KEY(model_id) REFERENCES dds.dim_models(id);

        ALTER TABLE dds.fact_models_metrics 
        DROP CONSTRAINT IF EXISTS fk_fact_models_metrics_date_id_dim_date_id,
        ADD CONSTRAINT fk_fact_models_metrics_date_id_dim_date_id 
        FOREIGN KEY(date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.fact_models_metrics 
        DROP CONSTRAINT IF EXISTS fk_fact_models_metrics_time_id_dim_time_id,
        ADD CONSTRAINT fk_fact_models_metrics_time_id_dim_time_id 
        FOREIGN KEY(time_id) REFERENCES dds.dim_time(id);

        ALTER TABLE dds.fact_posts_metrics 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_metrics_post_id_dim_posts_id,
        ADD CONSTRAINT fk_fact_posts_metrics_post_id_dim_posts_id 
        FOREIGN KEY(post_id) REFERENCES dds.dim_posts(id);

        ALTER TABLE dds.fact_posts_metrics 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_metrics_date_id_dim_date_id,
        ADD CONSTRAINT fk_fact_posts_metrics_date_id_dim_date_id 
        FOREIGN KEY(date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.fact_posts_metrics 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_metrics_time_id_dim_time_id,
        ADD CONSTRAINT fk_fact_posts_metrics_time_id_dim_time_id 
        FOREIGN KEY(time_id) REFERENCES dds.dim_time(id);

        ALTER TABLE dds.fact_posts_reactions 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_reactions_post_id_dim_posts_id,
        ADD CONSTRAINT fk_fact_posts_reactions_post_id_dim_posts_id 
        FOREIGN KEY(post_id) REFERENCES dds.dim_posts(id);

        ALTER TABLE dds.fact_posts_reactions 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_reactions_reaction_id_dim_reactions_id,
        ADD CONSTRAINT fk_fact_posts_reactions_reaction_id_dim_reactions_id 
        FOREIGN KEY(reaction_id) REFERENCES dds.dim_reactions(id);

        ALTER TABLE dds.fact_posts_reactions 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_reactions_user_id_parties_id,
        ADD CONSTRAINT fk_fact_posts_reactions_user_id_parties_id 
        FOREIGN KEY(user_id) REFERENCES dds.parties(id);

        ALTER TABLE dds.fact_posts_reactions 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_reactions_date_id_dim_date_id,
        ADD CONSTRAINT fk_fact_posts_reactions_date_id_dim_date_id 
        FOREIGN KEY(date_id) REFERENCES dds.dim_date(id);

        ALTER TABLE dds.fact_posts_reactions 
        DROP CONSTRAINT IF EXISTS fk_fact_posts_reactions_time_id_dim_time_id,
        ADD CONSTRAINT fk_fact_posts_reactions_time_id_dim_time_id 
        FOREIGN KEY(time_id) REFERENCES dds.dim_time(id);
        """

        cursor.execute(fk_sql)

        unique_sql = """
        -- UNIQUE для dim_date
        ALTER TABLE dds.dim_date 
        DROP CONSTRAINT IF EXISTS dim_date_date_key,
        ADD CONSTRAINT dim_date_date_key UNIQUE (date);
        
        -- UNIQUE для dim_time  
        ALTER TABLE dds.dim_time 
        DROP CONSTRAINT IF EXISTS dim_time_hour_key,
        ADD CONSTRAINT dim_time_hour_key UNIQUE (hour);
        
        -- UNIQUE для остальных
        ALTER TABLE dds.dim_tasks 
        DROP CONSTRAINT IF EXISTS dim_tasks_name_key,
        ADD CONSTRAINT dim_tasks_name_key UNIQUE (name);
        
        ALTER TABLE dds.dim_libraries 
        DROP CONSTRAINT IF EXISTS dim_libraries_name_key,
        ADD CONSTRAINT dim_libraries_name_key UNIQUE (name);
        
        ALTER TABLE dds.dim_reactions 
        DROP CONSTRAINT IF EXISTS dim_reactions_reaction_key,
        ADD CONSTRAINT dim_reactions_reaction_key UNIQUE (reaction);
        
        ALTER TABLE dds.dim_languages 
        DROP CONSTRAINT IF EXISTS dim_languages_code_key,
        ADD CONSTRAINT dim_languages_code_key UNIQUE (code);

        ALTER TABLE dds.dim_datasets
        DROP CONSTRAINT IF EXISTS dim_datasets_name_key,
        ADD CONSTRAINT dim_datasets_name_key UNIQUE (name);

        ALTER TABLE dds.dim_authors
        DROP CONSTRAINT IF EXISTS dim_authors_name_key,
        ADD CONSTRAINT dim_authors_name_key UNIQUE (name);

        ALTER TABLE dds.dim_keywords
        DROP CONSTRAINT IF EXISTS dim_keywords_name_key,
        ADD CONSTRAINT dim_keywords_name_key UNIQUE (name);

        ALTER TABLE dds.dim_papers
        DROP CONSTRAINT IF EXISTS dim_papers_source_id_key,
        ADD CONSTRAINT dim_papers_source_id_key UNIQUE (source_id);

        """

        cursor.execute(unique_sql)
        
        conn.commit()
        cursor.close()

    def init_postgres_ads():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        #  Models
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS ads;
        """)

        cursor.execute("""
        -- 0. help_table          
        CREATE TABLE IF NOT EXISTS ads.datamart_unique_top7d (
        model_id INT PRIMARY KEY,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 1. pipeline_tag PIE
        CREATE TABLE IF NOT EXISTS ads.datamart_pipelinetag_pie (
        pipeline_tag VARCHAR(100) PRIMARY KEY,
        models_count INTEGER NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 2. owners TREEMAP  
        CREATE TABLE IF NOT EXISTS ads.datamart_owners_treemap (
        owner_name VARCHAR(255),
        owner_type VARCHAR(20),
        models_count INTEGER NOT NULL,
        avg_trending_score NUMERIC(6,3),
        total_downloads BIGINT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (owner_name, owner_type)
        );

        -- 3. trending LINE (уже без unique_top7d)
        CREATE TABLE IF NOT EXISTS ads.datamart_trending_line (
        trend_date DATE PRIMARY KEY,
        avg_trending_score NUMERIC(6,3) NOT NULL,
        median_trending_score NUMERIC(6,3),
        models_count INTEGER NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 4. leaderboard 30D
        CREATE TABLE IF NOT EXISTS ads.datamart_leaderboard_30d (
        rank_downloads INTEGER PRIMARY KEY,
        model_name TEXT NOT NULL,
        owner_name VARCHAR(255) NOT NULL,
        modification_type VARCHAR(50),
        latest_downloads_30d BIGINT NOT NULL,
        latest_likes_30d BIGINT NOT NULL,
        likes_ratio NUMERIC(6,4),
        latest_trending_score NUMERIC(6,3),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 5-6. license + modification PIE
        CREATE TABLE IF NOT EXISTS ads.datamart_license_histogram (
        license VARCHAR(50) PRIMARY KEY,
        models_count INTEGER NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ads.datamart_modification_pie (
        modification_type VARCHAR(50) PRIMARY KEY,
        models_count INTEGER NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            deploy Nullable(String),
            dataset Array(String),
            arxiv UInt32,
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
            ai_summary Nullable(String),
            ai_keywords Array(String),
            organization Nullable(String),
            loaded_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY loaded_at;
        """)

        # posts
        conn.execute("""
        CREATE TABLE IF NOT EXISTS posts_ods (
            slug String,
            author Nullable(String),
            content_raw Nullable(String),
            published_at DateTime,
            updated_at DateTime,
            total_unique_impressions UInt32,
            num_comments UInt32,
            reactions Nullable(String),
            mentions Array(String),
            attachments Array(String),
            loaded_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY loaded_at;
        """)

    def populate_dim_date():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        start_date = date(2000, 1, 1)
        end_date = date(2030, 12, 31)
        
        dates_df = pd.date_range(start=start_date, end=end_date, freq='D')
        
        for dt in dates_df:
            cursor.execute("""
            INSERT INTO dds.dim_date (date, year, month, month_name, day, day_name, 
                                     week_day, is_weekend, quarter)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
            """, (
                dt.date(), dt.year, dt.month, dt.strftime('%B'),
                dt.day, dt.strftime('%A'), dt.weekday() + 1,
                dt.weekday() >= 5, (dt.month - 1) // 3 + 1
            ))
        
        conn.commit()
        cursor.close()

    def populate_dim_time():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        for hour in range(24):
            time_obj = pd.Timestamp(f"{hour:02d}:00:00").time()
            time_of_day = "morning" if hour < 12 else "afternoon" if hour < 18 else "evening"
            
            cursor.execute("""
            INSERT INTO dds.dim_time (time, hour, time_of_day, is_business_hour)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (hour) DO NOTHING
            """, (time_obj, hour, time_of_day, 9 <= hour < 18))
        
        conn.commit()
        cursor.close()

    def populate_dim_tasks():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
                
        task_categories = {
            'multimodal': multimodal_tasks,
            'computer_vision': computer_vision_tasks,
            'nlp': nlp_tasks,
            'audio': audio_tasks,
            'tabular': tabular_tasks,
            'rl': rl_tasks
        }
        
        for category, task_set in task_categories.items():
            for task_name in task_set:
                cursor.execute("""
                INSERT INTO dds.dim_tasks (name, type)
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING
                """, (task_name, category))
        
        conn.commit()
        cursor.close()

    def populate_dim_libraries():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
                
        for lib_name in libraries:
            cursor.execute("""
            INSERT INTO dds.dim_libraries (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
            """, (lib_name,))
        
        conn.commit()
        cursor.close()

    def populate_dim_reactions():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
                
        for reaction in reactoins:
            cursor.execute("""
            INSERT INTO dds.dim_reactions (reaction)
            VALUES (%s)
            ON CONFLICT (reaction) DO NOTHING
            """, (reaction,))
        
        conn.commit()
        cursor.close()

    def populate_dim_languages():
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
                
        for name, code in languages_2d:
            cursor.execute("""
            INSERT INTO dds.dim_languages (code, name)
            VALUES (%s, %s)
            ON CONFLICT (code) DO NOTHING
            """, (code, name))

        for name, code in languages_3d:
            cursor.execute("""
            INSERT INTO dds.dim_languages (code, name)
            VALUES (%s, %s)
            ON CONFLICT (code) DO NOTHING
            """, (code, name))
        
        conn.commit()
        cursor.close()

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
        except Exception:
            client.create_bucket(Bucket=bucket_name)

    init_postgres_ods_task = PythonOperator(task_id='init_postgres_ods', python_callable=init_postgres_ods)
    init_postgres_dds_task = PythonOperator(task_id='init_dds_postgres', python_callable=init_postgres_dds)
    init_postgres_ads_task = PythonOperator(task_id='init_ads_postgres', python_callable=init_postgres_ads)
    init_clickhouse_task = PythonOperator(task_id='init_clickhouse', python_callable=init_clickhouse)
    init_mongodb_task = PythonOperator(task_id='init_mongodb', python_callable=init_mongodb)
    init_minio_task = PythonOperator(task_id='init_minio', python_callable=init_minio)

    populate_date_task = PythonOperator(task_id='populate_dim_date', python_callable=populate_dim_date)
    populate_time_task = PythonOperator(task_id='populate_dim_time', python_callable=populate_dim_time)
    populate_tasks_task = PythonOperator(task_id='populate_dim_tasks', python_callable=populate_dim_tasks)
    populate_libraries_task = PythonOperator(task_id='populate_dim_libraries', python_callable=populate_dim_libraries)
    populate_reactions_task = PythonOperator(task_id='populate_dim_reactions', python_callable=populate_dim_reactions)
    populate_languages_task = PythonOperator(task_id='populate_dim_languages', python_callable=populate_dim_languages)

    init_postgres_ods_task >> init_postgres_dds_task
    init_postgres_dds_task >> [populate_date_task, populate_time_task, populate_tasks_task, 
                      populate_libraries_task, populate_reactions_task, populate_languages_task]
    [init_postgres_ods_task, init_clickhouse_task, init_mongodb_task, init_minio_task]


