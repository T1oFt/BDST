from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='models_ads',
    default_args=default_args,
    schedule=None,
    catchup=False,
)

create_unique_top7d = SQLExecuteQueryOperator(
    task_id='refresh_unique_top7d',
    sql="""
    TRUNCATE ads.datamart_unique_top7d;
    INSERT INTO ads.datamart_unique_top7d
    SELECT DISTINCT model_id
    FROM dds.fact_models_metrics fmm
    JOIN dds.dim_date dd ON fmm.date_id = dd.id
    JOIN dds.dim_models dm ON fmm.model_id = dm.id
    WHERE dd.date >= CURRENT_DATE - 7
    AND dm.is_current = true;
    """,
    conn_id='pg_hf_conn',
    dag=dag
)

refresh_pipelinetag = SQLExecuteQueryOperator(
    task_id='refresh_pipelinetag_pie',
    sql="""
    TRUNCATE ads.datamart_pipelinetag_pie;
    INSERT INTO ads.datamart_pipelinetag_pie
    SELECT COALESCE(dm.pipeline_tag, 'no_tag'),
           COUNT(DISTINCT u.model_id)
    FROM ads.datamart_unique_top7d u 
    JOIN dds.dim_models dm ON u.model_id = dm.id
    WHERE dm.is_current = true
    GROUP BY 1;
    """,
    conn_id='pg_hf_conn',
    dag=dag
)

refresh_owners = SQLExecuteQueryOperator(
    task_id='refresh_owners_treemap',
    sql="""
    TRUNCATE ads.datamart_owners_treemap;
    INSERT INTO ads.datamart_owners_treemap
    SELECT p.name, p.type, COUNT(DISTINCT u.model_id),
           ROUND(AVG(fmm.trending_score)::numeric, 3),
           SUM(fmm.downloads)
    FROM ads.datamart_unique_top7d u
    JOIN dds.dim_models dm ON u.model_id = dm.id
    JOIN dds.parties p ON dm.owner_id = p.id
    JOIN dds.fact_models_metrics fmm ON u.model_id = fmm.model_id
    WHERE dm.is_current = true
    GROUP BY p.id, p.name, p.type;
    """,
    conn_id='pg_hf_conn',
    dag=dag
)

refresh_trending = SQLExecuteQueryOperator(
    task_id='refresh_trending_line',
    sql="""
    TRUNCATE ads.datamart_trending_line;
    INSERT INTO ads.datamart_trending_line
    SELECT dd.date, AVG(fmm.trending_score), 
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fmm.trending_score),
           COUNT(*)
    FROM dds.fact_models_metrics fmm 
    JOIN dds.dim_date dd ON fmm.date_id = dd.id
    WHERE dd.date >= CURRENT_DATE - 30
    GROUP BY dd.date
    ORDER BY dd.date DESC;
    """,
    conn_id='pg_hf_conn',
    dag=dag
)

refresh_license = SQLExecuteQueryOperator(
    task_id='refresh_license_histogram',
    sql="""
    TRUNCATE ads.datamart_license_histogram;
    INSERT INTO ads.datamart_license_histogram
    SELECT COALESCE(dm.license, 'no_license') as license,
           COUNT(DISTINCT u.model_id) as models_count
    FROM ads.datamart_unique_top7d u 
    JOIN dds.dim_models dm ON u.model_id = dm.id
    WHERE dm.is_current = true
    GROUP BY 1;
    """,
    conn_id='pg_hf_conn',
    dag=dag
)

refresh_modification = SQLExecuteQueryOperator(
    task_id='refresh_modification_pie',
    sql="""
    TRUNCATE ads.datamart_modification_pie;
    INSERT INTO ads.datamart_modification_pie
    SELECT COALESCE(NULLIF(TRIM(UPPER(dm.modification)), ''), 'ORIGINAL') as modification_type,
           COUNT(DISTINCT u.model_id) as models_count
    FROM ads.datamart_unique_top7d u 
    JOIN dds.dim_models dm ON u.model_id = dm.id
    WHERE dm.is_current = true
    GROUP BY 1;
    """,
    conn_id='pg_hf_conn',
    dag=dag
)

refresh_leaderboard = SQLExecuteQueryOperator(
    task_id='refresh_leaderboard_30d',
    sql="""
    TRUNCATE ads.datamart_leaderboard_30d;
    WITH latest_metrics_30d AS (
      SELECT DISTINCT ON (model_id) model_id, trending_score, downloads, likes
      FROM dds.fact_models_metrics fmm
      JOIN dds.dim_date dd ON fmm.date_id = dd.id
      WHERE dd.date >= CURRENT_DATE - 30
      ORDER BY model_id, date_id DESC
    )
    INSERT INTO ads.datamart_leaderboard_30d
    SELECT RANK() OVER (ORDER BY lm.downloads DESC), dm.name, p.name,
           COALESCE(NULLIF(TRIM(UPPER(dm.modification)), ''), 'ORIGINAL'),
           lm.downloads, lm.likes,
           ROUND((lm.likes::float / NULLIF(lm.downloads, 0))::numeric, 4),
           lm.trending_score
    FROM latest_metrics_30d lm
    JOIN dds.dim_models dm ON lm.model_id = dm.id
    JOIN dds.parties p ON dm.owner_id = p.id
    WHERE dm.is_current = true AND lm.downloads > 0
    ORDER BY 1
    LIMIT 20;
    """,
    conn_id='pg_hf_conn',
    dag=dag
)

create_unique_top7d >> [refresh_owners, refresh_trending, refresh_license, refresh_leaderboard, refresh_modification, refresh_pipelinetag]
