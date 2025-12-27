from typing import Set, Dict
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def get_or_create_authors_ids(
    author_names: Set[str],
    pghook: PostgresHook,
) -> Dict[str, int]:

    if not author_names:
        return {}

    conn = pghook.get_conn()
    cursor = conn.cursor()
    ids_by_name: Dict[str, int] = {}

    try:
        execute_values(
            cursor,
            """
            INSERT INTO dds.dim_authors (name)
            VALUES %s
            ON CONFLICT (name) DO NOTHING
            """,
            [(name,) for name in author_names],
        )

        cursor.execute(
            """
            SELECT id, name
            FROM dds.dim_authors
            WHERE name = ANY(%s)
            """,
            (list(author_names),),
        )
        for _id, name in cursor.fetchall():
            ids_by_name[name] = _id

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    return ids_by_name


def get_or_create_keywords_ids(
    keyword_names: Set[str],
    pghook: PostgresHook,
) -> Dict[str, int]:
    """
    Вставляет ключевые слова в dds.dim_keywords (если их ещё нет)
    и возвращает словарь {name: id} для всех переданных имён.
    Требуется UNIQUE (name) на dds.dim_keywords.name.
    """
    if not keyword_names:
        return {}

    conn = pghook.get_conn()
    cursor = conn.cursor()
    ids_by_name: Dict[str, int] = {}

    try:
        execute_values(
            cursor,
            """
            INSERT INTO dds.dim_keywords (name)
            VALUES %s
            ON CONFLICT (name) DO NOTHING
            """,
            [(name,) for name in keyword_names],
        )

        cursor.execute(
            """
            SELECT id, name
            FROM dds.dim_keywords
            WHERE name = ANY(%s)
            """,
            (list(keyword_names),),
        )
        for _id, name in cursor.fetchall():
            ids_by_name[name] = _id

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    return ids_by_name


def get_or_create_datasets_ids(
    dataset_names: Set[str],
    pghook: PostgresHook,
) -> Dict[str, int]:
    if not dataset_names:
        return {}

    conn = pghook.get_conn()
    cursor = conn.cursor()
    ids_by_name: Dict[str, int] = {}

    try:
        execute_values(
            cursor,
            """
            INSERT INTO dds.dim_datasets (name)
            VALUES %s
            ON CONFLICT (name) DO NOTHING
            """,
            [(name,) for name in dataset_names],
        )

        cursor.execute(
            """
            SELECT id, name
            FROM dds.dim_datasets
            WHERE name = ANY(%s)
            """,
            (list(dataset_names),),
        )
        for _id, name in cursor.fetchall():
            ids_by_name[name] = _id

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    return ids_by_name


def safe_int(val):
                return int(val) if pd.notna(val) else 0