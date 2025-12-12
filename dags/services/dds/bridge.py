import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from psycopg2.extras import execute_values


class FlexibleBridgeFiller():
    def __init__(self, pg_hook: PostgresHook):
        self.hook: PostgresHook = pg_hook

    def fill_with_pandas(
        self,
        table_name: str,
        col1_name: str,
        col2_name: str,
        data: Dict[int, List[int]],
    ) -> int:
        records = []
        for id1, id2_list in data.items():
            for id2 in id2_list:
                records.append((id1, id2))

        if not records:
            return 0

        conn = self.hook.get_conn()
        cursor = conn.cursor()

        try:
            sql = f"""
                INSERT INTO dds.{table_name} ({col1_name}, {col2_name})
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(cursor, sql, records)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

        return len(records)