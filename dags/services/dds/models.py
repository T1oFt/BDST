import pandas as pd
import hashlib
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Optional, Dict, Any, Tuple, List, Set
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from huggingface_hub import get_user_overview
import re

from services.dds.help_dims import get_or_create_datasets_ids
from services.dds.parties import upsert_parties_scd2
from services.dds.bridge import FlexibleBridgeFiller

class ModelsDataExtractor:
    def __init__(self, pg_hook: PostgresHook):
        self.hook = pg_hook
    
    def get_models_by_loaded_at(self, 
                              loaded_at: str, 
                              columns: Optional[list] = None) -> pd.DataFrame:
        try:
            datetime.strptime(loaded_at, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            raise ValueError("loaded_at должен быть в формате 'YYYY-MM-DD HH:MM:SS.ffffff'")

        if columns:
            columns_str = ', '.join(columns)
        else:
            columns_str = '*'

        query = f"""
        SELECT {columns_str}
        FROM ods.models 
        WHERE loaded_at = '{loaded_at}'::timestamp
        """
        
        df = self.hook.get_pandas_df(query)
        
        return df


class ModelsDataTransformer:
    def transform_models_df(
        self,
        df: pd.DataFrame,
    ) -> Tuple[
        List[Dict[str, Any]],      # dim_models_raw
        List[Dict[str, Any]],      # fact_models_raw
        Set[Tuple[str, str]],                  # unique_owners
        Set[str],                  # unique_dates
        Set[str],                  # unique_times
        Set[str],                  # unique_datasets
        Dict[str, List[str]],      # model_name -> languages
        Dict[str, List[str]],      # model_name -> libraries
        Dict[str, List[str]],      # model_name -> tasks
        Dict[str, List[str]],      # model_name -> datasets
    ]:
        dim_models_raw: List[Dict[str, Any]] = []
        fact_models_raw: List[Dict[str, Any]] = []

        unique_owners: Set[Tuple[str, str]] = set()
        unique_dates: Set[str] = set()
        unique_times: Set[str] = set()
        unique_datasets: Set[str] = set()

        model_languages: Dict[str, List[str]] = {}
        model_libraries: Dict[str, List[str]] = {}
        model_tasks: Dict[str, List[str]] = {}
        model_datasets: Dict[str, List[str]] = {}

        for _, row in df.iterrows():
            model_name = str(row["id"])

            owner = (row.get("owner") or "").strip() or None
            if owner:
                try:
                    r = get_user_overview(owner)
                    owner_type = 'user'
                except Exception:
                    owner_type = 'org'

                unique_owners.add((owner, owner_type))

            created_date_str, created_time_str = self._extract_date_time_str(
                row.get("created_at")
            )
            loaded_date_str, loaded_time_str = self._extract_date_time_str(
                row.get("loaded_at")
            )

            for d in (created_date_str, loaded_date_str):
                if d:
                    unique_dates.add(d)
            for t in (created_time_str, loaded_time_str):
                if t:
                    unique_times.add(t)

            languages = row.get("language") or []
            libraries = row.get("library") or []
            tasks = row.get("task") or []
            datasets = row.get("dataset") or []

            for ds in datasets:
                unique_datasets.add(ds)

            model_languages[model_name] = languages
            model_libraries[model_name] = libraries
            model_tasks[model_name] = tasks
            model_datasets[model_name] = datasets

            languages_sorted = ",".join(sorted(languages)) or None
            libraries_sorted = ",".join(sorted(libraries)) or None
            tasks_sorted = ",".join(sorted(tasks)) or None
            datasets_sorted = ",".join(sorted(datasets)) or None

            hash_src = (
                f"{model_name}|{owner}|{row.get('library_name')}|"
                f"{row.get('pipeline_tag')}|{row.get('license')}|"
                f"{row.get('modification')}|{row.get('region')}|"
                f"{row.get('diffusers_pipeline')}|{row.get('deploy')}|"
                f"{languages_sorted}|{libraries_sorted}|"
                f"{tasks_sorted}|{datasets_sorted}"
            )
            hashdiff = hashlib.md5(hash_src.encode("utf-8")).hexdigest()

            dim_models_raw.append(
                {
                    "name": model_name,
                    "owner_name": owner,
                    "library_name": row.get("library_name"),
                    "pipeline_tag": row.get("pipeline_tag"),
                    "license": row.get("license"),
                    "modification": row.get("modification"),
                    "region": row.get("region"),
                    "diffusers_pipeline": row.get("diffusers_pipeline"),
                    "deploy": row.get('deploy'),
                    "created_date": created_date_str,
                    "created_time": created_time_str,
                    "hashdiff": hashdiff,
                }
            )

            fact_models_raw.append(
                {
                    "name": model_name,
                    "downloads": int(row.get("downloads") or 0),
                    "likes": int(row.get("likes") or 0),
                    "trending_score": float(
                        row.get("trending_score") or 0.0
                    ),
                    "loaded_date": loaded_date_str,
                    "loaded_time": loaded_time_str,
                }
            )

        return (
            dim_models_raw,
            fact_models_raw,
            unique_owners,
            unique_dates,
            unique_times,
            unique_datasets,
            model_languages,
            model_libraries,
            model_tasks,
            model_datasets,
        )

    @staticmethod
    def _extract_date_time_str(val) -> Tuple[Optional[str], Optional[str]]:
        if pd.isna(val) or not val:
            return None, None
        dt = pd.to_datetime(val)
        return dt.date().strftime("%Y-%m-%d"), dt.time().strftime("%H") + ":00:00"


class ModelsDataLoader:
    def __init__(self, postgres_conn_id: str = "dds_postgres"):
        self.postgres_conn_id = postgres_conn_id
        self.pghook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.bridge_filler = FlexibleBridgeFiller(self.pghook)
        self.transformer = ModelsDataTransformer()

    def _load_dates_times_maps(
        self,
        unique_dates: Set[str],
        unique_times: Set[str],
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        conn = self.pghook.get_conn()
        cursor = conn.cursor()
        date_map: Dict[str, int] = {}
        time_map: Dict[str, int] = {}

        try:
            if unique_dates:
                cursor.execute(
                    """
                    SELECT id, date
                    FROM dds.dim_date
                    WHERE date = ANY(%s::date[])
                    """,
                    (list(unique_dates),),
                )
                for _id, d in cursor.fetchall():
                    date_map[d.strftime("%Y-%m-%d")] = _id

            if unique_times:
                cursor.execute(
                    """
                    SELECT id, time
                    FROM dds.dim_time
                    WHERE time = ANY(%s::time[])
                    """,
                    (list(unique_times),),
                )
                for _id, t in cursor.fetchall():
                    time_map[t.strftime("%H:%M:%S")] = _id

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

        return date_map, time_map

    def _load_parties_map(
        self,
        unique_owners: Set[Tuple[str, str]],
        loaded_at: datetime,
    ) -> Dict[str, int]:
        if not unique_owners:
            return {}

        parties_payload = [
            {"name": name, "type": owner_type} for [name, owner_type] in unique_owners
        ]

        return upsert_parties_scd2(
            parties_data=parties_payload,
            pghook=self.pghook,
            loaded_at=loaded_at,
        )

    def _load_datasets_map(
        self,
        unique_datasets: Set[str],
    ) -> Dict[str, int]:
        return get_or_create_datasets_ids(
            dataset_names=unique_datasets,
            pghook=self.pghook,
        )

    def _load_simple_dim_map(
        self,
        table: str,
    ) -> Dict[str, int]:
        conn = self.pghook.get_conn()
        cursor = conn.cursor()
        result: Dict[str, int] = {}

        try:
            cursor.execute(f"SELECT id, name FROM dds.{table}")
            for _id, name in cursor.fetchall():
                result[name] = _id
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

        return result
    
    def _load_languages_dim_map(
        self,
    ) -> Dict[str, int]:
        conn = self.pghook.get_conn()
        cursor = conn.cursor()
        result: Dict[str, int] = {}

        try:
            cursor.execute(f"SELECT id, code FROM dds.dim_languages")
            for _id, name in cursor.fetchall():
                result[name] = _id
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

        return result

    def _upsert_dim_models_scd2(
        self,
        dim_models_raw: List[Dict[str, Any]],
        date_map: Dict[str, int],
        time_map: Dict[str, int],
        parties_map: Dict[str, int],
        loaded_at: datetime,
    ) -> Dict[str, int]:
        if not dim_models_raw:
            return {}

        conn = self.pghook.get_conn()
        cursor = conn.cursor()
        name_to_id: Dict[str, int] = {}

        try:
            future_valid_to = loaded_at + timedelta(days=365 * 100)

            for rec in dim_models_raw:
                name = rec["name"]
                owner_name = rec["owner_name"]

                owner_id = parties_map.get(owner_name)
                if owner_id is None:
                    continue

                created_date_id = (
                    date_map.get(rec["created_date"]) if rec["created_date"] else None
                )
                created_time_id = (
                    time_map.get(rec["created_time"]) if rec["created_time"] else None
                )

                if created_date_id is None or created_time_id is None:
                    continue

                hashdiff = rec["hashdiff"]

                cursor.execute(
                    """
                    SELECT id, hashdiff
                    FROM dds.dim_models
                    WHERE name = %s AND is_current = true
                    """,
                    (name,),
                )
                existing = cursor.fetchone()

                if existing and existing[1] == hashdiff:
                    name_to_id[name] = existing[0]
                    continue

                existing_id = None
                if existing:
                    existing_id = existing[0]

                try:
                    if existing_id is not None:
                        cursor.execute(
                            """
                            UPDATE dds.dim_models
                            SET valid_to = %s,
                                is_current = false
                            WHERE id = %s
                            """,
                            (loaded_at, existing_id),
                        )

                    cursor.execute(
                        """
                        INSERT INTO dds.dim_models (
                            name, owner_id,
                            library_name, pipeline_tag, license,
                            modification, region, diffusers_pipeline, deploy,
                            valid_from, valid_to, is_current,
                            date_id, time_id, hashdiff
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            name,
                            owner_id,
                            rec["library_name"],
                            rec["pipeline_tag"],
                            rec["license"],
                            rec["modification"],
                            rec["region"],
                            rec["diffusers_pipeline"],
                            rec["deploy"],
                            loaded_at,
                            future_valid_to,
                            True,
                            created_date_id,
                            created_time_id,
                            hashdiff,
                        ),
                    )
                    new_id = cursor.fetchone()[0]
                    conn.commit()
                    name_to_id[name] = new_id

                except Exception:
                    conn.rollback()
                    if existing_id is not None:
                        name_to_id[name] = existing_id
                    continue

        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

        return name_to_id

    def _upsert_fact_models_metrics(
        self,
        fact_models_raw: List[Dict[str, Any]],
        name_to_model_id: Dict[str, int],
        date_map: Dict[str, int],
        time_map: Dict[str, int],
    ) -> None:
        if not fact_models_raw:
            return

        conn = self.pghook.get_conn()
        cursor = conn.cursor()

        try:
            rows = []
            for rec in fact_models_raw:
                name = rec["name"]
                model_id = name_to_model_id.get(name)
                if not model_id:
                    continue

                date_id = date_map.get(rec["loaded_date"])
                time_id = time_map.get(rec["loaded_time"])

                if date_id is None or time_id is None:
                    continue

                rows.append(
                    (
                        model_id,
                        rec["downloads"],
                        rec["likes"],
                        rec["trending_score"],
                        date_id,
                        time_id,
                    )
                )

            if not rows:
                return

            sql = """
                INSERT INTO dds.fact_models_metrics (
                    model_id, downloads, likes, trending_score,
                    date_id, time_id
                )
                VALUES %s
                ON CONFLICT (model_id, date_id, time_id) DO UPDATE
                SET downloads = EXCLUDED.downloads,
                    likes = EXCLUDED.likes,
                    trending_score = EXCLUDED.trending_score
            """
            execute_values(cursor, sql, rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def load_all(
        self,
        dim_models_raw: List[Dict[str, Any]],
        fact_models_raw: List[Dict[str, Any]],
        unique_owners: Set[Tuple[str, str]],
        unique_dates: Set[str],
        unique_times: Set[str],
        unique_datasets: Set[str],
        model_languages: Dict[str, List[str]],
        model_libraries: Dict[str, List[str]],
        model_tasks: Dict[str, List[str]],
        model_datasets: Dict[str, List[str]],
        loaded_at: datetime,
    ) -> None:

        date_map, time_map = self._load_dates_times_maps(unique_dates, unique_times)
        parties_map = self._load_parties_map(unique_owners, loaded_at)
        datasets_map = self._load_datasets_map(unique_datasets)

        languages_map = self._load_languages_dim_map()
        libraries_map = self._load_simple_dim_map("dim_libraries")
        tasks_map = self._load_simple_dim_map("dim_tasks")

        name_to_model_id = self._upsert_dim_models_scd2(
            dim_models_raw=dim_models_raw,
            date_map=date_map,
            time_map=time_map,
            parties_map=parties_map,
            loaded_at=loaded_at,
        )

        self._upsert_fact_models_metrics(
            fact_models_raw=fact_models_raw,
            name_to_model_id=name_to_model_id,
            date_map=date_map,
            time_map=time_map,
        )

        bridge_models_languages = {}
        for name, langs in model_languages.items():
            mid = name_to_model_id.get(name)
            if not mid:
                continue
            ids = [languages_map[l] for l in langs if l in languages_map]
            if ids:
                bridge_models_languages[mid] = ids

        bridge_models_libraries = {}
        for name, libs in model_libraries.items():
            mid = name_to_model_id.get(name)
            if not mid:
                continue
            ids = [libraries_map[l] for l in libs if l in libraries_map]
            if ids:
                bridge_models_libraries[mid] = ids

        bridge_models_tasks = {}
        for name, tasks in model_tasks.items():
            mid = name_to_model_id.get(name)
            if not mid:
                continue
            ids = [tasks_map[t] for t in tasks if t in tasks_map]
            if ids:
                bridge_models_tasks[mid] = ids

        bridge_models_datasets = {}
        for name, dsets in model_datasets.items():
            mid = name_to_model_id.get(name)
            if not mid:
                continue
            ids = [datasets_map[d] for d in dsets if d in datasets_map]
            if ids:
                bridge_models_datasets[mid] = ids

        if bridge_models_languages:
            self.bridge_filler.fill_with_pandas(
                table_name="bridge_models_languages",
                col1_name="model_id",
                col2_name="language_id",
                data=bridge_models_languages,
            )

        if bridge_models_libraries:
            self.bridge_filler.fill_with_pandas(
                table_name="bridge_models_libraries",
                col1_name="model_id",
                col2_name="library_id",
                data=bridge_models_libraries,
            )

        if bridge_models_tasks:
            self.bridge_filler.fill_with_pandas(
                table_name="bridge_models_tasks",
                col1_name="model_id",
                col2_name="task_id",
                data=bridge_models_tasks,
            )

        if bridge_models_datasets:
            self.bridge_filler.fill_with_pandas(
                table_name="bridge_models_datasets",
                col1_name="model_id",
                col2_name="dataset_id",
                data=bridge_models_datasets,
            )
