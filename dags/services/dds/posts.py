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

class PostsDataExtractor:
    def __init__(self, pg_hook: PostgresHook):
        self.hook = pg_hook
    
    def get_posts_by_loaded_at(self, 
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
        FROM ods.posts
        WHERE loaded_at = '{loaded_at}'::timestamp
        """
        
        df = self.hook.get_pandas_df(query)
        
        return df
    

class PostsDataTransformer:
    def transform_posts_df(
        self,
        df: pd.DataFrame,
        loaded_at: datetime,
    ) -> Tuple[
        List[Dict[str, Any]],        # dim_posts_raw
        List[Dict[str, Any]],        # fact_posts_metrics_raw
        List[Dict[str, Any]],        # fact_posts_reactions_raw
        Set[Tuple[str, str]],        # unique_parties
        Set[str],                    # unique_dates
        Set[str],                    # unique_times
    ]:
        dim_posts_raw: List[Dict[str, Any]] = []
        fact_posts_metrics_raw: List[Dict[str, Any]] = []
        fact_posts_reactions_raw: List[Dict[str, Any]] = []

        unique_parties: Set[Tuple[str, str]] = set()
        unique_dates: Set[str] = set()
        unique_times: Set[str] = set()

        loaded_date_str, loaded_time_str = self._extract_date_time_str(loaded_at)

        for _, row in df.iterrows():
            slug = str(row["slug"])

            author = (row.get("author") or "").strip() or None
            if author:
                unique_parties.add((author, "user"))

            pub_date_str, pub_time_str = self._extract_date_time_str(
                row.get("published_at")
            )
            upd_date_str, upd_time_str = self._extract_date_time_str(
                row.get("updated_at")
            )

            for d in (pub_date_str, upd_date_str, loaded_date_str):
                if d:
                    unique_dates.add(d)
            for t in (pub_time_str, upd_time_str, loaded_time_str):
                if t:
                    unique_times.add(t)

            hash_src = (
                f"{slug}|{author}|{row.get('content_raw')}|"
                f"{pub_date_str}|{pub_time_str}|{upd_date_str}|{upd_time_str}"
            )
            hashdiff = hashlib.md5(hash_src.encode("utf-8")).hexdigest()

            dim_posts_raw.append(
                {
                    "slug": slug,
                    "author_name": author,
                    "content_raw": row.get("content_raw") or "",
                    "publish_date": pub_date_str,
                    "publish_time": pub_time_str,
                    "update_date": upd_date_str,
                    "update_time": upd_time_str,
                    "hashdiff": hashdiff,
                }
            )

            fact_posts_metrics_raw.append(
                {
                    "slug": slug,
                    "total_unique_impressions": int(
                        row.get("total_unique_impressions") or 0
                    ),
                    "num_comments": int(row.get("num_comments") or 0),
                    "loaded_date": loaded_date_str,
                    "loaded_time": loaded_time_str,
                }
            )

            reactions = row.get("reactions") or []
            num_reactions_total = 0

            for r in reactions:
                reaction_code = r.get("reaction")
                users = r.get("users") or []
                count = int(r.get("count") or 0)

                if not reaction_code or not users:
                    continue

                num_reactions_total += count

                for u in users:
                    user_name = (u or "").strip()
                    if not user_name:
                        continue
                    unique_parties.add((user_name, "user"))

                    fact_posts_reactions_raw.append(
                        {
                            "slug": slug,
                            "reaction": reaction_code,
                            "user_name": user_name,
                            "date": loaded_date_str,
                            "time": loaded_time_str,
                        }
                    )

            fact_posts_metrics_raw[-1]["num_reactions"] = num_reactions_total

        return (
            dim_posts_raw,
            fact_posts_metrics_raw,
            fact_posts_reactions_raw,
            unique_parties,
            unique_dates,
            unique_times,
        )

    @staticmethod
    def _extract_date_time_str(val) -> Tuple[Optional[str], Optional[str]]:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None, None
        dt = pd.to_datetime(val)
        return dt.date().strftime("%Y-%m-%d"), dt.time().strftime("%H") + ":00:00"


class PostsDataLoader:
    def __init__(self, postgres_conn_id: str = "dds_postgres"):
        self.postgres_conn_id = postgres_conn_id
        self.pghook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.transformer = PostsDataTransformer()

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
        unique_parties: Set[Tuple[str, str]],
        loaded_at: datetime,
    ) -> Dict[str, int]:
        if not unique_parties:
            return {}

        parties_payload = [
            {"name": name, "type": p_type} for (name, p_type) in unique_parties
        ]

        return upsert_parties_scd2(
            parties_data=parties_payload,
            pghook=self.pghook,
            loaded_at=loaded_at,
        )

    def _upsert_dim_posts_scd2(
        self,
        dim_posts_raw: List[Dict[str, Any]],
        date_map: Dict[str, int],
        time_map: Dict[str, int],
        parties_map: Dict[str, int],
        loaded_at: datetime,
    ) -> Dict[str, int]:
        if not dim_posts_raw:
            return {}

        conn = self.pghook.get_conn()
        cursor = conn.cursor()
        slug_to_id: Dict[str, int] = {}

        try:
            future_valid_to = loaded_at + timedelta(days=365 * 100)

            for rec in dim_posts_raw:
                slug = rec["slug"]
                author_name = rec["author_name"]

                author_id = parties_map.get(author_name)
                if author_id is None:
                    continue

                pub_date_id = (
                    date_map.get(rec["publish_date"]) if rec["publish_date"] else None
                )
                pub_time_id = (
                    time_map.get(rec["publish_time"]) if rec["publish_time"] else None
                )

                if pub_date_id is None or pub_time_id is None:
                    continue

                upd_date_id = (
                    date_map.get(rec["update_date"]) if rec["update_date"] else None
                )
                upd_time_id = (
                    time_map.get(rec["update_time"]) if rec["update_time"] else None
                )

                hashdiff = rec["hashdiff"]

                cursor.execute(
                    """
                    SELECT id, hashdiff
                    FROM dds.dim_posts
                    WHERE slug = %s AND is_current = true
                    """,
                    (slug,),
                )
                existing = cursor.fetchone()

                if existing and existing[1] == hashdiff:
                    slug_to_id[slug] = existing[0]
                    continue

                existing_id = None
                if existing:
                    existing_id = existing[0]

                try:
                    if existing_id is not None:
                        cursor.execute(
                            """
                            UPDATE dds.dim_posts
                            SET valid_to = %s,
                                is_current = false
                            WHERE id = %s
                            """,
                            (loaded_at, existing_id),
                        )

                    cursor.execute(
                        """
                        INSERT INTO dds.dim_posts (
                            slug, author_id, content_raw,
                            valid_from, valid_to, is_current,
                            publish_date_id, publish_time_id,
                            update_date_id, update_time_id,
                            hashdiff
                        )
                        VALUES (%s, %s, %s,
                                %s, %s, %s,
                                %s, %s,
                                %s, %s,
                                %s)
                        RETURNING id
                        """,
                        (
                            slug,
                            author_id,
                            rec["content_raw"],
                            loaded_at,
                            future_valid_to,
                            True,
                            pub_date_id,
                            pub_time_id,
                            upd_date_id,
                            upd_time_id,
                            hashdiff,
                        ),
                    )
                    new_id = cursor.fetchone()[0]
                    conn.commit()
                    slug_to_id[slug] = new_id

                except Exception:
                    conn.rollback()
                    if existing_id is not None:
                        slug_to_id[slug] = existing_id
                    continue

        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

        return slug_to_id

    def _upsert_fact_posts_metrics(
        self,
        fact_posts_metrics_raw: List[Dict[str, Any]],
        slug_to_post_id: Dict[str, int],
        date_map: Dict[str, int],
        time_map: Dict[str, int],
    ) -> None:
        if not fact_posts_metrics_raw:
            return

        conn = self.pghook.get_conn()
        cursor = conn.cursor()

        try:
            rows = []
            for rec in fact_posts_metrics_raw:
                slug = rec["slug"]
                post_id = slug_to_post_id.get(slug)
                if not post_id:
                    continue

                date_id = date_map.get(rec["loaded_date"])
                time_id = time_map.get(rec["loaded_time"])

                if date_id is None or time_id is None:
                    continue

                rows.append(
                    (
                        post_id,
                        rec["total_unique_impressions"],
                        rec["num_comments"],
                        rec["num_reactions"],
                        date_id,
                        time_id,
                    )
                )

            if not rows:
                return

            sql = """
                INSERT INTO dds.fact_posts_metrics (
                    post_id, total_unique_impressions, num_comments, num_reactions,
                    date_id, time_id
                )
                VALUES %s
                ON CONFLICT (post_id, date_id, time_id) DO UPDATE
                SET total_unique_impressions = EXCLUDED.total_unique_impressions,
                    num_comments = EXCLUDED.num_comments,
                    num_reactions = EXCLUDED.num_reactions
            """
            execute_values(cursor, sql, rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def _insert_fact_posts_reactions(
        self,
        fact_posts_reactions_raw: List[Dict[str, Any]],
        slug_to_post_id: Dict[str, int],
        parties_map: Dict[str, int],
        date_map: Dict[str, int],
        time_map: Dict[str, int],
    ) -> None:
        if not fact_posts_reactions_raw:
            return

        conn = self.pghook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT id, reaction FROM dds.dim_reactions")
            reaction_map = {name: _id for _id, name in cursor.fetchall()}

            rows = []
            for rec in fact_posts_reactions_raw:
                slug = rec["slug"]
                post_id = slug_to_post_id.get(slug)
                if not post_id:
                    continue

                reaction = rec["reaction"]
                reaction_id = reaction_map.get(reaction)
                if reaction_id is None:
                    continue

                user_name = rec["user_name"]
                user_id = parties_map.get(user_name)
                if user_id is None:
                    continue

                date_id = date_map.get(rec["date"])
                time_id = time_map.get(rec["time"])

                if date_id is None or time_id is None:
                    continue

                rows.append(
                    (
                        post_id,
                        reaction_id,
                        user_id,
                        date_id,
                        time_id,
                    )
                )

            if not rows:
                return

            sql = """
                INSERT INTO dds.fact_posts_reactions (
                    post_id, reaction_id, user_id, date_id, time_id
                )
                VALUES %s
                ON CONFLICT DO NOTHING
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
        dim_posts_raw: List[Dict[str, Any]],
        fact_posts_metrics_raw: List[Dict[str, Any]],
        fact_posts_reactions_raw: List[Dict[str, Any]],
        unique_parties: Set[Tuple[str, str]],
        unique_dates: Set[str],
        unique_times: Set[str],
        loaded_at: datetime,
    ) -> None:

        date_map, time_map = self._load_dates_times_maps(unique_dates, unique_times)
        parties_map = self._load_parties_map(unique_parties, loaded_at)

        name_to_party_id = parties_map

        slug_to_post_id = self._upsert_dim_posts_scd2(
            dim_posts_raw=dim_posts_raw,
            date_map=date_map,
            time_map=time_map,
            parties_map=name_to_party_id,
            loaded_at=loaded_at,
        )

        self._upsert_fact_posts_metrics(
            fact_posts_metrics_raw=fact_posts_metrics_raw,
            slug_to_post_id=slug_to_post_id,
            date_map=date_map,
            time_map=time_map,
        )

        self._insert_fact_posts_reactions(
            fact_posts_reactions_raw=fact_posts_reactions_raw,
            slug_to_post_id=slug_to_post_id,
            parties_map=name_to_party_id,
            date_map=date_map,
            time_map=time_map,
        )
