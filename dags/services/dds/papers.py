import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Optional, Dict, Any, Tuple, List, Set
from datetime import datetime
from psycopg2.extras import execute_values
import re

from services.dds.help_dims import get_or_create_authors_ids, get_or_create_keywords_ids
from services.dds.parties import upsert_parties_scd2
from services.dds.bridge import FlexibleBridgeFiller

class PapersDataExtractor:
    def __init__(self, pg_hook: PostgresHook):
        self.hook = pg_hook
    
    def get_papers_by_loaded_at(self, 
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
        FROM ods.papers 
        WHERE loaded_at = '{loaded_at}'::timestamp
        """
        
        df = self.hook.get_pandas_df(query)
        
        return df


class PapersDataTransformer:
    def transform_papers_df(
        self,
        df: pd.DataFrame,
    ) -> Tuple[
        List[Dict[str, Any]],  # dim_papers_raw
        Set[str],              # unique_dates
        Set[str],              # unique_times
        Set[str],              # unique_authors
        Set[str],              # unique_keywords
        Set[Tuple[str, str]],              # unique_parties
        Dict[str, List[str]],      # source_id -> [authors]
        Dict[str, List[str]],      # source_id -> [keywords]
    ]:
        dim_papers_raw: List[Dict[str, Any]] = []
        unique_dates: Set[str] = set()
        unique_times: Set[str] = set()
        unique_authors: Set[str] = set()
        unique_keywords: Set[str] = set()
        unique_parties: Set[str, str] = set()
        source_authors: Dict[int, List[str]] = {}
        source_keywords: Dict[int, List[str]] = {}

        for _, row in df.iterrows():
            ods_id = row['id']

            pub_date_str, pub_time_str = self._extract_date_time_str(row.get('published_at'))
            sub_date_str, sub_time_str = self._extract_date_time_str(row.get('submitted_at'))

            for d in (pub_date_str, sub_date_str):
                if d:
                    unique_dates.add(d)
            for t in (pub_time_str, sub_time_str):
                if t:
                    unique_times.add(t)

            submitted_by = (row.get('submitted_by') or '').strip() or None
            organization = (row.get('organization') or '').strip() or None
            if submitted_by:
                unique_parties.add((submitted_by, 'user'))
            if organization:
                unique_parties.add((organization, 'org'))

            authors = row.get('authors', [])
            keywords = row.get('ai_keywords', [])
            unique_authors.update(authors)
            unique_keywords.update(keywords)

            source_authors[ods_id] = authors
            source_keywords[ods_id] = keywords

            dim_papers_raw.append({
                'source_id': ods_id,
                'title': row.get('title', ''),
                'summary': row.get('summary', ''),
                'upvotes': row.get('upvotes', 0),
                'comments': row.get('comments', 0),
                'ai_summary': row.get('ai_summary', ''),
                'publish_date_id': pub_date_str,
                'publish_time_id': pub_time_str,
                'submitted_date_id': sub_date_str,
                'submitted_time_id': sub_time_str,
                'submitted_by_id': submitted_by,
                'organization_id': organization,
            })

        return (
            dim_papers_raw, 
            unique_dates, 
            unique_times, 
            unique_authors, 
            unique_keywords, 
            unique_parties, 
            source_authors, 
            source_keywords,
        )

    @staticmethod
    def _extract_date_time_str(val) -> Tuple[Optional[str], Optional[str]]:
        if pd.isna(val) or not val:
            return None, None
        dt = pd.to_datetime(val)
        return dt.date().strftime('%Y-%m-%d'), (dt.time().strftime('%H') + ":00:00")

    @staticmethod
    def finalize_papers_for_load(
        dim_papers_raw: List[Dict[str, Any]],
        date_map: Dict[str, int],
        time_map: Dict[str, int],
        party_map: Dict[str, int],
    ) -> List[Dict[str, Any]]:

        finalized: List[Dict[str, Any]] = []

        for rec in dim_papers_raw:
            rec_copy = rec.copy()

            d = rec_copy.get('publish_date_id')
            t = rec_copy.get('publish_time_id')
            sd = rec_copy.get('submitted_date_id')
            st = rec_copy.get('submitted_time_id')
            sub = rec_copy.get('submitted_by_id')
            org = rec_copy.get('organization_id')

            rec_copy['publish_date_id'] = date_map.get(d) if d else None
            rec_copy['publish_time_id'] = time_map.get(t) if t else None
            rec_copy['submitted_date_id'] = date_map.get(sd) if sd else None
            rec_copy['submitted_time_id'] = time_map.get(st) if st else None
            rec_copy['submitted_by_id'] = party_map.get(sub) if sub else None
            rec_copy['organization_id'] = party_map.get(org) if org else None

            finalized.append(rec_copy)

        return finalized


class PapersDataLoader:
    """
    Loads data to:
      - dds.dim_papers
      - dds.parties (SCD2)
      - dds.dim_keywords
      - dds.dim_authors
      - dds.bridge_papers_keywords
      - dds.bridge_papers_authors
    """

    def __init__(self, postgres_conn_id: str = "dds_postgres"):
        self.postgres_conn_id = postgres_conn_id
        self.pghook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.bridge_filler = FlexibleBridgeFiller(self.pghook)

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
            {"name": name, "type": party_type} for [name, party_type] in unique_parties
        ] 

        ids_by_name = upsert_parties_scd2(
            parties_data=parties_payload,
            pghook=self.pghook,
            loaded_at=loaded_at,
        )

        return ids_by_name

    def _load_dim_papers(
        self,
        dim_papers_ready: List[Dict[str, Any]],
    ) -> Dict[int, int]:
        if not dim_papers_ready:
            return {}

        df = pd.DataFrame(dim_papers_ready)

        conn = self.pghook.get_conn()
        cursor = conn.cursor()
        source_to_paper: Dict[int, int] = {}

        try:
            cols = [
                "source_id",
                "title",
                "summary",
                "upvotes",
                "comments",
                "ai_summary",
                "publish_date_id",
                "publish_time_id",
                "submitted_date_id",
                "submitted_time_id",
                "submitted_by_id",
                "organization_id",
            ]

            execute_values_sql = """
                INSERT INTO dds.dim_papers (
                    source_id, title, summary,
                    upvotes, comments, ai_summary,
                    publish_date_id, publish_time_id,
                    submitted_date_id, submitted_time_id,
                    submitted_by_id, organization_id
                )
                VALUES %s
                ON CONFLICT (source_id) DO NOTHING
                RETURNING id, source_id
            """

            def _normalize(v):
                return None if pd.isna(v) else v

            rows = []
            for _, row in df[cols].iterrows():
                rows.append(tuple(_normalize(row[c]) for c in cols))

            execute_values(
                cursor,
                execute_values_sql,
                rows,
            )


            for paper_id, source_id in cursor.fetchall():
                source_to_paper[source_id] = int(paper_id)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

        return source_to_paper

    def load_all(
        self,
        dim_papers_raw: List[Dict[str, Any]],
        unique_dates: Set[str],
        unique_times: Set[str],
        unique_authors: Set[str],
        unique_keywords: Set[str],
        unique_parties: Set[str],
        source_authors: Dict[int, List[str]],
        source_keywords: Dict[int, List[str]],
        loaded_at: datetime,
    ) -> None:
        """
          1. date/time → maps
          2. authors/keywords → dim_*/maps
          3. parties → dds.parties/map
          4. finalize dim_papers
          5. insert dim_papers → {source_id: paper_id}
          6. bridge_papers_authors / bridge_papers_keywords
        """

        date_map, time_map = self._load_dates_times_maps(unique_dates, unique_times)

        authors_id_map = get_or_create_authors_ids(
            author_names=unique_authors,
            pghook=self.pghook,
        )

        keywords_id_map = get_or_create_keywords_ids(
            keyword_names=unique_keywords,
            pghook=self.pghook,
        )

        parties_id_map = self._load_parties_map(unique_parties, loaded_at)

        dim_papers_ready = PapersDataTransformer.finalize_papers_for_load(
            dim_papers_raw=dim_papers_raw,
            date_map=date_map,
            time_map=time_map,
            party_map=parties_id_map,
        )

        source_to_paper_id = self._load_dim_papers(dim_papers_ready)

        paper_authors_bridge = {}
        for source_id, authors in source_authors.items():
            paper_id = source_to_paper_id.get(source_id)
            if not paper_id:
                continue
            author_ids = [
                authors_id_map[a]
                for a in authors
                if a in authors_id_map
            ]
            if author_ids:
                paper_authors_bridge[paper_id] = author_ids

        paper_keywords_bridge = {}
        for source_id, keywords in source_keywords.items():
            paper_id = source_to_paper_id.get(source_id)
            if not paper_id:
                continue
            keyword_ids = [
                keywords_id_map[k]
                for k in keywords
                if k in keywords_id_map
            ]
            if keyword_ids:
                paper_keywords_bridge[paper_id] = keyword_ids

        if paper_authors_bridge:
            self.bridge_filler.fill_with_pandas(
                table_name="bridge_papers_authors",
                col1_name="paper_id",
                col2_name="author_id",
                data=paper_authors_bridge,
            )

        if paper_keywords_bridge:
            self.bridge_filler.fill_with_pandas(
                table_name="bridge_papers_keywords",
                col1_name="paper_id",
                col2_name="keyword_id",
                data=paper_keywords_bridge,
            )
