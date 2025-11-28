import json
import time
from datetime import datetime, timedelta


TEMP_TABLE_PREFIX = "models_bench_"


class BenchmarkModelsLoader:
    def __init__(self, pg_hook, mongo_hook, clickhouse_hook, temp_suffix):
        self.pg_hook = pg_hook
        self.mongo_hook = mongo_hook
        self.ch_hook = clickhouse_hook
        self.temp_suffix = temp_suffix
        self.pg_table = f"{TEMP_TABLE_PREFIX}{temp_suffix}"
        self.mongo_collection_name = f"{TEMP_TABLE_PREFIX}{temp_suffix}"
        self.ch_table = f"{TEMP_TABLE_PREFIX}{temp_suffix}"

    def create_temp_tables(self):
        pg_conn = self.pg_hook.get_conn()
        with pg_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.pg_table} (
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
        pg_conn.commit()

        ch_client = self.ch_hook.get_conn()
        ch_client.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.ch_table} (
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
            ) ENGINE = Memory;
        """)

    def create_dimension_tables(self):
        pg_conn = self.pg_hook.get_conn()
        with pg_conn.cursor() as cur:
            dim_table = f"{self.pg_table}_dim"
            cur.execute(f"DROP TABLE IF EXISTS {dim_table}")
            cur.execute(f"""
                CREATE TABLE {dim_table} AS
                SELECT DISTINCT
                    modification,
                    md5(modification)::uuid AS modification_id
                FROM {self.pg_table}
                WHERE modification IS NOT NULL AND modification != '';
            """)
            cur.execute(f"CREATE INDEX ON {dim_table}(modification);")
        pg_conn.commit()

        ch = self.ch_hook.get_conn()
        dim_table = f"{self.ch_table}_dim"
        ch.execute(f"DROP TABLE IF EXISTS {dim_table}")
        ch.execute(f"""
            CREATE TABLE {dim_table} ENGINE = Memory AS
            SELECT DISTINCT
                modification,
                hex(MD5(modification)) AS modification_id
            FROM {self.ch_table}
            WHERE NOT empty(modification);
        """)

    def drop_temp_objects(self):
        pg_conn = self.pg_hook.get_conn()
        with pg_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {self.pg_table}")
            cur.execute(f"DROP TABLE IF EXISTS {self.pg_table}_dim")
        pg_conn.commit()
        
        ch_client = self.ch_hook.get_conn()
        ch_client.execute(f"DROP TABLE IF EXISTS {self.ch_table}")
        ch_client.execute(f"DROP TABLE IF EXISTS {self.ch_table}_dim")

        db = self.mongo_hook.get_conn().get_database()
        db.drop_collection(self.mongo_collection_name)
        db.drop_collection(f"{self.mongo_collection_name}_dim")
    

    def load_postgresql(self, models: list, loaded_at: datetime):
        start = time.time()
        pg_conn = self.pg_hook.get_conn()
        cursor = pg_conn.cursor()
        insert_sql = f"""
        INSERT INTO {self.pg_table} (
            id, owner, author, created_at, downloads, likes, library_name,
            pipeline_tag, trending_score, language, library, task, license,
            base_models, modification, region, diffusers_pipeline, deploy,
            dataset, arxiv, loaded_at
        ) VALUES (
            %(id)s, %(owner)s, %(author)s, %(created_at)s, %(downloads)s, %(likes)s,
            %(library_name)s, %(pipeline_tag)s, %(trending_score)s,
            %(language)s, %(library)s, %(task)s, %(license)s,
            %(base_models)s, %(modification)s, %(region)s, %(diffusers_pipeline)s,
            %(deploy)s, %(dataset)s, %(arxiv)s, %(loaded_at)s
        );
        """
        for model in models:
            model_with_ts = {**model, 'loaded_at': loaded_at}
            cursor.execute(insert_sql, model_with_ts)
        pg_conn.commit()
        cursor.close()
        duration = time.time() - start
        print(f"[PostgreSQL] Запись {len(models)} записей: {duration:.2f} сек")
        return duration


    def load_mongodb(self, models: list, loaded_at: datetime):
        start = time.time()
        collection = self.mongo_hook.get_collection(self.mongo_collection_name)
        docs = [{**model, 'loaded_at': loaded_at} for model in models]
        collection.insert_many(docs)
        duration = time.time() - start
        print(f"[MongoDB] Запись {len(models)} записей: {duration:.2f} сек")
        return duration


    def load_clickhouse(self, models: list, loaded_at: datetime):
        start = time.time()
        client = self.ch_hook.get_conn()
        rows = []
        for model in models:
            rows.append((
                model.get('id'),
                model.get('owner'),
                model.get('author'),
                model.get('created_at'),
                model.get('downloads', 0) or 0,
                model.get('likes', 0) or 0,
                model.get('library_name'),
                model.get('pipeline_tag'),
                model.get('trending_score', 0) or 0,
                model.get('language') or [],
                model.get('library') or [],
                model.get('task') or [],
                model.get('license'),
                model.get('base_models') or [],
                model.get('modification'),
                model.get('region'),
                model.get('diffusers_pipeline'),
                model.get('deploy') or [],
                model.get('dataset') or [],
                model.get('arxiv') or [],
                loaded_at
            ))
        client.execute(f"""
            INSERT INTO {self.ch_table} (
                id, owner, author, created_at, downloads, likes, library_name,
                pipeline_tag, trending_score, language, library, task, license,
                base_models, modification, region, diffusers_pipeline, deploy,
                dataset, arxiv, loaded_at
            ) VALUES
        """, rows)
        duration = time.time() - start
        print(f"[ClickHouse] Запись {len(models)} записей: {duration:.2f} сек")
        return duration

    def run_benchmarks(self):
        results = {}

        #PostgreSQL
        pg_conn = self.pg_hook.get_conn()
        cur = pg_conn.cursor()
        pg_res = {}
        queries_pg = {
            "avg_likes": f"SELECT AVG(likes) FROM {self.pg_table}",
            "top10_downloads": f"SELECT id, downloads FROM {self.pg_table} ORDER BY downloads DESC LIMIT 10",
            "modification_stats": f"""
                SELECT 
                    modification,
                    COUNT(*) as row_count
                FROM {self.pg_table}
                WHERE modification IS NOT NULL
                GROUP BY modification
                ORDER BY row_count DESC;
            """
        }
        for name, q in queries_pg.items():
            start = time.time()
            cur.execute(q)
            cur.fetchall()
            pg_res[name] = time.time() - start
        cur.close()
        results["postgresql"] = pg_res

        #ClickHouse
        ch = self.ch_hook.get_conn()
        ch_res = {}
        queries_ch = {
            "avg_likes": f"SELECT avg(likes) FROM {self.ch_table}",
            "top10_downloads": f"SELECT id, downloads FROM {self.ch_table} ORDER BY downloads DESC LIMIT 10",
            "modification_stats": f"""
                SELECT
                    modification,
                    count(*) AS cnt
                FROM {self.ch_table}
                WHERE modification != ''
                GROUP BY modification
                ORDER BY cnt DESC
            """
        }
        for name, q in queries_ch.items():
            start = time.time()
            ch.execute(q)
            ch_res[name] = time.time() - start
        results["clickhouse"] = ch_res

        #MongoDB
        coll = self.mongo_hook.get_collection(self.mongo_collection_name)
        mongo_res = {}
        mongo_pipelines = {
            "avg_likes": [{"$group": {"_id": None, "avg": {"$avg": "$likes"}}}],
            "top10_downloads": [{"$sort": {"downloads": -1}}, {"$limit": 10}],
            "modification_stats": [
                {"$match": {"modification": {"$ne": ""}}},
                {
                    "$group": {
                        "_id": "$modification",
                        "total_mod_count": {"$sum": 1}
                    }
                },
                {"$sort": {"total_mod_count": -1}}
            ]
        }
        for name, pipeline in mongo_pipelines.items():
            start = time.time()
            list(coll.aggregate(pipeline))
            mongo_res[name] = time.time() - start
        results["mongodb"] = mongo_res

        return results
    
    def run_join_benchmarks(self):
        results = {}

        # PostgreSQL JOIN
        pg_conn = self.pg_hook.get_conn()
        cur = pg_conn.cursor()
        start = time.time()
        cur.execute(f"""
            SELECT d.modification, COUNT(*) AS cnt
            FROM {self.pg_table} f
            JOIN {self.pg_table}_dim d ON f.modification = d.modification
            GROUP BY d.modification
            ORDER BY cnt DESC
            LIMIT 10;
        """)
        cur.fetchall()
        pg_time = time.time() - start
        cur.close()
        results["postgresql"] = {"join_modification_stats": pg_time}

        # ClickHouse JOIN
        ch = self.ch_hook.get_conn()
        start = time.time()
        ch.execute(f"""
            SELECT d.modification, count(*) AS cnt
            FROM {self.ch_table} AS f
            ANY INNER JOIN {self.ch_table}_dim AS d ON f.modification = d.modification
            GROUP BY d.modification
            ORDER BY cnt DESC
            LIMIT 10;
        """)
        ch_time = time.time() - start
        results["clickhouse"] = {"join_modification_stats": ch_time}

        # MongoDB $lookup
        coll = self.mongo_hook.get_collection(self.mongo_collection_name)
        dim_coll_name = f"{self.mongo_collection_name}_dim"

        pipeline_dim = [
            {"$match": {"modification": {"$ne": ""}}},
            {"$group": {"_id": "$modification"}},
            {"$project": {"_id": 0, "modification": "$_id"}}
        ]
        dim_docs = list(coll.aggregate(pipeline_dim))
        dim_coll = self.mongo_hook.get_collection(dim_coll_name)
        if dim_docs:
            dim_coll.insert_many(dim_docs)

        start = time.time()
        list(coll.aggregate([
            {"$match": {"modification": {"$ne": ""}}},
            {"$lookup": {
                "from": dim_coll_name,
                "localField": "modification",
                "foreignField": "modification",
                "as": "dim"
            }},
            {"$unwind": "$dim"},
            {"$group": {"_id": "$dim.modification", "cnt": {"$sum": 1}}},
            {"$sort": {"cnt": -1}},
            {"$limit": 10}
        ]))
        mongo_time = time.time() - start

        results["mongodb"] = {"join_modification_stats": mongo_time}

        return results

