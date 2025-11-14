from huggingface_hub import list_models
from datetime import datetime
import logging

class ModelsExtractor:
    def __init__(self, limit=50):
        self.limit = limit
    
    def extract(self):
        return list(list_models(limit=self.limit))

class ModelsTransformer:
    def extract_owner(self, model_id):
        return model_id.split('/')[0] if '/' in model_id else None
    
    def format_datetime(self, dt):
        if not dt:
            return None
        if isinstance(dt, datetime):
            return dt.isoformat()
        return str(dt)

    def transform(self, raw_model):
        try:
            data = {
                'id': getattr(raw_model, 'id', None),
                'owner': self.extract_owner(getattr(raw_model, 'id', '')),
                'author': getattr(raw_model, 'author', None),
                'sha': getattr(raw_model, 'sha', None),
                'created_at': self.format_datetime(getattr(raw_model, 'created_at', None)),
                'last_modified': self.format_datetime(getattr(raw_model, 'last_modified', None)),
                'private': getattr(raw_model, 'private', False),
                'disabled': getattr(raw_model, 'disabled', None),
                'downloads': getattr(raw_model, 'downloads', 0),
                'likes': getattr(raw_model, 'likes', 0),
                'library_name': getattr(raw_model, 'library_name', None),
                'tags': getattr(raw_model, 'tags', []) or [],
                'pipeline_tag': getattr(raw_model, 'pipeline_tag', None),
                'trending_score': getattr(raw_model, 'trending_score', 0),
            }
            return data
        except Exception as e:
            logging.error(f"ModelsTransformer error: {e}")
            return {}

class ModelsLoader:
    def __init__(self, pg_hook, mongo_hook, clickhouse_hook):
        self.pg_hook = pg_hook
        self.mongo_hook = mongo_hook
        self.clickhouse_hook = clickhouse_hook

    def load_postgresql(self, models: list):
        pg_conn = self.pg_hook.get_conn()
        cursor = pg_conn.cursor()
        insert_sql = """
        INSERT INTO models (id, owner, author, sha, created_at, last_modified, private, disabled, downloads,
                            downloads_all_time, likes, library_name, tags, pipeline_tag, trending_score)
        VALUES (%(id)s, %(owner)s, %(author)s, %(sha)s, %(created_at)s, %(last_modified)s, %(private)s, %(disabled)s,
                %(downloads)s, %(downloads_all_time)s, %(likes)s, %(library_name)s, %(tags)s, %(pipeline_tag)s, %(trending_score)s)
        ON CONFLICT (id) DO UPDATE SET
            downloads = EXCLUDED.downloads,
            likes = EXCLUDED.likes,
            last_modified = EXCLUDED.last_modified,
            trending_score = EXCLUDED.trending_score;
        """
        for model in models:
            model['tags'] = ','.join(model.get('tags', []))
            cursor.execute(insert_sql, model)
        pg_conn.commit()
        cursor.close()

    def load_mongodb(self, models: list):
        collection = self.mongo_hook.get_collection("models")
        for model in models:
            collection.update_one({'id': model['id']}, {'$set': model}, upsert=True)

    def load_clickhouse(self, models: list):
        for model in models:
            tags_str = ','.join(model.get('tags', []))
            query = f"""
            INSERT INTO models (id, owner, author, sha, created_at, last_modified,
                private, disabled, downloads, downloads_all_time, likes, library_name,
                tags, pipeline_tag, trending_score) VALUES (
                '{model.get('id', '')}', '{model.get('owner', '')}', '{model.get('author', '')}', '{model.get('sha', '')}',
                '{model.get('created_at', '1970-01-01T00:00:00')}', '{model.get('last_modified', '1970-01-01T00:00:00')}',
                {1 if model.get('private') else 0}, {1 if model.get('disabled') else 0},
                {model.get('downloads', 0)}, {model.get('downloads_all_time', 0)}, {model.get('likes', 0)},
                '{model.get('library_name', '')}', '{tags_str}', '{model.get('pipeline_tag', '')}', {model.get('trending_score', 0)}
            )
            """
            self.clickhouse_hook.run(query)

