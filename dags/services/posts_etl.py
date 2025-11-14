import requests
from datetime import datetime, timedelta, timezone
import logging


class PostsExtractor:
    def __init__(self):
        pass
    
    def extract(self, skip=0):
        params = {"skip": skip}
        response = requests.get("https://huggingface.co/api/posts", params=params)
        if response.status_code != 200:
            logging.error(f"Failed to fetch posts, status_code={response.status_code}")
            return []
        return response.json().get('socialPosts', [])
    
    def daily_extract(self):
        now = datetime.now(timezone.utc)
        one_day_ago = now - timedelta(hours=24)
        n = 0
        posts = []
        while True:
            batch = self.extract(skip=n)
            if not batch:
                break

            posts.extend(batch)

            last_post_time = batch[-1].get('published_at')
            if last_post_time is None:
                logging.error("Failed to extract last post time")
                break

            if last_post_time < one_day_ago:
                break

            n += 10

class PostsTransformer:
    def format_datetime(self, dt_str):
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except Exception:
            return None
    
    def transform(self, post):
        try:
            data = {
                'slug': post.get('slug'),
                'author_name': post.get('author', {}).get('name'),
                'author_id': post.get('author', {}).get('_id'),
                'content_raw': post.get('rawContent'),
                'published_at': self.format_datetime(post.get('publishedAt')),
                'updated_at': self.format_datetime(post.get('updatedAt')),
                'total_unique_impressions': post.get('totalUniqueImpressions'),
                'num_comments': post.get('numComments'),
                'reactions': post.get('reactions', []),
                'mentions': post.get('mentions', []),
                'attachments': post.get('attachments', []),
            }
            return data
        except Exception as e:
            logging.error(f"PostsTransformer error: {e}")
            return {}


class PostsLoader:
    def __init__(self, pg_hook, mongo_hook, clickhouse_hook):
        self.pg_hook = pg_hook
        self.mongo_hook = mongo_hook
        self.clickhouse_hook = clickhouse_hook

    def load_postgresql(self, posts: list):
        pg_conn = self.pg_hook.get_conn()
        cursor = pg_conn.cursor()
        insert_sql = """
        INSERT INTO posts (slug, author_name, author_id, content_raw, published_at, updated_at,
                           total_unique_impressions, num_comments)
        VALUES (%(slug)s, %(author_name)s, %(author_id)s, %(content_raw)s, %(published_at)s,
                %(updated_at)s, %(total_unique_impressions)s, %(num_comments)s)
        ON CONFLICT (slug) DO UPDATE SET
            updated_at = EXCLUDED.updated_at,
            total_unique_impressions = EXCLUDED.total_unique_impressions,
            num_comments = EXCLUDED.num_comments;
        """
        for post in posts:
            cursor.execute(insert_sql, post)
        pg_conn.commit()
        cursor.close()

    def load_mongodb(self, posts: list):
        collection = self.mongo_hook.get_collection("posts")
        for post in posts:
            collection.update_one({'slug': post['slug']}, {'$set': post}, upsert=True)

    def load_clickhouse(self, posts: list):
        for post in posts:
            query = f"""
            INSERT INTO posts (slug, author_name, author_id, content_raw, published_at, updated_at,
                total_unique_impressions, num_comments) VALUES (
                '{post.get('slug', '')}', '{post.get('author_name', '')}', '{post.get('author_id', '')}',
                '{post.get('content_raw', '')}', '{post.get('published_at', '1970-01-01T00:00:00')}',
                '{post.get('updated_at', '1970-01-01T00:00:00')}', {post.get('total_unique_impressions', 0)},
                {post.get('num_comments', 0)}
            )
            """
            self.clickhouse_hook.run(query)
