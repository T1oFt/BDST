import requests
import json
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

            logging.info(batch[-1].get('publishedAt'))
            
            try:
                last_post_time = datetime.fromisoformat(batch[-1].get('publishedAt').replace('Z', '+00:00'))
            except Exception as e:
                logging.error(f"Failed to extract last post time: {e}")
                break

            if last_post_time is None:
                logging.error("Failed to extract last post time")
                break

            if last_post_time < one_day_ago:
                break

            n += 10
        return posts

class PostsTransformer:
    def format_datetime(self, dt_str):
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except Exception:
            return None
    
    def transform(self, post):
        try:
            mention_names = [
                m.get('name') for m in post.get('mentions', [])
                if m.get('name') is not None
            ]

            attachment_types = [
                a.get('type') for a in post.get('attachments', [])
                if a.get('type') is not None
            ]
            
            data = {
                'slug': post.get('slug'),
                'author': post.get('author', {}).get('name'),
                'content_raw': post.get('rawContent'),
                'published_at': self.format_datetime(post.get('publishedAt')),
                'updated_at': self.format_datetime(post.get('updatedAt')),
                'total_unique_impressions': post.get('totalUniqueImpressions'),
                'num_comments': post.get('numComments'),
                'reactions': post.get('reactions', []),
                'mentions': mention_names,
                'attachments': attachment_types,
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

    def load_all(self, posts: list, loaded_at: datetime):
        self.load_postgresql(posts, loaded_at)
        self.load_mongodb(posts, loaded_at)
        self.load_clickhouse(posts, loaded_at)

    def load_postgresql(self, posts: list, loaded_at: datetime):
        pg_conn = self.pg_hook.get_conn()
        cursor = pg_conn.cursor()
        insert_sql = """
        INSERT INTO ods.posts (
            slug, author, content_raw, published_at, updated_at,
            total_unique_impressions, num_comments, reactions, mentions, attachments, loaded_at
        ) VALUES (
            %(slug)s, %(author)s, %(content_raw)s, %(published_at)s,
            %(updated_at)s, %(total_unique_impressions)s, %(num_comments)s, %(reactions)s, %(mentions)s, %(attachments)s, %(loaded_at)s
        );
        """
        for post in posts:
            post_with_ts = {**post, 'loaded_at': loaded_at, 'reactions': json.dumps(post['reactions'])}
            cursor.execute(insert_sql, post_with_ts)
        pg_conn.commit()
        cursor.close()

    def load_mongodb(self, posts: list, loaded_at: datetime):
        collection = self.mongo_hook.get_collection("posts_ods")
        for post in posts:
            doc = {**post, 'loaded_at': loaded_at}
            collection.insert_one(doc)

    def load_clickhouse(self, posts: list, loaded_at: datetime):
        client = self.clickhouse_hook.get_conn()
        rows = []
        for post in posts:
            rows.append((
                post.get('slug'),
                post.get('author'),
                post.get('content_raw'),
                post.get('published_at'),
                post.get('updated_at'),
                post.get('total_unique_impressions', 0) or 0,
                post.get('num_comments', 0) or 0,
                json.dumps(post.get('reactions', [])),        
                post.get('mentions', []) or [],
                post.get('attachments', []) or [],
                loaded_at
            ))
        client.execute(
            """
            INSERT INTO posts_ods (
                slug, author, content_raw, published_at, updated_at,
                total_unique_impressions, num_comments, reactions, mentions, attachments, loaded_at
            ) VALUES
            """,
            rows
        )
