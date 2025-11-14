from huggingface_hub import list_papers
from datetime import datetime
import logging

class PapersExtractor:
    def __init__(self, query="*"):
        self.query = query

    def extract(self):
        return list(list_papers(query=self.query))

class PapersTransformer:
    def format_datetime(self, dt):
        if not dt:
            return None
        if isinstance(dt, datetime):
            return dt.isoformat()
        return str(dt)

    def transform(self, paper):
        try:
            data = {
                'id': getattr(paper, 'id', None),
                'authors': getattr(paper, 'authors', []) or [],
                'published_at': self.format_datetime(getattr(paper, 'published_at', None)),
                'title': getattr(paper, 'title', None),
                'summary': getattr(paper, 'summary', None),
                'upvotes': getattr(paper, 'upvotes', 0),
                'discussion_id': getattr(paper, 'discussion_id', None),
                'source': getattr(paper, 'source', None),
                'comments': getattr(paper, 'comments', 0),
                'submitted_at': self.format_datetime(getattr(paper, 'submitted_at', None)),
                'submitted_by': getattr(paper, 'submitted_by', None),
            }
            return data
        except Exception as e:
            logging.error(f"PapersTransformer error: {e}")
            return {}


class PapersLoader:
    def __init__(self, pg_hook, mongo_hook, clickhouse_hook):
        self.pg_hook = pg_hook
        self.mongo_hook = mongo_hook
        self.clickhouse_hook = clickhouse_hook

    def load_postgresql(self, papers: list):
        pg_conn = self.pg_hook.get_conn()
        cursor = pg_conn.cursor()
        insert_sql = """
        INSERT INTO papers (id, published_at, title, summary, upvotes, discussion_id,
                            source, comments, submitted_at, submitted_by)
        VALUES (%(id)s, %(published_at)s, %(title)s, %(summary)s, %(upvotes)s, %(discussion_id)s,
                %(source)s, %(comments)s, %(submitted_at)s, %(submitted_by)s)
        ON CONFLICT (id) DO UPDATE SET
            upvotes = EXCLUDED.upvotes,
            comments = EXCLUDED.comments,
            submitted_at = EXCLUDED.submitted_at;
        """
        for paper in papers:
            cursor.execute(insert_sql, paper)
        pg_conn.commit()
        cursor.close()

    def load_mongodb(self, papers: list):
        collection = self.mongo_hook.get_collection("papers")
        for paper in papers:
            collection.update_one({'id': paper['id']}, {'$set': paper}, upsert=True)

    def load_clickhouse(self, papers: list):
        for paper in papers:
            query = f"""
            INSERT INTO papers (id, published_at, title, summary, upvotes, discussion_id,
                source, comments, submitted_at, submitted_by) VALUES (
                '{paper.get('id', '')}', '{paper.get('published_at', '1970-01-01T00:00:00')}',
                '{paper.get('title', '')}', '{paper.get('summary', '')}', {paper.get('upvotes', 0)},
                '{paper.get('discussion_id', '')}', '{paper.get('source', '')}', {paper.get('comments', 0)},
                '{paper.get('submitted_at', '1970-01-01T00:00:00')}', '{paper.get('submitted_by', '')}'
            )
            """
            self.clickhouse_hook.run(query)
