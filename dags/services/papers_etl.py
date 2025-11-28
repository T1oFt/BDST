from datetime import datetime
import logging

from huggingface_hub import list_papers


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
                'id': paper.get('id'),
                'authors': paper.get('authors') or [],
                'published_at': self.format_datetime(paper.get('published_at')),
                'title': paper.get('title'),
                'summary': paper.get('summary'),
                'upvotes': paper.get('upvotes', 0),
                'discussion_id': paper.get('discussion_id'),
                'source': paper.get('source'),
                'comments': paper.get('comments', 0),
                'submitted_at': self.format_datetime(paper.get('submitted_at')),
                'submitted_by': paper.get('submitted_by'),
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

    def load_all(self, papers: list, loaded_at: datetime):
        self.load_postgresql(papers, loaded_at)
        self.load_mongodb(papers, loaded_at)
        self.load_clickhouse(papers, loaded_at)

    def load_postgresql(self, papers: list, loaded_at: datetime):
        pg_conn = self.pg_hook.get_conn()
        cursor = pg_conn.cursor()
        insert_sql = """
        INSERT INTO papers_ods (
            id, published_at, title, summary, upvotes, discussion_id,
            source, comments, submitted_at, submitted_by, loaded_at
        ) VALUES (
            %(id)s, %(published_at)s, %(title)s, %(summary)s, %(upvotes)s, %(discussion_id)s,
            %(source)s, %(comments)s, %(submitted_at)s, %(submitted_by)s, %(loaded_at)s
        );
        """
        for paper in papers:
            paper_with_ts = {**paper, 'loaded_at': loaded_at}
            cursor.execute(insert_sql, paper_with_ts)
        pg_conn.commit()
        cursor.close()

    def load_mongodb(self, papers: list, loaded_at: datetime):
        collection = self.mongo_hook.get_collection("papers_ods")
        for paper in papers:
            doc = {**paper, 'loaded_at': loaded_at}
            collection.insert_one(doc)

    def load_clickhouse(self, papers: list, loaded_at: datetime):
        client = self.clickhouse_hook.get_conn()
        rows = []
        for paper in papers:
            rows.append((
                paper.get('id'),
                paper.get('published_at'),
                paper.get('title'),
                paper.get('summary'),
                paper.get('upvotes', 0) or 0,
                paper.get('discussion_id'),
                paper.get('source'),
                paper.get('comments', 0) or 0,
                paper.get('submitted_at'),
                paper.get('submitted_by'),
                loaded_at
            ))
        client.execute(
            """
            INSERT INTO papers_ods (
                id, published_at, title, summary, upvotes, discussion_id,
                source, comments, submitted_at, submitted_by, loaded_at
            ) VALUES
            """,
            rows
        )
