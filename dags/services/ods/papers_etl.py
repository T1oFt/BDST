from datetime import datetime
import logging

from huggingface_hub import paper_info
import requests
from bs4 import BeautifulSoup
import re


class PapersExtractor:

    def _get_daily_paper_ids(self):
        date = datetime.now().strftime("%Y-%m-%d")
        daily_url = f"https://huggingface.co/papers/date/{date}"
        
        try:
            response = requests.get(daily_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            paper_links = soup.find_all('a', href=re.compile(r'/papers/[^/]+'))
            
            paper_ids = []
            for link in paper_links:
                href = link.get('href')
                if href and '/papers/' in href:
                    paper_id = href.split('/papers/')[-1].strip('/')
                    if re.match(r'^\d{4}\.\d{5}$', paper_id) and paper_id not in paper_ids:
                        paper_ids.append(paper_id)

            return paper_ids
            
        except Exception as e:
            print(f"Ошибка парсинга страницы {daily_url}: {e}")
            return []


    def extract(self):
        daily_paper_ids = self._get_daily_paper_ids()
        
        papers_data = []
        for paper_id in daily_paper_ids:
            paper = paper_info(paper_id)
            paper.submitted_by = paper.submitted_by.username
            if paper:
                papers_data.append(paper)
        
        return papers_data
    
    def extract_papers_info(self, ids):
        papers = []
        for paper_id in ids:
            paper = paper_info(paper_id)
            if paper:
                papers.append(paper)
        return papers


class PapersTransformer:
    def format_datetime(self, dt):
        if not dt:
            return None
        if isinstance(dt, datetime):
            return dt.isoformat()
        return str(dt)

    def transform(self, paper):
        try:
            org = paper.get('organization', None)
            if org is not None:
                org = org.get('name', None)
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
                'ai_summary': paper.get('ai_summary', None),
                'ai_keywords': paper.get('ai_keywords', []),
                'organization': org,
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
        INSERT INTO ods.papers (
            id, authors, published_at, title, summary, upvotes, discussion_id,
            source, comments, submitted_at, submitted_by, ai_summary, ai_keywords, organization, loaded_at
        ) VALUES (
            %(id)s, %(authors)s, %(published_at)s, %(title)s, %(summary)s, %(upvotes)s, %(discussion_id)s,
            %(source)s, %(comments)s, %(submitted_at)s, %(submitted_by)s, %(ai_summary)s, %(ai_keywords)s, %(organization)s, %(loaded_at)s
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
                paper.get('authors') or [],
                paper.get('published_at'),
                paper.get('title'),
                paper.get('summary'),
                paper.get('upvotes', 0) or 0,
                paper.get('discussion_id'),
                paper.get('source'),
                paper.get('comments', 0) or 0,
                paper.get('submitted_at'),
                paper.get('submitted_by'),
                paper.get('ai_summary'),
                paper.get('ai_keywords', []) or [],
                paper.get('organization'),
                loaded_at
            ))
        client.execute(
            """
            INSERT INTO papers_ods (
                id, authors, published_at, title, summary, upvotes, discussion_id,
                source, comments, submitted_at, submitted_by, ai_summary, ai_keywords, organization, loaded_at
            ) VALUES
            """,
            rows
        )
