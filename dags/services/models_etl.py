from datetime import datetime
import logging

from huggingface_hub import list_models

from services.categories import (
    language_codes, libraries, tasks
)


class ModelsExtractor:
    def __init__(self, limit=50, sort='trending_score'):
        self.sort = sort
        self.limit = limit

    def extract(self):
        return list(list_models(sort=self.sort, limit=self.limit))
    

class ModelsTransformer:
    def __init__(self):
        self.language_codes = language_codes
        self.libraries = libraries
        self.all_tasks = tasks

    def transform(self, raw_model):
        try:
            tags = raw_model.get('tags', [])
            categories = self.extract_categories(tags)

            data = {
                'id': raw_model.get('id'),
                'owner': self.extract_owner(raw_model.get('id', '')),
                'author': raw_model.get('author'),
                'created_at': self.format_datetime(raw_model.get('created_at')),
                'downloads': raw_model.get('downloads', 0),
                'likes': raw_model.get('likes', 0),
                'library_name': raw_model.get('library_name'),
                'pipeline_tag': raw_model.get('pipeline_tag'),
                'trending_score': raw_model.get('trending_score', 0),
                'language': categories.get('language'),
                'library': categories.get('library'),
                'task': categories.get('task'),
                'license': categories.get('license'),
                'base_models': categories.get('base_models'),
                'modification': categories.get('modification'),
                'region': categories.get('region'),
                'diffusers_pipeline': categories.get('diffusers_pipeline'),
                'deploy': categories.get('deploy'),
                'dataset': categories.get('dataset'),
                'arxiv': categories.get('arxiv'),
            }
            return data
        except Exception as e:
            logging.error(f"ModelsTransformer error: {e}")
            return {}

    def extract_owner(self, model_id):
        return model_id.split('/')[0] if '/' in model_id else None
    
    def format_datetime(self, dt):
        if not dt:
            return None
        if isinstance(dt, datetime):
            return dt.isoformat()
        return str(dt)
    
    def extract_categories(self, tags):
        categories = {
            'language': None,
            'library': None,
            'task': None,
            'license': None,
            'base_models': None,
            'modification': None,
            'region': None,
            'diffusers_pipeline': None,
            'deploy': None,
            'dataset': None,
            'arxiv': None,
        }
        
        languages = []
        libraries = []
        tasks = []
        base_models = set()
        deploy_platforms = []
        datasets = []
        arxiv_papers = []

        for tag in tags:
            tag_lower = tag.lower()
            
            if ':' not in tag and tag in self.language_codes:
                languages.append(tag)
            
            elif ':' not in tag and tag_lower in self.libraries:
                libraries.append(tag_lower)

            elif ':' not in tag and tag_lower in self.all_tasks:
                tasks.append(tag_lower)

            elif tag.startswith('license:'):
                categories['license'] = tag.split('license:', 1)[1]

            elif tag.startswith('base_model:'):
                model_info = tag.split(':')
                try:
                    base_models.add(model_info[2])
                    categories['modification'] = model_info[1]
                except IndexError:
                    base_models.add(model_info[1])

            elif tag.startswith('region:'):
                categories['region'] = tag.split('region:', 1)[1]

            elif tag.startswith('diffusers:'):
                pipeline = tag.split('diffusers:', 1)[1]
                categories['diffusers_pipeline'] = pipeline

            elif tag.startswith('deploy:'):
                platform = tag.split('deploy:', 1)[1]
                deploy_platforms.append(platform)

            elif tag.startswith('dataset:'):
                dataset_name = tag.split('dataset:', 1)[1]
                datasets.append(dataset_name)

            elif tag.startswith('arxiv:'):
                paper_id = tag.split('arxiv:', 1)[1]
                arxiv_papers.append(paper_id)

        if languages: categories['language'] = languages
        if libraries: categories['library'] = libraries
        if tasks: categories['task'] = tasks
        if base_models: categories['base_models'] = list(base_models)
        if deploy_platforms: categories['deploy'] = deploy_platforms
        if datasets: categories['dataset'] = datasets
        if arxiv_papers: categories['arxiv'] = arxiv_papers
        
        return categories
    

class ModelsLoader:
    def __init__(self, pg_hook, mongo_hook, clickhouse_hook):
        self.pg_hook = pg_hook
        self.mongo_hook = mongo_hook
        self.clickhouse_hook = clickhouse_hook

    def load_all(self, models: list, loaded_at: datetime):
        self.load_postgresql(models, loaded_at)
        self.load_mongodb(models, loaded_at)
        self.load_clickhouse(models, loaded_at)

    def load_postgresql(self, models: list, loaded_at: datetime):
        pg_conn = self.pg_hook.get_conn()
        cursor = pg_conn.cursor()
        insert_sql = """
        INSERT INTO models_ods (
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

    def load_mongodb(self, models: list, loaded_at: datetime):
        collection = self.mongo_hook.get_collection("models_ods")
        for model in models:
            model_doc = {**model, 'loaded_at': loaded_at}
            collection.insert_one(model_doc)

    def load_clickhouse(self, models: list, loaded_at: datetime):
        client = self.clickhouse_hook.get_conn()
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
        client.execute(
            """
            INSERT INTO models_ods (
                id, owner, author, created_at, downloads, likes, library_name,
                pipeline_tag, trending_score, language, library, task, license,
                base_models, modification, region, diffusers_pipeline, deploy,
                dataset, arxiv, loaded_at
            ) VALUES
            """,
            rows
        )
