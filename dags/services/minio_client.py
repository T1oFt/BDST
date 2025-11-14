import os
import json
import boto3
from dotenv import load_dotenv

load_dotenv()

class MinioClient:
    def __init__(self):
        self.bucket = os.getenv('MINIO_BUCKET', 'etl-data')
        self.client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9002'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadminpassword'),
            region_name='us-east-1',
        )

    def upload_json(self, data, key):
        json_data = json.dumps(data).encode('utf-8')
        self.client.put_object(Bucket=self.bucket, Key=key, Body=json_data)

    def download_json(self, key):
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return json.loads(obj['Body'].read())
