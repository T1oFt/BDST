# dags/hooks/minio_hook.py
import json
from datetime import datetime
from typing import Any, Dict
from dateutil import parser

from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowException
import boto3


class MinioHook(BaseHook):

    def __init__(self, minio_conn_id: str = "minio_default") -> None:
        super().__init__()
        self.minio_conn_id = minio_conn_id

    def get_conn(self):
        conn = BaseHook.get_connection(self.minio_conn_id)

        if not conn:
            raise AirflowException(f"Connection {self.minio_conn_id} not found")

        extra_config = conn.extra_dejson

        client = boto3.client(
            "s3",
            endpoint_url=extra_config.get("endpoint_url"),
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name=extra_config.get("region_name", "us-east-1"),
        )
        return client

    @staticmethod
    def _json_serial(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def upload_json(self, data: Any, bucket: str, key: str) -> None:
        json_data = json.dumps(data, default=self._json_serial).encode("utf-8")
        client = self.get_conn()
        client.put_object(Bucket=bucket, Key=key, Body=json_data)
        self.log.info("Uploaded JSON to s3://%s/%s", bucket, key)

    def download_json(self, bucket: str, key: str) -> Dict:
        client = self.get_conn()
        obj = client.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read())
        self.log.info("Downloaded JSON from s3://%s/%s", bucket, key)
        return data
    
    def deserialize_datetimes(self, records, datetime_fields):
        for record in records:
            for field in datetime_fields:
                if field in record and isinstance(record[field], str):
                    record[field] = parser.isoparse(record[field])
        return records