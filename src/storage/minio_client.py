from minio import Minio
from minio.error import S3Error
import logging
import os

class MinIOClient:
    def __init__(self, 
                 endpoint=None,
                 access_key='minioadmin',
                 secret_key='minioadmin123'):
        
        # Use environment variable or default
        if endpoint is None:
            endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
            
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.logger = logging.getLogger(__name__)
        self._create_buckets()
    
    def _create_buckets(self):
        """Create required buckets"""
        buckets = ['tracks-data', 'artists-data', 'albums-data', 'emotion-data', 'models', 'processed-data']
        
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    self.logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                self.logger.error(f"Error creating bucket {bucket}: {e}")
    
    def upload_file(self, bucket_name, file_path, object_name):
        """Upload file to MinIO"""
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            self.logger.info(f"Uploaded {file_path} to {bucket_name}/{object_name}")
            return True
        except S3Error as e:
            self.logger.error(f"Error uploading file: {e}")
            return False
    
    def download_file(self, bucket_name, object_name, file_path):
        """Download file from MinIO"""
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            self.logger.info(f"Downloaded {bucket_name}/{object_name} to {file_path}")
            return True
        except S3Error as e:
            self.logger.error(f"Error downloading file: {e}")
            return False
    
    def list_objects(self, bucket_name):
        """List objects in bucket"""
        try:
            objects = self.client.list_objects(bucket_name)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            self.logger.error(f"Error listing objects: {e}")
            return []