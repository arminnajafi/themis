import boto3
import uuid
import logging

import importlib.resources as resolver
import themis.resources as resources

from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


def upload_resource(
        path: str,
        bucket_name: str,
        upload_folder_path: str,
        region_name: str = None,
        s3: S3Client = None) -> str:
    logger.info(f"Upload resource {path} to s3://{bucket_name}/{upload_folder_path}")
    if s3 is None:
        s3 = boto3.client("s3", region_name=region_name)

    resource_path = resolver.files(resources).joinpath(path)
    upload_path = f"{upload_folder_path}/{uuid.uuid4()}/{path}"
    with resolver.as_file(resource_path) as f:
        s3.put_object(
            Body=f.read_bytes(),
            Bucket=bucket_name,
            Key=upload_path)
    return f"s3://{bucket_name}/{upload_path}"
