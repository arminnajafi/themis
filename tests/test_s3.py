import boto3
import moto
import themis.aws.s3 as abs_s3


@moto.mock_s3
def test_upload_resource():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test_bucket")
    uploaded_uri = abs_s3.upload_resource(
        path="test/test.txt",
        bucket_name="test_bucket",
        upload_folder_path="folder",
        s3=s3)
    assert "s3://test_bucket/folder" in uploaded_uri
    assert "test/test.txt" in uploaded_uri
    result = s3.get_object(Bucket="test_bucket", Key=uploaded_uri.split("://")[1].split("/", 1)[1])
    assert result['Body'].read().decode() == "some text"
