import boto3
from elt_app.utils.config import AWS_BUCKET_ACESS_KEY,AWS_BUCKET_SECRET_KEY,REGION_NAME,BUCKET_NAME

def get_last_file_s3(prefix:str,extension=".parquet"): 
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_BUCKET_ACESS_KEY,
        aws_secret_access_key=AWS_BUCKET_SECRET_KEY,
        region_name=REGION_NAME
    )

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    )

    if "Contents" not in response:
        return None

    # Lọc ra tất cả file parquet
    parquet_files = [
        obj for obj in response["Contents"]
        if obj["Key"].endswith(extension)
    ]

    if not parquet_files:
        return None

    # Lấy file mới nhất dựa trên LastModified
    latest_file = max(parquet_files, key=lambda x: x["LastModified"])

    # Trả về path chuẩn
    return f"s3a://{BUCKET_NAME}/{latest_file['Key']}"
