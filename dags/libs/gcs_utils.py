"""
封装所有 Google Cloud Storage 操作：
- list_raw_csv：列出 raw 区域的 csv 文件
- copy_blob：复制对象到指定 archive 路径
"""

from google.cloud import storage

BUCKET_NAME = "vernal-guide-465502-j6-etl-bucket"
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

def list_raw_csv():
    return [b for b in bucket.list_blobs(prefix="raw/") if b.name.endswith(".csv")]

def copy_blob(src, dst):
    blob = bucket.blob(src)
    bucket.copy_blob(blob, bucket, dst)
