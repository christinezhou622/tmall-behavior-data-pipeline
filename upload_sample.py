import os
from google.cloud import storage

# 1. 你的 JSON 密钥文件名
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "de-zoomcamp-tmall-85347b8176d4.json"

def upload_to_gcs(bucket_name, source_file, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    print(f"🚀 正在上传 {source_file}...")
    blob.upload_from_filename(source_file)
    print(f"✅ 上传成功！路径: gs://{bucket_name}/{destination_blob}")

if __name__ == "__main__":
    # 2. 你的桶名（和 main.tf 一致）
    MY_BUCKET = "tmall-data-lake-de-zoomcamp-tmall" 
    
    # 3. 使用那个 155KB 的 Sample 文件名
    # 注意：如果你的文件名里有空格或特殊字符，请直接复制文件名填入
    SAMPLE_FILE = "(sample)sam_tianchi_2014002_rec_tmall_log.csv"
    
    upload_to_gcs(MY_BUCKET, SAMPLE_FILE, "raw/sample_log.csv")