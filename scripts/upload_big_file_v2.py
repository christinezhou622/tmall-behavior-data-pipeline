import os
from google.cloud import storage
from google.api_core import retry

# 1. 密钥
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "de-zoomcamp-tmall-85347b8176d4.json"

def upload_large_file(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # 核心修正 1：分块改小一点（比如 50MB），这样单次请求不容易超时
    blob.chunk_size = 50 * 1024 * 1024 

    print(f"🚀 启动 V2 增强版搬运程序...")
    print(f"📦 正在搬运: {source_file_name}")

    try:
        # 核心修正 2：将超时时间从默认 120s 增加到 1800s (30分钟)
        # 核心修正 3：增加重试策略，应对网络波动
        blob.upload_from_filename(
            source_file_name, 
            timeout=1800, 
            retry=retry.Retry(predicate=retry.if_transient_error)
        )
        print(f"✅ 奇迹发生了！7.6GB 巨兽已安全到达 GCS。")
    except Exception as e:
        print(f"❌ 还是断了，报错详情: {e}")
        print("💡 小建议：如果一直断，可以检查一下梯子/代理是否稳定，或者尝试关闭代理直接直连。")

if __name__ == "__main__":
    MY_BUCKET = "tmall-data-lake-de-zoomcamp-tmall"
    BIG_FILE = "tmall_log_parta.parquet"
    upload_large_file(MY_BUCKET, BIG_FILE, "raw/tmall_log_full.parquet")