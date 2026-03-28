import os
from google.cloud import bigquery

# 设置密钥
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "de-zoomcamp-tmall-85347b8176d4.json"

client = bigquery.Client()
project_id = "de-zoomcamp-tmall"
dataset_id = "tmall_data_all"
table_id = "raw_full_log"

uri = "gs://tmall-data-lake-de-zoomcamp-tmall/raw/tmall_log_full.parquet"

# 核心配置：使用 Character Map V2 强行加载
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    # ❗ 这里的设置是解决报错的关键
    # 它告诉 BigQuery：如果列名不合法，请自动修复它 (V2 映射)
    column_name_character_map="V2"
)

print(f"🚀 正在使用 V2 映射模式强行注入 7.6GB 全量数据...")

try:
    load_job = client.load_table_from_uri(
        uri, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config
    )
    load_job.result()
    print(f"🎉 奇迹真的发生了！全量数据加载成功。")
    print(f"💡 提示：因为列名是乱的，稍后我们在 SQL 转换时，将使用位置或者列搜索来提取数据。")
except Exception as e:
    print(f"❌ 依然失败: {e}")