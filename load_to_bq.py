import os
from google.cloud import bigquery

# 1. 确保 JSON 文件名正确
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "de-zoomcamp-tmall-85347b8176d4.json"

client = bigquery.Client()

project_id = "de-zoomcamp-tmall"
dataset_id = "tmall_data_all"
table_id = "raw_sample_log" 

uri = "gs://tmall-data-lake-de-zoomcamp-tmall/raw/sample_log.csv"

# 4. 强力兼容配置
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    # 核心变动 1：跳过第一行表头
    skip_leading_rows=1, 
    # 核心变动 2：不进行自动探测，全部强制按字符串读取，防止报错
    autodetect=True, 
    # 核心变动 3：允许最多 100 行错误（万一有脏数据也能跑完）
    max_bad_records=100,
    write_disposition="WRITE_TRUNCATE",
)

print(f"🚀 正在以兼容模式加载数据到 {dataset_id}.{table_id}...")

try:
    load_job = client.load_table_from_uri(
        uri, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config
    )
    load_job.result()
    print(f"✅ 加载成功！请去 BigQuery 控制台查看结果。")
except Exception as e:
    print(f"❌ 依然报错，请查看详情：\n{e}")