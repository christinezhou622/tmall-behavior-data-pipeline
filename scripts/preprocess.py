import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# --- 请核对文件名 ---
input_file = 'tianchi_2014002_rec_tmall_log_parta.txt' 
output_parquet = 'tmall_log_parta.parquet'

if not os.path.exists(input_file):
    print(f"❌ 找不到文件：{input_file}")
else:
    # 减小 chunk_size 到 50 万行，确保内存安全
    chunk_size = 500000 
    writer = None

    print(f"🚀 正在处理 19GB 巨型文件，请耐心等待...")

    try:
        # 天池数据可能是没有表头的，如果运行报错，我们再加 names=[...]
        # 这里先假设它有表头或者格式标准
        for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size, low_memory=False)):
            if i == 0:
                table = pa.Table.from_pandas(chunk)
                writer = pq.ParquetWriter(output_parquet, table.schema)
            
            table = pa.Table.from_pandas(chunk)
            writer.write_table(table)
            
            if (i + 1) % 10 == 0: # 每 500 万行报一次进度
                print(f"✅ 已处理 { (i+1) * 0.5 } 百万行...")

        if writer:
            writer.close()
        print(f"🎉 转换成功！生成的 Parquet 应该只有 2-3GB 左右。")
        
    except Exception as e:
        print(f"😱 运行出错: {e}")