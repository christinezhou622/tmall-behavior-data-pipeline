terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  # 1. 确保你的 JSON 文件名和下面一模一样
  credentials = file("./de-zoomcamp-tmall-85347b8176d4.json") 
  
  # 2. 已经填好你的真实 Project ID
  project     = "de-zoomcamp-tmall" 
  region      = "asia-east1"
}

# 创建数据湖 (Google Cloud Storage Bucket)
resource "google_storage_bucket" "data-lake-bucket" {
  # ❗ 注意：Bucket 名字全球唯一。如果报错说 "already taken"，
  # 请在下面这行名字后面随便加几个数字，比如：tmall-data-lake-de-zoomcamp-20260328
  name          = "tmall-data-lake-de-zoomcamp-tmall" 
  location      = "ASIA"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# 创建数据仓库 (BigQuery Dataset)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "tmall_data_all"
  project    = "de-zoomcamp-tmall" 
  location   = "asia-east1"
}