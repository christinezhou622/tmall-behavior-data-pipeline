# 🛒 Tmall User Behavior Analysis Pipeline (GCP & Terraform)

## 📊 Project Overview
This project builds a robust, end-to-end data engineering pipeline to process and visualize 19GB of Tmall user behavior data. The pipeline automates infrastructure deployment, handles large-scale data ingestion, performs complex SQL transformations, and provides business insights through a live dashboard.

**Total Rows Processed:** Over 100 Million records.

## 🏗️ Architecture
- **Infrastructure as Code (IaC):** Terraform for automated GCP resource provisioning (GCS, BigQuery).
- **Data Lake:** Google Cloud Storage (GCS) for raw data storage.
- **Data Warehouse:** Google BigQuery for high-performance analytical processing.
- **ETL Logic:** Python scripts with chunked uploading for large-scale Parquet files (7.6GB+).
- **Data Transformation:** Advanced SQL with Regex parsing to resolve schema-on-read challenges.
- **BI Tool:** Google Looker Studio for interactive visualization.

## 🚀 Key Technical Challenges & Solutions

### 1. Handling Large-Scale Uploads (7.6GB Parquet)
**Challenge:** Standard upload scripts failed due to SSL timeouts and network instability during the 7.6GB file transfer.
**Solution:** Developed a custom Python uploader with **Chunked Transfer** (50MB chunks), increased timeouts (1800s), and a **Retry Logic** to ensure successful ingestion to GCS.

### 2. Resolving "Messy" Schema (The V6 Regex Breakthrough)
**Challenge:** The raw Parquet file was encoded as a single `STRUCT` column with missing delimiters and illegal characters in field names, causing BigQuery ingestion failures.
**Solution:** Implemented **Character Map V2** for ingestion and used **Advanced Regex Pattern Matching** in BigQuery SQL to surgically extract fields (User ID, Item ID, Action, Timestamp) from the chaotic raw string without relying on delimiters.

## 📈 Dashboard Insights
![Dashboard Preview](your-dashboard-screenshot-link-here)
- **漏斗分析 (Funnel):** Page View (93.6%) -> Add to Cart -> Favorite -> Purchase.
- **活跃趋势 (Active Trends):** Real-time tracking of daily active users (DAU) reaching peaks of 4M+ records per day.

## 📂 Project Structure
- `/terraform`: Infrastructure configuration files (`main.tf`).
- `/scripts`: Python ETL scripts for GCS upload and BQ loading.
- `/sql`: Data transformation and cleaning logic.

## 🛠️ How to Run
1. **Provision Infrastructure:** Run `terraform apply` in the `/terraform` folder.
2. **Data Ingestion:** Run `python scripts/upload_big_file.py` (ensure you have GCP service account JSON).
3. **BigQuery Load:** Run `python scripts/load_full_to_bq.py` with Character Map V2 enabled.
4. **Transform:** Execute the SQL scripts in BigQuery to generate the `fact_user_behavior` table.
