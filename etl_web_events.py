from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

# ====== 全局常量 ======
PROJECT_ID   = "vernal-guide-465502-j6"
BUCKET       = "vernal-guide-465502-j6-etl-bucket"
RAW_PREFIX   = "raw/*.csv"            # 待加载文件
ARCHIVE_PRE  = "archive/"             # 归档目录
RAW_TABLE    = "raw.web_events_raw"   # BigQuery 中的 raw 表
PROD_TABLE   = "prod.web_events"      # BigQuery 中的 prod 表

default_args = {
    "owner":      "airflow",
    "start_date": datetime(2025, 7, 15),   # 随意，过去时间即可
    "retries":    0,                       # 出错就直接 FAIL，不自动重试
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_web_events_hourly",
    default_args=default_args,
    schedule_interval="@hourly",     # 每小时跑一次
    catchup=False,
    tags=["demo", "gcs", "bq"],
) as dag:

    # 1️⃣ GCS ➜ BigQuery raw（自动建表 / 追加）
    load_csv_to_raw = GCSToBigQueryOperator(
        task_id="gcs_to_bq_raw",
        bucket=BUCKET,
        source_objects=[RAW_PREFIX],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,      # 第一行是列头
        autodetect=True,          # 自动推断 schema
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
    )

    # 2️⃣ raw ➜ prod（简单清洗 + 追加）
    transform_to_prod = BigQueryInsertJobOperator(
        task_id="transform_to_prod",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROD_TABLE}` (user_id, event_time, page, device)
                    SELECT
                        user_id,
                        TIMESTAMP(event_time),
                        LOWER(TRIM(page)),
                        UPPER(TRIM(device))
                    FROM `{RAW_TABLE}`
                    WHERE TIMESTAMP(event_time) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
                """,
                "useLegacySql": False,
            }
        },
    )

    # 3️⃣ 归档已处理文件到 archive/
    archive_files = GCSDeleteObjectsOperator(
        task_id="archive_files",
        bucket_name=BUCKET,
        prefix="raw/",              # 删除 raw/ 下所有已消费 CSV
        trigger_rule="all_success"  # 只有前两步都成功才归档
    )

    load_csv_to_raw >> transform_to_prod >> archive_files

