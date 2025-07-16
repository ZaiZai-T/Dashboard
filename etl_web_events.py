from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago

PROJECT_ID   = "vernal-guide-465502-j6"
DATASET_RAW  = "raw"
DATASET_PROD = "prod"
TABLE_RAW    = "web_events_raw"
TABLE_PROD   = "web_events"

BUCKET       = "vernal-guide-465502-j6-etl-bucket"
RAW_PREFIX   = "raw/"
ARCHIVE_PREFIX = "archive/{{ ds }}/"      # 每天一个子目录

default_args = {"start_date": days_ago(1)}

with DAG(
    "etl_web_events_hourly",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
) as dag:

    # 1) GCS → BigQuery raw 追加，不删文件
    load_raw = GCSToBigQueryOperator(
        task_id="load_to_bq_raw",
        bucket=BUCKET,
        source_objects=[f"{RAW_PREFIX}*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}:{DATASET_RAW}.{TABLE_RAW}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    # 2) 移动已处理文件到 archive/（copy+delete）
    archive = GCSToGCSOperator(
        task_id="archive_csv",
        source_bucket=BUCKET,
        source_object=f"{RAW_PREFIX}*.csv",
        destination_bucket=BUCKET,
        destination_object=ARCHIVE_PREFIX,
        move_object=True,          # copy + delete
    )

    # 3) 清洗并 append 到 prod
    transform = BigQueryInsertJobOperator(
        task_id="clean_transform_to_prod",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_PROD}.{TABLE_PROD}`
                SELECT
                  user_id,
                  TIMESTAMP(event_time) AS event_time,
                  LOWER(TRIM(page))     AS page,
                  UPPER(TRIM(device))   AS device
                FROM `{PROJECT_ID}.{DATASET_RAW}.{TABLE_RAW}`
                WHERE DATE(event_time) = CURRENT_DATE()  -- 只拉今天进来的
                """,
                "useLegacySql": False,
            }
        },
    )

    load_raw >> archive >> transform
