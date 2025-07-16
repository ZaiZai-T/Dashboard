"""
封装 BigQuery 日志操作：
- has_been_processed：判断文件是否已处理过
- log_load：写入处理日志
"""

from google.cloud import bigquery
from datetime import datetime

BQ_LOG_TABLE = "vernal-guide-465502-j6.etl_log.etl_file_log"
bq = bigquery.Client()

def has_been_processed(file_name):
    QUERY = f"""
        SELECT COUNT(*) as cnt FROM `{BQ_LOG_TABLE}`
        WHERE file_name = @file_name
    """
    job = bq.query(QUERY, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_name", "STRING", file_name)
        ]
    ))
    result = [row for row in job][0]
    return result.cnt > 0

def log_load(file_name, data_type, rows_loaded, status):
    table = bq.get_table(BQ_LOG_TABLE)
    bq.insert_rows_json(table, [{
        "file_name": file_name,
        "data_type": data_type,
        "load_time": datetime.utcnow().isoformat(),
        "rows_loaded": rows_loaded,
        "status": status
    }])
