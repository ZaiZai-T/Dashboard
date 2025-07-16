"""
DAG: etl_archive_router
-----------------------------------------------------------
功能：
  • 定时扫描 GCS bucket 的 raw/ 目录
  • 根据 config/route_config.yaml 中的规则，
    将文件复制到指定 archive 子目录
  • 严格模式：若存在未匹配文件则任务失败（可在 YAML 开关）
作者：Junyan 项目 · 2025‑07‑16
-----------------------------------------------------------
上传路径：
  gs://<composer‑bucket>/dags/etl_archive_router.py
"""

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# 业务逻辑函数
from lib.archive_router import route_and_copy_files

# DAG 默认参数
default_args = {
    "owner": "etl_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="etl_archive_router",
    description="Move raw CSV files to the correct archive folders",
    start_date=pendulum.datetime(2025, 7, 16, tz="UTC"),
    schedule_interval="*/15 * * * *",  # 每 15 分钟运行一次
    catchup=False,
    default_args=default_args,
    tags=["etl", "routing", "gcs"],
) as dag:

    with TaskGroup(group_id="route_files") as route_group:
        copy_task = PythonOperator(
            task_id="copy_raw_to_archive",
            python_callable=route_and_copy_files,
        )

    # 如后续你想加别的任务（例如通知 / 统计），可以在这里 >> 链接
    # route_group >> downstream_task
