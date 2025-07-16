"""
读取 config/route_config.yaml，
判断 raw/ 中每个文件应复制到哪个 archive 子目录，并复制文件。
如果无匹配路由，则报错。
如果文件已处理过，则跳过（查重）。
"""

import os, yaml
from pathlib import PurePosixPath
from .gcs_utils import list_raw_csv, copy_blob
from .bq_loader import has_been_processed

# 配置文件路径适配 dags/ 中运行
CONFIG = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "../config/route_config.yaml")))

def route_and_copy_files():
    copied = []
    for blob in list_raw_csv():
        file_name = PurePosixPath(blob.name).name

        if has_been_processed(file_name):
            continue  # 已处理过则跳过

        matched = False
        for prefix, rule in CONFIG["rules"].items():
            if file_name.startswith(prefix):
                dest = f"{rule['archive_prefix']}{file_name}"
                copy_blob(blob.name, dest)
                copied.append((file_name, rule['archive_prefix']))
                matched = True
                break

        if not matched:
            raise ValueError(f"未找到文件 [{file_name}] 的匹配路由规则，请检查 config/route_config.yaml")

    return copied
