import csv, time, shutil, logging, os
from utils.uploader import upload_csv,download_csv, read_csv_by_sheet
import numpy as np
import datetime, time
import pandas as pd


def write_and_upload_csv(data_list, csv_file, table, sheet_name, firstline=None, clear=True, wait=None):
    """
    将列表数据写入 CSV 文件，并尝试上传到指定表格。

    参数：
    - data_list: 数据列表
    - csv_file: 输出 CSV 文件路径
    - table: 目标表名
    - sheet_name: 目标工作表名
    - firstline: 可选的第一行标题
    - clear: 上传时是否清空表格
    - wait: 失败重试间隔（秒），默认不等待或设置为 5 秒
    """
    # 将数据写入 CSV 文件
    with open(csv_file, 'w', newline='', encoding="utf-8") as csv_result:
        writer = csv.writer(csv_result)

        # 写入第一行（如有）
        if firstline and isinstance(firstline, (list, tuple)):
            writer.writerow(firstline)

        # 写入数据
        writer.writerows(data_list)

    # 上传 CSV 文件
    retry_attempts = 20  # 最大重试次数
    wait_time = wait if wait is not None else 0  # 默认不等待

    for attempt in range(retry_attempts):
        try:
            # 调用上传函数
            upload_csv(csv_file, table, sheet_name, clear=clear)
            break  # 成功后退出循环
        except Exception as e:
            if attempt < retry_attempts - 1:
                logging.warning(f"Upload failed (attempt {attempt + 1}). Retrying in {wait_time} secs... Error: {e}")
                time.sleep(wait_time)
            else:
                logging.error(f"Max retry attempts reached. Failed to upload to {table} - {sheet_name}. Error: {e}")




