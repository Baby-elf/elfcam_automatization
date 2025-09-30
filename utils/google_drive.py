from pathlib import Path
import logging
import mimetypes
from typing import Optional
from googleapiclient.http import MediaFileUpload
import os.path, io
from os import listdir
from os.path import isfile, join
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import requests
import re
from tqdm import tqdm
import shutil

def cancel_repetive(drive_service, image_folder_name_id):
    result = None
    all_files = []
    registred_file_names = []
    i = 0
    while True:
        if result != None and not result.get('nextPageToken'):
            break
        if result == None:
            listRequest = drive_service.files().list(q="'" + image_folder_name_id + "' in parents",fields="nextPageToken, files(id, name, mimeType)")
        else:
            listRequest = drive_service.files().list(q="'" + image_folder_name_id + "' in parents",fields="nextPageToken, files(id, name, mimeType)", pageToken = result.get('nextPageToken'))
        result = listRequest.execute()
        files = result.get('files')
        for file in files:
            all_files.append(file)
            registred_file_names.append(file['name'])
        #print("checking images: " + str(i * 100) )
        i+= 1
    return all_files,registred_file_names

def google_auth(switch=1):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive']
    token_path = "utils/google-drive-api/token.json"
    if switch == 1:
        credentials = "utils/google-drive-api/credentials.json"
    elif switch == 2:
        credentials = "utils/google-drive-api/client_secrets.json"

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'w') as token:
            # print("Writing new token")
            token.write(creds.to_json())

    drive_service = build('drive', 'v3', credentials=creds)
    return drive_service


def upload_file(
        folder_name: str = "Pdf",
        mypath: str = "pdfs",
        fn: str = "2021-03-10 00:10.pdf",
        file_type: Optional[str] = None,
        skip: bool = True,
        create_if_missing: bool = False,
):
    """
    将本地文件上传到 Google Drive 指定文件夹。

    参数：
        folder_name: 目标 Drive 文件夹名称。
        mypath: 本地目录路径。
        fn: 本地文件名（含扩展名）。
        file_type: 文件类型（如 "pdf"、"html"、"txt" 等）。若为 None 则自动识别 MIME。
        skip: 若为 True，检测到同名文件已存在则跳过上传。
        create_if_missing: 若目标文件夹不存在，是否自动创建。

    返回：
        成功：返回上传文件的 fileId（字符串）
        被跳过或失败：返回 ""（空字符串）
        异常：返回 None
    依赖：
        - 需要你已有的 google_auth() 与 cancel_repetive(drive_service, folder_id) 两个函数：
            google_auth() -> 认证并返回 Drive service 对象
            cancel_repetive(...) -> 返回 (all_files, registred_file_names)
    """
    try:
        # 1) 获取 Drive service
        drive_service = google_auth()

        # 2) 查找或创建目标文件夹
        q = f"mimeType = 'application/vnd.google-apps.folder' and name = '{folder_name}' and trashed = false"
        folder_list = drive_service.files().list(q=q, fields="files(id, name)").execute()
        files = folder_list.get("files", [])

        if not files:
            if not create_if_missing:
                raise FileNotFoundError(f"文件夹 '{folder_name}' 未找到。")
            # 创建文件夹
            folder_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            folder = drive_service.files().create(body=folder_metadata, fields="id,name").execute()
            folder_name_id = folder["id"]
            logging.info(f"未找到文件夹，已创建：'{folder_name}'，ID: {folder_name_id}")
        else:
            folder_name_id = files[0]["id"]
            logging.info(f"找到文件夹 '{folder_name}'，ID: {folder_name_id}")

        # 3) 获取已注册文件名，避免重复上传
        all_files, registred_file_names = cancel_repetive(drive_service, folder_name_id)

        # 4) 组装本地文件路径并检查
        file_path = Path(mypath) / fn
        if not file_path.exists():
            raise FileNotFoundError(f"本地文件不存在：{file_path}")

        logging.info(f"准备上传文件: {file_path}")

        # 5) 计算 MIME type（优先使用 file_type 覆盖；否则自动识别）
        #    内置映射，补充部分常见类型
        ext_map = {
            "pdf": "application/pdf",
            "html": "text/html",
            "htm": "text/html",
            "txt": "text/plain",
            "csv": "text/csv",
            "json": "application/json",
            "xml": "application/xml",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "png": "image/png",
            "gif": "image/gif",
            "md": "text/markdown",
        }

        guessed_mime = None
        if file_type:
            # 手动覆盖
            key = file_type.lower().lstrip(".")
            guessed_mime = ext_map.get(key)
            if not guessed_mime:
                # 尝试 mimetypes
                guessed_mime = mimetypes.types_map.get("." + key)
        else:
            # 自动根据文件名识别
            guessed_mime, _ = mimetypes.guess_type(str(file_path))
            if not guessed_mime:
                # 若失败，再用扩展名映射兜底
                key = file_path.suffix.lower().lstrip(".")
                guessed_mime = ext_map.get(key)

        # 最终兜底为二进制流
        mimetype = guessed_mime or "application/octet-stream"
        logging.info(f"MIME type: {mimetype}")

        # 6) 构造元数据与媒体体
        file_metadata = {"name": fn, "parents": [folder_name_id]}
        media = MediaFileUpload(str(file_path), mimetype=mimetype, resumable=True)

        # 7) 是否跳过重复
        if skip and fn in registred_file_names:
            logging.info(f"文件 '{fn}' 已存在于 '{folder_name}'，跳过上传。")
            return ""

        # 8) 上传
        try:
            created = (
                drive_service.files()
                    .create(body=file_metadata, media_body=media, fields="id")
                    .execute()
            )
            file_id = created.get("id", "")
            if file_id:
                logging.info(f"文件上传成功，文件 ID: {file_id}")
            else:
                logging.warning("文件上传后未返回 ID。")
            return file_id
        except Exception as e:
            logging.error(f"文件上传失败: {e}")
            return ""

    except FileNotFoundError as fnf_error:
        logging.error(str(fnf_error))
        return None
    except Exception as e:
        logging.error(f"发生错误: {e}")
        return None
