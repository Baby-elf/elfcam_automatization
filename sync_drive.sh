#!/bin/bash
set -x  # 开启调试输出

# Remote 名称（你新建的，能看到 Mon Drive）
REMOTE="gdrive_mine"

# Google Drive 上的文件夹名字（不是 ID）
FOLDER_NAME="Final_Video"

# 本地目录
LOCAL_DIR="/var/www/elfcams/videos/"

# 执行同步
/usr/bin/rclone sync ${REMOTE}:${FOLDER_NAME} "${LOCAL_DIR}" \
    --progress \
    --fast-list
