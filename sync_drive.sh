#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
set -x  # 调试输出

#-----------------------------
# 配置区
#-----------------------------
REMOTE="gdrive_mine"                 # rclone 的远端名
FOLDER_NAME="Final_Video"            # Google Drive 上的文件夹名（非 ID）
LOCAL_DIR="/var/www/elfcams/videos"  # 目标本地目录（无尾斜杠也行）
PROJECT_DIR="/root/elfcam_automatization"  # 你的项目根目录（含 elfcam_video_links.py）
VENV_DIR="${PROJECT_DIR}/venv"
REQ_FILE="${PROJECT_DIR}/requirements.txt"
LOG_DIR="/var/log/elfcam"
LOG_FILE="${LOG_DIR}/sync_and_build.log"

# Google 凭据（避免硬编码到仓库）
export GOOGLE_APPLICATION_CREDENTIALS="${PROJECT_DIR}/utils/google-drive-api/credentials.json"
# 如果你的代码读取 token.json/credentials.json 的相对路径，务必先 cd 到项目目录

#-----------------------------
# 预检查
#-----------------------------
command -v rclone >/dev/null 2>&1 || { echo "rclone 未安装"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 未安装"; exit 1; }

mkdir -p "${LOCAL_DIR}" "${LOG_DIR}"

#-----------------------------
# 1) 同步 GDrive -> 本地
#   注意：sync 会删除本地多余文件，和云端保持一致
#-----------------------------
/usr/bin/rclone sync "${REMOTE}:${FOLDER_NAME}" "${LOCAL_DIR}" \
  --progress \
  --fast-list \
  --log-file "${LOG_FILE}" \
  --log-level INFO

# 可选：设定权限
# chown -R www-data:www-data "${LOCAL_DIR}"
# find "${LOCAL_DIR}" -type d -exec chmod 755 {} \;
# find "${LOCAL_DIR}" -type f -exec chmod 644 {} \;

#-----------------------------
# 2) 进入项目并准备虚拟环境（若不存在才创建）
#-----------------------------
cd "${PROJECT_DIR}"

if [ ! -d "${VENV_DIR}" ]; then
  python3 -m venv "${VENV_DIR}"
fi

# 激活 venv
# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip setuptools wheel

# 安装依赖（有 requirements 就按它装；没有就装最小集）
if [ -f "${REQ_FILE}" ]; then
  pip install -r "${REQ_FILE}"
else
  pip install \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-auth-httplib2 \
    requests \
    tqdm
fi

#-----------------------------
# 3) 运行脚本（在 venv 内）
#-----------------------------
python "${PROJECT_DIR}/elfcam_video_links.py" | tee -a "${LOG_FILE}"
