cat > /root/elfcam_automatization/run_push_asin_to_ean_to_website.sh <<'EOF'
#!/bin/bash
# 每天一个日志文件
LOG="/var/log/run_push_asin_to_ean_to_website_$(date +%F).log"
exec >> "$LOG" 2>&1

echo "[$(date '+%F %T')] run_push_asin_to_ean_to_website.sh started"

# 出错就停，并记录报错行号
set -euo pipefail
trap 'rc=$?; echo "[ERROR $(date "+%F %T")] at line $LINENO, exit code $rc"; exit $rc' ERR

# 切到项目目录（elfcam_automatization）
cd /root/elfcam_automatization

# 使用这个项目自己的 venv Python
PY="/root/elfcam_automatization/venv/bin/python"
echo "[INFO] Using python interpreter: $PY"

# 清理 __pycache__ （可选）
find /root/elfcam_automatization -name '__pycache__' -type d -prune -exec rm -rf {} +

echo "===== $(date '+%F %T') 开始执行 update_single_item_two_phase.py ====="
"$PY" /root/elfcam_automatization/update_single_item_two_phase.py
echo "===== $(date '+%F %T') update_single_item_two_phase.py 执行结束 ====="

echo "[$(date '+%F %T')] run_push_asin_to_ean_to_website.sh finished"
EOF
