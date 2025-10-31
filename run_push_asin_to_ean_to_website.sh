cat > /root/erpwms/run_push_asin_to_ean_to_website.sh <<'EOF'
#!/bin/bash
# 每天一个日志文件
LOG="/var/log/run_push_asin_to_ean_to_website_$(date +%F).log"
exec >> "$LOG" 2>&1

echo "[$(date '+%F %T')] run_push_asin_to_ean_to_website.sh started"

# 出错即退出，并打印错误行号
set -euo pipefail
trap 'rc=$?; echo "[ERROR $(date "+%F %T")] at line $LINENO, exit code $rc"; exit $rc' ERR

# 进入项目路径
cd /root/erpwms/backend

# 使用项目虚拟环境 Python
PY="/root/erpwms/backend/venv/bin/python"
echo "[INFO] Using python interpreter: $PY"

# 清理缓存（可选）
find /root/erpwms/backend -name '__pycache__' -type d -prune -exec rm -rf {} +

echo "===== $(date '+%F %T') 开始执行 update_single_item_two_phase.py ====="
"$PY" /root/erpwms/backend/update_single_item_two_phase.py
echo "===== $(date '+%F %T') update_single_item_two_phase.py 执行结束 ====="

echo "[$(date '+%F %T')] run_push_asin_to_ean_to_website.sh finished"
EOF
