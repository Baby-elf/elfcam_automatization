
import os, csv, urllib.parse, html
from datetime import datetime
from utils.google_drive import upload_file

# —— 基本参数 —— #
ROOT_DIR = "/var/www/elfcams/videos"                        # 递归扫描的本地目录
BASE = "https://elfcams.com/wp-content/uploads/videos"      # 访问前缀
OUTDIR = os.environ.get("OUTDIR", os.path.expanduser("~/elfcam_automatisation/videolinks/"))

# 时间戳（例：20250930-154512）
ts = datetime.now().strftime("%Y%m%d-%H%M%S")

# 支持的视频后缀
exts = {'.mp4', '.mov', '.m4v', '.webm', '.avi', '.mkv'}

# —— 收集链接 —— #
rows = []
for cur, _, files in os.walk(ROOT_DIR):
    for name in sorted(files):
        ext = os.path.splitext(name)[1].lower()
        if ext in exts:
            abs_path = os.path.join(cur, name)
            rel_path = os.path.relpath(abs_path, ROOT_DIR)  # 相对路径（含子目录）
            url = f"{BASE}/{urllib.parse.quote(rel_path.replace(os.sep, '/'), safe='/')}"
            rows.append((rel_path, url))

# 确保输出目录存在
os.makedirs(OUTDIR, exist_ok=True)

# —— 输出到 CSV / MD / HTML，文件名带时间戳 —— #
csv_path  = os.path.join(OUTDIR, f"elfcams_video_links_{ts}.csv")
md_path   = os.path.join(OUTDIR, f"elfcams_video_links_{ts}.md")
html_path = os.path.join(OUTDIR, f"elfcams_video_links_{ts}.html")

with open(csv_path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["path","url"]); w.writerows(rows)

with open(md_path, "w", encoding="utf-8") as f:
    f.write("# ElfCams Video Links\n\n")
    for rel, url in rows:
        f.write(f"- [{rel}]({url})\n")

with open(html_path, "w", encoding="utf-8") as f:
    f.write("<ul>\n")
    for rel, url in rows:
        f.write(f'  <li><a href="{html.escape(url)}" target="_blank" rel="noopener">{html.escape(rel)}</a></li>\n')
    f.write("</ul>\n")

print("✅ 生成完成：")
print("CSV :", csv_path)
print("MD  :", md_path)
print("HTML:", html_path)

upload_file(folder_name= "elfcam_video_link", mypath= OUTDIR, fn="elfcams_video_links_{ts}.html")
#upload_file(folder_name= "elfcam_video_link", mypath= "videolinks", fn="a.html")
