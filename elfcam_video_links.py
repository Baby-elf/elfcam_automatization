
import os, csv, urllib.parse, html
from datetime import datetime
from utils.google_drive import upload_file, cleanup_expired_files


upload_file(folder_name= "elfcam_video_link", mypath= OUTDIR, fn=f"elfcams_video_links_{ts}.html")
#upload_file(folder_name= "elfcam_video_link", mypath= "videolinks", fn=f"elfcams_video_links_20250930-20_31_34.html")

cleanup_expired_files(keep_latest=3)

#print(f"elfcams_video_links_{ts}.html上传完成")
#upload_file(folder_name= "elfcam_video_link", mypath= "videolinks", fn="a.html")
