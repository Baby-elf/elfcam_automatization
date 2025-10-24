import csv, datetime
import gspread, time
import logging
from oauth2client.service_account import ServiceAccountCredentials

# 配置日志
logging.basicConfig(
    level=logging.INFO,  # 设置日志级别为 INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # 设置日志格式
    handlers=[logging.StreamHandler()]  # 默认输出到控制台
)

# 获取认证和客户端的公共函数
def get_google_client(google_drive_api='utils/google-drive-api/google-sheet-api.json'):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(google_drive_api, scope)
    client = gspread.authorize(credentials)
    return client

# 上传 CSV 到指定 Google Sheet
def upload_csv(csvFile, table, sheet_name, sheet_size=(0, 0), clear=True):
    client = get_google_client()
    sh = client.open(table)

    try:
        sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logging.info(f"Sheet {sheet_name} not found. Creating new worksheet.")
        sh.add_worksheet(sheet_name, sheet_size[0] + 3, sheet_size[1] + 2, 0)

    if clear:
        logging.info(f"{table} - {sheet_name} clearing sheets.")
        sh.worksheet(sheet_name).clear()

    with open(csvFile, encoding='utf-8') as f:
        data = list(csv.reader(f))

    sh.values_update(
        sheet_name,
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': data}
    )

# 下载 CSV 从指定 Google Sheet
def download_csv(table_name, sheet_name, csv_file='', wait=False, google_drive_api='api_parser/google-drive-api/google-sheet-api-2.json'):
    if wait: wait = 4  # 默认等待时间为4秒
    client = get_google_client(google_drive_api)
    sh = client.open(table_name)

    if wait:
        for i in range(20):
            try:
                values = sh.worksheet(sheet_name).get_all_values()
                if csv_file:
                    with open(csv_file, 'w', newline='', encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerows(values)
                return values
            except Exception as e:
                logging.warning(f"{table_name} - {sheet_name} waiting {wait} sec. Error: {e}")
                time.sleep(wait)
    else:
        values = sh.worksheet(sheet_name).get_all_values()
        if csv_file:
            with open(csv_file, 'w', newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(values)
        return values

# 根据 ID 读取 CSV 数据
def read_csv_by_id(table_name, sheet_id):
    client = get_google_client()
    sh = client.open(table_name)
    sheet = sh.get_worksheet(sheet_id)
    return sheet.get_all_values()

# 根据 sheet 名读取 CSV 数据
def read_csv_by_sheet(table_name, sheet_name, wait=False):
    if wait: wait = 4

    client = get_google_client()
    sh = client.open(table_name)
    logging.info(f"Reading data from {table_name} {sheet_name} ")
    if wait:
        for i in range(20):
            try:
                return sh.worksheet(sheet_name).get_all_values()
            except Exception as e:
                logging.warning(f"{table_name} - {sheet_name} waiting {wait} sec. Error: {e}")
                time.sleep(wait)
    else:
        return sh.worksheet(sheet_name).get_all_values()

# 获取所有工作表名称
def get_all_sheets_from_table(table_name):
    client = get_google_client()
    sh = client.open(table_name)
    return [worksheet.title for worksheet in sh.worksheets()]

# 排序工作表
def sort_sheets(table_name, reverse=True, order_func_name="normal", countries=""):
    def normal(ws):
        return ws.title

    def vendor(ws):
        if "New" in ws.title:
            return str(datetime.datetime.now() - datetime.timedelta(1))[:10]
        elif "FBC" in ws.title:
            return ws.title[-10:]
        else:
            return ws.title

    def fba(ws):
        if "New" in ws.title or "Estimation" in ws.title:
            return str(datetime.datetime.now() - datetime.timedelta(1))[:10]
        elif "Package" in ws.title:
            return "0"
        else:
            return ws.title

    def sc_account(ws):
        order = 0
        len_countries = len(countries) + 2
        for i, country in enumerate(countries):
            if country in ws.title.lower():
                order = len_countries - i
                break
        if "income" in ws.title.lower():
            order += 0.5
        if "upload" in ws.title.lower():
            order = 98
        elif "summary" in ws.title.lower():
            order = 99
        return order

    client = get_google_client()
    sh = client.open(table_name)

    order_funcs = {
        "normal": normal,
        "vendor": vendor,
        "fba": fba,
        "sc_account": sc_account
    }

    order_func = order_funcs.get(order_func_name, normal)
    order_worksheets = sorted(sh.worksheets(), key=order_func, reverse=reverse)

    sh.reorder_worksheets(order_worksheets)

# 删除指定的工作表
def delete_sheet_by_name(table, sheet_name):
    client = get_google_client()
    sh = client.open(table)
    try:
        sheet = sh.worksheet(sheet_name)
        sh.del_worksheet(sheet)
        logging.info(f"Sheet {sheet_name} has been deleted.")
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"There is no sheet named {sheet_name}.")

# 主程序
if __name__ == '__main__':
    table = 'ELFCAM-Database'
    sheet_name = 'Info'

    download_csv(table, sheet_name, 'kk.csv')
