#!/usr/bin/env python3
"""
update_prices_only_full.py

将 Website-Price 工作表中的 regular_price / sale_price 写入到网站数据库 (wp_postmeta)。
仅当 Change_Price == "yes"（不区分大小写）时才更新对应 website_id 的产品。

行为要点：
  - 解析 website_id: "parent_child"；child==0 => 单体；child!=0 => 变体（写入变体本身）
  - 写入 meta keys: _regular_price, _sale_price, _price
  - 事务提交前会校验连接的数据库名与脚本中的 DB_NAME 一致，避免误写
  - 写入后读回 meta 做校验并用 logging 输出
  - DRY-RUN 支持（APPLY=False）
  - 可选：同步 parent 的 _price（SYNC_PARENT_PRICE 开关）
"""

import logging
import pymysql
from decimal import Decimal, InvalidOperation
from datetime import datetime

# ----------------------------
# 配置区（按需修改）
# ----------------------------
DB_HOST = 'localhost'
DB_USER = 'elfcam_admin'
DB_PASS = '08LJ3VZhTyOXTtOtbx7ouQNFjUF+x67BmboxjE5vsAg='
DB_NAME = 'elfcams_db'
DB_CHARSET = 'utf8mb4'

# 控制开关
APPLY = True               # False = dry-run；True = 写入
SYNC_PARENT_PRICE = False  # True = 写入变体后更新 parent 的 _price 为该 parent 变体中的最低 _price

# 初始化 logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# ----------------------------
# 工具函数（与你现有脚本保持风格一致）
# ----------------------------
def get_conn():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        database=DB_NAME, charset=DB_CHARSET,
        cursorclass=pymysql.cursors.DictCursor, autocommit=False
    )

def parse_website_id(website_id):
    """
    parse 'parent_child' -> (target_post_id, is_variation)
    child == 0 -> (parent, False)
    child != 0 -> (child, True)
    """
    try:
        parts = str(website_id).split('_')
        if len(parts) != 2:
            return None, None
        parent = int(parts[0])
        child = int(parts[1])
        return (parent, False) if child == 0 else (child, True)
    except Exception:
        return None, None

def get_post_row(cursor, post_id):
    cursor.execute("SELECT ID, post_type, post_parent, post_title FROM wp_posts WHERE ID=%s LIMIT 1", (post_id,))
    return cursor.fetchone()

def upsert_meta(cursor, post_id, meta_key, meta_value):
    """
    插入或更新 wp_postmeta 的 meta_key/meta_value（以字符串形式写入）
    """
    cursor.execute("SELECT meta_id FROM wp_postmeta WHERE post_id=%s AND meta_key=%s LIMIT 1", (post_id, meta_key))
    if cursor.fetchone():
        cursor.execute("UPDATE wp_postmeta SET meta_value=%s WHERE post_id=%s AND meta_key=%s", (meta_value, post_id, meta_key))
    else:
        cursor.execute("INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)", (post_id, meta_key, meta_value))

def normalize_price_for_db(v):
    """
    将价格字符串规范化为不含千位逗号的纯数字字符串（保留原始小数部分）。
    若传入空值/None/空串，返回 ""。
    如果无法解析为数值，则返回原始字符串的去空格版本（以防某些特殊标记需要存储）。
    """
    if v is None:
        return ""
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    s = str(v).strip()
    if s == "":
        return ""
    s2 = s.replace(',', '')
    try:
        # 验证是数字
        Decimal(s2)
        return s2
    except (InvalidOperation, ValueError):
        return s

def read_db_string_as_decimal(s):
    """
    尝试把从 DB 读出的 meta_value 转为 Decimal，供比较用；若不能解析返回 None。
    """
    if s is None:
        return None
    try:
        s2 = str(s).strip().replace(',', '')
        if s2 == "":
            return None
        return Decimal(s2)
    except Exception:
        return None

# ----------------------------
# 主流程
# ----------------------------
def main():
    from utils.utils import read_csv_by_sheet
    from utils.uploader import update_google_sheet_cell

    website_price = read_csv_by_sheet("Website-Price", "Elfcam")
    if not website_price or len(website_price) < 1:
        logging.error("Website-Price sheet empty or read failure.")
        return

    header = website_price[0]
    rows = website_price[1:]
    index_dict = {r:i for i, r in enumerate(header)}


    # 必需的列检查
    if "id" not in index_dict:
        logging.error("'id' column missing in Website-Price sheet header: %s", header)
        return

    # 允许 Change_Price 列名有大小写差异或附带空格，做一次容错查找
    change_key = None
    for k in index_dict.keys():
        if "change_price" in k.lower():
            change_key = k
            break
    # 也允许类似 "Change_Price" 精确存在

    if change_key is None:
        logging.warning("'Change_Price' column missing — script will treat rows as no-change unless column added.")
    else:
        logging.info("Using Change_Price column header: %s", change_key)

    logging.info("START processing %d rows", len(rows))
    update = False
    for idx, row in enumerate(rows, 1):

        try:
            WEBSITE_ID = row[index_dict.get("id")]
            def get_col(col_name):
                # 允许列名大小写不敏感（直接用 header 中的键进行索引）
                for k in index_dict.keys():
                    if str(k).strip().lower() == str(col_name).strip().lower():
                        return row[index_dict[k]]
                return ""

            # 读取 change_flag 安全地
            change_flag = ""
            if change_key is not None:
                change_flag = row[index_dict[change_key]]

            regular_price_raw = get_col("regular_price")
            sale_price_raw = get_col("sale_price")

            logging.debug("ROW %d raw values: WEBSITE_ID=%r Change_Price=%r regular_price=%r sale_price=%r",
                          idx, WEBSITE_ID, change_flag, regular_price_raw, sale_price_raw)

            if not WEBSITE_ID:
                logging.info("Row %d: missing WEBSITE_ID, skip.", idx)
                continue

            # 只有 change_flag == "yes"（忽略大小写）才处理
            if not isinstance(change_flag, str) or change_flag.strip().lower() != "yes":
                #logging.info("Row %d: Change_Price not 'yes' -> skip.", idx)
                continue

            # parse website id
            target_post_id, is_variation = parse_website_id(WEBSITE_ID)
            if target_post_id is None:
                logging.error("Row %d: invalid WEBSITE_ID format: %r. Expected 'parent_child'. Skipping.", idx, WEBSITE_ID)
                continue
            logging.info("Row %d: parsed -> target_post_id=%s, is_variation=%s", idx, target_post_id, is_variation)

            # dry-run: 仅打印将要写入的内容
            regular_price = normalize_price_for_db(regular_price_raw)
            sale_price = normalize_price_for_db(sale_price_raw)
            price_to_set = sale_price if sale_price not in ("", None) else regular_price

            if not APPLY:
                logging.info("Row %d [DRY-RUN] would upsert: write_post_id=%s (variation? %s) _regular_price=%r _sale_price=%r _price=%r",
                             idx, target_post_id, is_variation, regular_price, sale_price, price_to_set)
                continue

            # 实际写入 - 建立连接并校验连接到的 DB 名称
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    # 校验实际连接的数据库名，防止误写
                    cur.execute("SELECT DATABASE() AS dbname")
                    dbname = cur.fetchone().get('dbname')
                    if dbname != DB_NAME:
                        logging.error("Row %d: connected to database %r, expected %r. Aborting this row for safety.", idx, dbname, DB_NAME)
                        conn.close()
                        continue

                    # 确保 post 存在并检查类型一致性
                    post_row = get_post_row(cur, target_post_id)
                    if not post_row:
                        logging.error("Row %d: post %s not found in wp_posts. Skipping.", idx, target_post_id)
                        conn.close()
                        continue

                    # 检查解析后 is_variation 与数据库里 post_type 是否一致
                    if is_variation:
                        if post_row.get('post_type') != 'product_variation':
                            logging.warning("Row %d: parsed as variation but DB post_type is %r. Skipping to avoid wrong writes.", idx, post_row.get('post_type'))
                            conn.close()
                            continue
                    else:
                        # single product expected; typical post_type is 'product'
                        if post_row.get('post_type') not in ('product', 'product_variation'):
                            logging.info("Row %d: target post_type is %r, proceeding to write as single product (verify if necessary).", idx, post_row.get('post_type'))

                    write_post_id = target_post_id

                    # upsert metas
                    logging.info("Row %d: upserting metas to post_id=%s ...", idx, write_post_id)
                    upsert_meta(cur, write_post_id, "_regular_price", regular_price)
                    upsert_meta(cur, write_post_id, "_sale_price", sale_price)
                    upsert_meta(cur, write_post_id, "_price", price_to_set)
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    info = "Row " + str(idx) + ": [OK] committed in " + now_str
                    # commit
                    conn.commit()
                    #logging.info(info, idx)

                    #rows[idx-1][index_dict[change_key]] = info
                    update_google_sheet_cell("Website-Price", "Elfcam", int(idx-1), int(index_dict[change_key]), info)
                    # verify: 读回 meta 值并打印
                    cur.execute("""
                        SELECT meta_key, meta_value FROM wp_postmeta
                        WHERE post_id=%s AND meta_key IN ('_regular_price','_sale_price','_price')
                    """, (write_post_id,))
                    rows_back = cur.fetchall()
                    logging.info("Row %d: [VERIFY] written metas: %s", idx, rows_back)


                    # # 可选：同步 parent 的 _price（设为子变体中最小 price）
                    # if SYNC_PARENT_PRICE and is_variation and post_row.get('post_parent'):
                    #     parent_id = post_row.get('post_parent')
                    #     try:
                    #         # 读取所有子变体的 _price
                    #         cur.execute("""
                    #             SELECT pm.meta_value AS price_val
                    #             FROM wp_posts p
                    #             JOIN wp_postmeta pm ON p.ID = pm.post_id AND pm.meta_key = '_price'
                    #             WHERE p.post_parent=%s AND p.post_type='product_variation'
                    #         """, (parent_id,))
                    #         price_vals = [r['price_val'] for r in cur.fetchall()]
                    #         decimal_prices = [read_db_string_as_decimal(v) for v in price_vals]
                    #         decimal_prices = [p for p in decimal_prices if p is not None]
                    #         if decimal_prices:
                    #             min_price = str(min(decimal_prices))
                    #             logging.info("Row %d: SYNC_PARENT_PRICE: setting parent_id=%s _price = %s", idx, parent_id, min_price)
                    #             upsert_meta(cur, parent_id, "_price", min_price)
                    #             conn.commit()
                    #             # verify parent meta
                    #             cur.execute("SELECT meta_key, meta_value FROM wp_postmeta WHERE post_id=%s AND meta_key='_price'", (parent_id,))
                    #             logging.info("Row %d: [VERIFY parent] %s", idx, cur.fetchall())
                    #         else:
                    #             logging.info("Row %d: SYNC_PARENT_PRICE: no valid child _price values found for parent, skipping parent sync.", idx)
                    #     except Exception as e_sync:
                    #         conn.rollback()
                    #         logging.error("Row %d: ERROR while syncing parent price: %s", idx, e_sync)

            except Exception as e_inner:
                try:
                    conn.rollback()
                except:
                    pass
                logging.error("Row %d: DB error for post %s: %s", idx, target_post_id, e_inner)
            finally:
                try:
                    conn.close()
                except:
                    pass

        except Exception as e_outer:
            logging.error("Row %d: outer error processing row: %s", idx, e_outer)


    # if update:
    #     new_rows = []
    #     for row in rows:
    #
    #         row[index_dict.get("image")] = "=IMAGE(" + '"' +row[index_dict.get("main_image")] + '"' + ")"
    #         new_rows.append(row)
    #     write_and_upload_csv(new_rows,"csv/csv.csv", "Website-Price", "Elfcam", header)

if __name__ == "__main__":
    main()
