#!/usr/bin/env python3
"""
update_single_item_no_parser.py (WEBSITE_ID only)

说明：
- 仅通过 WEBSITE_ID 指定目标（格式 parent_child，例如 "2627_2628" 或 "1366_0"）。
- 不再使用 TARGET_ASIN / TARGET_EAN。
- 所有参数在脚本顶部配置：WEBSITE_ID, NEW_VALUES, APPLY。
- 默认 dry-run（APPLY=False）。确认无误后把 APPLY=True 并重运行以实际写入数据库。
- 使用 PyMySQL 连接 MySQL；请先备份数据库。
"""

import pymysql
import sys

# ----------------------------
# 配置区（请根据需要修改）
# ----------------------------
DB_HOST = 'localhost'
DB_USER = 'elfcam_admin'
DB_PASS = '08LJ3VZhTyOXTtOtbx7ouQNFjUF+x67BmboxjE5vsAg='
DB_NAME = 'elfcams_db'
DB_CHARSET = 'utf8mb4'

# 必填：WEBSITE_ID 格式 parent_child，例如 "2627_2628" 或 "1366_0"


# 是否真正写入数据库（False = dry-run，仅显示；True = apply）
APPLY = True

# 要写入的新值（只写入非空字段）
# meta keys 会被写入： _asin, _ean, _goods_code, _fnsku, _weight


# ----------------------------
# 以下通常不用修改
# ----------------------------
def get_conn():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        database=DB_NAME, charset=DB_CHARSET,
        cursorclass=pymysql.cursors.DictCursor, autocommit=False
    )

def parse_website_id(website_id):
    """
    返回 post_id_to_update（int）以及 is_variation(bool).
    website_id 格式: parent_child 如 2627_2628 or 1366_0
    若 child == 0 => simple product with id parent (post_id = parent)
    若 child != 0 => variation (post_id = child)
    """
    try:
        parts = website_id.split('_')
        if len(parts) != 2:
            return None, None
        parent = int(parts[0])
        child = int(parts[1])
        if child == 0:
            return parent, False
        else:
            return child, True
    except Exception:
        return None, None

def get_post_row(cursor, post_id):
    cursor.execute("SELECT ID, post_type, post_parent, post_title FROM wp_posts WHERE ID=%s LIMIT 1", (post_id,))
    return cursor.fetchone()

def fetch_meta(cursor, post_id):
    cursor.execute("""
        SELECT meta_key, meta_value FROM wp_postmeta
        WHERE post_id=%s AND meta_key IN ('_asin','_ean','_goods_code','_fnsku','_weight')
    """, (post_id,))
    return {r['meta_key']: r['meta_value'] for r in cursor.fetchall()}

def upsert_meta(cursor, post_id, meta_key, meta_value):
    cursor.execute("SELECT meta_id FROM wp_postmeta WHERE post_id=%s AND meta_key=%s LIMIT 1", (post_id, meta_key))
    if cursor.fetchone():
        cursor.execute("UPDATE wp_postmeta SET meta_value=%s WHERE post_id=%s AND meta_key=%s", (meta_value, post_id, meta_key))
    else:
        cursor.execute("INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)", (post_id, meta_key, meta_value))

def ensure_brand_term(cursor, brand_name):
    brand_name = brand_name.strip()
    if not brand_name:
        return None
    cursor.execute("SELECT term_id FROM wp_terms WHERE name=%s LIMIT 1", (brand_name,))
    r = cursor.fetchone()
    if r:
        term_id = r['term_id']
    else:
        slug = brand_name.lower().replace(' ', '-')
        cursor.execute("INSERT INTO wp_terms (name, slug) VALUES (%s, %s)", (brand_name, slug))
        term_id = cursor.lastrowid
    cursor.execute("SELECT term_taxonomy_id FROM wp_term_taxonomy WHERE term_id=%s AND taxonomy='brand' LIMIT 1", (term_id,))
    r2 = cursor.fetchone()
    if r2:
        tt_id = r2['term_taxonomy_id']
    else:
        cursor.execute("INSERT INTO wp_term_taxonomy (term_id, taxonomy, description, parent, count) VALUES (%s, 'brand', '', 0, 0)", (term_id,))
        tt_id = cursor.lastrowid
    return tt_id

def attach_brand_to_post(cursor, post_id, tt_id):
    if not tt_id:
        return
    cursor.execute("SELECT 1 FROM wp_term_relationships WHERE object_id=%s AND term_taxonomy_id=%s LIMIT 1", (post_id, tt_id))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO wp_term_relationships (object_id, term_taxonomy_id) VALUES (%s, %s)", (post_id, tt_id))

def main():
    from utils.utils import read_csv_by_sheet
    asin_to_ean = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN")
    index = asin_to_ean[0]
    asin_to_ean_table = asin_to_ean[1:]
    index_dict = {r:i for i,r in enumerate(index)}

    for row in asin_to_ean_table[:32]:
        WEBSITE_ID = row[index_dict.get("website_id")]
        ASIN = row[index_dict.get("asin")]
        EAN = row[index_dict.get("ean")]
        GOODS_CODE = row[index_dict.get("goods_code")]
        FNSKU = row[index_dict.get("fba_id")]
        WEIGHT = row[index_dict.get("weight")]
        BRAND = row[index_dict.get("brand")]
        NEW_VALUES = {
            'asin': ASIN,
            'ean': EAN,
            'goods_code': GOODS_CODE,
            'fnsku': FNSKU,
            'weight': WEIGHT,  # 或你实际的重量
            'brand': BRAND  # 如果你想写 brand
        }

        if not WEBSITE_ID:
            print(f"{GOODS_CODE} : 无Website id， 继续")
            continue

        target_post_id, is_variation = parse_website_id(WEBSITE_ID)
        if target_post_id is None:
            print(f"错误：WEBSITE_ID 格式无效：{WEBSITE_ID}")
            sys.exit(1)

        print(f"解析 WEBSITE_ID -> target_post_id: {target_post_id}, is_variation: {is_variation}")

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                post_row = get_post_row(cur, target_post_id)
                if not post_row:
                    print(f"错误：post_id {target_post_id} 在 wp_posts 中未找到。")
                    return

                # 输出基础信息
                print("post_row:", post_row)
                if post_row.get('post_type') == 'product_variation':
                    calc_wid = f"{post_row.get('post_parent')}_{post_row.get('ID')}"
                else:
                    calc_wid = f"{post_row.get('ID')}_0"
                print("计算的 website_id:", calc_wid)

                metas = fetch_meta(cur, target_post_id)
                print("当前 meta:")
                for mk, mv in metas.items():
                    print(f"  {mk} = {mv}")

                # 准备要写入的 meta（只包含非空 NEW_VALUES）
                to_write = {}
                if NEW_VALUES.get('asin'):
                    to_write['_asin'] = NEW_VALUES['asin']
                if NEW_VALUES.get('ean'):
                    to_write['_ean'] = NEW_VALUES['ean']
                if NEW_VALUES.get('goods_code'):
                    to_write['_goods_code'] = NEW_VALUES['goods_code']
                if NEW_VALUES.get('fnsku'):
                    to_write['_fnsku'] = NEW_VALUES['fnsku']
                if NEW_VALUES.get('weight'):
                    to_write['_weight'] = str(NEW_VALUES['weight'])

                brand_name = NEW_VALUES.get('brand', '').strip()

                print("\n将要写入（仅列出非空项）：")
                if to_write:
                    for k, v in to_write.items():
                        print(f"  {k} = {v}")
                else:
                    print("  （无 meta 项要写）")
                if brand_name:
                    print(f"  brand (taxonomy 'brand') = {brand_name}")

                if not APPLY:
                    print("\nDRY-RUN 模式：未做写入。若确认写入，请把脚本顶部 APPLY = True 并重运行。")
                    return

                # APPLY = True -> 写入
                print("\nAPPLY=True，开始事务写入...")
                try:
                    for mk, mv in to_write.items():
                        upsert_meta(cur, target_post_id, mk, mv)
                    if brand_name:
                        tt_id = ensure_brand_term(cur, brand_name)
                        attach_brand_to_post(cur, target_post_id, tt_id)
                    conn.commit()
                    # 写入完成后查询实际写入结果
                    print("\n写入后的实际 meta:")
                    metas = fetch_meta(cur, target_post_id)
                    for mk, mv in metas.items():
                        print(f"  {mk} = {mv}")

                    if brand_name:
                        tt_id = ensure_brand_term(cur, brand_name)
                        cur.execute("""
                            SELECT t.name
                            FROM wp_terms t
                            JOIN wp_term_taxonomy tt ON t.term_id = tt.term_id
                            JOIN wp_term_relationships tr ON tt.term_taxonomy_id = tr.term_taxonomy_id
                            WHERE tr.object_id=%s AND tt.taxonomy='brand'
                        """, (target_post_id,))
                        brands = [r['name'] for r in cur.fetchall()]
                        print("写入后的品牌:", brands)

                except Exception as e:
                    conn.rollback()
                    print("写入时发生异常，已回滚。错误：", e)
        except Exception as e:
            print("发生错误：", e)
        finally:
            conn.close()

if __name__ == "__main__":
    main()
