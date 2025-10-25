#!/usr/bin/env python3
"""
update_single_item_two_phase.py

Two phases:
  1) meta & brand 写入（不处理 category）
  2) 单独的分类写入循环（只处理 category / sub_category）
Controls:
  - APPLY: False = dry-run；True = actually write
  - REPLACE_CATEGORIES: True = 在 attach 前删除 post 上现有的 product_cat 关系（替换）；False = 追加
"""

import pymysql
import sys

# ----------------------------
# 配置区
# ----------------------------
DB_HOST = 'localhost'
DB_USER = 'elfcam_admin'
DB_PASS = '08LJ3VZhTyOXTtOtbx7ouQNFjUF+x67BmboxjE5vsAg='
DB_NAME = 'elfcams_db'
DB_CHARSET = 'utf8mb4'

# 控制开关
APPLY = True              # False = dry-run；True = 写入
REPLACE_CATEGORIES = False # 在分类写入时是否先删除原有 product_cat 关系（替换）

# ----------------------------
# 以下通常不用改
# ----------------------------
def get_conn():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        database=DB_NAME, charset=DB_CHARSET,
        cursorclass=pymysql.cursors.DictCursor, autocommit=False
    )

def parse_website_id(website_id):
    try:
        parts = website_id.split('_')
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
    brand_name = (brand_name or '').strip()
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
        return r2['term_taxonomy_id']
    cursor.execute("INSERT INTO wp_term_taxonomy (term_id, taxonomy, description, parent, count) VALUES (%s, 'brand', '', 0, 0)", (term_id,))
    return cursor.lastrowid

def attach_brand_to_post(cursor, post_id, tt_id):
    if not tt_id:
        return
    cursor.execute("SELECT 1 FROM wp_term_relationships WHERE object_id=%s AND term_taxonomy_id=%s LIMIT 1", (post_id, tt_id))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO wp_term_relationships (object_id, term_taxonomy_id) VALUES (%s, %s)", (post_id, tt_id))

def ensure_category_term(cursor, cat_name, taxonomy='product_cat', parent_tt_id=None):
    cat_name = (cat_name or '').strip()
    if not cat_name:
        return None
    cursor.execute("SELECT term_id FROM wp_terms WHERE name=%s LIMIT 1", (cat_name,))
    r = cursor.fetchone()
    if r:
        term_id = r['term_id']
    else:
        slug = cat_name.lower().replace(' ', '-')
        cursor.execute("INSERT INTO wp_terms (name, slug) VALUES (%s, %s)", (cat_name, slug))
        term_id = cursor.lastrowid

    cursor.execute("SELECT term_taxonomy_id FROM wp_term_taxonomy WHERE term_id=%s AND taxonomy=%s LIMIT 1", (term_id, taxonomy))
    r2 = cursor.fetchone()
    if r2:
        return r2['term_taxonomy_id']
    parent = parent_tt_id if parent_tt_id else 0
    cursor.execute("INSERT INTO wp_term_taxonomy (term_id, taxonomy, description, parent, count) VALUES (%s,%s,'',%s,0)", (term_id, taxonomy, parent))
    return cursor.lastrowid

def attach_term_to_post(cursor, post_id, tt_id):
    if not tt_id:
        return
    cursor.execute("SELECT 1 FROM wp_term_relationships WHERE object_id=%s AND term_taxonomy_id=%s LIMIT 1", (post_id, tt_id))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO wp_term_relationships (object_id, term_taxonomy_id) VALUES (%s,%s)", (post_id, tt_id))

def remove_product_cat_relationships(cursor, post_id):
    cursor.execute("""
        DELETE tr FROM wp_term_relationships tr
        JOIN wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
        WHERE tr.object_id=%s AND tt.taxonomy='product_cat'
    """, (post_id,))

# ----------------------------
# Main: two-phase processing
# ----------------------------
def main():
    from utils.utils import read_csv_by_sheet
    asin_to_ean = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN")
    index = asin_to_ean[0]
    table = asin_to_ean[1:]
    index_dict = {r:i for i,r in enumerate(index)}

    # --- Phase 1: meta & brand (不处理分类) ---
    print("=== PHASE 1: meta & brand 更新（不处理 category） ===")
    for row in table[:32]:  # 测试时保留切片；生产可改为 table
        WEBSITE_ID = row[index_dict.get("website_id")]
        if not WEBSITE_ID:
            print("[SKIP] missing WEBSITE_ID, skip row")
            continue

        ASIN = row[index_dict.get("asin")]
        EAN = row[index_dict.get("ean")]
        GOODS_CODE = row[index_dict.get("goods_code")]
        FNSKU = row[index_dict.get("fba_id")]
        WEIGHT = row[index_dict.get("weight")]
        BRAND = row[index_dict.get("brand")]

        NEW_VALUES = {
            'asin': ASIN, 'ean': EAN, 'goods_code': GOODS_CODE,
            'fnsku': FNSKU, 'weight': WEIGHT, 'brand': BRAND
        }

        target_post_id, is_variation = parse_website_id(WEBSITE_ID)
        if target_post_id is None:
            print(f"[ERROR] invalid WEBSITE_ID: {WEBSITE_ID}")
            continue

        # 简短输出
        print(f"[PH1] post_id={target_post_id} will_write_meta_keys={[k for k,v in NEW_VALUES.items() if v]} brand={(BRAND or '(none)')}")

        if not APPLY:
            print("  [DRY-RUN] PH1 not applied.")
            continue

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # check post exists
                post_row = get_post_row(cur, target_post_id)
                if not post_row:
                    print(f"  [ERROR] post {target_post_id} not found.")
                    continue

                # upsert meta
                for key in ('asin','ean','goods_code','fnsku','weight'):
                    v = NEW_VALUES.get(key)
                    if v:
                        upsert_meta(cur, target_post_id, f"_{key}", str(v))

                # brand
                brand_name = (NEW_VALUES.get('brand') or '').strip()
                if brand_name:
                    tt = ensure_brand_term(cur, brand_name)
                    attach_brand_to_post(cur, target_post_id, tt)

                conn.commit()
                print(f"  [PH1 OK] updated post {target_post_id}")
        except Exception as e:
            conn.rollback()
            print(f"  [PH1 ERROR] post {target_post_id}: {e}")
        finally:
            conn.close()

    # --- Phase 2: 独立分类写入循环（只用 WEBSITE_ID + category/sub_category） ---
    print("\n=== PHASE 2: category / sub-category 写入（独立循环） ===")
    for row in table[:32]:
        WEBSITE_ID = row[index_dict.get("website_id")]
        if not WEBSITE_ID:
            print("[SKIP] missing WEBSITE_ID, skip row")
            continue

        # 从 CSV 独立读取 category 字段（若没有则为空字符串）
        CATEGORY = "category"
        SUB_CATEGORY = "sub"

        # 若两者都为空则跳过
        if not CATEGORY and not SUB_CATEGORY:
            continue

        target_post_id, is_variation = parse_website_id(WEBSITE_ID)
        if target_post_id is None:
            print(f"[ERROR] invalid WEBSITE_ID: {WEBSITE_ID}")
            continue

        # 简短输出
        print(f"[PH2] post_id={target_post_id} category={(CATEGORY or '(none)')} sub={(SUB_CATEGORY or '(none)')} replace={REPLACE_CATEGORIES}")

        if not APPLY:
            print("  [DRY-RUN] PH2 not applied.")
            continue

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # check post exists
                post_row = get_post_row(cur, target_post_id)
                if not post_row:
                    print(f"  [ERROR] post {target_post_id} not found.")
                    continue

                # 如果设置替换：先删除原有 product_cat 关系
                if REPLACE_CATEGORIES:
                    remove_product_cat_relationships(cur, target_post_id)

                # 创建 / 获取 category / sub-category term_taxonomy_id
                cat_tt = None
                if CATEGORY:
                    cat_tt = ensure_category_term(cur, CATEGORY)
                if SUB_CATEGORY:
                    sub_tt = ensure_category_term(cur, SUB_CATEGORY, parent_tt_id=cat_tt)
                    attach_term_to_post(cur, target_post_id, sub_tt)
                elif cat_tt:
                    attach_term_to_post(cur, target_post_id, cat_tt)

                conn.commit()

                # 简短输出已写入的分类（读取 DB）
                cur.execute("""
                    SELECT t.name
                    FROM wp_terms t
                    JOIN wp_term_taxonomy tt ON t.term_id = tt.term_id
                    JOIN wp_term_relationships tr ON tt.term_taxonomy_id = tr.term_taxonomy_id
                    WHERE tr.object_id=%s AND tt.taxonomy='product_cat'
                """, (target_post_id,))
                cats = [r['name'] for r in cur.fetchall()]
                print(f"  [PH2 OK] categories: {cats}")

        except Exception as e:
            conn.rollback()
            print(f"  [PH2 ERROR] post {target_post_id}: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
