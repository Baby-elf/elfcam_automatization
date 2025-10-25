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


        ASIN = row[index_dict.get("asin")]
        EAN = row[index_dict.get("ean")]
        GOODS_CODE = row[index_dict.get("goods_code")]
        FNSKU = row[index_dict.get("fba_id")]
        WEIGHT = row[index_dict.get("weight")]
        BRAND = row[index_dict.get("brand")]
        if not WEBSITE_ID:
            print(f"[SKIP]{GOODS_CODE}: missing WEBSITE_ID, skip row")
            continue
        NEW_VALUES = {
            'asin': ASIN, 'ean': EAN, 'goods_code': GOODS_CODE,
            'fnsku': FNSKU, 'weight': WEIGHT, 'brand': BRAND
        }

        target_post_id, is_variation = parse_website_id(WEBSITE_ID)
        if target_post_id is None:
            print(f"[ERROR] invalid WEBSITE_ID: {WEBSITE_ID}")
            continue

        # 简短输出
        print(f"[PH1] post_id={WEBSITE_ID} will_write_meta_keys={[k + ' ' + v for k,v in NEW_VALUES.items() if v]} brand={(BRAND or '(none)')}")

        if not APPLY:
            print("  [DRY-RUN] PH1 not applied.")
            continue

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # check post exists
                post_row = get_post_row(cur, target_post_id)
                if not post_row:
                    print(f"  [ERROR] post {WEBSITE_ID} not found.")
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
                # --------------------------
                # 写入分类 / 子分类
                # --------------------------

                # 决定 taxonomy attach 对象：父商品（parent）还是自己
                attach_to_id = target_post_id
                if post_row.get('post_type') == 'product_variation' and post_row.get('post_parent'):
                    attach_to_id = post_row.get('post_parent')
                print(f"[INFO] taxonomy attach target: {attach_to_id}, meta target: {target_post_id}")


                # 查询打印结果
                cur.execute("""
                    SELECT t.name
                    FROM wp_terms t
                    JOIN wp_term_taxonomy tt ON t.term_id = tt.term_id
                    JOIN wp_term_relationships tr ON tt.term_taxonomy_id = tr.term_taxonomy_id
                    WHERE tr.object_id=%s AND tt.taxonomy='product_cat'
                """, (attach_to_id,))
                cats = [r['name'] for r in cur.fetchall()]
                print(f"[OK] taxonomy categories (attached to {attach_to_id}): {cats}")

                meta_cat = fetch_meta(cur, target_post_id).get('_category')
                meta_sub = fetch_meta(cur, target_post_id).get('_sub_category')
                print(f"[OK] meta _category={meta_cat}, _sub_category={meta_sub}")

                conn.commit()
                print(f"  [PH1 OK] updated post {WEBSITE_ID}")
        except Exception as e:
            conn.rollback()
            print(f"  [PH1 ERROR] post {WEBSITE_ID}: {e}")
        finally:
            conn.close()

    # --- Phase 2: 独立分类写入循环（只用 WEBSITE_ID + category/sub_category） ---
    website_price = read_csv_by_sheet("Website-Price", "Elfcam")
    index_dict = {r:i for i, r in enumerate(website_price[0])}
    rows = website_price[1:]
    orders = read_csv_by_sheet("Website-Price", "order")
    index_dict_order = {r:i for i, r in enumerate(orders[0])}
    order_dict = {r[index_dict_order["catalog_ss_item"]]: r[index_dict_order["catalog_ss_subgroup"]] + "-" + r[index_dict_order["catalog_ss_group"]] for r in orders[1:]}
    print("\n=== PHASE 2: cat / subcat 写入到变体/单体（meta + optional taxonomy） ===")

    # quick sanity checks
    print("DEBUG: rows count =", len(rows))
    if len(rows) == 0:
        print("DEBUG: No rows found in Website-Price sheet. Check read_csv_by_sheet result and sheet name.")
    for i, r in enumerate(rows[:3]):
        print(f"DEBUG sample row {i} keys/len: {len(r)}; preview: {r[:5] if isinstance(r, (list, tuple)) else r}")

    # ensure index keys exist
    if "id" not in index_dict or "category" not in index_dict:
        print("ERROR: expected 'id' or 'category' column missing in Website-Price sheet header:", website_price[0])
    else:
        print("DEBUG: index_dict has id and category indices:", index_dict["id"], index_dict["category"])

    # loop
    for idx, row in enumerate(rows[:32], 1):
        try:
            WEBSITE_ID = row[index_dict.get("id")]
            SUBCAT = row[index_dict.get("category")]
            CAT = order_dict.get(SUBCAT)

            print(f"\n[PH2 #{idx}] raw WEBSITE_ID={WEBSITE_ID!r}, SUBCAT={SUBCAT!r}, mapped CAT={CAT!r}")

            if not WEBSITE_ID:
                print("  [SKIP] missing WEBSITE_ID, skip row")
                continue

            if not CAT and not SUBCAT:
                print("  [SKIP] both CAT and SUBCAT empty, nothing to do")
                continue

            target_post_id, is_variation = parse_website_id(WEBSITE_ID)
            print(f"  parsed -> target_post_id={target_post_id}, is_variation={is_variation}")
            if target_post_id is None:
                print(f"  [ERROR] invalid WEBSITE_ID format: {WEBSITE_ID!r}")
                continue

            if not APPLY:
                print("  [DRY-RUN] PH2 not applied.")
                continue

            conn = get_conn()
            cur = conn.cursor()
            try:
                post_row = get_post_row(cur, target_post_id)
                if not post_row:
                    print(f"  [ERROR] post {target_post_id} not found in wp_posts")
                    cur.close()
                    conn.close()
                    continue

                # 写 meta（写到该 post，不区分 simple/variation）
                if CAT:
                    print(f"  upsert_meta: _cat = {CAT!r}")
                    upsert_meta(cur, target_post_id, '_cat', CAT)
                if SUBCAT:
                    print(f"  upsert_meta: _subcat = {SUBCAT!r}")
                    upsert_meta(cur, target_post_id, '_subcat', SUBCAT)

                # 可选： 同时 attach taxonomy（如果你需要前台分类生效）
                # attach 到父商品（若是变体）
                attach_to_id = target_post_id
                if post_row.get('post_type') == 'product_variation' and post_row.get('post_parent'):
                    attach_to_id = post_row.get('post_parent')

                print(f"  will attach taxonomy (if any) to object_id={attach_to_id}")

                if CAT or SUBCAT:
                    # get/create term_taxonomy ids
                    cat_tt = ensure_category_term(cur, CAT) if CAT else None
                    sub_tt = ensure_category_term(cur, SUBCAT, parent_tt_id=cat_tt) if SUBCAT else None
                    print(f"  got cat_tt={cat_tt}, sub_tt={sub_tt}")

                    if REPLACE_CATEGORIES:
                        print("  REPLACE_CATEGORIES True -> removing existing product_cat relations")
                        remove_product_cat_relationships(cur, attach_to_id)

                    to_attach = sub_tt or cat_tt
                    if to_attach:
                        attach_term_to_post(cur, attach_to_id, to_attach)
                        print(f"  attached term_taxonomy_id={to_attach} to object_id={attach_to_id}")

                conn.commit()

                # verify: read back meta and taxonomy for the attach_to_id and target_post_id
                cur.execute(
                    "SELECT meta_key, meta_value FROM wp_postmeta WHERE post_id=%s AND meta_key IN ('_cat','_subcat')",
                    (target_post_id,))
                metas = cur.fetchall()
                print("  meta rows for target_post:", metas)

                cur.execute("""
                    SELECT t.name
                    FROM wp_terms t
                    JOIN wp_term_taxonomy tt ON t.term_id = tt.term_id
                    JOIN wp_term_relationships tr ON tt.term_taxonomy_id = tr.term_taxonomy_id
                    WHERE tr.object_id=%s AND tt.taxonomy='product_cat'
                """, (attach_to_id,))
                cats = [r['name'] for r in cur.fetchall()]
                print("  taxonomy attached to object:", cats)

                print(f"  [PH2 OK] row #{idx} updated post {target_post_id}")

            except Exception as e_inner:
                conn.rollback()
                print(f"  [PH2 ERROR] row #{idx} post {WEBSITE_ID}: {e_inner}")
            finally:
                try:
                    cur.close()
                except:
                    pass
                try:
                    conn.close()
                except:
                    pass

        except Exception as e_outer:
            print(f"[PH2 OUTER ERROR] processing row #{idx}: {e_outer}")


if __name__ == "__main__":
    main()
