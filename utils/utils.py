import csv, time, shutil, logging, os
from utils.uploader import upload_csv,download_csv, read_csv_by_sheet
import numpy as np
import datetime, time
import pandas as pd

def cut_string(string, first = 21, last = 5 , max_len = 28):
    if len(string) > max_len:
        return string[:first] + "..." + string[-last:]
    else:
        return string

def find_match(csv_path):
    match_dict = {}
    key_check = []

    with open(csv_path, 'r', newline='', encoding="utf-8") as matcher:
        rows = csv.reader(matcher, delimiter=',')
        for row in rows:
            if len(row) == 0: continue
#            if row[0] in key_check: raise Exception("Please Check this duplicated key : " + row[0])
            key_check.append(row[0])
            match_dict[row[0]] = [row[1], row[2]]

    return match_dict

def find_info(csv_path_0, x = 2):
    match_dict = {}

    with open(csv_path_0, 'r', newline='') as matcher:
        rows = csv.reader(matcher, delimiter=',')
        for row in rows:
            if len(row) == 0: continue
            key = row[0] + "___" + row[1]
            match_dict[key] = row[x]

    return match_dict




def data_base_reader(wait_time=10):
    """
    读取并返回数据库中的信息，包括 Info、Timbre、Weight 和 Weight_confirmed。

    参数：
    - download: 是否需要下载 CSV 文件
    - elfcam_file: ELFCAM 信息文件路径
    - ando_file: ANDO 信息文件路径
    - wait_time: 读取 CSV 的等待时间（秒）

    返回：
    - 数据库信息的列表，包括 [Info, timbres, Weight, Weight_confirmed]
    """
    # 设置日志记录
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info("Starting database reading process...")

    # 如果需要下载文件
    # if download:
    #     try:
    #         #logging.info("Downloading ANDO Info...")
    #         #download_csv("ANDO-Database", "Info", ando_file, True)
    #         logging.info("Downloading ELFCAM Info...")
    #         download_csv("ELFCAM-Sales-List", "Info", elfcam_file, True)
    #     except Exception as e:
    #         logging.error(f"Error downloading CSV files: {e}")
    #         return []

    # 检查文件是否存在，避免不必要的读取操作
    # if not os.path.exists(elfcam_file) or not os.path.exists(ando_file):
    #     logging.error(f"Required CSV files not found: {elfcam_file}, {ando_file}")
    #     return []

    # 读取文件并提取信息
    if True:
        #logging.info("Reading Info from CSV files...")
        #Info = find_info(elfcam_file, ando_file, 2)

        # 读取其他数据
        #logging.info("Reading Timbre List...")
        # download_csv("Timbre-List", "timbre_list", "csv/timbres.csv", True)
        # timbres =find_info("csv/timbres.csv")
        Timbres = read_csv_by_sheet("Timbre-List", "timbre_list", wait=wait_time)

        # download_csv("ELFCAM-Sales-List","Info", "csv/info.csv", True)
        # Weight = find_info("csv/info.csv")
        Weight = read_csv_by_sheet("ELFCAM-Sales-List", "Info", wait=wait_time)

        info = {a[0] + "___" +a[1] : a[2] for a in Weight}
        weight = {a[0] + "___" +a[1] : a[3] for a in Weight}

        # logging.info("Reading confirmed Weight information...")
        # Weight_confirmed = find_info(elfcam_file, ando_file, 6)
        return (info, Timbres, weight, "")

    # except Exception as e:
    #     logging.error(f"Error reading database information: {e}")
    #     return []






def statistic_read_and_writer(statistic_list):
    FBV_Stat_csv = 'csv/FBV-2021.csv'
    FBV_Stat_csv_back = 'csv/FBV-2021_back.csv'
    FBV_Stat_csv_registered = 'csv/FBV-2021-r.csv'
    shutil.copyfile(FBV_Stat_csv, FBV_Stat_csv_back)
    this_mounth = str(datetime.datetime.now())[:7]
    Data_base_info_csv = 'csv/ELFCAM-Database - info.csv'
#read statistic
    print('The total number of new items is:' + str(sum(statistic_list.values())))

    data = pd.read_csv(FBV_Stat_csv, header=0, index_col = 0)
    try:
        data[this_mounth]
    except:
        data[this_mounth] = ''

    for item in statistic_list.items():
        try:
            data.loc[item[0]]
        except:
            data.loc[item[0]] = ''
        try:
            data.loc[item[0], this_mounth]
        except:
            print("need to add this item " + str(item[0]) + " and check the mounth " + str(this_mounth))

        if data.loc[item[0],this_mounth] == '' or np.isnan(data.loc[item[0],this_mounth]):
            data.loc[item[0], this_mounth] = 0
        if item[1] != 0:
            #print(data.loc[item[0],this_mounth])
            data.loc[item[0],this_mounth] = data.loc[item[0],this_mounth] + item[1]
            #data.loc[item[0],this_mounth] = data.loc[item[0],this_mounth] + item[1]

    data.to_csv(FBV_Stat_csv_registered, index=True, header=True)
    #upload_csv(FBV_Stat_csv_registered,'ELFCAM-Stock', 'FBV-2021', (data.shape[0]+2, data.shape[1] +2))


    #print(data.loc['Adapter'])
    #a = data.loc[['Adapter-APC-APC___1', this_mounth]]
    #print(a)


def check_usable_list(data_base_timbre_list):
    # 定义权重列表
    timbre_weights = [20, 50, 100, 250, 500]

    # 创建过滤条件的函数
    def filter_timbres(condition_fn):
        return {
            weight: [
                [db[0], db[1]] for db in data_base_timbre_list
                if len(db[2].strip()) == 0 and int(db[1]) == weight and condition_fn(db[0])
            ]
            for weight in timbre_weights
        }

    # France timbres condition (not 'LG', 'LW', 'LS')
    france_condition = lambda x: x[:2] not in ['LG', 'LW', 'LS']
    useable_list = filter_timbres(france_condition)

    # Foreign timbres condition (only 'LG', 'LW' but not 'LS')
    foreign_condition = lambda x: x[:2] in ['LG', 'LW'] and x[:2] != 'LS'
    useable_list_foreign = filter_timbres(foreign_condition)

    # 输出剩余的 timbres 数量
    logging.info(f"法国剩余邮票: (20,50,100,250,500): "
                 f"{len(useable_list[20])}, {len(useable_list[50])}, {len(useable_list[100])}, "
                 f"{len(useable_list[250])}, {len(useable_list[500])}")

    logging.info(f"欧洲剩余邮票: (20,50,100,250,500): "
                 f"{len(useable_list_foreign[20])}, {len(useable_list_foreign[50])}, "
                 f"{len(useable_list_foreign[100])}, {len(useable_list_foreign[250])}, "
                 f"{len(useable_list_foreign[500])}")

    return useable_list, useable_list_foreign


def create_salelist(orders):

    today_preparation = datetime.datetime.today().strftime("%Y-%m-%d")
    rows = read_csv_by_sheet("ELFCAM-Sales-List","DAILY")
    exsisting_list = [row[1] for row in rows if len(row) > 0]

    for order in orders:
        if order.order_id in exsisting_list: continue
        order_id = order.order_id
        creation_date = order.creation_date.strftime("%Y-%m-%d %H:%M:%S")
        name = order.name
        phone = order.phone.replace("+", "")
        order_channel = order.channel



        for product_id, product in enumerate(order.readable_sku_list):

            product_line = product[0] + "___" + product[1]
            quantity = order.quantity_list[product_id]
            order_item = [creation_date, order_id, product_line, quantity,order_channel, name, phone, today_preparation]
            rows.append(order_item)
    rows = sorted(rows, key=lambda row:(row[0], row[1], row[2]))
    write_and_upload_csv(rows, "csv/sale_list", "ELFCAM-Sales-List", "DAILY", wait=True)



    # with open(Sale_list_csv, 'r+', newline ='') as Sale_list:
    #     rows = csv.reader(Sale_list, delimiter=',')
    #     exsisting_list = [row[0] for row in rows if len(row) > 0]
    #     writer = csv.writer(Sale_list)
    #     writer.writerow('')
    #     for i, order in enumerate(orders):
    #         if order.order_id in exsisting_list:
    #             continue
    #         order_item = [order.order_id]
    #         for product_id, product in enumerate(order.redable_sku_list):
    #             product_line = product[0] + "___" + product[1]
    #             quantity = order.quantity_list[product_id]
    #             order_item.append(product_line +"!" + quantity)
    #         order_items.append(order_item)
            #writer.writerow(order_item)

    #property_of_sale_list = ""
    # with open(Sale_list_csv, 'r', newline='') as Sale_list:
    #     rows = csv.reader(Sale_list, delimiter=',')
    #     for row in rows:
    #         if len(row) == 0:
    #             continue
    #         if len(row[0]) == 0:
    #             property_of_sale_list = row[1:]
    #             #statistic_count = {}
    #             continue
    #         for product in row[1:]:
    #             product_line, quantity = product.split("!")[0], product.split("!")[1]
    #             if product_line in statistic_count.keys():
    #                 statistic_count[product_line] += int(quantity)
    #             else:
    #                 statistic_count[product_line] = int(quantity)



    return

        #upload_csv(Sale_list_csv, 'ELFCAM-Database', 'sale_list', np.shape(order_items))


def get_data_timbre_repository():

    data_base = data_base_reader()
    data_timbre_repository = {a[2]: ["Colissimo", a[0]] for a in data_base[1]}

    logging.info("getting timbre")

    order_line_index = {index: i for i, index in enumerate(read_csv_by_sheet("ELFCAM-Sales-List", "Colis-Today")[0])}
    order_line = read_csv_by_sheet("ELFCAM-Sales-List", "Colis-Today", wait=True)[1:]

    colis_lines_dict = {
        colis[order_line_index["order-id"]]: [colis[order_line_index["carrier"]], colis[order_line_index["tracking"]], colis[order_line_index["available"]]]
        for colis in order_line if len(colis[order_line_index["tracking"]]) > 0 }

    logging.info("getting colis")
    data_timbre_repository.update(colis_lines_dict)

    return data_timbre_repository, colis_lines_dict

def creat_price_offer(offers, price_csv ="csv/cdiscount_price.csv" , cloud_csv_name = 'Cdiscount_price', channel = "ELFCAM", other_lines = []):
    def normalize_string(string):
        string = string.replace('?m', 'um')
        string = string.replace('?', '')
        return string

    lists = []
    for offer in offers:
        title = normalize_string(offer.title)
        lists.append([offer.readable_sku[0] , offer.readable_sku[1],offer.country, offer.price, offer.stock, offer.asin, offer.id, offer.ean, title, offer.description, offer.description2, offer.description3, offer.description4, offer.description_general])
    firstline = ["goods_code1", "goods_code2", "country", "price", "stock", "asin", "id", "ean", "title", "FB", "onemonth_sale", "active", "best_offer?", "description"]
    write_and_upload_csv(lists + other_lines, price_csv, channel + "-Database", cloud_csv_name, firstline = firstline, wait=True)
    #
    # with open(price_csv,  "w", newline = '', encoding='utf-8') as csv_result:
    #     csv_writer = csv.writer(csv_result)
    #     csv_writer.writerows(list)
    # upload_csv(price_csv, 'ELFCAM-Database', cloud_csv_name)


def statistic_transfer(statistic):
    statistic_final = {}
    wp = {}
    with open('csv/ELFCAM-Database - Info.csv', 'r', newline='', encoding="utf-8") as matcher:
        rows = csv.reader(matcher, delimiter=',')
        for i, row in enumerate(rows):
            wp[row[0] + "___" + row[1]] = [a for a in row[4:] if len(a) > 0]


    for st_key, st_value in statistic.items():
        if st_key not in wp.keys():
            #print(st_key)
            continue
        transform = wp[st_key]
        if len(transform) == 0:
            if st_key in statistic_final.keys():
                statistic_final[st_key] += st_value
            else:
                statistic_final[st_key] = st_value
        else:
            for tf in transform:
                if tf in statistic_final.keys():
                    statistic_final[tf] += st_value
                else:
                    statistic_final[tf] = st_value
    return statistic_final



def csv_dict_reader(property_csv):
    with open(property_csv, 'r') as property_list:
        csv_rows = list(csv.reader(property_list, delimiter=','))
        shape = np.shape(csv_rows)
        csv_dict = [{csv_rows[0][a] : csv_rows[b][a] for a in range(shape[1])} for b in range(1, shape[0])]
    return csv_dict




def check_special_caracters(txt):
    # 允许的字符集（可能还需要进一步调整）
    normarl_strings = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890-,.@+:/()'?!(§&\"\'<>$£€%=;*¨®")

    special_caracters_need_to_be_checked = {}

    print("Checking special characters...")

    try:
        with open(txt, "r", encoding='utf-8') as file:
            file_contents = file.read()
            file_splited = file_contents.split('\n')

            # 遍历每一行的字符
            for order_id, order in enumerate(file_splited):
                for o in order:
                    # 检查字符是否在允许的字符集中，并且不在特殊字符字典中
                    if o not in normarl_strings and len(o.strip()) > 0 and o not in special_english_dict:
                        # 收集需要检查的特殊字符
                        special_caracters_need_to_be_checked[o] = (order_id + 1, order)

    except FileNotFoundError:
        print(f"Error: The file {txt} was not found.")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return

    # 如果有特殊字符需要检查
    if special_caracters_need_to_be_checked:
        print("The following special characters need to be checked:")
        for char, (line_num, line) in special_caracters_need_to_be_checked.items():
            print(f"Character: {char} found in line {line_num}: {line}")

        raise Exception(
            "Please carefully check the characters and make a reflection on ELFCAM-Database/transfer_to_english.")

    else:
        print("Checking completed successfully. No special characters found.")


# def check_special_caracters(txt):
#     normarl_strings = "abcedfghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890-,.@+:/()'?!(§&\"\'<>$£€%=;*¨®"
#     special_caracters_need_to_be_checked = {}
#     print("checking special caracters...")
#     with open(txt, "r", encoding='utf-8') as file:
#         file_contents = file.read()
#         file_splited = file_contents.split('\n')
#         for order_id, order in enumerate(file_splited):
#             for o in order:
#                 if o not in normarl_strings and len(o.strip()) > 0 and o not in special_english_dict.keys():
#                     special_caracters_need_to_be_checked[o] = o
#     if len(special_caracters_need_to_be_checked.keys()) > 0:
#         print(list(special_caracters_need_to_be_checked.keys()))
#         raise Exception("please carefully check the caracters and make a refelection on ELFCAM-Database/transfer_to_english ")
#     else:
#         print("checking over")


# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def check_special_characters_for_orders(orders):
    # 定义有效字符集
    normal_strings = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890-,.@+:/()'?!(§&\"\'<>$£€%=;*¨®"
    special_characters_need_to_be_checked = {}

    logging.info("Checking special characters...")

    # 遍历订单
    for order in orders:
        # 拼接所有相关的字段：name, instruction_name, firstline, second_line, city_line
        combined_fields = order.name + order.instruction_name + order.firstline + order.second_line + order.city_line

        # 遍历拼接后的字符串，检查每个字符
        for char in combined_fields:
            if char not in normal_strings and char.strip() and char not in special_english_dict:
                special_characters_need_to_be_checked[char] = char

    # 如果发现特殊字符，抛出异常
    if special_characters_need_to_be_checked:
        logging.error("Found special characters: %s", list(special_characters_need_to_be_checked.keys()))
        raise Exception("Please carefully check the characters and reflect on ELFCAM-Database/transfer_to_english.")
    else:
        logging.info("No special characters found. Checking over.")

    # with open(txt, "r", encoding='utf-8') as file:
    #     file_contents = file.read()
    #     file_splited = file_contents.split('\n')
    #     for order_id, order in enumerate(file_splited):
    #         for o in order:
    #             if o not in normarl_strings and len(o.strip()) > 0 and o not in special_english_dict.keys():
    #                 special_caracters_need_to_be_checked[o] = o


#from api_parser.offers.Amazon_Offer import Amazon_Offer


def check_all_amazon_sku(file):
    with open(file, 'r', encoding='utf-8', errors='ignore') as file:
        file_contents = file.read()
        file_splited = file_contents.split('\n')
        skus = []
        for line_id, line in enumerate(file_splited):
            if line_id == 0:
                sku_dict = {l: l_id for l_id, l in enumerate(line.split('\t'))}
            else:
                sku = line.split('\t')
                skus.append(sku)

    return sku_dict, skus

def asin_to_sku(reading_or_not = True, dicts = ""):
    if reading_or_not:
        Asin_values = read_csv_by_sheet("ELFCAM-Database", "Amazon_price", wait=True)
        Amazon_match = read_csv_by_sheet("ELFCAM-Database", "Amazon_match", wait=True)
        price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list", wait=True)
        asin_to_eans = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN", wait=True)
    else:
        Asin_values, Amazon_match, price_lists, asin_to_eans = dicts[0], dicts[1], dicts[2], dicts[3]

    # 创建字典，将 price_lists[0] 作为表头，映射列名到索引位置
    index_dict = {index: i for i, index in enumerate(price_lists[0])}

    # 获取 ASIN 和商品编码（goods_code）的索引
    asin_index = index_dict["asin"]
    goods_code_index = index_dict["goods_code"]

    # 生成 ASIN 到 SKU（goods_code）的映射字典
    asin_to_sku_in_price_list_dict = {
        str.strip(price_list[asin_index]): price_list[goods_code_index]
        for price_list in price_lists[1:]  # 跳过表头，从数据部分开始
    }

    # 生成匹配关系字典，ASIN -> [SKU, 额外信息]
    match_dict = {match[0]: [match[1], match[2]] for match in Amazon_match}

    # 存储最终 ASIN 到可读 SKU 的映射
    asin_to_readable_sku_dict = {}

    # 存储已经使用过的 ASIN，防止重复
    used_keys = []

    # 遍历 ASIN -> SKU 映射，筛选有效数据
    for key, value in asin_to_sku_in_price_list_dict.items():
        if key in used_keys:
            raise Exception("请检查此 ASIN 在价格表中的重复项: ", key)  # 发现重复 ASIN，抛出异常

        if key not in asin_to_readable_sku_dict.keys() and len(value) > 0 and len(key) > 0:
            if "___" not in value:  # 如果 SKU 不包含 "___"，则跳过
                print(value)
                continue
            # 拆分 "___" 前后的部分，并存入字典
            asin_to_readable_sku_dict[key] = [value.split('___')[0], value.split('___')[1]]

    # 遍历 Amazon_match 数据，补充 ASIN -> SKU 信息
    for key, value in match_dict.items():
        # 检查 ASIN 是否有效（长度为 10 且以 'B' 开头）
        if len(key) > 0 and key[0] == 'B' and len(key) == 10:
            if key not in asin_to_readable_sku_dict.keys() and len(value[0]) > 0:
                asin_to_readable_sku_dict[key] = value  # 添加到最终映射

    # 遍历 Asin_values，补充 ASIN -> SKU 信息
    for a in Asin_values:
        value = [a[0], a[1]]  # SKU 信息
        key = a[5]  # ASIN
        if key not in asin_to_readable_sku_dict.keys() and len(value) > 0 and len(key) > 0:
            asin_to_readable_sku_dict[key] = value

    # 处理 ASIN 到 EAN（国际商品编码）的映射
    index_dict = {index: i for i, index in enumerate(asin_to_eans[0])}

    for asin_to_ean in asin_to_eans[1:]:  # 跳过表头，从数据部分开始
        goods_code = asin_to_ean[index_dict["goods_code"]]

        # 跳过空值和表头重复值
        if len(goods_code) == 0 or goods_code == "goods_code":
            continue

        # 确保 SKU 包含 "___"
        if "___" not in goods_code:
            continue

        key = asin_to_ean[index_dict["asin"]]  # ASIN 码
        value = [goods_code.split("___")[0], goods_code.split("___")[1]]  # 拆分 SKU

        # 如果 ASIN 尚未记录，且值有效，则添加
        if key not in asin_to_readable_sku_dict.keys() and len(value) > 0 and len(key) > 0:
            asin_to_readable_sku_dict[key] = value

    return asin_to_readable_sku_dict

def readable_sku_to_website_id():
    website_match = read_csv_by_sheet("ELFCAM-Database", "Website_match")
    website_dict = {w_m[1] + "___" + w_m[2] : w_m[0] for w_m in website_match if len(w_m[2]) > 0}
    website_dict.update({w_m[1]: w_m[0] for w_m in website_match if len(w_m[2]) == 0})

    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()
    readablesku_to_dict_ignore_case.set_dict(website_dict)
    return readablesku_to_dict_ignore_case

def cd_ean_to_sku():
    cd_ean_values = read_csv_by_sheet("ELFCAM-Database", "Cdiscount_price")

    # values_to_be_checked = {a[0]:''  for a in all_values if a[1] == ''}
    cd_ean_to_readable_sku = {a[7]:[a[0], a[1]] for a in cd_ean_values}
    return cd_ean_to_readable_sku

def ean_to_sku(reading_or_not = True, dicts = ""):
    # 读取 "price_list" 表格，获取 EAN 到 SKU 的映射
    if reading_or_not:
        price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list", wait=True)
        ean_to_sku_lists_from_table = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN", wait=True)
        extenstion_ean_to_sku_lists = read_csv_by_sheet("ELFCAM-Database", "Cdiscount_extension", wait=True)
    else:
        price_lists, ean_to_sku_lists_from_table, extenstion_ean_to_sku_lists = dicts[0], dicts[1], dicts[2]

    # 生成列名到索引的映射字典
    index_dict = {index: i for i, index in enumerate(price_lists[0])}

    # 获取 EAN 和 SKU 的索引
    ean_index = index_dict["ean"]
    sku_index = index_dict["goods_code"]

    # 创建初始的 EAN -> SKU 映射字典
    ean_to_sku = {
        price_list[ean_index]: price_list[sku_index]
        for price_list in price_lists[1:]  # 跳过表头
    }

    # 读取 "Asin_to_EAN" 表格，补充 EAN -> SKU 信息


    for ean_to_sku_list in ean_to_sku_lists_from_table:
        _ean = ean_to_sku_list[1].zfill(13)[-13:]  # 处理 EAN，确保是 13 位
        _sku = ean_to_sku_list[2]

        # 只有当 EAN 不在字典中且 SKU 不为空时才添加
        if _ean not in ean_to_sku and len(_sku) != 0:
            ean_to_sku[_ean] = _sku

    # 读取 "Cdiscount_extension" 表格，进一步扩展 EAN -> SKU 映射


    # 重新生成列索引映射，确保正确提取数据
    index_dict = {index: i for i, index in enumerate(extenstion_ean_to_sku_lists[0])}

    # 遍历扩展表，补充 EAN -> SKU 映射
    for extenstion_ean_to_sku_list in extenstion_ean_to_sku_lists[1:]:  # 跳过表头
        ean = extenstion_ean_to_sku_list[index_dict["ean"]]
        sku = extenstion_ean_to_sku_list[index_dict["goods_code"]]

        # 只在 SKU 有效时才添加到字典
        if ean and sku:
            ean_to_sku[ean] = sku

    return ean_to_sku



def sku_to_title(title="title", reading_or_not = True, dicts = ""):
    """
    从 'price_list' 表格中提取 SKU -> 商品标题(title) 的映射。

    参数:
        title (str): 需要提取的标题列名，默认为 "title"。

    返回:
        dict: SKU 到商品标题的映射字典 {SKU: Title}
    """
    # 读取价格表数据
    if reading_or_not:
        price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list", wait=True)
    else:
        price_lists = dicts[0]

    # 生成列索引映射
    index_dict = {index: i for i, index in enumerate(price_lists[0])}

    # 获取 'title' 和 'goods_code' (SKU) 的索引
    if title not in index_dict or "goods_code" not in index_dict:
        raise KeyError(f"'{title}' 或 'goods_code' 列未在表格中找到，请检查表头")

    title_index = index_dict[title]
    sku_index = index_dict["goods_code"]

    # 构建 SKU -> 商品标题映射字典
    sku_to_title_dict = {
        row[sku_index]: row[title_index]
        for row in price_lists[1:]  # 跳过表头
        if row[sku_index] and row[title_index]  # 过滤空值
    }

    return sku_to_title_dict


def ean_to_asin(reading_or_not = True, dicts = ""):
    # 读取 "price_list" 表格，获取 EAN -> ASIN 映射
    if reading_or_not:
        price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list", wait=True)
        # 读取 "Asin_to_EAN" 表格，提取 EAN -> ASIN 映射
        asin_to_ean_lists = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN", wait=True)

        # 读取 "vc_cost" 表格，提取 EAN -> ASIN 数据
        asin_to_ean_vc_lists = read_csv_by_sheet("ELFCAM-Database", "vc_cost", wait=True)
    else:
        price_lists,asin_to_ean_lists,asin_to_ean_vc_lists  = dicts[0], dicts[1], dicts[2]

    # 生成列名到索引的映射字典
    index_dict = {index: i for i, index in enumerate(price_lists[0])}

    # 获取 ASIN 和 EAN 的索引
    asin_index = index_dict["asin"]
    ean_index = index_dict["ean"]

    # 创建初始 EAN -> ASIN 映射字典
    ean_to_asin = {
        price_list[ean_index]: price_list[asin_index]
        for price_list in price_lists[1:]  # 跳过表头
    }



    # 生成该表的列索引映射
    ean_asin_index_dict = {index: i for i, index in enumerate(asin_to_ean_lists[0])}

    # 创建 EAN -> ASIN 映射字典
    ean_to_asin_from_asin_to_ean_table = {
        row[ean_asin_index_dict["ean"]]: row[ean_asin_index_dict["asin"]]
        for row in asin_to_ean_lists[1:]  # 跳过表头
    }


    # 假设表格中 ASIN 在索引 2，EAN 在索引 3
    ean_to_asin_vc_table = {
        row[3]: row[2]
        for row in asin_to_ean_vc_lists[1:]  # 跳过表头
    }

    # 合并不同表格的数据
    ean_to_asin_from_asin_to_ean_table.update(ean_to_asin_vc_table)  # 先更新 vc_cost 数据
    ean_to_asin_from_asin_to_ean_table.update(ean_to_asin)  # 再更新 price_list 数据

    return ean_to_asin_from_asin_to_ean_table


def read_csv_and_create_dict(sheet_name, key_column, value_column, wait=False):
    """读取 CSV 并根据指定的列创建字典"""
    try:
        data = read_csv_by_sheet("ELFCAM-Database", sheet_name, wait=wait)
        index_dict = {index: i for i, index in enumerate(data[0])}
        key_index = index_dict[key_column]
        value_index = index_dict[value_column]

        result_dict = {row[key_index]: row[value_index] for row in data[1:]}
        #logging.info(f"Successfully read {sheet_name} and created dictionary.")
        return result_dict
    except Exception as e:
        logging.error(f"Failed to read {sheet_name} or create dictionary: {e}")
        return {}


def readable_sku_to_ean():
    """创建可读取的 SKU 到 EAN 的映射，并返回一个忽略大小写的字典"""
    # 读取 price_list 表并创建 SKU 到 EAN 的映射字典
    #readable_sku_to_ean_dict = read_csv_and_create_dict("price_list", "goods_code", "ean")

    # 读取 Asin_to_EAN 表并创建 SKU 到 EAN 的映射字典
    asin_to_ean_dict = read_csv_and_create_dict("Asin_to_EAN", "goods_code", "ean", wait=True)

    # 合并字典
    #readable_sku_to_ean_dict.update(asin_to_ean_dict)
    #logging.info("Dictionaries merged successfully.")

    # 创建忽略大小写的字典
    readablesku_to_dict_ignore_case = Ignore_Case_dict()
    readablesku_to_dict_ignore_case.set_dict(asin_to_ean_dict)

    return readablesku_to_dict_ignore_case

def readable_sku_to_ean_with_merge():
    """创建可读取的 SKU 到 EAN 的映射，并返回一个忽略大小写的字典"""
    # 读取 price_list 表并创建 SKU 到 EAN 的映射字典
    readable_sku_to_ean_dict = read_csv_and_create_dict("price_list", "goods_code", "ean")

    # 读取 Asin_to_EAN 表并创建 SKU 到 EAN 的映射字典
    asin_to_ean_dict = read_csv_and_create_dict("Asin_to_EAN", "goods_code", "ean", wait=True)

    # 合并字典
    readable_sku_to_ean_dict.update(asin_to_ean_dict)
    logging.info("Dictionaries merged successfully.")

    # 创建忽略大小写的字典
    readablesku_to_dict_ignore_case = Ignore_Case_dict()
    readablesku_to_dict_ignore_case.set_dict(asin_to_ean_dict)

    return readablesku_to_dict_ignore_case

def is_latin1(s):
    try:
        s.encode('latin-1')
        return True
    except UnicodeEncodeError:
        return False

def asin_to_ean(reading_or_not = True, dicts = ""):


    def check_consistency(table_dict, asin_to_ean):
        # 获取logger对象
        logger = logging.getLogger(__name__)

        # 遍历每个key, value对
        for key, value in table_dict.items():
            # 如果key在asin_to_ean字典中存在
            if key in asin_to_ean.keys():
                # 如果value为空或key为"asin"，或者asin_to_ean[key]为空，跳过
                if len(value) == 0 or key == "asin" or len(asin_to_ean[key]) == 0:
                    continue
                # 比较最后13个字符是否一致
                if value.zfill(13)[-13:] != asin_to_ean[key].zfill(13)[-13:]:
                    risk = f"{key} this asin is not consistent"
                    # 使用logging记录不一致的情况
                    logger.warning(
                        f"{risk} {value[-13:]} (in Asin to Ean Table) with {asin_to_ean[key].zfill(13)[-13:]} (in Database Pricelist Table)")

                    # 用户确认是否继续
                    #check_result = input("Please check this, and continue (y/n):")
                    # if check_result.lower() == "y":
                    #     continue
                    # else:
                    #     raise Exception(risk, value[-13:])
            else:
                # 如果key不在asin_to_ean字典中，新增asin和ean对
                asin_to_ean[key] = value.zfill(13)
                # logger.info(f"{key} this listing is from the additional table")  # 可选，是否打印额外添加的asin

        return asin_to_ean

    # 读取数据：如果 reading_or_not 为 True，从数据库读取，否则使用传入的字典
    if reading_or_not:
        price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list", wait=5)
        asin_to_ean_lists = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN", wait=5)
        #asin_to_ean_vc_lists = read_csv_by_sheet("ELFCAM-Database", "vc_cost", wait=5)
    else:
        price_lists, asin_to_ean_lists = dicts[0], dicts[1]
            #, asin_to_ean_vc_lists = dicts[0], dicts[1], dicts[2]

    index_dict = {index: i for i, index in enumerate(price_lists[0])}
    asin_index = index_dict["asin"]
    ean_index = index_dict["ean"]
    asin_to_ean = {price_list[asin_index].strip(): price_list[ean_index] for price_list in price_lists[1:]}

    # 读取Asin_to_EAN表

    asin_to_ean_index_dict = {index: i for i, index in enumerate(asin_to_ean_lists[0])}
    asin_to_ean_from_asin_to_ean_table = {a[asin_to_ean_index_dict["asin"]].strip(): a[asin_to_ean_index_dict["ean"]] for a in
                                          asin_to_ean_lists}



    # 读取vc_cost表

    #asin_to_ean_vc_table = {a[2]: a[3] for a in asin_to_ean_vc_lists}

    # 检查一致性
    #check_consistency(asin_to_ean_vc_table, asin_to_ean)
    check_consistency(asin_to_ean_from_asin_to_ean_table, asin_to_ean)

    # asin_to_ean.update(asin_to_ean_from_asin_to_ean_table)

    return asin_to_ean_from_asin_to_ean_table

def amazonsku_to_asin():
    Asin_values = read_csv_by_sheet("ELFCAM-Database", "Amazon_price")

    # values_to_be_checked = {a[0]:''  for a in all_values if a[1] == ''}
    asin_to_readable_sku = {a[6]:a[5] for a in Asin_values}
    return asin_to_readable_sku


def readablesku_to_asin(reading_or_not = True, dicts = ""):
    if reading_or_not:
        Price_lists = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN")
    else:
        Price_lists = dicts

    price_list_index = {string: i for i, string in enumerate(Price_lists[0])}

    readablesku_to_asin_dict = {price_list[price_list_index["goods_code"]] : price_list[price_list_index["asin"]] for price_list in Price_lists[1:]}

    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readablesku_to_asin_dict)


    return readablesku_to_dict_ignore_case

# def asin_to_readable_sku():
#     Price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list")
#
#     price_list_index = {string: i for i, string in enumerate(Price_lists[0])}
#
#     asin_to_readable_sku_dict = {price_list[price_list_index["asin"]]: price_list[price_list_index["goods_code"]]  for price_list in Price_lists[1:]}
#
#     return asin_to_readable_sku_dict


def readablesku_to_fba_id():

    WMS_Stock = read_csv_by_sheet("ELFCAM_WMS", "New_WMS_Stock", wait=True)

    index_dict = {index: i for i, index in enumerate(WMS_Stock[0])}
    goods_code_index = index_dict[[index for index in index_dict.keys() if "last updated" in index][0]]
    fba_id_index = index_dict["fba_id"]
    readablesku_to_fba_id_dict = {wms_list[goods_code_index] : wms_list[fba_id_index] for wms_list in WMS_Stock[1:] if len(wms_list[fba_id_index]) > 0}

    Asin_to_Ean = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN", wait=True)
    index_dict = {index: i for i, index in enumerate(Asin_to_Ean[0])}
    goods_code_index = index_dict["goods_code"]
    fba_id_index = index_dict["fba_id"]
    readablesku_to_fba_id_dict.update({a_t_e[goods_code_index] : a_t_e[fba_id_index] for a_t_e in Asin_to_Ean[1:] if len(a_t_e[fba_id_index]) > 0})

    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readablesku_to_fba_id_dict)


    return readablesku_to_dict_ignore_case


def asin_to_fba_id(reading_or_not = True, dicts = ""):
    """
    构建 ASIN 到 FBA ID 的映射关系。

    数据来源：
    1. `ELFCAM_WMS` 数据库的 `New_WMS_Stock` 表
    2. `ELFCAM-Database` 数据库的 `Asin_to_EAN` 表

    返回:
        dict: ASIN 到 FBA ID 的映射字典 {ASIN: FBA_ID}
    """
    if reading_or_not:
        # 读取 WMS 库存表
        WMS_Stock = read_csv_by_sheet("ELFCAM_WMS", "New_WMS_Stock", wait=True)
        # 读取 Asin_to_EAN 数据表
        Asin_to_Ean = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN", wait=True)
    else:
        WMS_Stock, Asin_to_Ean = dicts[0], dicts[1]


    # 生成列索引映射
    index_dict = {index: i for i, index in enumerate(WMS_Stock[0])}

    # 校验必要字段是否存在
    if "asin" not in index_dict or "fba_id" not in index_dict:
        raise KeyError("WMS 库存表中缺少 'asin' 或 'fba_id' 列，请检查数据")

    asin_index = index_dict["asin"]
    fba_id_index = index_dict["fba_id"]

    # 构建 ASIN -> FBA ID 映射字典 (过滤空值)
    asin_to_fba_id_dict = {
        row[asin_index]: row[fba_id_index]
        for row in WMS_Stock[1:]  # 跳过表头
        if row[asin_index] and row[fba_id_index]  # 确保 ASIN 和 FBA ID 非空
    }



    # 生成列索引映射
    index_dict = {index: i for i, index in enumerate(Asin_to_Ean[0])}

    # 校验必要字段是否存在
    if "asin" not in index_dict or "fba_id" not in index_dict:
        raise KeyError("Asin_to_EAN 表中缺少 'asin' 或 'fba_id' 列，请检查数据")

    asin_index = index_dict["asin"]
    fba_id_index = index_dict["fba_id"]

    # 更新 ASIN -> FBA ID 映射字典 (以 Asin_to_EAN 数据表为补充)
    asin_to_fba_id_dict.update({
        row[asin_index]: row[fba_id_index]
        for row in Asin_to_Ean[1:]  # 跳过表头
        if row[asin_index] and row[fba_id_index]  # 确保 ASIN 和 FBA ID 非空
    })

    return asin_to_fba_id_dict


def fba_id_to_readablesku():
    WMS_Stock = read_csv_by_sheet("ELFCAM_WMS", "New_WMS_Stock")
    Fba_id_to_readable_sku = {fba[2]: fba[0] for fba in read_csv_by_sheet("ELFCAM-Database", "fba_id_match")[1:]}


    index_dict = {index: i for i, index in enumerate(WMS_Stock[0])}
    goods_code_index = index_dict[[index for index in index_dict.keys() if "last updated" in index][0]]
    fba_id_index = index_dict["fba_id"]
    fba_id_to_readablesku_dict = {price_list[fba_id_index] : price_list[goods_code_index] for price_list in WMS_Stock[1:]}

    Fba_id_to_readable_sku.update(fba_id_to_readablesku_dict)
    #fba_id_to_readablesku_dict.update(Fba_id_to_readable_sku)

    return Fba_id_to_readable_sku

def fba_id_to_sku():
    WMS_Stock = read_csv_by_sheet("ELFCAM_WMS", "New_WMS_Stock")
    Fba_id_to_sku = {fba[0]: fba[1] for fba in read_csv_by_sheet("ELFCAM-Database", "fba_id_match")[1:]}

    index_dict = {index: i for i, index in enumerate(WMS_Stock[0])}
    sku_index = index_dict["sku"]
    fba_id_index = index_dict["fba_id"]
    fba_id_to_sku_dict = {price_list[fba_id_index] : price_list[sku_index] for price_list in WMS_Stock[1:]}

    Fba_id_to_sku.update(fba_id_to_sku_dict)
    #fba_id_to_sku_dict.update(Fba_id_to_sku)

    return Fba_id_to_sku

def fba_id_to_ean():
    Asin_to_ean = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN")
    Asin_to_ean_list = Asin_to_ean[1:]
    Asin_to_ean_index = {index: i for i, index in enumerate(Asin_to_ean[0])}
    ean_index = Asin_to_ean_index["ean"]
    fba_id_index = Asin_to_ean_index["fba_id"]

    return  {asin_to_ean[fba_id_index] : asin_to_ean[ean_index] for asin_to_ean in Asin_to_ean_list}

def readablesku_to_sku():
    WMS_Stock = read_csv_by_sheet("ELFCAM_WMS", "New_WMS_Stock")

    index_dict = {index: i for i, index in enumerate(WMS_Stock[0])}
    goods_code_index = index_dict[[index for index in index_dict.keys() if "last updated" in index][0]]
    sku_index = index_dict["sku"]
    readablesku_to_sku_dict = {price_list[goods_code_index] : price_list[sku_index] for price_list in WMS_Stock[1:]}

    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()
    readablesku_to_dict_ignore_case.set_dict(readablesku_to_sku_dict)
    return readablesku_to_dict_ignore_case


def get_wms_dict():
    stock_values = read_csv_by_sheet("ELFCAM_WMS", "New_WMS_Stock", wait=True)
    wms_dict = {a[0]: a[3] for a in stock_values}
    return wms_dict

def check_skus_to_amazonmatch():
    asin_to_readable_sku_dict = asin_to_sku()
    amazon_sku_to_asin_dict = amazonsku_to_asin()
    match_dict = find_match("csv/amazon_match.csv")
    #print(match_dict)

    new_match = {}

    for match_key, match_value in match_dict.items():
        if len(match_value[0]) > 0:
            new_match[match_key] = match_value
        elif match_key in amazon_sku_to_asin_dict.keys():
            asin = amazon_sku_to_asin_dict[match_key]
            if asin in asin_to_readable_sku_dict.keys():
                new_match[match_key] = asin_to_readable_sku_dict[asin]
            else:
                new_match[match_key] = ["",""]
                #print(asin_to_readable_sku_dict[asin])
            #else:
                #print(asin)
        else:
            new_match[match_key] = ["",""]


    new_match_dict = {key : [key, value[0], value[1]] for key, value in new_match.items()}


    for amazon_sku, asin in amazon_sku_to_asin_dict.items():
        if amazon_sku not in match_dict.keys():
            if asin in asin_to_readable_sku_dict.keys():
                readable_sku_0, readable_sku_1 = asin_to_readable_sku_dict[asin][0],asin_to_readable_sku_dict[asin][1]
                new_match_dict[amazon_sku] = [amazon_sku, readable_sku_0, readable_sku_1 ]
                print(amazon_sku + " is " + readable_sku_0 + "___" + readable_sku_1 )
            else:
                print(amazon_sku + " is not in")

    new_match_list = list(new_match_dict.values())

    write_and_upload_csv(new_match_list,"csv/Amazon_match.csv", "ELFCAM-Database", "Amazon_match", "")

def check_skus_to_cdiscountmatch():
    price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list")
    cdiscount_match = read_csv_by_sheet("ELFCAM-Database", "Cdiscount_match")
    index_dict = {index: i for i, index in enumerate(price_lists[0])}
    cdiscount_match_dict = {cd[0]: [cd[1], cd[2]] for cd in cdiscount_match}
    ean_to_sku_dict = ean_to_sku()

    for cd_match_key, cd_match_value in cdiscount_match_dict.items():
        if len(cd_match_value[0]) == 0:
            ean = cd_match_key[-13:]
            if ean in ean_to_sku_dict.keys():
                sku = ean_to_sku_dict[ean]
                print(cd_match_key + " is " + sku)
                cdiscount_match_dict[cd_match_key] = [sku.split('___')[0], sku.split('___')[1]]

    new_match_list = [[key, value[0], value[1]] for key, value in cdiscount_match_dict.items()]
    write_and_upload_csv(new_match_list,"csv/cdiscount_match.csv", "ELFCAM-Database", "Cdiscount_match", "")


def check_skus_to_rakutenmatch():
    from API.Rakuten_Suivi import get_rakuten_stock
    from api_parser.Parsers import parser_rakuten_offer
    price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list")
    rakuten_match = read_csv_by_sheet("ELFCAM-Database", "Rakuten_match")
    index_dict = {index: i for i, index in enumerate(price_lists[0])}
    rakuten_match_dict = {rk[0]: [rk[1], rk[2]] for rk in rakuten_match}
    ean_to_sku_dict = ean_to_sku()

    Rakuten_offer_XML = "XML/xml_rakuten_offers.xml"
    get_rakuten_stock(Rakuten_offer_XML)
    rakuten_offers = parser_rakuten_offer(Rakuten_offer_XML, match=False)
    rakuten_ids = [offer.ean for offer in rakuten_offers]

    #print(ean_to_sku_dict)
    for rk_match_key, rk_match_value in rakuten_match_dict.items():
        if len(rk_match_value[0]) == 0:
            ean = rk_match_key[-13:]
            if ean in ean_to_sku_dict.keys():
                sku = ean_to_sku_dict[ean]
                rakuten_match_dict[rk_match_key] = [sku.split('___')[0], sku.split('___')[1]]


    for rakuten_id in rakuten_ids:

        ean = rakuten_id.zfill(13)[-13:]
        if ean in ean_to_sku_dict.keys() and ean not in rakuten_match_dict.keys():
            sku = ean_to_sku_dict[ean]
            rakuten_match_dict[ean] = [sku.split('___')[0], sku.split('___')[1]]
        elif ean in rakuten_match_dict.keys():
            continue
        else:
            print(ean)
            continue

    new_match_list = [[key, value[0], value[1]] for key, value in rakuten_match_dict.items()]
    write_and_upload_csv(new_match_list,"csv/rakuten_match.csv", "ELFCAM-Database", "Rakuten_match", "")

def check_skus_to_leroymerlinmatch():
    leroymerlin_price = read_csv_by_sheet("ELFCAM-Database", "Leroy_Merlin_price")
    leroymerlin_match = read_csv_by_sheet("ELFCAM-Database", "Leroy_Merlin_match")

    index_dict = {index: i for i, index in enumerate(leroymerlin_match[0])}
    leroymerlin_match_dict = {lm[0]: [lm[1], lm[2]] for lm in leroymerlin_match}

    index_dict = {index: i for i, index in enumerate(leroymerlin_price[0])}
    leroymerlinsku_to_ean_dict = {lm[index_dict["asin"]]:lm[index_dict["ean"]] for lm in leroymerlin_price[1:]}

    ean_to_sku_dict = ean_to_sku()
    for leroymerlin_sku, ean in leroymerlinsku_to_ean_dict.items():
        if leroymerlin_sku not in leroymerlin_match_dict.keys():
            the_ean = ean.zfill(13)
            if the_ean in ean_to_sku_dict.keys():
                readablesku = ean_to_sku_dict[the_ean]
                readablesku_0, readablesku_1 = readablesku.split("___")[0], readablesku.split("___")[1]
                leroymerlin_match_dict[leroymerlin_sku] = [readablesku_0, readablesku_1]
                print(leroymerlin_sku + " is " + readablesku)
            else:
                print(the_ean + " is not in ean databases")

    new_leroymerlin_match_list = [[key, value[0], value[1]] for key, value in leroymerlin_match_dict.items()]

    write_and_upload_csv(new_leroymerlin_match_list, "csv/leroymerlin_match.csv", "ELFCAM-Database", "Leroy_Merlin_match", "")

def check_skus_to_websitematch():
        #leroymerlin_price = read_csv_by_sheet("ELFCAM-Database", "Leroy_Merlin_price")
        Website_match = read_csv_by_sheet("ELFCAM-Database", "Website_match")
        readable_sku_to_ean_dict = readable_sku_to_ean()


        index_dict = {index: i for i, index in enumerate(Website_match[0])}
        new_website_match = []
        for wb in Website_match:
            readable_sku = wb[1] + "___" + wb[2]
            if len(wb[3]) == 0:
                if readable_sku_to_ean_dict.check_key(readable_sku):
                    ean = readable_sku_to_ean_dict[readable_sku]
                else:
                    ean = ''
            else:
                ean = wb[3]
            new_website_match.append(wb[:3] + [ean])
        new_website_match = sorted(new_website_match, key=lambda w:(w[1], w[2].zfill(5)))
        write_and_upload_csv(new_website_match, "csv/website_match.csv", "ELFCAM-Database", "Website_match", "")

        # website_match_dict = {lm[0]: [lm[1], lm[2]] for lm in Website_match}
        #
        # index_dict = {index: i for i, index in enumerate(leroymerlin_price[0])}
        # leroymerlinsku_to_ean_dict = {lm[index_dict["asin"]]: lm[index_dict["ean"]] for lm in leroymerlin_price[1:]}
        #
        # ean_to_sku_dict = ean_to_sku()
        # for leroymerlin_sku, ean in leroymerlinsku_to_ean_dict.items():
        #     if leroymerlin_sku not in leroymerlin_match_dict.keys():
        #         the_ean = ean.zfill(13)
        #         if the_ean in ean_to_sku_dict.keys():
        #             readablesku = ean_to_sku_dict[the_ean]
        #             readablesku_0, readablesku_1 = readablesku.split("___")[0], readablesku.split("___")[1]
        #             leroymerlin_match_dict[leroymerlin_sku] = [readablesku_0, readablesku_1]
        #             print(leroymerlin_sku + " is " + readablesku)
        #         else:
        #             print(the_ean + " is not in ean databases")
        #
        # new_leroymerlin_match_list = [[key, value[0], value[1]] for key, value in leroymerlin_match_dict.items()]
        #
        # write_and_upload_csv(new_leroymerlin_match_list, "csv/leroymerlin_match.csv", "ELFCAM-Database",
        #                      "Leroy_Merlin_match", "")

    #print(leroymerlin_sku, ean_to_sku_dict[ean.zfill(13)])


def update_info():
    price_lists_goods_code = [p_l[0] for p_l in read_csv_by_sheet("ELFCAM-Database", "price_list")[1:]]
    infos = read_csv_by_sheet("ELFCAM-Sales-List", "Info") + read_csv_by_sheet("ANDO-Database", "Info")
    info_goods_code = [info[0] + "___" + info[1] for info in infos]
    new_infos = infos

    exception_list = ["all", "deactivite", "", "goods_code2"]
    for goods_code in price_lists_goods_code:
        goods_code1, goods_code2 = goods_code.split("___")[0], goods_code.split("___")[1]
        if goods_code2 not in exception_list and goods_code not in info_goods_code:
            new_infos.append([goods_code1, goods_code2])

    write_and_upload_csv(new_infos,"csv/info.csv", "ELFCAM-Sales-List", "Info")


def readablesku_to_image(reading_or_not = True, dicts = ""):
    if reading_or_not:

        price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list", wait=True)

    else:
        price_lists = dicts[0]

    #price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list",wait=True)
    index_dict = {index: i for i, index in enumerate(price_lists[0])}
    main_image_index = index_dict["image_url_1"]
    goods_code_index = index_dict["goods_code"]
    readablesku_to_image_dict = {price_list[goods_code_index]: price_list[main_image_index] for price_list in price_lists[1:]}

    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readablesku_to_image_dict)

    return readablesku_to_dict_ignore_case


def write_and_upload_csv(data_list, csv_file, table, sheet_name, firstline=None, clear=True, wait=None):
    """
    将列表数据写入 CSV 文件，并尝试上传到指定表格。

    参数：
    - data_list: 数据列表
    - csv_file: 输出 CSV 文件路径
    - table: 目标表名
    - sheet_name: 目标工作表名
    - firstline: 可选的第一行标题
    - clear: 上传时是否清空表格
    - wait: 失败重试间隔（秒），默认不等待或设置为 5 秒
    """
    # 将数据写入 CSV 文件
    with open(csv_file, 'w', newline='', encoding="utf-8") as csv_result:
        writer = csv.writer(csv_result)

        # 写入第一行（如有）
        if firstline and isinstance(firstline, (list, tuple)):
            writer.writerow(firstline)

        # 写入数据
        writer.writerows(data_list)

    # 上传 CSV 文件
    retry_attempts = 20  # 最大重试次数
    wait_time = wait if wait is not None else 0  # 默认不等待

    for attempt in range(retry_attempts):
        try:
            # 调用上传函数
            upload_csv(csv_file, table, sheet_name, clear=clear)
            break  # 成功后退出循环
        except Exception as e:
            if attempt < retry_attempts - 1:
                logging.warning(f"Upload failed (attempt {attempt + 1}). Retrying in {wait_time} secs... Error: {e}")
                time.sleep(wait_time)
            else:
                logging.error(f"Max retry attempts reached. Failed to upload to {table} - {sheet_name}. Error: {e}")



def get_last_sarturday(weeks = 0):
    #day_dict = {"SUN": 1, "SAT": 2, "FRI": 3, "THU": 4, "WED": 5, "TUE": 6, "MON": 7}
    #gap_day = 2
    today = datetime.datetime.today()
    gap_day = today.weekday() + 1
    today = datetime.datetime.today()
    idx = (gap_day) % 7
    if idx < 2:
        idx = (gap_day) % 7 + 7
    #last_sarturday = today - datetime.timedelta(1 + 7 * (weeks-1) + idx)
    last_sarturday = today - datetime.timedelta(1 + 7 * (weeks) + idx)
    last_of_last_sunday = today - datetime.timedelta(7 * (weeks + 1) + idx)

    print("Getting data from " + str(last_of_last_sunday) + " to " + str(last_sarturday))
    return last_sarturday,last_of_last_sunday

def get_previous_day(year, month, day):
    from datetime import datetime, timedelta

    given_date = datetime(year, month, day)
    previous_day = given_date - timedelta(days=1)
    return previous_day.year, previous_day.month, previous_day.day

def get_last_sunday(weeks = 0):
    #day_dict = {"SUN": 1, "SAT": 2, "FRI": 3, "THU": 4, "WED": 5, "TUE": 6, "MON": 7}
    #gap_day = 2
    today = datetime.datetime.today()
    gap_day = today.weekday() + 1
    today = datetime.datetime.today()
    idx = (gap_day) % 7
    if idx < 2:
        idx = (gap_day) % 7 + 7

    last_sunday = today - datetime.timedelta(7 * (weeks-1) + idx)
    last_of_last_sunday = today - datetime.timedelta(7 * (weeks) + idx)

    print("Getting data from " + str(last_of_last_sunday) + " to " + str(last_sunday))
    return last_sunday,last_of_last_sunday

def get_days(number_of_last_week, days_before):
    from datetime import datetime, timedelta
    # 1. 获取当前日期
    today = datetime.now()

    # 2. 找到上周三
    # `weekday()`: 周一是 0，周三是 2
    days_since_last_wednesday = (today.weekday() - number_of_last_week) % 7 + 7
    last_wednesday = today - timedelta(days=days_since_last_wednesday)

    # 3. 计算上周三之前 40 天的日期
    forty_days_before_last_wednesday = last_wednesday - timedelta(days=days_before)

    return last_wednesday, forty_days_before_last_wednesday


def get_range(day = "SUN", gap_day = 30):
    #day_dict = {"SUN": 1, "SAT": 2, "FRI": 3, "THU": 4, "WED": 5, "TUE": 6, "MON": 7}
    #gap_day = 2
    #today.weekday() + 1
    today = datetime.datetime.today()
    idx = (2) % 7
    last_sun = today - datetime.timedelta(7 + idx)
    last_day = last_sun - datetime.timedelta(gap_day)

    print("Getting data from " + str(last_day) + " to " + str(last_sun))
    return last_day, last_sun


def get_latest_file(file_path, key_word = "order", extension = ".xlsx"):
    from os import listdir
    from os.path import isfile, join

    downloads_path = file_path
    onlyfiles = [f for f in listdir(downloads_path) if isfile(join(downloads_path, f))]

    orderfiles = sorted([order_file for order_file in onlyfiles if (key_word in order_file) and (extension in order_file) and ("$" not in order_file)],
                        key=lambda p: os.path.getmtime(downloads_path + "/" + p))
    latest_file = orderfiles[-1]
    latest_file_path = downloads_path + "/" + latest_file

    return latest_file_path


def get_latest_files(file_path, key_word = "order", extension = ".xlsx", days = 1):
    from os import listdir
    from os.path import isfile, join


    downloads_path = file_path
    onlyfiles = [f for f in listdir(downloads_path) if isfile(join(downloads_path, f))]


    orderfiles = sorted([order_file for order_file in onlyfiles if (key_word in order_file) and (extension in order_file) and ("$" not in order_file)],
                        key=lambda p: os.path.getmtime(downloads_path + "/" + p))

    today_orderfiles = [downloads_path + "/" + f for f in orderfiles if datetime.datetime.fromtimestamp(os.path.getmtime(downloads_path + "/" + f)).date() == datetime.datetime.today().date()]

    # latest_file = orderfiles[-1]
    # latest_file_path = downloads_path + "/" + latest_file

    return today_orderfiles

def readable_sku_to_lm_sku():
    lm_match = read_csv_by_sheet("ELFCAM-Database", "Leroy_Merlin_match", True)
    readable_sku_to_lm_sku_dict = {}
    for lm in lm_match:
        if len(lm[1]) == 0: continue
        readable_sku = lm[1] + "___" + lm[2]
        if readable_sku not in readable_sku_to_lm_sku_dict.keys():
            readable_sku_to_lm_sku_dict[readable_sku] = [lm[0]]
        else:
            if lm[0] not in readable_sku_to_lm_sku_dict[readable_sku]:
                readable_sku_to_lm_sku_dict[readable_sku].append(lm[0])

    lm_price = read_csv_by_sheet("ELFCAM-Database", "Leroy_Merlin_price")
    index_dict = {r:i for i,r in enumerate(lm_price[0])}
    for lm in lm_price[1:]:
        if len(lm[index_dict["goods_code1"]]) == 0: continue
        readable_sku = lm[index_dict["goods_code1"]] + "___" + lm[index_dict["goods_code2"]]
        if readable_sku not in readable_sku_to_lm_sku_dict.keys():
            readable_sku_to_lm_sku_dict[readable_sku] = [lm[index_dict["asin"]]]
        else:
            if lm[index_dict["asin"]] not in readable_sku_to_lm_sku_dict[readable_sku]:
                readable_sku_to_lm_sku_dict[readable_sku].append(lm[index_dict["asin"]])

    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readable_sku_to_lm_sku_dict)

    return readablesku_to_dict_ignore_case

def readable_sku_to_am_sku_by_countries():
    am_price = read_csv_by_sheet("ELFCAM-Database", "Amazon_price", True)
    readable_sku_to_am_sku_dict = {}
    index_dict = {index: i for i, index in enumerate(am_price[0])}
    for am in am_price:
        country = am[index_dict["country"]]
        readable_sku = am[index_dict["goods_code1"]] + "___" + am[index_dict["goods_code2"]]
        sku = am[index_dict["id"]]
        if readable_sku not in readable_sku_to_am_sku_dict.keys():
            readable_sku_to_am_sku_dict[readable_sku] = {country : [sku]}
        elif country not in readable_sku_to_am_sku_dict[readable_sku].keys():
            readable_sku_to_am_sku_dict[readable_sku].update({country : [sku]})
        else:
            readable_sku_to_am_sku_dict[readable_sku][country].append(sku)
    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readable_sku_to_am_sku_dict)

    return readablesku_to_dict_ignore_case

def readable_sku_to_am_sku():
    am_price = read_csv_by_sheet("ELFCAM-Database", "Amazon_price", True)
    readable_sku_to_am_sku_dict = {}
    index_dict = {index: i for i, index in enumerate(am_price[0])}
    for am in am_price:
        readable_sku = am[index_dict["goods_code1"]] + "___" + am[index_dict["goods_code2"]]
        sku = am[index_dict["id"]]
        if readable_sku not in readable_sku_to_am_sku_dict.keys():
            readable_sku_to_am_sku_dict[readable_sku] = [sku]
        elif sku not in readable_sku_to_am_sku_dict[readable_sku]:
            readable_sku_to_am_sku_dict[readable_sku].append(sku)

    am_match = read_csv_by_sheet("ELFCAM-Database", "Amazon_match", True)
    for am in am_match:
        readable_sku = am[1] + "___" + am[2]
        sku = am[0]
        if readable_sku not in readable_sku_to_am_sku_dict.keys():
            readable_sku_to_am_sku_dict[readable_sku] = [sku]
        elif sku not in readable_sku_to_am_sku_dict[readable_sku]:
            readable_sku_to_am_sku_dict[readable_sku].append(sku)
    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readable_sku_to_am_sku_dict)

    return readablesku_to_dict_ignore_case

def readable_sku_to_amazon_price():
    price_lists = read_csv_by_sheet("ELFCAM-Database", "price_list")
    index_dict = {index: i for i, index in enumerate(price_lists[0])}
    amazon_price_index = index_dict["amazon_price"]
    goods_code_index = index_dict["goods_code"]
    readablesku_to_amazon_price_dict = {price_list[goods_code_index]: price_list[amazon_price_index] for price_list in
                                 price_lists[1:]}
    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readablesku_to_amazon_price_dict)

    return readablesku_to_dict_ignore_case

def readable_sku_to_inventory_needed_to_be_updated(channel = "lm"):
    price_lists = read_csv_by_sheet("ELFCAM_WMS", "Update_Inventory")
    index_dict = {index: i for i, index in enumerate(price_lists[0])}
    inventory_index = index_dict["inventory"]
    goods_code_index = index_dict["goods_code"]
    suggested_price_index = index_dict[channel + "_suggested_price"]
    update_index = index_dict["update"]
    readable_sku_to_inventory_needed_to_be_updated_dict = {price_list[goods_code_index]: [price_list[inventory_index], price_list[update_index], price_list[suggested_price_index]] for price_list in price_lists[1:] if len(price_list[update_index]) > 0 }
    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readable_sku_to_inventory_needed_to_be_updated_dict)

    return readablesku_to_dict_ignore_case

def update_file(Table, Sheet, csv_path):
    origin_info = read_csv_by_sheet(Table, Sheet)
    with open(csv_path, 'r', newline='') as csv_reader:
        rows = csv.reader(csv_reader, delimiter=',')
        for row in rows:
            if "goods_code" in row[0]: continue
            key = row[0] + "___" + row[1]
            if key not in [x[0] + "___" + x[1] for x in origin_info[1:]]:
                origin_info.append(row)

    write_and_upload_csv(origin_info, "csv/origin_info.csv", Table, Sheet, wait=True)



def update_matches(database="ELFCAM-Database", wait=5):
    # 定义要下载的表
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    matches = [
        ("Cdiscount_match", "csv/cdiscount_match.csv"),
        ("Rakuten_match", "csv/rakuten_match.csv"),
        ("Amazon_match", "csv/amazon_match.csv"),
        ("Website_match", "csv/website_match.csv"),
        # ("Manomano_match", "csv/manomano_match.csv"),  # 如果需要下载此文件，可以取消注释
        ("Leroy_Merlin_match", "csv/leroy_merlin_match.csv"),
    ]

    logging.info(f"开始从 {database} 下载所有匹配表，共 {len(matches)} 个")

    for match_name, file_path in matches:
        try:
            logging.info(f"正在下载 {match_name} 匹配表...")
            download_csv(database, match_name, file_path, wait=wait)
            logging.info(f"成功下载 {match_name} 匹配表")
        except Exception as e:
            logging.error(f"下载 {match_name} 匹配表时出错：{e}")

    logging.info("所有匹配表下载任务完成")



def update_file_and_delete_duplicates(Table, Sheet, csv_path):
    with open(csv_path, 'r', newline='', encoding='utf-8') as csv_file:
        # 直接将文件内容读取到列表
        rows_table = list(csv.reader(csv_file, delimiter=','))




    origin_info = read_csv_by_sheet(Table, Sheet)

    logging.info("These matches are going to update")
    for row in rows_table:
        logging.info(row)


    d = {item[0]: item for item in origin_info}

    d.update({item[0]: item for item in rows_table})

    values = list(d.values())

    sorted_values = sorted(values, key=lambda item: (item[1]+item[2].zfill(5) if item[1] != '' else '~'), reverse=False)

    # with open(csv_path, 'r', newline='') as csv_reader:
    #     rows = csv.reader(csv_reader, delimiter=',')
    #     for row in rows:
    #         if "goods_code" in row[0]: continue
    #         key = row[0] + "___" + row[1]
    #         if key not in goods_code_list:
    #             continue
    #
    #         if key not in [x[0] + "___" + x[1] for x in origin_info[1:]]:
    #             origin_info.append(row)

    write_and_upload_csv(sorted_values, "csv/origin_info.csv", Table, Sheet, wait=True)

def recheck_asin_to_ean(all_to_be_checked, all_ean_checked):

    if not all_ean_checked:
        asin_to_eans = read_csv_by_sheet("ELFCAM-Database", "Asin_to_EAN", wait=True)
        index_dict = {index: i for i,index in enumerate(asin_to_eans[0])}
        asin_to_ean_existing_dict = {asin_to_ean[index_dict["asin"]] : asin_to_ean for asin_to_ean in asin_to_eans[1:]}
        all_to_be_checked_dict = {atb[0]: atb for atb in all_to_be_checked}
        asin_to_ean_existing_dict.update(all_to_be_checked_dict)
        new_asin_to_ean_list = sorted(asin_to_ean_existing_dict.values(), key=lambda k: (k[3],k[2]))

        first_line = ["asin",	"ean",	"goods_code",	"fba_id",	"check"]
        # write_and_upload_csv(new_asin_to_ean_list, "csv/asin_to_ean.csv", "ELFCAM-Database", "Asin_to_EAN", first_line, wait=True)
        raise Exception(f"{all_ean_checked}please carefully check the consistency of the ASIN , EAN and FBA_id in 'Asin_to_ean' table and refill it")
    # print("out", po.asin, po.goods_code, po.po_num, po.ean, po.place, po.title, po.date, po.quantity)


def readable_sku_to_internal_codes():
    price_lists = read_csv_by_sheet("ELFCAM_WMS", "Update_Inventory")
    index_dict = {index: i for i, index in enumerate(price_lists[0])}
    goods_code_index = index_dict["goods_code"]
    internal_code_index = index_dict["internal_code"]
    readable_sku_to_internal_codes_dict = {price_list[goods_code_index]: price_list[internal_code_index] for price_list in price_lists}
    #### 更换字典  CHANGING : change to ignore case dict ####
    readablesku_to_dict_ignore_case = Ignore_Case_dict()

    readablesku_to_dict_ignore_case.set_dict(readable_sku_to_internal_codes_dict)

    return readablesku_to_dict_ignore_case

def generate_internal_codes():
    today_string = datetime.datetime.today().strftime("%y%m%d")
    print(today_string)
# def readable_sku_to_amazon_sku_list():
#     New_WMS_Stock = read_csv_by_sheet("ELFCAM_WMS", "New_WMS_Stock")
#     index_dict = {r:i for i,r in enumerate(New_WMS_Stock[0])}
#     return {l[0] : l[index_dict["sku"]].split(',') for l in New_WMS_Stock[1:]}
import barcode
from barcode.writer import ImageWriter
def save_barcode(barcode_num,path):
    EAN = barcode.get_barcode_class('ean13')
    ean = EAN(barcode_num.zfill(13), writer=ImageWriter())
    fullname = ean.save(path)


def save_special_barcode(barcode_num,path ,protocol = 'code39'):
    if 'code39' in protocol:
        from barcode import Code39
        with open(path + ".png", 'wb') as f:
            Code39(barcode_num,add_checksum=False ,writer=ImageWriter()).write(f)
    elif '128' in protocol:
        sample_barcode = barcode.get(protocol, barcode_num,  writer=ImageWriter())
        generated_filename = sample_barcode.save(path)

    elif 'ean13' in protocol.lower():
        EAN = barcode.get_barcode_class('ean13')
        ean = EAN(barcode_num.zfill(13), writer=ImageWriter())
        fullname = ean.save(path)


def price_with_dot_to_pure_price(a):
    the_price = a.replace(".", ",")
    theprice_list = list(the_price)
    if len(theprice_list) > 2 and theprice_list[-3] == ",":
        theprice_list[-3] = "*"
    elif len(theprice_list) > 2 and theprice_list[-2] == ",":
        theprice_list[-2] = "*"
    elif len(theprice_list) > 1 and theprice_list[-2] == ",":
        theprice_list[-2] = "*"
    theprice = "".join(theprice_list)
    theprice = theprice.replace(",", "").replace("*", ".")
    return theprice


from PyPDF2 import PdfReader, PdfWriter
def add_watermark(pdf_file_in, pdf_file_mark, pdf_file_out):
    pdf_output = PdfWriter()
    input_stream = open(pdf_file_in, 'rb')
    pdf_input = PdfReader(input_stream, strict=False)

    # 获取PDF文件的页数
    pageNum = len(pdf_input.pages)

    # 读入水印pdf文件
    pdf_watermark = PdfReader(open(pdf_file_mark, 'rb'), strict=False)
    # 给每一页打水印
    for i in range(pageNum):
        page = pdf_input.pages[i]
        page.merge_page(pdf_watermark.pages[i])
        page.compress_content_streams()  # 压缩内容
        pdf_output.add_page(page)
    pdf_output.write(open(pdf_file_out, 'wb'))


import chardet

# 特殊字符映射为英文的字典
#special_english_dict = {a[0]: a[1] for a in read_csv_by_sheet("ELFCAM-Database", "transfer_to_english", wait=5)}


def transfer_to_english(text):
    try:
        # 尝试使用 ISO-8859-1 解码
        text = text.encode('ISO-8859-1').decode('ISO-8859-1')
    except Exception as e:
        # ISO-8859-1 解码失败，使用 UTF-8 忽略错误部分
        text = text.encode('utf-8', 'ignore').decode('utf-8')

    # 检测文本编码类型
    detected_encoding = chardet.detect(text.encode('utf-8'))

    # 替换特殊字符为英文
    # for key, value in special_english_dict.items():
    #     text = text.replace(key, value)

    # 如果检测到的编码是 UTF-8，并且文本在 ISO-8859-1 中无效，置为空字符串
    if detected_encoding.get("encoding") == "utf-8":
        try:
            # 检查文本是否能用 ISO-8859-1 编码，若不能则引发异常
            text.encode('ISO-8859-1')
        except UnicodeEncodeError:
            text = ""  # 置空文本
            logging.warning("Detected invalid ISO-8859-1 characters, text has been cleared.")

    return text


def generate_sequence(codes,n = 1):
    """
    构造一个数列 [Goods_code_1, Quantity_1, Goods_code_2, Quantity_2, ...]
    :param n: 商品数量
    :return: 数列列表
    """
    result = []
    for i in range(1, n + 1):
        for code in codes:
            result.append(f"{code}_{i}")  # 商品编码

    return result