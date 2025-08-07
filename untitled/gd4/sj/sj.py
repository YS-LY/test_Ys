import json
import random
from datetime import datetime, timedelta


# 生成随机日期的工具函数
def random_date(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")


# 基础数据配置
categories = [
    {"id": "c001", "name": "糕点"},
    {"id": "c002", "name": "零食"},
    {"id": "c003", "name": "饮料"},
    {"id": "c004", "name": "生鲜"},
    {"id": "c005", "name": "粮油"},
    {"id": "c006", "name": "日用品"}
]

traffic_sources = [
    "手淘搜索", "效果广告", "抖音推荐", "微信小程序",
    "直播带货", "淘宝首页", "朋友推荐", "百度搜索"
]


# 生成商品基础信息表数据
def generate_ods_goods_base(num=250):
    data = []
    for i in range(1, num + 1):
        goods_id = f"g{i:03d}"  # g001到g250
        category = random.choice(categories)
        # 生成符合分类的商品名称
        name_prefix = random.choice(["", "优质", "特级", "精选", "进口"])
        category_names = {
            "c001": ["蛋黄酥", "面包", "月饼", "蛋糕", "蛋挞"],
            "c002": ["薯片", "巧克力", "牛肉干", "坚果", "果冻"],
            "c003": ["可乐", "果汁", "矿泉水", "奶茶", "酸奶"],
            "c004": ["苹果", "香蕉", "猪肉", "青菜", "鸡蛋"],
            "c005": ["大米", "面粉", "食用油", "酱油", "盐"],
            "c006": ["纸巾", "牙刷", "洗发水", "洗衣粉", "洗洁精"]
        }
        goods_name = f"{name_prefix}{random.choice(category_names[category['id']])}"
        if not name_prefix:  # 避免空前缀
            goods_name = random.choice(category_names[category['id']])

        data.append({
            "table_name": "ods_goods_base",
            "goods_id": goods_id,
            "goods_name": goods_name,
            "category_id": category["id"],
            "category_name": category["name"],
            "shop_id": f"s{random.randint(1, 20):03d}",  # s001到s020
            "is_price_strength": random.randint(0, 1),
            "create_time": random_date("2025-01-01", "2025-07-31")
        })
    return data


# 生成商品销售明细表数据
def generate_ods_goods_sales_detail(goods_ids, num=250):
    data = []
    for _ in range(num):
        goods_id = random.choice(goods_ids)
        sku_id = f"s{goods_id[1:]}"  # 从goods_id提取后三位生成sku_id
        pay_amount = round(random.uniform(1000.00, 10000.00), 2)
        sales_num = random.randint(50, 500)
        pay_buyer_num = random.randint(int(sales_num * 0.5), int(sales_num * 0.9))  # 买家数少于销量
        data.append({
            "table_name": "ods_goods_sales_detail",
            "goods_id": goods_id,
            "sku_id": sku_id,
            "pay_amount": pay_amount,
            "sales_num": sales_num,
            "pay_buyer_num": pay_buyer_num,
            "pay_date": random_date("2025-01-01", "2025-08-06")
        })
    return data


# 生成商品流量表数据
def generate_ods_goods_traffic(goods_ids, num=250):
    data = []
    for _ in range(num):
        data.append({
            "table_name": "ods_goods_traffic",
            "goods_id": random.choice(goods_ids),
            "traffic_source": random.choice(traffic_sources),
            "visitor_num": random.randint(100, 1000),
            "visit_date": random_date("2025-01-01", "2025-08-06")
        })
    return data


# 生成商品价格力表数据
def generate_ods_goods_price_strength(goods_ids, num=250):
    data = []
    for _ in range(num):
        star = random.randint(1, 5)
        data.append({
            "table_name": "ods_goods_price_strength",
            "goods_id": random.choice(goods_ids),
            "price_strength_star": star,
            "is_low_star": 1 if star <= 2 else 0,  # 1-2星为低星
            "record_date": random_date("2025-01-01", "2025-08-06")
        })
    return data


# 主函数：生成数据并分表保存
if __name__ == "__main__":
    # 生成商品基础数据并提取goods_id用于关联其他表
    print("正在生成商品基础信息表数据...")
    base_data = generate_ods_goods_base(250)
    goods_ids = [item["goods_id"] for item in base_data]

    # 生成其他表数据
    print("正在生成商品销售明细表数据...")
    sales_data = generate_ods_goods_sales_detail(goods_ids, 250)

    print("正在生成商品流量表数据...")
    traffic_data = generate_ods_goods_traffic(goods_ids, 250)

    print("正在生成商品价格力表数据...")
    price_data = generate_ods_goods_price_strength(goods_ids, 250)

    # 分表保存为JSON Lines格式
    with open("ods_goods_base.json", "w", encoding="utf-8") as f:
        for item in base_data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    with open("ods_goods_sales_detail.json", "w", encoding="utf-8") as f:
        for item in sales_data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    with open("ods_goods_traffic.json", "w", encoding="utf-8") as f:
        for item in traffic_data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    with open("ods_goods_price_strength.json", "w", encoding="utf-8") as f:
        for item in price_data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    print("数据生成完成，已保存以下文件：")
    print("ods_goods_base.json")
    print("ods_goods_sales_detail.json")
    print("ods_goods_traffic.json")
    print("ods_goods_price_strength.json")