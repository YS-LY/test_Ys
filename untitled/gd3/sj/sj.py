import json
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta

# 初始化Faker
fake = Faker('zh_CN')

# 生成基础数据池，确保各表数据关联
user_ids = [f"user_{uuid.uuid4().hex[:8]}" for _ in range(500)]  # 500个用户ID
product_ids = [f"prod_{uuid.uuid4().hex[:6]}" for _ in range(300)]  # 300个商品ID
category_data = {
    "C001": "电子产品",
    "C002": "服装鞋帽",
    "C003": "食品饮料",
    "C004": "家居用品",
    "C005": "图书音像",
    "C006": "美妆个护",
    "C007": "母婴用品",
    "C008": "运动户外"
}
category_ids = list(category_data.keys())
source_channels = ["搜索引擎", "社交媒体", "直接访问", "外部链接", "广告推广", "朋友推荐"]
terminal_types = ["PC", "无线"]

# 生成日期范围（最近30天）
end_date = datetime.now()
start_date = end_date - timedelta(days=30)


def random_datetime(start, end):
    """生成指定范围内的随机日期时间"""
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


def generate_商品访问日志(num=1000):
    """生成商品访问日志数据"""
    data = []
    for _ in range(num):
        log_id = f"log_{uuid.uuid4().hex[:10]}"
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        visit_time = random_datetime(start_date, end_date)
        terminal = random.choice(terminal_types)
        stay_time = random.randint(5, 300)  # 5-300秒
        is_click = random.choice([0, 1])
        source = random.choice(source_channels)
        log_date = visit_time.strftime("%Y%m%d")

        record = {
            "log_id": log_id,
            "user_id": user_id,
            "product_id": product_id,
            "visit_time": visit_time.isoformat(),
            "terminal": terminal,
            "stay_time": stay_time,
            "is_click": is_click,
            "source": source,
            "log_date": log_date
        }
        data.append(record)
    return data


def generate_商品收藏加购(num=1000):
    """生成商品收藏加购数据"""
    data = []
    for _ in range(num):
        record_id = f"fav_{uuid.uuid4().hex[:10]}"
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        op_type = random.choice(["收藏", "加购"])
        op_time = random_datetime(start_date, end_date)
        add_cart_count = random.randint(1, 5) if op_type == "加购" else None
        terminal = random.choice(terminal_types)
        op_date = op_time.strftime("%Y%m%d")

        record = {
            "record_id": record_id,
            "user_id": user_id,
            "product_id": product_id,
            "op_type": op_type,
            "op_time": op_time.isoformat(),
            "add_cart_count": add_cart_count,
            "terminal": terminal,
            "op_date": op_date
        }
        data.append(record)
    return data


def generate_商品交易(num=1000):
    """生成商品交易数据"""
    data = []
    for _ in range(num):
        order_id = f"order_{uuid.uuid4().hex[:10]}"
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        category_id = random.choice(category_ids)
        product_price = round(random.uniform(10, 2000), 2)
        buy_count = random.randint(1, 10)
        buy_amount = round(product_price * buy_count, 2)
        pay_status = random.choice(["未支付", "已支付"])

        create_time = random_datetime(start_date, end_date)
        pay_time = None
        pay_amount = None

        if pay_status == "已支付":
            # 支付时间晚于创建时间，但不超过24小时
            pay_delay = timedelta(seconds=random.randint(60, 86400))
            pay_time = create_time + pay_delay
            pay_amount = buy_amount

        terminal = random.choice(terminal_types)
        order_date = create_time.strftime("%Y%m%d")

        record = {
            "order_id": order_id,
            "user_id": user_id,
            "product_id": product_id,
            "category_id": category_id,
            "product_price": float(product_price),
            "buy_count": buy_count,
            "buy_amount": float(buy_amount),
            "pay_status": pay_status,
            "pay_time": pay_time.isoformat() if pay_time else None,
            "pay_amount": float(pay_amount) if pay_amount else None,
            "terminal": terminal,
            "create_time": create_time.isoformat(),
            "order_date": order_date
        }
        data.append(record)
    return data


def generate_商品基础信息(num=1000):
    """生成商品基础信息数据"""
    data = []
    # 确保商品ID与其他表对应
    for i in range(min(num, len(product_ids))):
        product_id = product_ids[i]
        category_id = random.choice(category_ids)
        category_name = category_data[category_id]

        # 根据类目生成相关的商品名称
        if category_name == "电子产品":
            product_names = ["智能手机", "笔记本电脑", "平板电脑", "智能手表", "蓝牙耳机", "移动电源"]
        elif category_name == "服装鞋帽":
            product_names = ["T恤", "牛仔裤", "运动鞋", "连衣裙", "夹克", "帽子"]
        elif category_name == "食品饮料":
            product_names = ["巧克力", "薯片", "可乐", "果汁", "饼干", "方便面"]
        elif category_name == "家居用品":
            product_names = ["床单", "枕头", "毛巾", "餐具", "垃圾桶", "收纳盒"]
        elif category_name == "图书音像":
            product_names = ["小说", "教科书", "历史书", "音乐CD", "电影DVD", "杂志"]
        elif category_name == "美妆个护":
            product_names = ["洗面奶", "口红", "香水", "洗发水", "面膜", "防晒霜"]
        elif category_name == "母婴用品":
            product_names = ["婴儿奶粉", "纸尿裤", "玩具", "婴儿车", "奶瓶", "童装"]
        else:  # 运动户外
            product_names = ["运动鞋", "运动服", "瑜伽垫", "跑步机", "篮球", "登山包"]

        product_name = f"{random.choice(product_names)}"
        product_price = round(random.uniform(10, 2000), 2)
        update_time = random_datetime(start_date, end_date)
        update_date = update_time.strftime("%Y%m%d")

        record = {
            "product_id": product_id,
            "product_name": product_name,
            "category_id": category_id,
            "category_name": category_name,
            "product_price": float(product_price),
            "update_time": update_time.isoformat(),
            "update_date": update_date
        }
        data.append(record)

    # 如果需要生成的数量超过商品ID数量，补充一些额外的商品
    # for i in range(len(product_ids), num):
    #     product_id = f"prod_{uuid.uuid4().hex[:6]}"
    #     category_id = random.choice(category_ids)
    #     category_name = category_data[category_id]
    #     product_name = f"{fake.word()} {fake.word()}"
    #     product_price = round(random.uniform(10, 2000), 2)
    #     update_time = random_datetime(start_date, end_date)
    #     update_date = update_time.strftime("%Y%m%d")

        # record = {
        #     "product_id": product_id,
        #     "product_name": product_name,
        #     "category_id": category_id,
        #     "category_name": category_name,
        #     "product_price": float(product_price),
        #     "update_time": update_time.isoformat(),
        #     "update_date": update_date
        # }
        # data.append(record)

    return data


if __name__ == "__main__":
    # 生成各表数据
    print("正在生成商品访问日志数据...")
    visit_logs = generate_商品访问日志(1000)

    print("正在生成商品收藏加购数据...")
    favorite_cart = generate_商品收藏加购(1000)

    print("正在生成商品交易数据...")
    transactions = generate_商品交易(1000)

    print("正在生成商品基础信息数据...")
    product_info = generate_商品基础信息(1000)

    # 保存为JSON文件（JSON Lines格式）
    with open("ods_商品访问日志.json", "w", encoding="utf-8") as f:
        for item in visit_logs:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    with open("ods_商品收藏加购.json", "w", encoding="utf-8") as f:
        for item in favorite_cart:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    with open("ods_商品交易.json", "w", encoding="utf-8") as f:
        for item in transactions:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    with open("ods_商品基础信息.json", "w", encoding="utf-8") as f:
        for item in product_info:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")

    print("数据生成完成，已保存为JSON文件。")