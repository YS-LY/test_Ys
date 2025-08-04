import json
import random
from datetime import datetime, timedelta
import uuid


# 生成随机日期时间的函数
def random_datetime(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


# 日期范围设置
start_date = datetime(2025, 7, 30)
end_date = datetime(2025, 8, 30)
current_date = datetime.now()

# 生成活动数据
activity_data = []
activity_ids = []
for i in range(100):  # 生成100个活动
    activity_id = f"act_{uuid.uuid4().hex[:8]}"
    activity_ids.append(activity_id)

    start_time = random_datetime(start_date, end_date)
    # 活动持续1-30天
    end_time = start_time + timedelta(days=random.randint(1, 30))

    # 活动状态判断
    if end_time < current_date:
        status = "已结束"
    else:
        status = "进行中"

    activity_level = random.choice(["商品级", "SKU级"])
    discount_type = random.choice(["固定优惠", "自定义优惠"])

    # 根据优惠类型设置金额
    max_discount_amount = random.randint(10, 200) if discount_type == "自定义优惠" else None

    data = {
        "activity_id": activity_id,
        "activity_name": f"客服专属优惠活动{i + 1}",
        "activity_level": activity_level,
        "discount_type": discount_type,
        "max_discount_amount": max_discount_amount,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "status": status,
        "create_time": (start_time - timedelta(days=random.randint(1, 7))).isoformat(),
        "update_time": random_datetime(start_time, end_time).isoformat() if random.random() > 0.3 else None,
        "is_valid": 1 if random.random() > 0.1 else 0,
        "raw_data": json.dumps({"source": "CRM系统", "version": "1.0"})
    }
    activity_data.append(data)

# 写入活动数据到JSON文件
with open("activity_data.json", "w", encoding="utf-8") as f:
    for item in activity_data:
        json.dump(item, f, ensure_ascii=False)
        f.write("\n")

# 生成商品关联数据
product_rel_data = []
product_ids = [f"prod_{uuid.uuid4().hex[:6]}" for _ in range(200)]  # 200个商品
sku_ids = [f"sku_{uuid.uuid4().hex[:8]}" for _ in range(500)]  # 500个SKU

for i in range(300):  # 生成300条关联数据
    rel_id = f"rel_{uuid.uuid4().hex[:10]}"
    activity_id = random.choice(activity_ids)
    product_id = random.choice(product_ids)

    # 根据活动级别决定是否有SKU
    activity = next(a for a in activity_data if a["activity_id"] == activity_id)
    if activity["activity_level"] == "SKU级":
        sku_id = random.choice(sku_ids)
    else:
        sku_id = None

    # 根据优惠类型决定是否有固定优惠金额
    fixed_discount = random.randint(5, 100) if activity["discount_type"] == "固定优惠" else None

    data = {
        "rel_id": rel_id,
        "activity_id": activity_id,
        "product_id": product_id,
        "sku_id": sku_id,
        "fixed_discount": fixed_discount,
        "max_buy_count": random.randint(1, 10),
        "add_time": random_datetime(
            datetime.fromisoformat(activity["start_time"]),
            datetime.fromisoformat(activity["end_time"])
        ).isoformat(),
        "is_delete": 0 if random.random() > 0.15 else 1
    }
    product_rel_data.append(data)

# 写入商品关联数据到JSON文件
with open("product_rel_data.json", "w", encoding="utf-8") as f:
    for item in product_rel_data:
        json.dump(item, f, ensure_ascii=False)
        f.write("\n")

# 生成优惠发送明细数据
send_detail_data = []
customer_ids = [f"cust_{uuid.uuid4().hex[:8]}" for _ in range(300)]  # 300个客户
service_ids = [f"serv_{uuid.uuid4().hex[:6]}" for _ in range(50)]  # 50个客服

for i in range(500):  # 生成500条发送数据
    send_id = f"send_{uuid.uuid4().hex[:12]}"
    activity_id = random.choice(activity_ids)
    activity = next(a for a in activity_data if a["activity_id"] == activity_id)

    # 找到该活动关联的商品
    related_products = [p for p in product_rel_data if p["activity_id"] == activity_id and p["is_delete"] == 0]
    if not related_products:
        continue

    product_rel = random.choice(related_products)
    product_id = product_rel["product_id"]
    sku_id = product_rel["sku_id"]

    # 确定发送金额
    if activity["discount_type"] == "固定优惠":
        send_discount_amount = product_rel["fixed_discount"]
    else:
        send_discount_amount = random.randint(10, activity["max_discount_amount"])

    validity_period = random.randint(1, 24)
    send_time = random_datetime(
        datetime.fromisoformat(activity["start_time"]),
        datetime.fromisoformat(activity["end_time"])
    )
    expire_time = (send_time + timedelta(hours=validity_period)).isoformat()

    data = {
        "send_id": send_id,
        "activity_id": activity_id,
        "product_id": product_id,
        "sku_id": sku_id,
        "customer_id": random.choice(customer_ids),
        "service_id": random.choice(service_ids),
        "send_discount_amount": send_discount_amount,
        "validity_period": validity_period,
        "send_time": send_time.isoformat(),
        "expire_time": expire_time,
        "remark": f"客服手动发送优惠{send_discount_amount}元" if random.random() > 0.5 else None
    }
    send_detail_data.append(data)

# 写入发送明细数据到JSON文件
with open("send_detail_data.json", "w", encoding="utf-8") as f:
    for item in send_detail_data:
        json.dump(item, f, ensure_ascii=False)
        f.write("\n")

# 生成优惠核销数据
verify_detail_data = []
order_ids = [f"order_{uuid.uuid4().hex[:10]}" for _ in range(400)]  # 400个订单

# 只核销30%的发送记录
for send_detail in send_detail_data:
    if random.random() > 0.3:
        continue

    verify_id = f"verify_{uuid.uuid4().hex[:12]}"
    send_time = datetime.fromisoformat(send_detail["send_time"])
    expire_time = datetime.fromisoformat(send_detail["expire_time"])

    # 核销时间在发送后、过期前
    verify_time = random_datetime(send_time, expire_time)

    # 支付金额 = 随机金额 + 优惠金额
    pay_amount = round(random.uniform(100, 2000) + send_detail["send_discount_amount"], 2)

    data = {
        "verify_id": verify_id,
        "send_id": send_detail["send_id"],
        "order_id": random.choice(order_ids),
        "pay_amount": pay_amount,
        "verify_time": verify_time.isoformat(),
        "buy_count": random.randint(1, min(5, send_detail.get("max_buy_count", 5)))
    }
    verify_detail_data.append(data)

# 写入核销数据到JSON文件
with open("verify_detail_data.json", "w", encoding="utf-8") as f:
    for item in verify_detail_data:
        json.dump(item, f, ensure_ascii=False)
        f.write("\n")

print(f"生成数据完成：")
print(f"活动数据：{len(activity_data)}条")
print(f"商品关联数据：{len(product_rel_data)}条")
print(f"优惠发送数据：{len(send_detail_data)}条")
print(f"优惠核销数据：{len(verify_detail_data)}条")
print(f"总计：{len(activity_data) + len(product_rel_data) + len(send_detail_data) + len(verify_detail_data)}条")
