from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, DecimalType, MapType, StructType, StructField
)
import random
from decimal import Decimal, ROUND_HALF_UP
import os

# 配置Python环境（替换为实际路径）
os.environ["PYSPARK_PYTHON"] = "C:/Users/ch/.conda/envs/pythonProject12/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/ch/.conda/envs/pythonProject12/python.exe"

# 初始化SparkSession，符合文档技术要求
spark = SparkSession.builder \
    .appName("GenerateCompetitorData") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

# 文档定义的对比维度及细分权重
dimensions = [
    "traffic_acquisition", "traffic_conversion",
    "content_marketing", "customer_acquisition", "service_quality"
]
sub_dim_weights = {
    "traffic_acquisition": {"visitor_num": Decimal("0.40"), "pv": Decimal("0.60")},
    "traffic_conversion": {"pay_rate": Decimal("0.60"), "cart_rate": Decimal("0.40")},
    "content_marketing": {"click_rate": Decimal("0.70"), "exposure": Decimal("0.30")},
    "customer_acquisition": {"new_user_rate": Decimal("0.50"), "cost_per_new": Decimal("0.50")},
    "service_quality": {"praise_rate": Decimal("0.60"), "after_sale_rate": Decimal("0.40")}
}

# 生成10000条数据
random.seed(2025)  # 固定种子，确保可复现
data = []
for i in range(10000):
    # 本品ID
    product_id = f"P{i + 1:05d}"

    # 相似商品品类ID
    similar_category_id = f"C{random.randint(1, 20):03d}"

    # 对比维度（从文档定义的5个维度中随机选择）
    dimension = random.choice(dimensions)

    # 本品得分（0-100分，保留2位小数）
    own_value = Decimal(str(round(random.uniform(30, 100), 2))).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)

    # 相似商品平均得分（围绕本品得分±15分波动）
    similar_avg = own_value + Decimal(str(round(random.uniform(-15, 15), 2)))
    similar_avg = Decimal(str(max(0, min(100, float(similar_avg))))).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)

    # 差异值（本品 - 相似平均）
    difference = (own_value - similar_avg).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)

    # 细分维度权重（使用文档定义的权重）
    sub_weights = sub_dim_weights[dimension]

    # 统计日期（文档关联的2025年1月）
    stat_date = f"2025-01-{random.randint(1, 31):02d}"

    data.append([
        product_id, similar_category_id, dimension,
        own_value, similar_avg, difference,
        sub_weights, stat_date
    ])

# 定义表结构Schema（匹配文档字段类型）
schema = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("similar_category_id", StringType(), nullable=True),
    StructField("dimension", StringType(), nullable=False),
    StructField("own_value", DecimalType(5, 2), nullable=True),
    StructField("similar_avg_value", DecimalType(5, 2), nullable=True),
    StructField("difference", DecimalType(5, 2), nullable=True),
    StructField("sub_dimension_weights", MapType(StringType(), DecimalType(3, 2)), nullable=True),
    StructField("stat_date", StringType(), nullable=False)
])

# 创建DataFrame
df = spark.createDataFrame(data, schema=schema)

# 写入表中（符合文档存储要求）
df.write \
    .mode("append") \
    .format("hive") \
    .partitionBy("stat_date") \
    .saveAsTable("gd.ads_product_competitor_diagnosis")

# 验证数据量
count = spark.sql("SELECT COUNT(*) FROM gd.ads_product_competitor_diagnosis").collect()[0][0]
print(f"成功生成{count}条数据至gd.ads_product_competitor_diagnosis表")

spark.stop()