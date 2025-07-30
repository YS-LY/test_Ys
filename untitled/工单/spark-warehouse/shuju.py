# from pyspark.sql import SparkSession
# from pyspark.sql.types import (
#     StringType, DecimalType, IntegerType, StructType, StructField
# )
#
# # 初始化SparkSession，配置Hive动态分区为非严格模式（解决核心错误）
# spark = SparkSession.builder \
#     .appName("InsertDataToAdsTable") \
#     .config("spark.sql.catalogImplementation", "hive") \
#     .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
#     .config("hive.exec.dynamic.partition", "true")   \
#     .config("hive.exec.dynamic.partition.mode", "nonstrict")   \
#     .enableHiveSupport() \
#     .getOrCreate()
#
# # CSV文件路径（替换为实际路径）
# csv_path = "ads_product_value_evaluation_10000.csv"
#
# # 定义与表结构一致的Schema（匹配文档ADS层字段）
# schema = StructType([
#     StructField("product_id", StringType(), nullable=False),
#     StructField("product_name", StringType(), nullable=True),
#     StructField("stat_date", StringType(), nullable=False),  # 动态分区字段
#     StructField("traffic_acquisition_score", DecimalType(5, 2), nullable=True),
#     StructField("traffic_conversion_score", DecimalType(5, 2), nullable=True),
#     StructField("content_marketing_score", DecimalType(5, 2), nullable=True),
#     StructField("customer_acquisition_score", DecimalType(5, 2), nullable=True),
#     StructField("service_quality_score", DecimalType(5, 2), nullable=True),
#     StructField("total_score", DecimalType(5, 2), nullable=True),
#     StructField("level", StringType(), nullable=True),
#     StructField("amount", DecimalType(12, 2), nullable=True),
#     StructField("price", DecimalType(8, 2), nullable=True),
#     StructField("sales_num", IntegerType(), nullable=True),
#     StructField("visitor_num", IntegerType(), nullable=True),
#     StructField("visitor_sales_ratio", DecimalType(5, 2), nullable=True)
# ])
#
# # 读取CSV数据
# df = spark.read \
#     .format("csv") \
#     .option("header", "true") \
#     .schema(schema) \
#     .load(csv_path)
#
# # 写入数据：适配Hive动态分区，符合文档存储要求
# df.write \
#     .mode("append") \
#     .format("hive")  \
#     .partitionBy("stat_date")   \
#     .saveAsTable("gd.ads_product_value_evaluation")
#
# # 验证数据插入结果
# count = spark.sql("SELECT COUNT(*) FROM gd.ads_product_value_evaluation").collect()[0][0]
# print(f"成功插入{count}条数据到gd.ads_product_value_evaluation表")
#
# spark.stop()





















from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, DecimalType, IntegerType, StructType, StructField
)

# 初始化SparkSession，适配Hive数仓环境
spark = SparkSession.builder \
    .appName("CreateAndInsertAdsProductValueEvaluation") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.exec.dynamic.partition", "true")   \
    .config("hive.exec.dynamic.partition.mode", "nonstrict")   \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 先在Hive中创建表（符合文档全品价值评估的ADS层设计）
create_table_sql = """
CREATE TABLE IF NOT EXISTS gd.ads_product_value_evaluation (
    product_id STRING COMMENT '商品唯一标识ID',
    product_name STRING COMMENT '商品名称',
    traffic_acquisition_score DECIMAL(5,2) COMMENT '流量获取维度评分',
    traffic_conversion_score DECIMAL(5,2) COMMENT '流量转化维度评分',
    content_marketMENT '客户拉新维度评分',
    service_quality_score DECIMAL(5,2) COMMENT 'ing_score DECIMAL(5,2) COMMENT '内容营销维度评分',
    customer_acquisition_score DECIMAL(5,2) COM服务质量维度评分',
    total_score DECIMAL(5,2) COMMENT '综合评分（基于五维度计算）',
    level STRING COMMENT '评分等级（A/B/C/D级，85+→A级，70-85→B级，50-70→C级，<50→D级）',
    amount DECIMAL(12,2) COMMENT '销售额（金额维度，用于评分-金额分析）',
    price DECIMAL(8,2) COMMENT '单价（价格维度，用于价格-销量分析）',
    sales_num INT COMMENT '销量（用于价格-销量/访客-销量分析）',
    visitor_num INT COMMENT '访客数（用于访客-销量分析）',
    visitor_sales_ratio DECIMAL(5,2) COMMENT '访客-销量比（销量/访客数，评估访客转化效率）'
)
PARTITIONED BY (stat_date STRING COMMENT '统计日期，按天分区')
STORED AS PARQUET
COMMENT '全品价值评估ADS层表，任务编号：大数据-电商数仓-07-商品主题商品诊断看板，支持基于评分-金额/价格-销量/访客-销量维度的商品价值分析🔶1-20🔶🔶1-21🔶🔶1-26🔶'
"""
spark.sql(create_table_sql)

# 2. 读取CSV数据（Schema与表结构严格一致）
csv_path = "ads_product_value_evaluation_10000.csv"  # 替换为实际路径
schema = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("stat_date", StringType(), nullable=False),  # 分区字段
    StructField("traffic_acquisition_score", DecimalType(5, 2), nullable=True),
    StructField("traffic_conversion_score", DecimalType(5, 2), nullable=True),
    StructField("content_marketing_score", DecimalType(5, 2), nullable=True),
    StructField("customer_acquisition_score", DecimalType(5, 2), nullable=True),
    StructField("service_quality_score", DecimalType(5, 2), nullable=True),
    StructField("total_score", DecimalType(5, 2), nullable=True),
    StructField("level", StringType(), nullable=True),
    StructField("amount", DecimalType(12, 2), nullable=True),
    StructField("price", DecimalType(8, 2), nullable=True),
    StructField("sales_num", IntegerType(), nullable=True),
    StructField("visitor_num", IntegerType(), nullable=True),
    StructField("visitor_sales_ratio", DecimalType(5, 2), nullable=True)
])

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(csv_path)

# 3. 写入Hive表（动态分区模式，符合文档存储要求）
df.write \
    .mode("append") \
    .format("hive")  \
    .partitionBy("stat_date")   \
    .saveAsTable("gd.ads_product_value_evaluation")

# 4. 验证数据写入结果
count = spark.sql("SELECT COUNT(*) FROM gd.ads_product_value_evaluation").collect()[0][0]
print(f"成功插入{count}条数据到gd.ads_product_value_evaluation表，表结构符合文档全品价值评估需求")

spark.stop()