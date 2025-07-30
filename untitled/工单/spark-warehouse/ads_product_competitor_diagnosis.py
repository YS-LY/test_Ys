from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, DecimalType, IntegerType, StructType, StructField
)

# 初始化SparkSession，适配Hive环境并配置动态分区
spark = SparkSession.builder \
    .appName("CreateAdsProductValueEvaluationInGdDB") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.exec.dynamic.partition", "true")   \
    .config("hive.exec.dynamic.partition.mode", "nonstrict")   \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 确保gd数据库存在（文档中ADS层数据所属库）
spark.sql("CREATE DATABASE IF NOT EXISTS gd COMMENT '电商数仓ADS层，存储商品诊断看板相关数据'")

# 2. 在gd库中创建表（严格匹配文档全品价值评估的ADS层设计）
create_table_sql = """
CREATE TABLE IF NOT EXISTS gd.ads_product_value_evaluation (
    product_id STRING COMMENT '商品唯一标识ID',
    product_name STRING COMMENT '商品名称',
    traffic_acquisition_score DECIMAL(5,2) COMMENT '流量获取维度评分（用于综合评分计算）',
    traffic_conversion_score DECIMAL(5,2) COMMENT '流量转化维度评分（用于综合评分计算）',
    content_marketing_score DECIMAL(5,2) COMMENT '内容营销维度评分（用于综合评分计算）',
    customer_acquisition_score DECIMAL(5,2) COMMENT '客户拉新维度评分（用于综合评分计算）',
    service_quality_score DECIMAL(5,2) COMMENT '服务质量维度评分（用于综合评分计算）',
    total_score DECIMAL(5,2) COMMENT '综合评分（基于上述五维度计算）',
    level STRING COMMENT '评分等级（A/B/C/D级，85分以上为A级，70-85分为B级，50-70分为C级，50分以下为D级）🔶1-26🔶',
    amount DECIMAL(12,2) COMMENT '销售额（金额维度，支撑"评分-金额"分析）🔶1-20🔶',
    price DECIMAL(8,2) COMMENT '单价（价格维度，支撑"价格-销量"分析）🔶1-20🔶',
    sales_num INT COMMENT '销量（支撑"价格-销量"/"访客-销量"分析）🔶1-20🔶',
    visitor_num INT COMMENT '访客数（支撑"访客-销量"分析）🔶1-20🔶',
    visitor_sales_ratio DECIMAL(5,2) COMMENT '访客-销量比（销量/访客数，评估访客转化效率）'
)
PARTITIONED BY (stat_date STRING COMMENT '统计日期，按天分区存储')
STORED AS PARQUET
COMMENT '全品价值评估ADS层表，任务编号：大数据-电商数仓-07-商品主题商品诊断看板，用于全面了解店铺商品分布，聚焦核心商品和潜力商品🔶1-21🔶'
"""
spark.sql(create_table_sql)

# 3. 读取CSV数据（Schema与表结构严格一致）
csv_path = "ads_product_value_evaluation_10000.csv"  # 替换为实际CSV路径
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

# 4. 将数据写入gd库中的表（动态分区模式，符合文档存储要求）
df.write \
    .mode("append") \
    .format("hive")  \
    .partitionBy("stat_date")   \
    .saveAsTable("gd.ads_product_value_evaluation")

# 5. 验证表创建及数据插入结果
table_exists = spark.catalog.tableExists("gd.ads_product_value_evaluation")
count = spark.sql("SELECT COUNT(*) FROM gd.ads_product_value_evaluation").collect()[0][0] if table_exists else 0
print(f"gd库中{'已成功创建' if table_exists else '未创建'}ads_product_value_evaluation表，成功插入{count}条数据")

spark.stop()