from pyspark.sql import SparkSession

# 初始化SparkSession，启用Hive支持（适配数仓ADS层表存储）
spark = SparkSession.builder \
    .appName("CreateAdsProductValueEvaluationTable") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# 第一步：创建ads数据库（若不存在），解决SCHEMA_NOT_FOUND错误
spark.sql("CREATE DATABASE IF NOT EXISTS gd COMMENT '电商数仓ADS层，存储商品诊断看板相关数据'")

# 第二步：创建gd.ads_product_value_evaluation表，遵循文档ADS层设计要求
create_table_sql = """
CREATE TABLE IF NOT EXISTS gd.ads_product_value_evaluation (
    product_id STRING COMMENT '商品唯一标识（主键）',
    product_name STRING COMMENT '商品名称',
    stat_date STRING COMMENT '统计日期（格式：yyyy-MM-dd，分区字段）',
    traffic_acquisition_score DECIMAL(5,2) COMMENT '流量获取维度评分（0-100分）',
    traffic_conversion_score DECIMAL(5,2) COMMENT '流量转化维度评分（0-100分）',
    content_marketing_score DECIMAL(5,2) COMMENT '内容营销维度评分（0-100分）',
    customer_acquisition_score DECIMAL(5,2) COMMENT '客户拉新维度评分（0-100分）',
    service_quality_score DECIMAL(5,2) COMMENT '服务质量维度评分（0-100分）',
    total_score DECIMAL(5,2) COMMENT '综合评分（各维度加权求和）',
    level STRING COMMENT '商品等级（A：≥85分；B：70-85分；C：50-70分；D：<50分）',
    amount DECIMAL(12,2) COMMENT '商品总金额（单价×销量）',
    price DECIMAL(8,2) COMMENT '商品单价',
    sales_num INT COMMENT '商品销量',
    visitor_num INT COMMENT '商品访客数',
    visitor_sales_ratio DECIMAL(5,2) COMMENT '访客-销量比（访客数/销量）'
)
PARTITIONED BY (stat_date)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ads.db/ads_product_value_evaluation'
COMMENT '全品价值评估ADS层表，支持基于评分-金额/价格-销量/访客-销量维度分析'
"""

# 执行建表语句
spark.sql(create_table_sql)

# 验证表结构
print("gd.ads_product_value_evaluation表结构：")
spark.sql("DESCRIBE gd.ads_product_value_evaluation").show(truncate=False)

spark.sql("""
select * from gd.ads_product_value_evaluation
""").show()

# 停止SparkSession
spark.stop()