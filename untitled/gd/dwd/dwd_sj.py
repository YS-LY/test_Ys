from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, split, current_timestamp
import os

# 初始化Spark（符合文档中Spark 3.2及以上版本要求）
spark = SparkSession.builder \
    .appName("商品诊断看板-DWD层修复") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://cdh02:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取ODS数据并清洗（满足文档数据质量要求）
ods_df = spark.table("gd.ods_product_behavior")
dwd_df = ods_df \
    .filter("product_id is not null") \
    .withColumn("traffic_cnt", when(
        col("traffic_cnt").cast("BIGINT").between(0, 9223372036854775807),
        col("traffic_cnt").cast("BIGINT")
    ).otherwise(0)) \
    .withColumn("click_cnt", when(
        col("click_cnt").cast("BIGINT").between(0, 9223372036854775807),
        col("click_cnt").cast("BIGINT")
    ).otherwise(0)) \
    .withColumn("category", split(col("product_name"), "_")[0]) \
    .withColumn("click_rate", round(col("click_cnt")/col("traffic_cnt"), 4)) \
    .withColumn("etl_time", current_timestamp().cast("string")) \
    .select(
        "product_id", "product_name", "category", "traffic_cnt",
        "click_cnt", "click_rate", "stat_date", "etl_time"
    )

# 重建表结构（匹配文档指标体系）
spark.sql("DROP TABLE IF EXISTS gd.dwd_product_behavior_detail")
spark.sql("""
    CREATE TABLE gd.dwd_product_behavior_detail (
        product_id STRING, product_name STRING, category STRING,
        traffic_cnt BIGINT, click_cnt BIGINT, click_rate DOUBLE,
        stat_date DATE, etl_time STRING
    )
    PARTITIONED BY (stat_date)
    STORED AS PARQUET
""")

# 禁用字典编码（解决编码异常）
dwd_df.write \
    .option("parquet.enable.dictionary", "false") \
    .partitionBy("stat_date") \
    .mode("overwrite") \
    .saveAsTable("gd.dwd_product_behavior_detail")

# 验证数据（符合文档验收标准中的测试要求）
spark.sql("SELECT * FROM gd.dwd_product_behavior_detail WHERE stat_date='2025-07-01' LIMIT 5").show()

spark.stop()