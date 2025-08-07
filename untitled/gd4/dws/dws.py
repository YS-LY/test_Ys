from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, lit
from pyspark.sql.types import *

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("dws_goods_summary_tables") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.local.dir", "D:/spark-temp") \
    .enableHiveSupport()  \
    .getOrCreate()

# 切换到gd02数据库
spark.sql("USE gd02")

# 1. 创建并插入DWS_商品销售汇总表（按商品+日期汇总）
# 读取清洗后的销售表和商品基础表
dwd_sales_clean = spark.table("dwd_goods_sales_clean")
ods_goods_base = spark.table("ods_goods_base")

# 关联表并聚合计算
dws_sales_sum_df = dwd_sales_clean \
    .join(ods_goods_base, on="goods_id", how="left") \
    .groupBy(
        col("goods_id"),
        col("category_name"),
        col("pay_date")
    ) \
    .agg(
        spark_sum("total_sales").cast(DecimalType(10, 2)).alias("total_sales"),
        spark_sum("total_sales_num").alias("total_sales_num")
    ) \
    .select(
        col("goods_id"),
        col("category_name"),
        col("total_sales"),
        col("total_sales_num"),
        col("pay_date").alias("stat_date")
    )

# 写入目标表
dws_sales_sum_df.write \
    .mode("overwrite") \
    .format("orc") \
    .partitionBy("stat_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/dws_goods_sales_sum") \
    .saveAsTable("gd02.dws_goods_sales_sum")


# 2. 创建并插入DWS_价格力商品汇总表（价格力分层统计）
# 读取价格力表和商品基础表
ods_price_strength = spark.table("ods_goods_price_strength")

# 关联表并按分类统计
dws_price_sum_df = ods_price_strength \
    .join(ods_goods_base, on="goods_id", how="left") \
    .groupBy(
        col("category_name"),
        col("record_date")
    ) \
    .agg(
        # 统计4-5星的优秀商品数量
        spark_sum(when(col("price_strength_star") >= 4, 1).otherwise(0)).alias("excellent_num"),
        # 统计1-2星的较差商品数量
        spark_sum(when(col("price_strength_star") <= 2, 1).otherwise(0)).alias("poor_num")
    ) \
    .select(
        col("category_name"),
        col("excellent_num"),
        col("poor_num"),
        col("record_date").alias("stat_date")
    )

# 写入目标表
dws_price_sum_df.write \
    .mode("overwrite") \
    .format("orc") \
    .partitionBy("stat_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/dws_price_strength_sum") \
    .saveAsTable("gd02.dws_price_strength_sum")

# 数据质量验证
print(f"商品销售汇总表数据量: {dws_sales_sum_df.count()}")
print(f"价格力商品汇总表数据量: {dws_price_sum_df.count()}")

# 停止SparkSession
spark.stop()
