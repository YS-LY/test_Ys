from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from pyspark.sql.types import *

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ads_goods_analysis_tables") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.local.dir", "D:/spark-temp") \
    .enableHiveSupport()  \
    .getOrCreate()

# 切换到gd02数据库
spark.sql("USE gd02")

# 1. 创建并插入ADS_全部商品排行表（支撑商品排行看板）
# 读取DWS销售汇总表和商品基础表，并设置别名避免字段冲突
dws_sales_sum = spark.table("dws_goods_sales_sum").alias("s")
ods_goods_base = spark.table("ods_goods_base").alias("b")

# 定义排名窗口（按销售额降序）
rank_window = Window.orderBy(col("total_sales").desc())

# 关联表并计算排名（明确指定字段来源，解决歧义）
ads_all_goods_rank_df = dws_sales_sum \
    .join(ods_goods_base, on="goods_id", how="left") \
    .select(
        col("goods_id"),
        col("b.goods_name"),  # 明确使用商品基础表的商品名称
        col("s.total_sales"),  # 明确使用销售汇总表的销售额
        col("s.total_sales_num"),  # 明确使用销售汇总表的销量
        col("s.category_name"),  # 明确使用销售汇总表的分类名称（解决歧义）
        col("s.stat_date")
    ) \
    .withColumn("rank", row_number().over(rank_window).cast(IntegerType())) \
    .select(
        col("rank"),
        col("goods_id"),
        col("goods_name"),
        col("total_sales"),
        col("total_sales_num"),
        col("category_name"),
        col("stat_date")
    )

# 写入目标表
ads_all_goods_rank_df.write \
    .mode("overwrite") \
    .format("orc") \
    .partitionBy("stat_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/ads_all_goods_rank") \
    .saveAsTable("gd02.ads_all_goods_rank")


# 2. 创建并插入ADS_价格力预警表（预警商品展示）
# 读取价格力表和商品基础表，设置别名
ods_price_strength = spark.table("ods_goods_price_strength").alias("p")
ods_goods_base = spark.table("ods_goods_base").alias("b")  # 重新获取并设置别名

# 筛选低星商品并关联基础信息
ads_price_warn_df = ods_price_strength \
    .filter(col("is_low_star") == 1) \
    .join(ods_goods_base, on="goods_id", how="left") \
    .select(
        col("goods_id"),
        col("b.goods_name"),  # 明确使用商品基础表的商品名称
        lit("持续低星商品").alias("warn_reason"),
        col("p.record_date").alias("warn_date")  # 明确使用价格力表的日期
    )

# 写入目标表
ads_price_warn_df.write \
    .mode("overwrite") \
    .format("orc") \
    .partitionBy("warn_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/ads_price_strength_warn") \
    .saveAsTable("gd02.ads_price_strength_warn")

# 数据质量验证
print(f"全商品排行表数据量: {ads_all_goods_rank_df.count()}")
print(f"价格力预警表数据量: {ads_price_warn_df.count()}")

# 停止SparkSession
spark.stop()
