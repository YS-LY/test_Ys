from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
import sys

# 初始化SparkSession（启用Hive支持）
spark = SparkSession.builder \
    .appName("ods_to_dwd_product_behavior") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置日志级别为WARN
spark.sparkContext.setLogLevel("WARN")

# 确保数据库存在并使用
spark.sql("CREATE DATABASE IF NOT EXISTS gd3")
spark.sql("USE gd3")

# 获取业务日期参数（支持外部传入）
# 定义业务日期参数（可根据实际情况从外部传入）
# bizdate = "${bizdate}"  # 例如："2024-01-01"

# --------------------------
# 1. DWD层-商品访问明细
# --------------------------
print("创建dwd_Product_access_details...")

try:
    # 错误点1: 读取的应该是ODS层原始表，而非DWD目标表
    ods_product_visit_log = spark.table("gd3.ods_product_visit_log")  # 修正为ODS层表名
    ods_product = spark.table("gd3.ods_product")

    # 关联表并处理字段
    dwd_product_visit = ods_product_visit_log.alias("a") \
        .join(
        ods_product.alias("b"),
        col("a.product_id") == col("b.product_id"),
        how="left"
    ) \
        .select(
        col("a.log_id").alias("visit_id"),  # 错误点2: 修正字段映射，log_id应对应visit_id
        col("a.user_id"),
        col("a.product_id"),
        col("b.category_id"),
        col("a.visit_time"),
        col("a.terminal"),
        col("a.stay_time"),  # 无需重复别名
        col("a.is_click"),
        date_format(col("a.visit_time"), "yyyy-MM-dd").alias("date")  # 错误点3: 修正为date而非data
    )

    # 写入DWD表（覆盖模式）
    dwd_product_visit.write \
        .mode("overwrite") \
        .saveAsTable("gd3.dwd_Product_access_details")

    print("dwd_Product_access_details 创建成功")

except Exception as e:
    print(f"创建dwd_Product_access_details 失败: {str(e)}")
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("请检查ODS层表是否存在:")
        print("1. gd3.ods_product_visit_log")
        print("2. gd3.ods_product")

# --------------------------
# 2. DWD层-商品收藏加购明细
# --------------------------
print("创建dwd_Product_collection_and_purchase_details...")

try:
    # 读取ODS层相关表
    ods_product_add = spark.table("gd3.ods_product_add")

    # 关联表并处理字段
    dwd_product_collect_cart = ods_product_add.alias("a") \
        .join(
        ods_product.alias("b"),  # 复用商品表
        col("a.product_id") == col("b.product_id"),
        how="left"
    ) \
        .select(
        col("a.record_id"),
        col("a.user_id"),
        col("a.product_id"),
        col("b.category_id"),
        col("a.op_type"),
        col("a.op_time"),
        col("a.add_cart_count"),
        col("a.terminal"),
        date_format(col("a.op_time"), "yyyy-MM-dd").alias("date")  # 修正为date
    )

    # 错误点4: 修正数据库名为gd3，保持一致
    dwd_product_collect_cart.write \
        .mode("overwrite") \
        .saveAsTable("gd3.dwd_Product_collection_and_purchase_details")

    print("dwd_Product_collection_and_purchase_details 创建成功")

except Exception as e:
    print(f"创建dwd_Product_collection_and_purchase_details 失败: {str(e)}")
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("请检查ODS层表是否存在: gd3.ods_product_add")


print("所有商品行为相关DWD层表处理完成！")
spark.stop()
