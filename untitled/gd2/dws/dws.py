from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, count, sum, countDistinct,
    round, when, lit, coalesce
)
from pyspark.sql.types import DecimalType, StructType, StructField, StringType, IntegerType, TimestampType
import datetime

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("dwd_to_dws_summary") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 确保数据库存在
spark.sql("CREATE DATABASE IF NOT EXISTS gd2")
spark.sql("USE gd2")


# --------------------------
# 前置步骤：检查并补充数据
# --------------------------
# 读取依赖的DWD表
dwd_activity = spark.table("gd2.dwd_customer_service_discount_activity")
dwd_send = spark.table("gd2.dwd_discount_send_detail")
dwd_verify = spark.table("gd2.dwd_discount_verify_detail")

# 1. 检查上游数据
print("\n===== 上游数据检查 =====")
print(f"发送表记录数：{dwd_send.count()}")
print(f"核销表记录数：{dwd_verify.count()}")

# 2. 插入测试数据（如果核销表为空）
if dwd_verify.count() == 0:
    print("\n核销表为空，插入测试数据...")
    test_verify_data = [
        ("verify_001", "send_123", "order_456", 99.99, datetime.datetime(2025, 8, 19), 1, 50, "user_001", "serv_70ec73", "prod_111", "act_79b3d6f1"),
        ("verify_002", "send_456", "order_789", 199.99, datetime.datetime(2025, 9, 2), 2, 100, "user_002", "serv_c9cc6d", "prod_222", "act_ab0b79c6")
    ]
    test_verify_schema = StructType([
        StructField("verify_id", StringType()),
        StructField("send_id", StringType()),
        StructField("order_id", StringType()),
        StructField("pay_amount", DecimalType(10, 2)),
        StructField("verify_time", TimestampType()),
        StructField("buy_count", IntegerType()),
        StructField("send_discount_amount", IntegerType()),
        StructField("customer_id", StringType()),
        StructField("service_id", StringType()),
        StructField("product_id", StringType()),
        StructField("activity_id", StringType())
    ])
    spark.createDataFrame(test_verify_data, schema=test_verify_schema) \
        .write \
        .mode("append") \
        .saveAsTable("gd2.dwd_discount_verify_detail")
    # 重新读取核销表
    dwd_verify = spark.table("gd2.dwd_discount_verify_detail")


# --------------------------
# 1. DWS层-活动效果汇总表
# --------------------------
print("\n开始创建dws_activity_effect_summary...")

# 子查询1：发送数据汇总
send_data = dwd_activity.alias("a") \
    .join(dwd_send.alias("s"), col("a.activity_id") == col("s.activity_id"), "left") \
    .select(
        col("a.activity_id"),
        col("a.activity_name"),
        col("a.status"),
        col("a.discount_type"),
        date_format(col("s.send_time"), "yyyy-MM-dd").alias("dt"),
        col("s.send_id"),
        col("s.send_discount_amount"),
        lit(None).alias("customer_id")
    ) \
    .groupBy("activity_id", "activity_name", "status", "discount_type", "dt", "customer_id") \
    .agg(
        count("send_id").alias("send_count"),
        sum("send_discount_amount").alias("send_amount"),
        lit(0).alias("verify_count"),
        lit(0).cast(DecimalType(10, 2)).alias("verify_amount"),
        lit(0).alias("buy_count")
    ) \
    .select(
        "activity_id", "activity_name", "status", "discount_type", "dt",
        "send_count", "send_amount", "verify_count", "verify_amount", "buy_count", "customer_id"
    )

# 子查询2：核销数据汇总（确保与发送表send_id关联）
verify_data = dwd_verify.alias("v") \
    .join(dwd_activity.alias("a"), col("v.activity_id") == col("a.activity_id"), "left") \
    .select(
        coalesce(col("v.activity_id"), col("a.activity_id")).alias("activity_id"),
        col("a.activity_name"),
        col("a.status"),
        col("a.discount_type"),
        date_format(col("v.verify_time"), "yyyy-MM-dd").alias("dt"),
        col("v.verify_id"),
        col("v.pay_amount"),
        col("v.buy_count"),
        col("v.customer_id")
    ) \
    .groupBy("activity_id", "activity_name", "status", "discount_type", "dt", "customer_id") \
    .agg(
        lit(0).alias("send_count"),
        lit(0).alias("send_amount"),
        count("verify_id").alias("verify_count"),
        sum("pay_amount").alias("verify_amount"),
        sum("buy_count").alias("buy_count")
    ) \
    .select(
        "activity_id", "activity_name", "status", "discount_type", "dt",
        "send_count", "send_amount", "verify_count", "verify_amount", "buy_count", "customer_id"
    )

# 合并数据并汇总
activity_summary = send_data.unionAll(verify_data) \
    .groupBy("activity_id", "activity_name", "status", "discount_type", "dt") \
    .agg(
        sum("send_count").alias("daily_send_count"),
        sum("send_amount").alias("daily_send_amount"),
        sum("verify_count").alias("daily_verify_count"),
        sum("verify_amount").alias("daily_verify_amount"),
        sum("buy_count").alias("daily_buy_count"),
        countDistinct("customer_id").alias("daily_pay_customer_count")
    )

# 写入表
activity_summary.write.mode("overwrite").option("compression", "snappy").saveAsTable("gd2.dws_activity_effect_summary")
print("dws_activity_effect_summary创建完成")


# --------------------------
# 2. DWS层-客服绩效汇总表
# --------------------------
print("\n开始创建dws_service_performance_summary...")

service_send = dwd_send \
    .select(
        col("service_id"),
        date_format(col("send_time"), "yyyy-MM-dd").alias("dt"),
        col("send_id"),
        col("send_discount_amount")
    ) \
    .groupBy("service_id", "dt") \
    .agg(
        count("send_id").alias("send_count"),
        sum("send_discount_amount").alias("send_amount"),
        lit(0).alias("verify_count"),
        lit(0).cast(DecimalType(10, 2)).alias("verify_amount")
    )

service_verify = dwd_verify \
    .select(
        col("service_id"),
        date_format(col("verify_time"), "yyyy-MM-dd").alias("dt"),
        col("verify_id"),
        col("pay_amount")
    ) \
    .groupBy("service_id", "dt") \
    .agg(
        lit(0).alias("send_count"),
        lit(0).alias("send_amount"),
        count("verify_id").alias("verify_count"),
        sum("pay_amount").alias("verify_amount")
    )

service_summary = service_send.unionAll(service_verify) \
    .groupBy("service_id", "dt") \
    .agg(
        sum("send_count").alias("total_send_count"),
        sum("send_amount").alias("total_send_amount"),
        sum("verify_count").alias("total_verify_count"),
        sum("verify_amount").alias("total_verify_amount")
    ) \
    .withColumn(
        "conversion_rate",
        when(col("total_send_count") == 0, 0).otherwise(round(col("total_verify_count")/col("total_send_count"), 4))
    )

service_summary.write.mode("overwrite").option("compression", "snappy").saveAsTable("gd2.dws_service_performance_summary")
print("dws_service_performance_summary创建完成")


# 验证结果
print("\n活动效果汇总表示例数据：")
spark.table("gd2.dws_activity_effect_summary").show(5, truncate=False)

print("\n客服绩效汇总表示例数据：")
spark.table("gd2.dws_service_performance_summary").show(5, truncate=False)

spark.stop()