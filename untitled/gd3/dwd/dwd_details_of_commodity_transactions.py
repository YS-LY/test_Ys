from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format
from pyspark.sql.types import DecimalType, TimestampType, StructType, StructField, StringType, IntegerType

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("fix_dwd_verify_detail") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.sql.parquet.writeLegacyFormat", "true")  \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# --------------------------
# 第一步：确保依赖的数据库和表存在
# --------------------------
# 创建并使用gd2数据库
spark.sql("CREATE DATABASE IF NOT EXISTS gd3")
spark.sql("USE gd3")

print("创建dwd_details_of_commodity_transactions...")

try:
    # 读取ODS层交易表
    ods_product_pay = spark.table("gd3.ods_product_pay")

    # 处理字段（确保类型转换和格式正确）
    dwd_product_trade = ods_product_pay \
        .select(
            col("order_id"),
            col("user_id"),
            col("product_id"),
            col("category_id"),
            col("product_price").cast(DecimalType(10, 2)).alias("product_price"),  # 确保价格精度
            col("buy_count").cast(IntegerType()).alias("buy_count"),
            col("buy_amount").cast(DecimalType(10, 2)).alias("buy_amount"),
            col("pay_status").cast(StringType()).alias("pay_status"),
            to_timestamp(col("pay_time"), "yyyy-MM-dd'T'HH:mm:ss").alias("pay_time"),  # 统一时间格式
            col("pay_amount").cast(DecimalType(10, 2)).alias("pay_amount"),
            col("terminal"),
            date_format(col("create_time"), "yyyy-MM-dd").alias("date")
        ) \
        .filter(col("order_id").isNotNull())  # 过滤关键字段为空的记录

    # 写入DWD表（覆盖模式，指定存储路径和格式配置）
    dwd_product_trade.write \
        .mode("overwrite") \
        .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd3/dwd_details_of_commodity_transactions") \
        .option("compression", "snappy") \
        .option("parquet.page.size", "1048576") \
        .saveAsTable("gd3.dwd_details_of_commodity_transactions")

    print("dwd_details_of_commodity_transactions 创建成功")

except Exception as e:
    print(f"创建dwd_details_of_commodity_transactions 失败: {str(e)}")
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("请检查ODS层表是否存在: gd3.ods_product_pay")