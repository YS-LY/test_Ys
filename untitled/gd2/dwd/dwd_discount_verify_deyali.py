from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
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
spark.sql("CREATE DATABASE IF NOT EXISTS gd2 LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd2'")
spark.sql("USE gd2")

# 检查并创建依赖的ODS表（如果不存在）
try:
    spark.table("gd2.ods_discount_verify_detail")
    print("gd2表已存在，直接使用...")
except:
    print("表不存在，开始创建...")
    # 定义ODS表结构（使用StructType明确字段类型）
    ods_verify_schema = StructType([
        StructField("verify_id", StringType(), False),
        StructField("send_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("pay_amount", DecimalType(10, 2), False),
        StructField("verify_time", TimestampType(), False),
        StructField("buy_count", IntegerType(), False)
    ])
    # 创建ODS表
    spark.createDataFrame([], schema=ods_verify_schema) \
        .write \
        .mode("overwrite") \
        .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd2/ods_discount_verify_detail") \
        .saveAsTable("gd2.ods_discount_verify_detail")
    print("ODS表创建完成")

# 检查并创建依赖的DWD发送表（如果不存在）
try:
    spark.table("gd2.dwd_discount_send_detail")
    print("DWD发送表已存在，直接使用...")
except:
    print("DWD发送表不存在，开始创建...")
    # 定义发送表结构
    send_schema = StructType([
        StructField("send_id", StringType(), False),
        StructField("activity_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("sku_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("send_discount_amount", IntegerType(), True)
    ])
    # 创建DWD发送表
    spark.createDataFrame([], schema=send_schema) \
        .write \
        .mode("overwrite") \
        .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd2/dwd_discount_send_detail") \
        .saveAsTable("gd2.dwd_discount_send_detail")
    print("DWD发送表创建完成")

# --------------------------
# 第二步：读取并处理数据
# --------------------------
# 读取ODS表，使用cast(DecimalType)转换数值类型（修正to_decimal错误）
ods_verify = spark.table("gd2.ods_discount_verify_detail") \
    .withColumn("pay_amount", col("pay_amount").cast(DecimalType(10, 2)))  \
    .withColumn("verify_time", to_timestamp(col("verify_time"), "yyyy-MM-dd'T'HH:mm:ss")) \
    .filter(col("verify_id").isNotNull() & col("send_id").isNotNull())

# 读取DWD发送表
dwd_send = spark.table("gd2.dwd_discount_send_detail") \
    .filter(col("send_id").isNotNull())

# --------------------------
# 第三步：关联表并创建DWD核销表
# --------------------------
dwd_verify = ods_verify.alias("v") \
    .join(
        dwd_send.alias("s"),
        col("v.send_id") == col("s.send_id"),
        how="left"
    ) \
    .select(
        col("v.verify_id"),
        col("v.send_id"),
        col("v.order_id"),
        col("v.pay_amount").cast(DecimalType(10, 2)).alias("pay_amount"),  # 确保Decimal类型
        col("v.verify_time").cast(TimestampType()).alias("verify_time"),
        col("v.buy_count"),
        col("s.send_discount_amount").cast(IntegerType()).alias("send_discount_amount"),
        col("s.customer_id"),
        col("s.service_id"),
        col("s.product_id"),
        col("s.activity_id")
    ) \
    .filter(col("verify_id").isNotNull())

# --------------------------
# 第四步：写入DWD表
# --------------------------
dwd_verify.write \
    .mode("overwrite") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd2/dwd_discount_verify_detail")  \
    .option("compression", "snappy") \
    .option("parquet.page.size", "1048576") \
    .saveAsTable("gd2.dwd_discount_verify_detail")

# --------------------------
# 验证结果
# --------------------------
print("验证DWD表数据：")
spark.table("gd2.dwd_discount_verify_detail").show(5)

spark.stop()