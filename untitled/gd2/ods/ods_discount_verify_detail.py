from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp  # 确保导入了col函数

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ods_to_ads_customer_service_discount") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.sql.parquet.writeLegacyFormat", "true")  \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.hive.convertMetastoreParquet", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# 确保gd2数据库存在
spark.sql("CREATE DATABASE IF NOT EXISTS gd2")
spark.sql("USE gd2")

# 定义优惠核销表Schema
ods_verify_schema = StructType([
    StructField("verify_id", StringType(), nullable=False),
    StructField("send_id", StringType(), nullable=False),#优惠发放记录的 ID
    StructField("order_id", StringType(), nullable=False),#订单 ID
    StructField("pay_amount", DecimalType(10, 2), nullable=False),#实际支付金额
    StructField("verify_time", TimestampType(), nullable=False),#核销时间
    StructField("buy_count", IntegerType(), nullable=False)#购买数量
])

# 读取JSON数据并进行类型转换处理
verify_df = spark.read \
    .schema(ods_verify_schema) \
    .json("D:/2211A/workspace/工单/gd2/verify_detail_data.json") \
    .withColumn("pay_amount", col("pay_amount").cast(DecimalType(10, 2))) \
    .withColumn("verify_time", to_timestamp(col("verify_time"), "yyyy-MM-dd'T'HH:mm:ss")) \
    .filter(col("verify_id").isNotNull() & col("send_id").isNotNull())

# 写入Hive表
verify_df.write \
    .mode("overwrite") \
    .partitionBy("verify_time") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd2/ods_discount_verify_detail") \
    .option("compression", "snappy") \
    .option("parquet.block.size", "134217728") \
    .saveAsTable("gd2.ods_discount_verify_detail")

print("优惠核销表已成功写入Hive的gd2数据库中")

# 验证数据
print("验证写入的数据:")
spark.table("gd2.ods_discount_verify_detail").show(5)

spark.stop()