from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ods_to_ads_product_transaction") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.sql.parquet.writeLegacyFormat", "true")  \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.hive.convertMetastoreParquet", "true") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Ensure gd2 database exists
spark.sql("CREATE DATABASE IF NOT EXISTS gd3")
spark.sql("USE gd3")

# Define product transaction table Schema
ods_product_transaction_schema = StructType([
    StructField("order_id", StringType(), nullable=False),  # Order ID
    StructField("user_id", StringType(), nullable=False),   # User ID
    StructField("product_id", StringType(), nullable=False),  # Product ID
    StructField("category_id", StringType(), nullable=False),  # Leaf category ID
    StructField("product_price", DecimalType(10, 2), nullable=False),  # Product unit price
    StructField("buy_count", IntegerType(), nullable=False),  # Quantity ordered
    StructField("buy_amount", DecimalType(10, 2), nullable=False),  # Ordered amount
    StructField("pay_status", StringType(), nullable=False),  # Payment status (unpaid/paid)
    StructField("pay_time", TimestampType(), nullable=False),  # Payment time
    StructField("pay_amount", DecimalType(10, 2), nullable=False),  # Paid amount
    StructField("terminal", StringType(), nullable=False),  # Terminal type (PC/wireless)
    StructField("create_time", TimestampType(), nullable=False),  # Order creation time
    StructField("order_date", StringType(), nullable=False)  # Partition field (order date)
])

# Read JSON data and perform type conversion
transaction_df = spark.read \
    .schema(ods_product_transaction_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd1/sj/ods_商品交易.json") \
    .withColumn("product_price", col("product_price").cast(DecimalType(10, 2))) \
    .withColumn("buy_amount", col("buy_amount").cast(DecimalType(10, 2))) \
    .withColumn("pay_amount", col("pay_amount").cast(DecimalType(10, 2))) \
    .withColumn("pay_time", to_timestamp(col("pay_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("create_time", to_timestamp(col("create_time"), "yyyy-MM-dd HH:mm:ss")) \
    .filter(col("order_id").isNotNull() & col("order_date").isNotNull())  # Basic filtering

# Write to Hive table
transaction_df.write \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd3/ods_product_pay") \
    .option("compression", "snappy") \
    .option("parquet.block.size", "134217728") \
    .saveAsTable("gd3.ods_product_pay")

print("商品基础信息表已成功写入Hive的gd3数据库中")
# Verify written data
print("验证写入的数据:")
spark.table("gd3.ods_product_pay").show(5)

spark.stop()
