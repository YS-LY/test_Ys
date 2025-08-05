from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ods_to_ads_product_base_info") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.sql.parquet.writeLegacyFormat", "true")  \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.hive.convertMetastoreParquet", "true") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 确保gd3数据库存在
spark.sql("CREATE DATABASE IF NOT EXISTS gd3")
spark.sql("USE gd3")

# 定义商品基础信息表Schema
ods_product_base_schema = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category_id", StringType(), nullable=False),
    StructField("category_name", StringType(), nullable=False),
    StructField("product_price", DecimalType(10, 2), nullable=False),
    StructField("update_time", TimestampType(), nullable=False),
    StructField("update_date", StringType(), nullable=False)  # 分区字段
])

# 读取数据并进行类型转换处理（假设数据源为JSON，路径需根据实际情况修改）
product_df = spark.read \
    .schema(ods_product_base_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd1/sj/ods_商品基础信息.json") \
    .withColumn("product_price", col("product_price").cast(DecimalType(10, 2))) \
    .withColumn("update_time", to_timestamp(col("update_time"), "yyyy-MM-dd HH:mm:ss")) \
    .filter(col("product_id").isNotNull() & col("update_date").isNotNull())  # 基础过滤

# 写入Hive表
product_df.write \
    .mode("overwrite") \
    .partitionBy("update_date")  \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd3/ods_product") \
    .option("compression", "snappy") \
    .option("parquet.block.size", "134217728") \
    .saveAsTable("gd3.ods_product")

print("商品基础信息表已成功写入Hive的gd3数据库中")

# 验证数据
print("验证写入的数据:")
spark.table("gd3.ods_product").show(5)

spark.stop()