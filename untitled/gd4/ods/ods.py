from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import *

# 初始化SparkSession（启用Hive支持）
spark = SparkSession.builder \
    .appName("ods_goods_tables_creation") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.local.dir", "D:/spark-temp") \
    .enableHiveSupport()  \
    .getOrCreate()

# 切换到gd3数据库
spark.sql("USE gd02")

# 1. 商品基础信息表（ods_goods_base）
ods_goods_base_schema = StructType([
    StructField("goods_id", StringType(), nullable=False),  # 商品ID
    StructField("goods_name", StringType(), nullable=True),  # 商品名称
    StructField("category_id", StringType(), nullable=True),  # 分类ID
    StructField("category_name", StringType(), nullable=True),  # 分类名称
    StructField("shop_id", StringType(), nullable=True),  # 店铺ID
    StructField("is_price_strength", IntegerType(), nullable=True),  # 是否价格力商品（0/1）
    StructField("create_time", StringType(), nullable=True)  # 创建时间
])

spark.read \
    .schema(ods_goods_base_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd/sj/ods_goods_base.json") \
    .withColumn("create_date", date_format(col("create_time"), "yyyy-MM-dd"))  \
    .write \
    .mode("overwrite") \
    .partitionBy("create_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/ods_goods_base") \
    .saveAsTable("gd02.ods_goods_base")


# 2. 商品销售明细表（ods_goods_sales_detail）
ods_goods_sales_detail_schema = StructType([
    StructField("goods_id", StringType(), nullable=False),  # 商品ID
    StructField("sku_id", StringType(), nullable=True),  # SKU ID
    StructField("pay_amount", DoubleType(), nullable=True),  # 销售额
    StructField("sales_num", IntegerType(), nullable=True),  # 销量
    StructField("pay_buyer_num", IntegerType(), nullable=True),  # 支付买家数
    StructField("pay_date", StringType(), nullable=True)  # 支付日期
])

spark.read \
    .schema(ods_goods_sales_detail_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd/sj/ods_goods_sales_detail.json") \
    .write \
    .mode("overwrite") \
    .partitionBy("pay_date")  \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/ods_goods_sales_detail") \
    .saveAsTable("gd02.ods_goods_sales_detail")


# 3. 商品流量表（ods_goods_traffic）
ods_goods_traffic_schema = StructType([
    StructField("goods_id", StringType(), nullable=False),  # 商品ID
    StructField("traffic_source", StringType(), nullable=True),  # 流量来源
    StructField("visitor_num", IntegerType(), nullable=True),  # 访客数
    StructField("visit_date", StringType(), nullable=True)  # 访问日期
])

spark.read \
    .schema(ods_goods_traffic_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd/sj/ods_goods_traffic.json") \
    .write \
    .mode("overwrite") \
    .partitionBy("visit_date")  \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/ods_goods_traffic") \
    .saveAsTable("gd02.ods_goods_traffic")


# 4. 商品价格力表（ods_goods_price_strength）
ods_goods_price_strength_schema = StructType([
    StructField("goods_id", StringType(), nullable=False),  # 商品ID
    StructField("price_strength_star", IntegerType(), nullable=True),  # 价格力星级（1-5）
    StructField("is_low_star", IntegerType(), nullable=True),  # 是否持续低星（0/1）
    StructField("record_date", StringType(), nullable=True)  # 记录日期
])

spark.read \
    .schema(ods_goods_price_strength_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd/sj/ods_goods_price_strength.json") \
    .write \
    .mode("overwrite") \
    .partitionBy("record_date")  \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/ods_goods_price_strength") \
    .saveAsTable("gd02.ods_goods_price_strength")

# 停止SparkSession
spark.stop()
