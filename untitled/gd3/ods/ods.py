from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import *

# 初始化SparkSession（启用Hive支持）
spark = SparkSession.builder \
    .appName("ods_product_tables_creation") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.local.dir", "D:/spark-temp")\
    .enableHiveSupport()  \
    .getOrCreate()

# 切换到gd2数据库
spark.sql("USE gd3")

# 1. 商品访问日志表（ods_商品访问日志）
ods_product_access_log_schema = StructType([
    StructField("log_id", StringType(), nullable=False),  # 日志唯一标识
    StructField("user_id", StringType(), nullable=True),   # 访问用户ID
    StructField("product_id", StringType(), nullable=True),   # 商品ID
    StructField("visit_time", TimestampType(), nullable=True),  # 访问时间
    StructField("terminal", StringType(), nullable=True),  # 终端类型(PC/无线)
    StructField("stay_time", IntegerType(), nullable=True),  # 停留时长(秒)
    StructField("is_click", IntegerType(), nullable=True),  # 是否有点击行为(0/1)
    StructField("source", StringType(), nullable=True)   # 流量来源渠道
])

# 读取JSON数据，添加分区字段（日志日期），写入Hive表
spark.read \
    .schema(ods_product_access_log_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd1/sj/ods_商品访问日志.json") \
    .withColumn("log_date", date_format(col("visit_time"), "yyyy-MM-dd"))  \
    .write \
    .mode("overwrite") \
    .partitionBy("log_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd3/ods_product_visit_log") \
    .saveAsTable("gd3.ods_product_visit_log")

# 2. 商品收藏加购表（ods_商品收藏加购）
ods_product_favorite_cart_schema = StructType([
    StructField("record_id", StringType(), nullable=False),  # 记录唯一标识
    StructField("user_id", StringType(), nullable=True),   # 用户ID
    StructField("product_id", StringType(), nullable=True),   # 商品ID
    StructField("op_type", StringType(), nullable=True),  # 操作类型(收藏/加购)
    StructField("op_time", TimestampType(), nullable=True),  # 操作时间
    StructField("add_cart_count", IntegerType(), nullable=True),  # 加购件数(仅加购时有值)
    StructField("terminal", StringType(), nullable=True)   # 终端类型(PC/无线)
])

spark.read \
    .schema(ods_product_favorite_cart_schema) \
    .json("D:/2211A/workspace/工单/第三周/gd1/sj/ods_商品收藏加购.json") \
    .withColumn("op_date", date_format(col("op_time"), "yyyy-MM-dd")) \
    .write \
    .mode("overwrite") \
    .partitionBy("op_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd3/ods_product_add") \
    .saveAsTable("gd3.ods_product_add")

# 3. 商品交易表（ods_商品交易）
# ods_product_transaction_schema = StructType([
#     StructField("order_id", StringType(), nullable=False),  # 订单ID
#     StructField("user_id", StringType(), nullable=True),   # 用户ID
#     StructField("product_id", StringType(), nullable=True),   # 商品ID
#     StructField("category_id", StringType(), nullable=True),  # 叶子类目ID
#     StructField("product_price", DecimalType(10, 2), nullable=True),  # 商品单价
#     StructField("buy_count", ByteType(), nullable=True),  # 拍下件数
#     StructField("buy_amount", DecimalType(10, 2), nullable=True),  # 拍下金额
#     StructField("pay_status", StringType(), nullable=True),  # 支付状态(未支付/已支付)
#     StructField("pay_time", TimestampType(), nullable=True),  # 支付时间
#     StructField("pay_amount", DecimalType(10, 2), nullable=True),  # 支付金额
#     StructField("terminal", StringType(), nullable=True),  # 终端类型(PC/无线)
#     StructField("create_time", TimestampType(), nullable=True)  # 订单创建时间
# ])
#
# spark.read \
#     .schema(ods_product_transaction_schema) \
#     .json("D:/2211A/workspace/工单/第三周/gd1/sj/ods_商品交易.json") \
#     .withColumn("order_date", date_format(col("create_time"), "yyyy-MM-dd")) \
#     .write \
#     .mode("overwrite") \
#     .partitionBy("order_date") \
#     .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd3/ods_product_pay") \
#     .saveAsTable("gd3.ods_product_pay")
#
# # 4. 商品基础信息表（ods_商品基础信息）
# ods_product_basic_info_schema = StructType([
#     StructField("product_id", StringType(), nullable=False),  # 商品ID
#     StructField("product_name", StringType(), nullable=True),  # 商品名称
#     StructField("category_id", StringType(), nullable=True),  # 叶子类目ID
#     StructField("category_name", StringType(), nullable=True),  # 叶子类目名称
#     StructField("product_price", DecimalType(10, 2), nullable=True),  # 商品定价
#     StructField("update_time", TimestampType(), nullable=True)  # 信息更新时间
# ])
#
# spark.read \
#     .schema(ods_product_basic_info_schema) \
#     .json("D:/2211A/workspace/工单/第三周/gd1/sj/ods_商品基础信息.json") \
#     .withColumn("update_date", date_format(col("update_time"), "yyyy-MM-dd")) \
#     .write \
#     .mode("overwrite") \
#     .partitionBy("update_date") \
#     .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd3/ods_product") \
#     .saveAsTable("gd3.ods_product")

# 停止SparkSession
spark.stop()