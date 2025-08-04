from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *

# 初始化SparkSession（仅启用Hive支持，配置从hive-site.xml读取）
spark = SparkSession.builder \
    .appName("ods_to_ads_customer_service_discount") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .enableHiveSupport()  \
    .getOrCreate()

# 切换到gd2数据库（确保已手动创建）
spark.sql("USE gd2")

# 1. 客服专属优惠活动表（读取JSON并写入Hive）
ods_activity_schema = StructType([
    StructField("activity_id", StringType(), nullable=False),#活动 ID
    StructField("activity_name", StringType(), nullable=True),#活动名称
    StructField("activity_level", StringType(), nullable=True),#活动级别
    StructField("discount_type", StringType(), nullable=True),#优惠类型
    StructField("max_discount_amount", IntegerType(), nullable=True),#最大优惠金额
    StructField("start_time", TimestampType(), nullable=True),#活动开始时间
    StructField("end_time", TimestampType(), nullable=True),#活动结束时间
    StructField("status", StringType(), nullable=True),#活动状态
    StructField("create_time", TimestampType(), nullable=True),#活动创建时间
    StructField("update_time", TimestampType(), nullable=True),#活动更新时间
    StructField("is_valid", IntegerType(), nullable=True),#是否有效标识
    StructField("raw_data", StringType(), nullable=True)#原始数据
])

spark.read \
    .schema(ods_activity_schema) \
    .json("D:/2211A/workspace/工单/gd2/activity_data.json") \
    .write \
    .mode("overwrite") \
    .partitionBy("create_time") \
    .saveAsTable("gd2.ods_customer_service_discount_activity")  # 已切换到gd2库，无需重复指定

# 2. 优惠商品关联表（同理简化）
ods_product_rel_schema = StructType([
    StructField("rel_id", StringType(), nullable=False),
    StructField("activity_id", StringType(), nullable=False),#活动 ID
    StructField("product_id", StringType(), nullable=False),#商品 ID
    StructField("sku_id", StringType(), nullable=True),#SKU ID
    StructField("fixed_discount", IntegerType(), nullable=True),#固定优惠金额
    StructField("max_buy_count", IntegerType(), nullable=True),#最大购买数量限制
    StructField("add_time", TimestampType(), nullable=True),#记录的创建时间
    StructField("is_delete", IntegerType(), nullable=True)#逻辑删除标识
])

spark.read \
    .schema(ods_product_rel_schema) \
    .json("D:/2211A/workspace/工单/gd2/product_rel_data.json") \
    .write \
    .mode("overwrite") \
    .partitionBy("add_time") \
    .saveAsTable("gd2.ods_discount_product_rel")

# 3. ODS层-优惠发送明细表（ods_discount_send_detail）
ods_send_detail_schema = StructType([
    StructField("send_id", StringType(), nullable=False),  # 发送ID（主键）
    StructField("activity_id", StringType(), nullable=False),  # 活动ID
    StructField("product_id", StringType(), nullable=False),  # 商品ID
    StructField("sku_id", StringType(), nullable=True),  # SKU ID
    StructField("customer_id", StringType(), nullable=False),  # 消费者ID
    StructField("service_id", StringType(), nullable=False),  # 客服ID
    StructField("send_discount_amount", IntegerType(), nullable=False),  # 实际发送优惠金额
    StructField("validity_period", IntegerType(), nullable=False),  # 有效期（小时，1-24）
    StructField("send_time", TimestampType(), nullable=False),  # 发送时间
    StructField("expire_time", TimestampType(), nullable=False),  # 过期时间（send_time + validity_period）
    StructField("remark", StringType(), nullable=True)  # 内部备注
])

spark.read \
    .schema(ods_send_detail_schema) \
    .json("D:/2211A/workspace/工单/gd2/send_detail_data.json") \
    .write \
    .mode("overwrite") \
    .partitionBy("send_time") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd2/ods_discount_send_detail")  \
    .saveAsTable("gd2.ods_discount_send_detail")  # 保存为Hive表



  # 停止SparkSession
spark.stop()
