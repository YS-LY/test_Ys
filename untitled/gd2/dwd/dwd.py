from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 初始化SparkSession（启用Hive支持）
spark = SparkSession.builder \
    .appName("ods_to_ads_customer_service_discount") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .enableHiveSupport()  \
    .getOrCreate()

# 确保dwd数据库存在
spark.sql("CREATE DATABASE IF NOT EXISTS gd2")
spark.sql("USE gd2")


# --------------------------
# 1. DWD层-活动明细表（dwd_customer_service_discount_activity）
# --------------------------
print("创建dwd_customer_service_discount_activity...")

# 读取ODS层活动表并过滤有效数据
ods_activity = spark.table("gd2.ods_customer_service_discount_activity")
dwd_activity = ods_activity.filter(
    (col("is_valid") == 1) &  # 只保留有效活动
    (col("activity_id").isNotNull())  # 排除主键为空的数据
).select(
    col("activity_id"),
    col("activity_name"),
    col("activity_level"),
    col("discount_type"),
    col("max_discount_amount"),
    col("start_time"),
    col("end_time"),
    col("status"),
    col("create_time"),
    col("update_time")
)

# 写入DWD表（覆盖模式）
dwd_activity.write \
    .mode("overwrite") \
    .saveAsTable("gd2.dwd_customer_service_discount_activity")


# --------------------------
# 2. DWD层-商品优惠明细表（dwd_product_discount_detail）
# --------------------------
print("创建dwd_product_discount_detail...")

# 读取ODS层商品关联表
ods_product_rel = spark.table("gd2.ods_discount_product_rel")
# 读取已创建的DWD活动表（用于关联）
dwd_activity = spark.table("gd2.dwd_customer_service_discount_activity")

# 关联表并计算优惠金额字段
dwd_product_discount = ods_product_rel.alias("r") \
    .join(
        dwd_activity.alias("a"),
        col("r.activity_id") == col("a.activity_id"),
        how="left"  # 左连接保留所有关联记录
    ) \
    .filter(
        (col("r.is_delete") == 0) &  # 未删除的关联关系
        (col("r.rel_id").isNotNull())  # 排除主键为空的数据
    ) \
    .select(
        col("r.rel_id"),
        col("r.activity_id"),
        col("r.product_id"),
        col("r.sku_id"),
        col("a.discount_type"),
        # 固定优惠金额：仅固定优惠类型生效
        when(
            col("a.discount_type") == "固定优惠",
            col("r.fixed_discount")
        ).otherwise(None).alias("fixed_discount_amount"),
        # 自定义优惠上限：仅自定义优惠类型生效
        when(
            col("a.discount_type") == "自定义优惠",
            col("a.max_discount_amount")
        ).otherwise(None).alias("custom_discount_limit"),
        col("r.max_buy_count"),
        col("r.add_time")
    )

# 写入DWD表（覆盖模式）
dwd_product_discount.write \
    .mode("overwrite") \
    .saveAsTable("gd2.dwd_product_discount_detail")


# --------------------------
# 3. DWD层-优惠发送明细表（dwd_discount_send_detail）
# --------------------------
print("创建dwd_discount_send_detail...")

# 读取ODS层发送明细表
ods_send_detail = spark.table("gd2.ods_discount_send_detail")
# 读取DWD活动表（用于补充活动类型）
dwd_activity = spark.table("gd2.dwd_customer_service_discount_activity")

# 关联活动表补充优惠类型
dwd_send_detail = ods_send_detail.alias("s") \
    .join(
        dwd_activity.alias("a"),
        col("s.activity_id") == col("a.activity_id"),
        how="left"
    ) \
    .filter(col("s.send_id").isNotNull())  \
    .select(
        col("s.send_id"),
        col("s.activity_id"),
        col("s.product_id"),
        col("s.sku_id"),
        col("s.customer_id"),
        col("s.service_id"),
        col("s.send_discount_amount"),
        col("s.validity_period"),
        col("s.send_time"),
        col("s.expire_time"),
        col("s.remark"),
        col("a.discount_type")  # 补充活动类型
    )

# 写入DWD表（覆盖模式）
dwd_send_detail.write \
    .mode("overwrite") \
    .saveAsTable("gd2.dwd_discount_send_detail")


# --------------------------
# 4. DWD层-优惠核销明细表（dwd_discount_verify_detail）
# --------------------------
print("创建dwd_discount_verify_detail...")

# 读取ODS层核销表
ods_verify_detail = spark.table("gd2.ods_discount_verify_detail")
# 读取已创建的DWD发送明细表（用于关联）
dwd_send_detail = spark.table("gd2.dwd_discount_send_detail")

# 关联发送表补充核销相关信息
dwd_verify_detail = ods_verify_detail.alias("v") \
    .join(
        dwd_send_detail.alias("s"),
        col("v.send_id") == col("s.send_id"),
        how="left"
    ) \
    .filter(col("v.verify_id").isNotNull())  \
    .select(
        col("v.verify_id"),
        col("v.send_id"),
        col("v.order_id"),
        col("v.pay_amount"),
        col("v.verify_time"),
        col("v.buy_count"),
        col("s.send_discount_amount"),  # 从发送表关联的优惠金额
        col("s.customer_id"),  # 从发送表关联的用户ID
        col("s.service_id"),  # 从发送表关联的客服ID
        col("s.product_id"),  # 从发送表关联的商品ID
        col("s.activity_id")  # 从发送表关联的活动ID
    )

# 写入DWD表（覆盖模式）
dwd_verify_detail.write \
    .mode("overwrite") \
    .saveAsTable("gd2.dwd_discount_verify_detail")


print("所有DWD层表创建完成！")
spark.stop()