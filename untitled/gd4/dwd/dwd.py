from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, lit, coalesce
from pyspark.sql.types import *

# 初始化SparkSession（复用已有配置）
spark = SparkSession.builder \
    .appName("dwd_goods_tables_creation") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.local.dir", "D:/spark-temp") \
    .enableHiveSupport()  \
    .getOrCreate()

# 切换到gd02数据库
spark.sql("USE gd02")

# 1. 创建并插入DWD_商品销售清洗表（过滤异常值）
# 读取源表并过滤异常值（增加多重异常值过滤）
ods_goods_sales_detail = spark.table("ods_goods_sales_detail")
dwd_sales_clean_df = ods_goods_sales_detail \
    .filter(
        (col("pay_amount") > 0) &  # 过滤销售额为负的异常值
        (col("sales_num") >= 0) &   # 过滤销量为负的异常值
        (col("pay_buyer_num") >= 0) &  # 过滤买家数为负的异常值
        (col("pay_buyer_num") <= col("sales_num"))  # 买家数不能超过销量
    ) \
    .select(
        col("goods_id"),
        col("sku_id"),
        col("pay_amount").cast(DecimalType(10, 2)).alias("total_sales"),
        col("sales_num").alias("total_sales_num"),
        col("pay_buyer_num"),
        col("pay_date")
    )

# 写入目标表（增加文件格式指定）
dwd_sales_clean_df.write \
    .mode("overwrite") \
    .format("orc") \
    .partitionBy("pay_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/dwd_goods_sales_clean") \
    .saveAsTable("gd02.dwd_goods_sales_clean")


# 2. 创建并插入DWD_商品流量清洗表（计算支付转化率）
# 计算销售表聚合数据（支付买家数总和，处理空值）
sales_agg_df = spark.table("ods_goods_sales_detail") \
    .groupBy("goods_id") \
    .agg(
        coalesce(spark_sum("pay_buyer_num"), lit(0)).alias("pay_buyer_num")  # 处理无销售数据的情况
    )

# 读取流量表并关联计算转化率（增加流量表异常值过滤）
ods_goods_traffic = spark.table("ods_goods_traffic")
dwd_traffic_clean_df = ods_goods_traffic \
    .filter(col("visitor_num") >= 0)  \
    .join(sales_agg_df, on="goods_id", how="left") \
    .select(
        col("goods_id"),
        col("traffic_source"),
        col("visitor_num"),
        # 处理除数为0和空值的情况
        when(
            col("visitor_num") > 0,
            (col("pay_buyer_num").cast(DecimalType(10, 4)) /
             col("visitor_num").cast(DecimalType(10, 4))).cast(DecimalType(5, 4))
        ).otherwise(lit(0)).alias("pay_conver_rate"),
        col("visit_date")
    )

# 写入目标表（增加文件格式指定）
dwd_traffic_clean_df.write \
    .mode("overwrite") \
    .format("orc") \
    .partitionBy("visit_date") \
    .option("path", "hdfs://cdh01:8020/bigdata_warehouse/gd02/dwd_goods_traffic_clean") \
    .saveAsTable("gd02.dwd_goods_traffic_clean")

# 验证数据质量
print(f"销售清洗表数据量: {dwd_sales_clean_df.count()}")
print(f"流量清洗表数据量: {dwd_traffic_clean_df.count()}")

# 停止SparkSession
spark.stop()
