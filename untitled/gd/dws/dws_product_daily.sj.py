from pyspark.sql import SparkSession
import os

# 确保临时目录存在
temp_dir = "D:/spark_temp"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("商品诊断看板数据处理") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://cdh02:9083") \
    .config("spark.local.dir", temp_dir) \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.util.ShutdownHookManager=ERROR") \
    .enableHiveSupport() \
    .getOrCreate()

# 创建gd数据库
spark.sql("CREATE DATABASE IF NOT EXISTS gd")

# 读取ODS数据（正确配置）
try:
    ods_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("encoding", "UTF-8") \
        .schema("""
            product_id STRING, product_name STRING, traffic_cnt BIGINT,
            click_cnt BIGINT, click_rate DOUBLE,conversion_rate DOUBLE, content_interact_cnt BIGINT,
            content_interact_rate DOUBLE,new_user_cnt BIGINT,visitor_cnt BIGINT,new_user_rate DOUBLE,
             service_rating DOUBLE, sales_amount DECIMAL(16,2),
            sales_cnt BIGINT,avg_price DECIMAL(16,2), stat_date DATE
        """) \
        .load("D:/2211A/workspace/工单/gd/ods/ods_product_behavior0.csv") \
        .filter("product_id != 'product_id'")  # 过滤表头
    ods_df.createOrReplaceTempView("ods_product_behavior")
except Exception as e:
    print(f"读取ODS表失败：{e}")
    spark.stop()
    exit(1)

# 计算DWS指标
dws_df = spark.sql("""
    SELECT
        product_id,
        product_name,
        traffic_cnt,
        click_cnt,
        -- 若原始click_rate为空则重新计算，否则使用原始值
        CASE 
            WHEN click_rate IS NOT NULL THEN click_rate 
            WHEN traffic_cnt = 0 THEN NULL 
            ELSE ROUND((click_cnt * 100.0 / traffic_cnt), 2) 
        END AS click_rate,
        conversion_rate,
        content_interact_cnt,
        -- 处理content_interact_rate的空值逻辑
        CASE 
            WHEN content_interact_rate IS NOT NULL THEN content_interact_rate 
            WHEN traffic_cnt = 0 THEN NULL 
            ELSE ROUND((content_interact_cnt * 100.0 / traffic_cnt), 2) 
        END AS content_interact_rate,
        new_user_cnt,
        visitor_cnt,
        -- 处理new_user_rate的空值逻辑
        CASE 
            WHEN new_user_rate IS NOT NULL THEN new_user_rate 
            WHEN visitor_cnt = 0 THEN NULL 
            ELSE ROUND((new_user_cnt * 100.0 / visitor_cnt), 2) 
        END AS new_user_rate,
        service_rating,
        sales_amount,
        sales_cnt,
        -- 处理avg_price的空值逻辑
        CASE 
            WHEN avg_price IS NOT NULL THEN avg_price 
            WHEN sales_cnt = 0 THEN NULL 
            ELSE ROUND((sales_amount / sales_cnt), 2) 
        END AS avg_price,
        stat_date
    FROM ods_product_behavior
""")


# 写入DWS表
try:
    dws_df.write \
        .partitionBy("stat_date") \
        .mode("overwrite") \
        .saveAsTable("gd.dws_product_daily")
except Exception as e:
    print(f"写入DWS表失败：{e}")
    spark.stop()
    exit(1)

# 展示结果
test_result = spark.sql("SELECT * FROM gd.dws_product_daily LIMIT 5")
print("商品诊断看板上线截图：")
test_result.show()

# 优雅关闭Spark
spark.stop()