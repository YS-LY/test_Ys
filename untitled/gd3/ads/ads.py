from pyspark.sql import SparkSession
import sys

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("create_ads_product_macro_monitor") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.local.dir", "D:/spark_temp") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.enable.dictionary", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 获取业务日期参数
bizdate = '2025-07-21'  # 可通过命令行参数传入：sys.argv[1]
print(f"执行业务日期: {bizdate}")

# 切换数据库
spark.sql("CREATE DATABASE IF NOT EXISTS gd3")
spark.sql("USE gd3")

# --------------------------
# 创建ADS层商品宏观监控看板表
# --------------------------
create_table_sql = """
CREATE TABLE IF NOT EXISTS gd3.ads_product_macro_monitor (
    statistical_time STRING COMMENT '统计时间',
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '叶子类目ID',
    category_name STRING COMMENT '叶子类目名称',
    visitor_count INT COMMENT '商品访客数（统计周期内访问详情页的去重人数）',
    view_count INT COMMENT '商品浏览量（统计周期内详情页被浏览次数累加）',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长（秒）',
    bounce_rate DECIMAL(10,4) COMMENT '商品详情页跳出率（无点击人数/访客数）',
    favorite_user_count INT COMMENT '商品收藏人数（统计日期内新增收藏的去重人数）',
    visit_to_collect_rate DECIMAL(10,4) COMMENT '访问收藏转化率（收藏人数/访客数）',
    add_to_cart_quantity INT COMMENT '商品加购件数（统计日期内新增加购的商品件数总和）',
    add_to_cart_user_count INT COMMENT '商品加购人数（统计日期内新增加购的去重人数）',
    visit_to_add_to_cart_rate DECIMAL(10,4) COMMENT '访问加购转化率（加购人数/访客数）',
    order_user_count INT COMMENT '下单买家数（统计时间内拍下的去重买家人数）',
    order_quantity INT COMMENT '下单件数（统计时间内累计拍下件数）',
    order_amount DECIMAL(10,2) COMMENT '下单金额（统计时间内累计拍下金额）',
    order_conversion_rate DECIMAL(10,4) COMMENT '下单转化率（下单买家数/访客数）',
    pay_user_count INT COMMENT '支付买家数（统计时间内完成支付的去重买家人数）',
    pay_quantity INT COMMENT '支付件数（统计时间内累计支付件数）',
    pay_amount DECIMAL(10,2) COMMENT '支付金额（统计时间内累计支付金额，未剔除退款）',
    payment_conversion_rate DECIMAL(10,4) COMMENT '支付转化率（支付买家数/访客数）',
    avg_order_value DECIMAL(10,2) COMMENT '客单价（支付金额/支付买家数）',
    avg_visitor_value DECIMAL(10,4) COMMENT '访客平均价值（支付金额/访客数）'
)
STORED AS PARQUET
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd3/ads_product_macro_monitor'
TBLPROPERTIES (
    'parquet.compression' = 'snappy',
    'comment' = '商品宏观监控看板数据，用于展示商品访问、收藏、加购、交易全链路指标'
);
"""
spark.sql(create_table_sql)
print("ads_product_macro_monitor表结构创建完成")

# --------------------------
# 插入日维度数据
# --------------------------
insert_data_sql = f"""
INSERT INTO gd3.ads_product_macro_monitor
SELECT
    a.date AS statistical_time,
    a.product_id,
    b.product_name,
    a.category_id,
    b.category_name,
    a.visitor_count,
    a.view_count,
    a.avg_stay_time,
    a.bounce_rate,
    a.favorite_user_count,
    CASE WHEN a.visitor_count = 0 THEN 0 ELSE a.favorite_user_count / a.visitor_count END AS visit_to_collect_rate,
    a.add_to_cart_quantity,
    a.add_to_cart_user_count,
    CASE WHEN a.visitor_count = 0 THEN 0 ELSE a.add_to_cart_user_count / a.visitor_count END AS visit_to_add_to_cart_rate,
    a.order_user_count,
    a.order_quantity,
    a.order_amount,
    CASE WHEN a.visitor_count = 0 THEN 0 ELSE a.order_user_count / a.visitor_count END AS order_conversion_rate,
    a.pay_user_count,
    a.pay_quantity,
    a.pay_amount,
    CASE WHEN a.visitor_count = 0 THEN 0 ELSE a.pay_user_count / a.visitor_count END AS payment_conversion_rate,
    CASE WHEN a.pay_user_count = 0 THEN 0 ELSE a.pay_amount / a.pay_user_count END AS avg_order_value,
    CASE WHEN a.visitor_count = 0 THEN 0 ELSE a.pay_amount / a.visitor_count END AS avg_visitor_value
FROM gd3.dws_daily_summary_of_products a
LEFT JOIN gd3.ods_product b 
    ON a.product_id = b.product_id
WHERE a.date = '{bizdate}';
"""
spark.sql(insert_data_sql)
print(f"ads_product_macro_monitor表 {bizdate} 日维度数据插入完成")

spark.stop()