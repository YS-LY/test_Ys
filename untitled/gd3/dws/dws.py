from pyspark.sql import SparkSession
import sys

# 初始化SparkSession（兼容文件中Parquet存储要求）
spark = SparkSession.builder \
    .appName("create_dws_product_daily_summary") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.local.dir", "D:/spark_temp") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.parquet.writeLegacyFormat", "true")  \
    .config("spark.sql.parquet.enable.dictionary", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 获取业务日期参数（符合文件中"日/周/月"时间维度要求）
# if len(sys.argv) > 1:
#     bizdate = sys.argv[1]
# else:
#     from datetime import datetime, timedelta
#
#     bizdate = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# print(f"执行业务日期: {bizdate}")
bizdate='2025-07-13'


# 切换数据库
spark.sql("CREATE DATABASE IF NOT EXISTS gd3")
spark.sql("USE gd3")

# --------------------------
# 1. 依赖DWD表创建（严格匹配文件指标定义）
# --------------------------
# 1.1 商品访问明细DWD表（对应文件"商品访客数/浏览量"指标源）
spark.sql("""
CREATE TABLE IF NOT EXISTS gd3.dwd_Product_access_details (
    visit_id STRING COMMENT '访问唯一标识（用于统计浏览量）',
    user_id STRING COMMENT '用户ID（用于统计访客数）',
    product_id STRING COMMENT '商品ID',
    category_id STRING COMMENT '叶子类目ID（文件中"细分至叶子类目"要求）',
    visit_time TIMESTAMP COMMENT '访问时间',
    terminal STRING COMMENT '终端类型(PC/无线)',
    stay_time INT COMMENT '停留时长(秒)（文件中"平均停留时长"计算源）',
    is_click INT COMMENT '是否点击（1=是，0=否，用于计算跳出率）',
    date STRING COMMENT '统计日期(yyyy-MM-dd)'
)
PARTITIONED BY (date)
STORED AS PARQUET
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd3/dwd_Product_access_details';
""")

# 1.2 商品收藏加购明细DWD表（对应文件"收藏/加购"指标源）
spark.sql("""
CREATE TABLE IF NOT EXISTS gd3.dwd_Product_collection_and_purchase_details (
    record_id STRING COMMENT '记录唯一标识',
    user_id STRING COMMENT '用户ID',
    product_id STRING COMMENT '商品ID',
    category_id STRING COMMENT '叶子类目ID',
    op_type STRING COMMENT '操作类型(收藏/加购)（文件中"收藏人数/加购件数"区分依据）',
    op_time TIMESTAMP COMMENT '操作时间',
    add_cart_count INT COMMENT '加购件数（文件中"加购件数"直接取值）',
    terminal STRING COMMENT '终端类型',
    date STRING COMMENT '统计日期(yyyy-MM-dd)'
)
PARTITIONED BY (date)
STORED AS PARQUET
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd3/dwd_Product_collection_and_purchase_details';
""")

# 1.3 商品交易明细DWD表（对应文件"下单/支付"指标源）
spark.sql("""
CREATE TABLE IF NOT EXISTS gd3.dwd_details_of_commodity_transactions (
    order_id STRING COMMENT '订单ID',
    user_id STRING COMMENT '用户ID（文件中"下单买家数/支付买家数"去重依据）',
    product_id STRING COMMENT '商品ID',
    category_id STRING COMMENT '叶子类目ID',
    product_price DECIMAL(10,2) COMMENT '商品单价',
    buy_count INT COMMENT '拍下件数（文件中"下单件数"直接取值）',
    buy_amount DECIMAL(10,2) COMMENT '拍下金额（文件中"下单金额"直接取值）',
    pay_status STRING COMMENT '支付状态(未支付/已支付)（文件中"支付指标"区分依据）',
    pay_time TIMESTAMP COMMENT '支付时间',
    pay_amount DECIMAL(10,2) COMMENT '支付金额（文件中"支付金额"直接取值）',
    terminal STRING COMMENT '终端类型',
    date STRING COMMENT '统计日期(yyyy-MM-dd)'
)
PARTITIONED BY (date)
STORED AS PARQUET
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd3/dwd_details_of_commodity_transactions';
""")
print("依赖的DWD层表创建/验证完成")

# --------------------------
# 2. DWS层商品每日汇总表（严格对齐文件指标体系）
# --------------------------
create_table_sql = """
CREATE TABLE IF NOT EXISTS gd3.dws_daily_summary_of_products (
    product_id STRING COMMENT '商品ID',
    category_id STRING COMMENT '叶子类目ID（文件中"区间分析细分至叶子类目"要求）',
    visitor_count INT COMMENT '商品访客数(去重)（文件定义：访问详情页的去重人数）',
    view_count INT COMMENT '商品浏览量（文件定义：详情页被浏览次数累加）',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)（文件定义：总停留时长/访客数）',
    bounce_rate DECIMAL(10,4) COMMENT '商品详情页跳出率（文件定义：无点击人数/访客数）',
    favorite_user_count INT COMMENT '商品收藏人数（文件定义：新增收藏去重人数）',
    add_to_cart_quantity INT COMMENT '商品加购件数（文件定义：新增加购件数总和）',
    add_to_cart_user_count INT COMMENT '商品加购人数（文件定义：新增加购去重人数）',
    order_user_count INT COMMENT '下单买家数（文件定义：拍下的去重买家人数）',
    order_quantity INT COMMENT '下单件数（文件定义：累计拍下件数）',
    order_amount DECIMAL(10,2) COMMENT '下单金额（文件定义：累计拍下金额）',
    pay_user_count INT COMMENT '支付买家数（文件定义：完成支付的去重买家人数）',
    pay_quantity INT COMMENT '支付件数（文件定义：累计支付件数）',
    pay_amount DECIMAL(10,2) COMMENT '支付金额（文件定义：累计支付金额，未剔除退款）'
)
PARTITIONED BY (date STRING COMMENT '统计日期(yyyy-MM-dd)（文件"日维度"统计要求）')
STORED AS PARQUET
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd3/dws_daily_summary_of_products'
TBLPROPERTIES (
    'parquet.compression' = 'snappy',
    'comment' = '商品每日汇总数据（文件"商品宏观监控看板"核心数据）'
);
"""
spark.sql(create_table_sql)
print("dws_daily_summary_of_products表结构创建完成")

# --------------------------
# 3. 插入数据（指标计算严格遵循文件定义）
# --------------------------
insert_data_sql = f"""
INSERT OVERWRITE TABLE gd3.dws_daily_summary_of_products PARTITION (date='{bizdate}')
SELECT
    product_id,
    category_id,
    COUNT(DISTINCT user_id) AS visitor_count,  -- 严格匹配"去重人数"定义
    COUNT(visit_id) AS view_count,  -- 严格匹配"浏览次数累加"定义
    AVG(stay_time) AS avg_stay_time,  -- 严格匹配"总时长/访客数"公式
    -- 跳出率计算完全符合文件"无点击人数/访客数"定义
    CASE WHEN COUNT(DISTINCT user_id) = 0 THEN 0 
         ELSE SUM(CASE WHEN is_click = 0 THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) 
    END AS bounce_rate,
    SUM(favorite_user_count) AS favorite_user_count,
    SUM(add_to_cart_quantity) AS add_to_cart_quantity,
    SUM(add_to_cart_user_count) AS add_to_cart_user_count,
    SUM(order_user_count) AS order_user_count,
    SUM(order_quantity) AS order_quantity,
    SUM(order_amount) AS order_amount,
    SUM(pay_user_count) AS pay_user_count,
    SUM(pay_quantity) AS pay_quantity,
    SUM(pay_amount) AS pay_amount
FROM (
    -- 访问指标（仅贡献访问相关字段，其他指标置0）
    SELECT
        product_id,
        category_id,
        user_id,
        visit_id,
        stay_time,
        is_click,
        0 AS favorite_user_count,
        0 AS add_to_cart_quantity,
        0 AS add_to_cart_user_count,
        0 AS order_user_count,
        0 AS order_quantity,
        0 AS order_amount,
        0 AS pay_user_count,
        0 AS pay_quantity,
        0 AS pay_amount
    FROM gd3.dwd_Product_access_details
    WHERE date = '{bizdate}'

    UNION ALL

    -- 收藏加购指标（严格区分"收藏/加购"操作类型）
    SELECT
        product_id,
        category_id,
        user_id,
        '' AS visit_id,
        0 AS stay_time,
        0 AS is_click,
        CASE WHEN op_type = '收藏' THEN 1 ELSE 0 END AS favorite_user_count,  -- 符合"新增收藏人数"定义
        CASE WHEN op_type = '加购' THEN add_cart_count ELSE 0 END AS add_to_cart_quantity,  -- 直接取用加购件数
        CASE WHEN op_type = '加购' THEN 1 ELSE 0 END AS add_to_cart_user_count,  -- 符合"新增加购人数"定义
        0 AS order_user_count,
        0 AS order_quantity,
        0 AS order_amount,
        0 AS pay_user_count,
        0 AS pay_quantity,
        0 AS pay_amount
    FROM gd3.dwd_Product_collection_and_purchase_details
    WHERE date = '{bizdate}'

    UNION ALL

    -- 交易指标（严格区分"支付状态"）
    SELECT
        product_id,
        category_id,
        user_id,
        '' AS visit_id,
        0 AS stay_time,
        0 AS is_click,
        0 AS favorite_user_count,
        0 AS add_to_cart_quantity,
        0 AS add_to_cart_user_count,
        1 AS order_user_count,  -- 符合"每个订单计1个买家"定义
        buy_count AS order_quantity,  -- 直接取用拍下件数
        buy_amount AS order_amount,  -- 直接取用拍下金额
        CASE WHEN pay_status = '已支付' THEN 1 ELSE 0 END AS pay_user_count,  -- 符合"支付买家数"定义
        CASE WHEN pay_status = '已支付' THEN buy_count ELSE 0 END AS pay_quantity,  -- 符合"支付件数"定义
        CASE WHEN pay_status = '已支付' THEN pay_amount ELSE 0 END AS pay_amount  -- 符合"支付金额"定义
    FROM gd3.dwd_details_of_commodity_transactions
    WHERE date = '{bizdate}'
) t
GROUP BY product_id, category_id;
"""
spark.sql(insert_data_sql)
print(f"dws_daily_summary_of_products表 {bizdate} 数据插入完成")

spark.stop()