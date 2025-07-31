from pyspark.sql import SparkSession
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gd")
    return spark

# -- ODS层相似商品行为表（支撑单品竞争力对比分析）
def execute_hive_table_creation(tableName):
    spark = get_spark_session()

    # 调整建表语句，添加IF NOT EXISTS避免表已存在的错误
    create_table_sql = """
  CREATE TABLE IF NOT EXISTS gd.dws_product_daily (
    product_id STRING COMMENT '商品唯一标识',
    product_name STRING COMMENT '商品名称',
    -- 流量获取维度
    traffic_cnt BIGINT COMMENT '访问量',
    click_cnt BIGINT COMMENT '点击量',
    click_rate DOUBLE COMMENT '点击率（点击量/访问量）',
    -- 流量转化维度
    conversion_rate DOUBLE COMMENT '转化率',
    -- 内容营销维度
    content_interact_cnt BIGINT COMMENT '内容互动量',
    content_interact_rate DOUBLE COMMENT '内容互动率（内容互动量/访问量）',
    -- 客户拉新维度
    new_user_cnt BIGINT COMMENT '拉新用户数',
    visitor_cnt BIGINT COMMENT '总访客数',
    new_user_rate DOUBLE COMMENT '新用户占比（拉新用户数/总访客数）',
    -- 服务质量维度
    service_rating DOUBLE COMMENT '服务评分',
    -- 销售维度（支撑全品价值评估）
    sales_amount DECIMAL(16,2) COMMENT '销售额',
    sales_cnt BIGINT COMMENT '销量',
    avg_price DECIMAL(16,2) COMMENT '平均价格（销售额/销量）'
)
PARTITIONED BY (stat_date DATE COMMENT '统计日期')
COMMENT '商品每日汇总表，支撑商品诊断看板多维度分析'
STORED AS PARQUET
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd/dws_product_daily';
    """

    print(f"[INFO] 开始创建表: {tableName}")
    # 执行多条SQL语句
    for sql in create_table_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {tableName} 创建成功")
if __name__ == "__main__":
    table_name = 'dws_product_daily'
    # 设置目标分区日期
    execute_hive_table_creation(table_name)