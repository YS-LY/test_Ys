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
  CREATE TABLE gd.dws_product_daily (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    traffic_cnt BIGINT COMMENT '流量数',
    click_cnt BIGINT COMMENT '点击数',
    click_rate DECIMAL(5,2) COMMENT '点击率',
    conversion_rate DECIMAL(5,2) COMMENT '转化率',
    content_interact_cnt BIGINT COMMENT '内容互动数',
    content_interact_rate DECIMAL(5,2) COMMENT '内容互动率',
    new_user_cnt BIGINT COMMENT '新用户数',
    visitor_cnt BIGINT COMMENT '访客数',
    new_user_rate DECIMAL(5,2) COMMENT '新用户占比',
    service_rating DECIMAL(5,2) COMMENT '服务评分',
    sales_amount DECIMAL(12,2) COMMENT '销售额',
    sales_cnt BIGINT COMMENT '销量',
    avg_price DECIMAL(10,2) COMMENT '平均价格',
    stat_date STRING COMMENT '统计日期'
) COMMENT '商品每日汇总表（支持商品诊断看板指标）'
PARTITIONED BY (stat_date)
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