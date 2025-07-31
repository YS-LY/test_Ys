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

    create_competitor_table_sql = """
   CREATE TABLE IF NOT EXISTS ods_competitor_behavior (
    product_id STRING COMMENT '本品ID',
    competitor_product_id STRING COMMENT '相似商品ID',
    competitor_traffic_cnt BIGINT COMMENT '相似商品访问量',
    competitor_conversion_rate DOUBLE COMMENT '相似商品转化率',
    competitor_content_interact_cnt BIGINT COMMENT '相似商品内容互动量',
    competitor_new_user_cnt BIGINT COMMENT '相似商品拉新数',
    competitor_service_rating DOUBLE COMMENT '相似商品服务评分',
    competitor_sales_cnt BIGINT COMMENT '相似商品销量',
    stat_date DATE COMMENT '统计日期',
    etl_time TIMESTAMP COMMENT '数据抽取时间'
) COMMENT '相似商品行为原始数据表，用于单品与相似商品的多维度对比'
STORED AS PARQUET
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd/ods_competitor_behavior';
    """

    print(f"[INFO] 开始创建表: {tableName}")
    # 执行多条SQL语句
    for sql in create_competitor_table_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {tableName} 创建成功")
if __name__ == "__main__":
    table_name = 'ods_competitor_behavior'
    # 设置目标分区日期
    execute_hive_table_creation(table_name)