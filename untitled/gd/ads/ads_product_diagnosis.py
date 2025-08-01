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


def execute_hive_table_creation(tableName):
    spark = get_spark_session()

    create_value_table_sql = """
  CREATE TABLE IF NOT EXISTS gd.ads_product_diagnosis (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    total_score DOUBLE COMMENT '综合评分（0-100）',
    score_level STRING COMMENT '评分等级（A/B/C/D）',
    traffic_rank INT COMMENT '流量排名',
    conversion_rank INT COMMENT '转化率排名',
    content_rank INT COMMENT '内容互动排名',
    new_user_rank INT COMMENT '拉新排名',
    service_rank INT COMMENT '服务质量排名',
    competitiveness_index DOUBLE COMMENT '竞争力指数（0-100）',
    stat_date DATE COMMENT '统计日期'
)
PARTITIONED BY (stat_date)
STORED AS PARQUET
COMMENT '商品诊断看板最终指标'
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd/ads_product_diagnosis';
    """

    print(f"[INFO] 开始创建表: {tableName}")
    # 执行多条SQL语句
    for sql in create_value_table_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {tableName} 创建成功")
if __name__ == "__main__":
    table_name = 'ads_product_diagnosis'
    # 设置目标分区日期
    execute_hive_table_creation(table_name)

