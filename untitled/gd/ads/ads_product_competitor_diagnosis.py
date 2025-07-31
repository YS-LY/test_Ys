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

    create_competitor_table_sql = """
    CREATE TABLE IF NOT EXISTS gd.ads_product_competitor_diagnosis (
        product_id STRING COMMENT '本品ID（主键）',
        similar_category_id STRING COMMENT '相似商品品类ID（圈定对比范围）',
        dimension STRING COMMENT '对比维度（traffic_acquisition/转化/内容营销/拉新/服务质量）',
        own_value DECIMAL(5,2) COMMENT '本品在该维度的得分',
        similar_avg_value DECIMAL(5,2) COMMENT '相似商品在该维度的平均得分',
        difference DECIMAL(5,2) COMMENT '差异值（本品-相似商品均值，负值表示低于平均）',
        sub_dimension_weights MAP<STRING, DECIMAL(3,2)> COMMENT '细分维度权重（如流量获取：访客数0.4、浏览量0.6）',
        stat_date STRING COMMENT '统计日期（分区字段）'
    )
    COMMENT '单品竞争力诊断ADS层表，任务编号：大数据-电商数仓-07-商品主题商品诊断看板'
    STORED AS orc
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd/ads_product_competitor_diagnosis';
    """

    print(f"[INFO] 开始创建表: {tableName}")
    # 执行多条SQL语句
    for sql in create_competitor_table_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {tableName} 创建成功")
if __name__ == "__main__":
    table_name = 'ads_product_competitor_diagnosis'
    # 设置目标分区日期
    execute_hive_table_creation(table_name)








