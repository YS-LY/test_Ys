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
    CREATE TABLE IF NOT EXISTS gd.ads_product_value_evaluation (
        product_id STRING COMMENT '商品唯一标识ID',
        product_name STRING COMMENT '商品名称',
        traffic_acquisition_score DECIMAL(5,2) COMMENT '流量获取维度评分',
        traffic_conversion_score DECIMAL(5,2) COMMENT '流量转化维度评分',
        content_marketing_score DECIMAL(5,2) COMMENT '内容营销维度评分',
        customer_acquisition_score DECIMAL(5,2) COMMENT '客户拉新维度评分',
        service_quality_score DECIMAL(5,2) COMMENT '服务质量维度评分',
        total_score DECIMAL(5,2) COMMENT '综合评分（基于五维度计算）',
        level STRING COMMENT '评分等级（A/B/C/D级，85+→A级，70-85→B级，50-70→C级，<50→D级）',
        amount DECIMAL(12,2) COMMENT '销售额（金额维度，支撑评分-金额分析）',
        price DECIMAL(8,2) COMMENT '单价（价格维度，支撑价格-销量分析）',
        sales_num INT COMMENT '销量（支撑价格-销量/访客-销量分析）',
        visitor_num INT COMMENT '访客数（支撑访客-销量分析）',
        visitor_sales_ratio DECIMAL(5,2) COMMENT '访客-销量比（销量/访客数）'
    )
    PARTITIONED BY (stat_date STRING COMMENT '统计日期，按天分区')
    STORED AS PARQUET
    COMMENT '全品价值评估ADS层表，任务编号：大数据-电商数仓-07-商品主题商品诊断看板，用于全面了解商品分布，聚焦核心商品🔶1-21🔶'
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd/ads_product_value_evaluation';
    """

    print(f"[INFO] 开始创建表: {tableName}")
    # 执行多条SQL语句
    for sql in create_value_table_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {tableName} 创建成功")
if __name__ == "__main__":
    table_name = 'ads_product_value_evaluation'
    # 设置目标分区日期
    execute_hive_table_creation(table_name)

