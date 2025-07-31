from pyspark.sql import SparkSession

def recreate_table_with_parquet():
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
      CREATE TABLE ods_product_behavior (
        product_id STRING COMMENT '商品唯一标识',
        product_name STRING COMMENT '商品名称',
        traffic_cnt BIGINT COMMENT '访问量（流量获取维度）',
        click_cnt BIGINT COMMENT '点击量（流量获取维度）',
        conversion_rate DOUBLE COMMENT '转化率（流量转化维度）',
        content_interact_cnt BIGINT COMMENT '内容互动量（内容营销维度）',
        new_user_cnt BIGINT COMMENT '拉新用户数（客户拉新维度）',
        service_rating DOUBLE COMMENT '服务评分（服务质量维度）',
        sales_amount DECIMAL(16,2) COMMENT '销售额（金额/价格维度）',
        sales_cnt BIGINT COMMENT '销量（销量维度）',
        visitor_cnt BIGINT COMMENT '访客数（访客维度）',
        stat_date DATE COMMENT '统计日期',
        etl_time TIMESTAMP COMMENT '数据抽取时间'
    ) COMMENT '商品行为原始数据表，用于全品价值评估和单品竞争力诊断'
    STORED AS PARQUET  -- 强制PARQUET格式，与文档要求一致
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gd/ods_product_behavior';
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
if __name__ == "__main__":
    recreate_table_with_parquet()