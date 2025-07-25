from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType
import os


def get_spark_session():
    """创建SparkSession并配置必要参数"""
    return (SparkSession.builder
            .appName("HiveETL")
            .config("hive.metastore.uris", "thrift://cdh01:9083")
            .config("spark.sql.hive.convertMetastoreOrc", "true")
            .config("fs.defaultFS", "hdfs://cdh01:8020")
            .config("dfs.client.use.datanode.hostname", "true")
            .config("spark.python.worker.reuse", "false")
            .config("spark.python.worker.connectionTimeout", "60000")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.cores.max", "2")
            .enableHiveSupport()
            .getOrCreate())


def check_hdfs_path(spark, hdfs_path):
    """检查并创建HDFS路径"""
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", "hdfs://cdh01:8020")
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if not fs.exists(path):
            fs.mkdirs(path)
            print(f"已创建HDFS路径: {hdfs_path}")
        else:
            print(f"HDFS路径已存在: {hdfs_path}")
    except Exception as e:
        print(f"HDFS操作失败: {str(e)}")
        try:
            spark.sql(f"dfs -mkdir -p {hdfs_path}")
            print(f"通过Hive创建路径成功: {hdfs_path}")
        except:
            print(f"请手动创建HDFS路径: {hdfs_path}")
            raise


def process_trans_order_stats_td(spark):
    # 配置参数
    db_name, target_table = "tms", "ads_trans_order_stats_td"
    table_path = "hdfs://cdh01:8020/warehouse/tms/ads/ads_trans_order_stats_td"
    current_dt = "20250712"
    temp_table = f"temp_{target_table}_{current_dt}"

    # 初始化
    spark.sql(f"USE {db_name}")
    check_hdfs_path(spark, table_path)

    # 读取历史数据
    try:
        existing_df = spark.table(target_table).cache()
        existing_df.count()
        print(f"已读取历史数据: {target_table}")
    except:
        existing_df = spark.createDataFrame([], StructType([
            StructField("dt", StringType(), True),
            StructField("bounding_order_count", LongType(), True),
            StructField("bounding_order_amount", DecimalType(16, 2), True)
        ]))

    # 定义表结构与读取函数
    def safe_read(table_name, schema):
        try:
            return spark.table(table_name)
        except:
            print(f"表 {table_name} 不存在，使用空表")
            return spark.createDataFrame([], schema)

    # 读取源数据并处理
    dispatch_td = safe_read("dws_trans_dispatch_td", StructType([
        StructField("dt", StringType(), True),
        StructField("order_count", LongType(), True),
        StructField("order_amount", DecimalType(16, 2), True)
    ])).filter(F.col("dt") == current_dt)

    bound_finish_td = safe_read("dws_trans_bound_finish_td", StructType([
        StructField("dt", StringType(), True),
        StructField("order_count", LongType(), True),
        StructField("order_amount", DecimalType(16, 2), True)
    ])).filter(F.col("dt") == current_dt) \
        .withColumn("order_count", F.col("order_count") * -1) \
        .withColumn("order_amount", F.col("order_amount") * -1)

    # 合并源数据并聚合
    new_data = dispatch_td.unionByName(bound_finish_td) \
        .groupBy("dt") \
        .agg(
        F.sum("order_count").alias("bounding_order_count"),
        F.sum("order_amount").alias("bounding_order_amount")
    )

    # 合并历史数据与新数据
    final_df = existing_df.unionByName(new_data)

    # 写入临时表
    final_df.write.mode("overwrite").saveAsTable(temp_table)
    print(f"数据已写入临时表: {temp_table}")

    # 重建目标表
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    spark.sql(f"""
    CREATE EXTERNAL TABLE {target_table}(
        `dt` string COMMENT '统计日期',
        `bounding_order_count` bigint COMMENT '运输中运单总数',
        `bounding_order_amount` decimal(16,2) COMMENT '运输中运单金额'
    ) COMMENT '历史至今运单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '{table_path}'
    """)

    # 迁移数据并清理
    spark.table(temp_table).write.mode("overwrite").option("sep", "\t").saveAsTable(target_table)
    spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
    print(f"数据处理完成，已写入 {target_table}")

    # 验证结果
    spark.table(target_table).show(truncate=False)


if __name__ == "__main__":
    os.environ["HADOOP_HOME"] = "C:/hadoop"
    os.environ["PATH"] += ";" + os.path.join(os.environ["HADOOP_HOME"], "bin")
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        process_trans_order_stats_td(spark)
    finally:
        spark.stop()
