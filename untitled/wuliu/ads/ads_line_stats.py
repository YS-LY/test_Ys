from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ByteType, LongType, DecimalType
import os


def get_spark_session():
    """创建SparkSession并配置必要参数"""
    return (SparkSession.builder
            .appName("HiveETL")
            .config("hive.metastore.uris", "thrift://cdh01:9083")
            .config("spark.sql.hive.convertMetastoreOrc", "true")
            .config("fs.defaultFS", "hdfs://cdh01:8020")
            .config("dfs.client.use.datanode.hostname", "true")
            # 关键配置：增加Python工作进程稳定性
            .config("spark.python.worker.reuse", "false")  # 禁用工作进程重用
            .config("spark.python.worker.connectionTimeout", "300000")  # 延长到5分钟
            .config("spark.python.worker.memory", "2g")  # 增加Python工作进程内存
            .config("spark.driver.maxResultSize", "4g")  # 增加驱动程序最大结果大小
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.network.timeout", "1200s")  # 网络超时增加到20分钟
            .config("spark.driver.memory", "8g")  # 进一步增加内存
            .config("spark.executor.memory", "8g")
            .config("spark.cores.max", "1")  # 减少核心数，降低并发压力
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


def process_line_stats(spark):
    # 配置参数
    db_name, target_table = "tms", "ads_line_stats"
    table_path = "hdfs://cdh01:8020/warehouse/tms/ads/ads_line_stats"
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
            StructField("recent_days", ByteType(), True),
            StructField("line_id", LongType(), True),
            StructField("line_name", StringType(), True),
            StructField("trans_finish_count", LongType(), True),
            StructField("trans_finish_distance", DecimalType(16, 2), True),
            StructField("trans_finish_dur_sec", LongType(), True),
            StructField("trans_finish_order_count", LongType(), True)
        ]))

    # 定义表结构与读取函数
    def safe_read(table_name, schema):
        try:
            return spark.table(table_name)
        except:
            print(f"表 {table_name} 不存在，使用空表")
            return spark.createDataFrame([], schema)

    # 读取源数据并聚合
    line_nd = safe_read("dws_trans_shift_trans_finish_nd", StructType([
        StructField("dt", StringType(), True),
        StructField("recent_days", ByteType(), True),
        StructField("line_id", LongType(), True),
        StructField("line_name", StringType(), True),
        StructField("trans_finish_count", LongType(), True),
        StructField("trans_finish_distance", DecimalType(16, 2), True),
        StructField("trans_finish_dur_sec", LongType(), True),
        StructField("trans_finish_order_count", LongType(), True)
    ])).filter(F.col("dt") == current_dt) \
        .groupBy("line_id", "line_name", "recent_days") \
        .agg(
        F.lit(current_dt).alias("dt"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        F.sum("trans_finish_order_count").alias("trans_finish_order_count")
    ) \
        .select("dt", "recent_days", "line_id", "line_name",
                "trans_finish_count", "trans_finish_distance",
                "trans_finish_dur_sec", "trans_finish_order_count")

    # 合并历史数据与新数据（对应SQL中的UNION）
    final_df = existing_df.unionByName(line_nd)

    # 写入临时表
    final_df.write.mode("overwrite").saveAsTable(temp_table)
    print(f"数据已写入临时表: {temp_table}")

    # 重建目标表
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    spark.sql(f"""
    CREATE EXTERNAL TABLE {target_table}(
        `dt` string COMMENT '统计日期',
        `recent_days` tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
        `line_id` bigint COMMENT '线路ID',
        `line_name` string COMMENT '线路名称',
        `trans_finish_count` bigint COMMENT '完成运输次数',
        `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
        `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
        `trans_finish_order_count` bigint COMMENT '运输完成运单数'
    ) COMMENT '线路分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '{table_path}'
    """)

    # 迁移数据并清理
    spark.table(temp_table).write.mode("overwrite").option("sep", "\t").saveAsTable(target_table)
    spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
    print(f"数据处理完成，已写入 {target_table}")

    # 验证结果（对应SQL中的select * from ads_line_stats）
    spark.table(target_table).show(truncate=False)


if __name__ == "__main__":
    os.environ["HADOOP_HOME"] = "C:/hadoop"
    os.environ["PATH"] += ";" + os.path.join(os.environ["HADOOP_HOME"], "bin")
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        process_line_stats(spark)
    finally:
        spark.stop()
