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


def process_city_stats(spark):
    # 配置参数
    db_name, target_table = "tms", "ads_city_stats"
    table_path = "hdfs://cdh01:8020/warehouse/tms/ads/ads_city_stats"
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
            StructField("recent_days", LongType(), True),
            StructField("city_id", LongType(), True),
            StructField("city_name", StringType(), True),
            StructField("order_count", LongType(), True),
            StructField("order_amount", DecimalType(16, 2), True),
            StructField("trans_finish_count", LongType(), True),
            StructField("trans_finish_distance", DecimalType(16, 2), True),
            StructField("trans_finish_dur_sec", LongType(), True),
            StructField("avg_trans_finish_distance", DecimalType(16, 2), True),
            StructField("avg_trans_finish_dur_sec", LongType(), True)
        ]))

    # 定义表结构与读取函数
    def safe_read(table_name, schema):
        try:
            return spark.table(table_name)
        except:
            print(f"表 {table_name} 不存在，使用空表")
            return spark.createDataFrame([], schema)

    # 处理最近1天的订单数据
    city_order_1d = safe_read("dws_trade_org_cargo_type_order_1d", StructType([
        StructField("dt", StringType(), True),
        StructField("city_id", LongType(), True),
        StructField("city_name", StringType(), True),
        StructField("order_count", LongType(), True),
        StructField("order_amount", DecimalType(16, 2), True)
    ])).filter(F.col("dt") == "2025-07-11") \
        .groupBy("city_id", "city_name") \
        .agg(
        F.lit(current_dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )

    # 处理最近1天的运输数据
    # 第一步：获取运输原始数据
    trans_origin = safe_read("dws_trans_org_truck_model_type_trans_finish_1d", StructType([
        StructField("org_id", LongType(), True),
        StructField("trans_finish_count", LongType(), True),
        StructField("trans_finish_distance", DecimalType(16, 2), True),
        StructField("trans_finish_dur_sec", LongType(), True)
    ])).filter(F.col("dt") == current_dt)

    # 第二步：获取机构信息
    organ = safe_read("dim_organ_full", StructType([
        StructField("id", LongType(), True),
        StructField("org_level", LongType(), True),
        StructField("region_id", LongType(), True)
    ])).filter(F.col("dt") == "2023-01-10") \
        .select(
        F.col("id").alias("org_id"),
        F.col("org_level"),
        F.col("region_id")
    )

    # 第三步：获取区域信息
    city_for_level1 = safe_read("dim_region_full", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("parent_id", LongType(), True)
    ])).filter(F.col("dt") == "20200623") \
        .select(
        F.col("id").alias("level1_id"),
        F.col("name").alias("level1_name"),
        F.col("parent_id").alias("level1_parent_id")
    )

    city_for_level2 = safe_read("dim_region_full", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True)
    ])).filter(F.col("dt") == "20200623") \
        .select(
        F.col("id").alias("level2_id"),
        F.col("name").alias("level2_name")
    )

    # 第四步：关联数据并处理城市ID和名称
    trans_1d = trans_origin.join(organ, on="org_id", how="left") \
        .join(city_for_level1, F.col("region_id") == F.col("level1_id"), how="left") \
        .join(city_for_level2, F.col("level1_parent_id") == F.col("level2_id"), how="left") \
        .withColumn("city_id",
                    F.when(F.col("org_level") == 1, F.col("level1_id"))
                    .otherwise(F.col("level2_id"))) \
        .withColumn("city_name",
                    F.when(F.col("org_level") == 1, F.col("level1_name"))
                    .otherwise(F.col("level2_name"))) \
        .select(
        "city_id", "city_name",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec"
    )

    # 第五步：聚合运输数据
    city_trans_1d = trans_1d.groupBy("city_id", "city_name") \
        .agg(
        F.lit(current_dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    )

    # 合并最近1天的订单和运输数据
    combined_1d = city_order_1d.join(
        city_trans_1d,
        on=["dt", "recent_days", "city_id", "city_name"],
        how="full_outer"
    ).select(
        F.coalesce(city_order_1d["dt"], city_trans_1d["dt"]).alias("dt"),
        F.coalesce(city_order_1d["recent_days"], city_trans_1d["recent_days"]).alias("recent_days"),
        F.coalesce(city_order_1d["city_id"], city_trans_1d["city_id"]).alias("city_id"),
        F.coalesce(city_order_1d["city_name"], city_trans_1d["city_name"]).alias("city_name"),
        "order_count", "order_amount",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    )

    # 处理最近n天的订单数据
    city_order_nd = safe_read("dws_trade_org_cargo_type_order_nd", StructType([
        StructField("dt", StringType(), True),
        StructField("recent_days", LongType(), True),
        StructField("city_id", LongType(), True),
        StructField("city_name", StringType(), True),
        StructField("order_count", LongType(), True),
        StructField("order_amount", DecimalType(16, 2), True)
    ])).filter(F.col("dt") == current_dt) \
        .groupBy("city_id", "city_name", "recent_days") \
        .agg(
        F.lit(current_dt).alias("dt"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )

    # 处理最近n天的运输数据
    city_trans_nd = safe_read("dws_trans_shift_trans_finish_nd", StructType([
        StructField("dt", StringType(), True),
        StructField("recent_days", LongType(), True),
        StructField("city_id", LongType(), True),
        StructField("city_name", StringType(), True),
        StructField("trans_finish_count", LongType(), True),
        StructField("trans_finish_distance", DecimalType(16, 2), True),
        StructField("trans_finish_dur_sec", LongType(), True)
    ])).filter(F.col("dt") == current_dt) \
        .groupBy("city_id", "city_name", "recent_days") \
        .agg(
        F.lit(current_dt).alias("dt"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    )

    # 合并最近n天的订单和运输数据
    combined_nd = city_order_nd.join(
        city_trans_nd,
        on=["dt", "recent_days", "city_id", "city_name"],
        how="full_outer"
    ).select(
        F.coalesce(city_order_nd["dt"], city_trans_nd["dt"]).alias("dt"),
        F.coalesce(city_order_nd["recent_days"], city_trans_nd["recent_days"]).alias("recent_days"),
        F.coalesce(city_order_nd["city_id"], city_trans_nd["city_id"]).alias("city_id"),
        F.coalesce(city_order_nd["city_name"], city_trans_nd["city_name"]).alias("city_name"),
        "order_count", "order_amount",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    )

    # 合并所有数据
    final_df = existing_df.unionByName(combined_1d).unionByName(combined_nd)

    # 写入临时表
    final_df.write.mode("overwrite").saveAsTable(temp_table)
    print(f"数据已写入临时表: {temp_table}")

    # 重建目标表
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    spark.sql(f"""
    CREATE EXTERNAL TABLE {target_table}(
        `dt` string COMMENT '统计日期',
        `recent_days` bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
        `city_id` bigint COMMENT '城市ID',
        `city_name` string COMMENT '城市名称',
        `order_count` bigint COMMENT '下单数',
        `order_amount` decimal(16,2) COMMENT '下单金额',
        `trans_finish_count` bigint COMMENT '完成运输次数',
        `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
        `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
        `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
        `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
    ) COMMENT '城市分析'
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
        process_city_stats(spark)
    finally:
        spark.stop()
