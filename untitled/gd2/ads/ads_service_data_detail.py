from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, date_sub, round, when
import logging
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def init_spark_session():
    """初始化SparkSession，统一资源配置"""
    logger.info("开始初始化SparkSession...")
    spark = SparkSession.builder \
        .appName("create_ads_service_data_detail") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1")  \
        .config("spark.default.parallelism", "1") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.driver.memory", "6g")  \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.adaptive.enabled", "false") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    logger.info("SparkSession初始化完成")
    return spark


def check_dependencies(spark):
    """检查依赖的数据库和表是否存在"""
    logger.info("检查依赖资源...")

    # 检查并创建数据库
    db_exists = spark.sql("show databases like 'gd2'").count() > 0
    if not db_exists:
        logger.info("数据库gd2不存在，创建数据库...")
        spark.sql("CREATE DATABASE IF NOT EXISTS gd2")
    spark.sql("USE gd2")  # 切换数据库

    # 检查源表
    try:
        table_exists = spark.sql(
            "show tables in gd2 like 'dws_service_performance_summary'"
        ).count() > 0
        if not table_exists:
            raise Exception("源表gd2.dws_service_performance_summary不存在")
        logger.info("依赖检查通过")
    except Exception as e:
        logger.error(f"依赖检查失败: {str(e)}", exc_info=True)
        raise


def create_ads_service_data_detail(spark):
    """创建客服数据明细表主逻辑"""
    try:
        # 读取源表并强制单分区
        logger.info("读取DWS层数据并强制单分区...")
        dws_service_df = spark.table("gd2.dws_service_performance_summary") \
            .filter(col("dt") >= F.date_sub(F.current_date(), 30)) \
            .repartition(1)  # 强制单分区处理

        # 数据量检查
        total_count = dws_service_df.count()
        logger.info(f"DWS层表数据量: {total_count} 条（单分区）")
        if total_count == 0:
            logger.warning("源表数据为空，将生成空结果表")
            return

        # 1. 按日统计数据（使用英文别名统一结构）
        logger.info("开始处理日数据...")
        daily_df = dws_service_df.select(
            col("service_id"),
            F.lit("day").alias("time_type"),
            col("dt").alias("start_date"),
            col("dt").alias("end_date"),
            col("total_send_count").alias("send_count"),
            col("total_send_amount").alias("send_amount"),
            col("total_verify_count").alias("pay_count"),
            col("total_verify_amount").alias("pay_amount"),
            # 处理空值和除零异常
            round(
                when(col("total_send_count") == 0, 0)
                .otherwise(col("conversion_rate")),
                4
            ).alias("conversion_rate")
        ).cache()
        logger.info(f"日数据处理完成，数据量: {daily_df.count()}")

        # 2. 按7天滚动窗口统计
        logger.info("开始处理7天窗口数据...")
        window_7days = Window \
            .partitionBy("service_id") \
            .orderBy("dt") \
            .rowsBetween(-6, 0)  # 包含当前行及前6行

        weekly_df = dws_service_df.select(
            col("service_id"),
            F.lit("7days").alias("time_type"),
            date_sub(col("dt"), 6).alias("start_date"),
            col("dt").alias("end_date"),
            sum("total_send_count").over(window_7days).alias("send_count"),
            sum("total_send_amount").over(window_7days).alias("send_amount"),
            sum("total_verify_count").over(window_7days).alias("pay_count"),
            sum("total_verify_amount").over(window_7days).alias("pay_amount"),
            # 计算7天核销转化率（处理除零）
            round(
                when(sum("total_send_count").over(window_7days) == 0, 0)
                .otherwise(sum("total_verify_count").over(window_7days) /
                           sum("total_send_count").over(window_7days)),
                4
            ).alias("conversion_rate")
        ).cache()
        logger.info(f"7天窗口数据处理完成，数据量: {weekly_df.count()}")

        # 3. 按30天滚动窗口统计
        logger.info("开始处理30天窗口数据...")
        window_30days = Window \
            .partitionBy("service_id") \
            .orderBy("dt") \
            .rowsBetween(-29, 0)  # 包含当前行及前29行

        monthly_df = dws_service_df.select(
            col("service_id"),
            F.lit("30days").alias("time_type"),
            date_sub(col("dt"), 29).alias("start_date"),
            col("dt").alias("end_date"),
            sum("total_send_count").over(window_30days).alias("send_count"),
            sum("total_send_amount").over(window_30days).alias("send_amount"),
            sum("total_verify_count").over(window_30days).alias("pay_count"),
            sum("total_verify_amount").over(window_30days).alias("pay_amount"),
            # 计算30天核销转化率（处理除零）
            round(
                when(sum("total_send_count").over(window_30days) == 0, 0)
                .otherwise(sum("total_verify_count").over(window_30days) /
                           sum("total_send_count").over(window_30days)),
                4
            ).alias("conversion_rate")
        ).cache()
        logger.info(f"30天窗口数据处理完成，数据量: {monthly_df.count()}")

        # 合并数据并强制单分区
        logger.info("合并所有时间维度数据...")
        union_df = daily_df.unionAll(weekly_df).unionAll(monthly_df).repartition(1)
        logger.info(f"合并后总数据量: {union_df.count()}")

        # 释放缓存
        daily_df.unpersist()
        weekly_df.unpersist()
        monthly_df.unpersist()

        # 缓存结果数据
        union_df.cache()
        logger.info("结果数据已缓存到内存")

        # 写入ADS层表
        logger.info("开始写入ADS层表...")
        start_time = time.time()

        # 若结果为空，显式创建表结构
        if union_df.count() == 0:
            logger.warning("结果数据为空，创建空表...")
            spark.sql("""
                CREATE TABLE IF NOT EXISTS gd2.ads_service_data_detail (
                    service_id STRING COMMENT '服务ID',
                    time_type STRING COMMENT '时间类型(day-日,7days-7天,30days-30天)',
                    start_date STRING COMMENT '统计开始日期',
                    end_date STRING COMMENT '统计结束日期',
                    send_count BIGINT COMMENT '发送次数',
                    send_amount DOUBLE COMMENT '发送金额',
                    pay_count BIGINT COMMENT '支付次数',
                    pay_amount DOUBLE COMMENT '支付金额',
                    conversion_rate DOUBLE COMMENT '核销转化率'
                )
                STORED AS ORC
                COMMENT 'ADS层-客服数据明细表'
            """)
        else:
            # 写入数据
            union_df.write \
                .mode("overwrite") \
                .format("orc") \
                .option("compression", "snappy") \
                .saveAsTable("gd2.ads_service_data_detail")

        write_duration = time.time() - start_time
        logger.info(f"数据写入完成，耗时: {write_duration:.2f}秒")

        # 验证写入结果
        logger.info("验证表写入结果...")
        written_count = spark.table("gd2.ads_service_data_detail").count()
        logger.info(f"ADS层表写入成功，共 {written_count} 条数据")

        union_df.unpersist()

    except Exception as e:
        logger.error(f"处理过程出错: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        spark = init_spark_session()
        check_dependencies(spark)
        create_ads_service_data_detail(spark)
    except Exception as e:
        logger.critical(f"程序执行失败: {str(e)}", exc_info=True)
        exit(1)
    finally:
        if 'spark' in locals() and spark is not None:
            logger.info("关闭SparkSession...")
            spark.stop()
        logger.info("程序执行结束")