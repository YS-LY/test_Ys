from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, countDistinct, round, when, date_sub
import logging
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def init_spark_session():
    """初始化SparkSession，强制单分区单核心执行"""
    logger.info("开始初始化SparkSession...")
    spark = SparkSession.builder \
        .appName("ads_tool_effect_overview_full") \
        .master("local[1]")  \
    .config("spark.sql.shuffle.partitions", "1")  \
    .config("spark.default.parallelism", "1") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("hive.metastore.client.connect.retry.delay", "5s") \
        .config("hive.metastore.client.socket.timeout", "300s") \
        .enableHiveSupport() \
        .getOrCreate()


    spark.sparkContext.setLogLevel("INFO")
    logger.info("SparkSession初始化完成")
    return spark


def check_dependencies(spark):
    """检查依赖的数据库和表是否存在"""
    logger.info("检查依赖资源...")

    # 检查数据库
    db_exists = spark.sql("show databases like 'gd2'").count() > 0
    if not db_exists:
        logger.info("数据库gd2不存在，创建数据库...")
        spark.sql("CREATE DATABASE IF NOT EXISTS gd2")

    # 检查源表
    try:
        table_exists = spark.sql(
            "show tables in gd2 like 'dws_activity_effect_summary'"
        ).count() > 0
        if not table_exists:
            raise Exception("源表gd2.dws_activity_effect_summary不存在")
        logger.info("依赖检查通过")
    except Exception as e:
        logger.error(f"依赖检查失败: {str(e)}", exc_info=True)
        raise


def create_ads_tool_effect_overview(spark):
    """创建工具效果总览表主逻辑（全流程单分区处理）"""
    try:
        # 读取源表并强制单分区
        logger.info("读取DWS层数据并强制单分区...")
        dws_activity_df = spark.table("gd2.dws_activity_effect_summary") \
            .filter(col("dt") >= F.date_sub(F.current_date(), 30)) \
            .repartition(1)  # 强制所有数据在1个分区

        # 数据量检查
        total_count = dws_activity_df.count()
        logger.info(f"DWS层表数据量: {total_count} 条（单分区）")
        if total_count == 0:
            logger.warning("源表数据为空，将生成空结果表")
            return

        # 按日统计（单分区）
        logger.info("开始处理日数据...")
        daily_df = dws_activity_df.select(
            F.lit("day").alias("time_type"),
            col("dt").alias("start_date"),
            col("dt").alias("end_date"),
            col("daily_send_count").alias("total_send_count"),
            col("daily_send_amount").alias("total_send_amount"),
            col("daily_verify_count").alias("total_verify_count"),
            col("daily_verify_amount").alias("total_verify_amount"),
            col("daily_pay_customer_count").alias("pay_customer_id")
        ).cache()
        logger.info(f"日数据处理完成，数据量: {daily_df.count()}")

        # 按7天滚动窗口统计（单分区）
        logger.info("开始处理7天窗口数据...")
        window_7days = Window \
            .partitionBy("activity_id") \
            .orderBy("dt") \
            .rowsBetween(-6, 0)

        weekly_df = dws_activity_df.select(
            F.lit("7days").alias("time_type"),
            date_sub(col("dt"), 6).alias("start_date"),
            col("dt").alias("end_date"),
            sum("daily_send_count").over(window_7days).alias("total_send_count"),
            sum("daily_send_amount").over(window_7days).alias("total_send_amount"),
            sum("daily_verify_count").over(window_7days).alias("total_verify_count"),
            sum("daily_verify_amount").over(window_7days).alias("total_verify_amount"),
            sum("daily_pay_customer_count").over(window_7days).alias("pay_customer_id")
        ).cache()
        logger.info(f"7天窗口数据处理完成，数据量: {weekly_df.count()}")

        # 按30天滚动窗口统计（单分区）
        logger.info("开始处理30天窗口数据...")
        window_30days = Window \
            .partitionBy("activity_id") \
            .orderBy("dt") \
            .rowsBetween(-29, 0)

        monthly_df = dws_activity_df.select(
            F.lit("30days").alias("time_type"),
            date_sub(col("dt"), 29).alias("start_date"),
            col("dt").alias("end_date"),
            sum("daily_send_count").over(window_30days).alias("total_send_count"),
            sum("daily_send_amount").over(window_30days).alias("total_send_amount"),
            sum("daily_verify_count").over(window_30days).alias("total_verify_count"),
            sum("daily_verify_amount").over(window_30days).alias("total_verify_amount"),
            sum("daily_pay_customer_count").over(window_30days).alias("pay_customer_id")
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

        # 聚合计算（单分区无Shuffle）
        logger.info("开始聚合计算最终结果...")
        result_df = union_df.groupBy("time_type", "start_date", "end_date") \
            .agg(
            sum("total_send_count").alias("total_send_count"),
            sum("total_send_amount").alias("total_send_amount"),
            sum("total_verify_count").alias("total_pay_count"),
            sum("total_verify_amount").alias("total_pay_amount"),
            countDistinct("pay_customer_id").alias("pay_customer_num"),
            round(
                when(countDistinct("pay_customer_id") == 0, 0)
                .otherwise(sum("total_verify_amount") / countDistinct("pay_customer_id")),
                2
            ).alias("avg_pay_per_customer")
        ).cache()

        # 强制计算并缓存结果（避免写入时计算）
        result_count = result_df.count()
        logger.info(f"聚合完成，结果数据量: {result_count}")

        # 写入结果表（单分区输出）
        logger.info("开始写入ADS层表...")
        start_time = time.time()

        # 若结果为空，创建空表
        if result_count == 0:
            logger.warning("结果数据为空，创建空表...")
            spark.sql("""
                CREATE TABLE IF NOT EXISTS gd2.ads_tool_effect_overview (
                    time_type STRING COMMENT '时间类型(day-日,7days-7天,30days-30天)',
                    start_date STRING COMMENT '统计开始日期',
                    end_date STRING COMMENT '统计结束日期',
                    total_send_count BIGINT COMMENT '累计发送次数',
                    total_send_amount DOUBLE COMMENT '累计发送金额',
                    total_pay_count BIGINT COMMENT '累计支付次数',
                    total_pay_amount DOUBLE COMMENT '累计支付金额',
                    pay_customer_num BIGINT COMMENT '支付买家数',
                    avg_pay_per_customer DOUBLE COMMENT '平均客单价'
                )
                STORED AS ORC
                COMMENT 'ADS层-工具效果总览表'
            """)
        else:
            # 写入数据
            result_df.write \
                .mode("overwrite") \
                .format("orc") \
                .option("compression", "snappy") \
                .saveAsTable("gd2.ads_tool_effect_overview")

        write_duration = time.time() - start_time
        logger.info(f"数据写入完成，耗时: {write_duration:.2f}秒")

        # 验证写入结果
        logger.info("验证表写入结果...")
        written_count = spark.table("gd2.ads_tool_effect_overview").count()
        logger.info(f"ADS层表写入成功，共 {written_count} 条数据")

        result_df.unpersist()

    except Exception as e:
        logger.error(f"处理过程出错: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        spark = init_spark_session()
        check_dependencies(spark)
        create_ads_tool_effect_overview(spark)
    except Exception as e:
        logger.critical(f"程序执行失败: {str(e)}", exc_info=True)
        exit(1)
    finally:
        if 'spark' in locals() and spark is not None:
            logger.info("关闭SparkSession...")
            spark.stop()
        logger.info("程序执行结束")