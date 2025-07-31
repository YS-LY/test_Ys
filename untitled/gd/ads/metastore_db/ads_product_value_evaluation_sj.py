from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when, row_number
from pyspark.sql.window import Window
import os

def process_ads_data():
    # 初始化SparkSession（确保与主程序配置一致）
    spark = SparkSession.builder \
        .appName("商品诊断看板-ADS层处理") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh02:9083") \
        .config("spark.local.dir", "D:/spark_temp") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # 1. 读取DWS层数据（商品每日汇总指标）
        base_df = spark.table("gd.dws_product_daily")
        print("成功读取DWS层数据")

        # 2. 定义各维度权重（匹配流量、转化、内容、拉新、服务五维度）
        # 权重总和为1，对应单品竞争力诊断的五个维度{insert\_element\_0\_}
        weight = {
            "click_rate": 0.2,       # 流量获取权重
            "conversion_rate": 0.2,  # 流量转化权重
            "content_interact_rate": 0.2,  # 内容营销权重
            "new_user_rate": 0.2,    # 客户拉新权重
            "service_rating": 0.2    # 服务质量权重
        }

        # 3. 计算综合评分（0-100分）
        # 评分模型覆盖流量、转化、内容、拉新、服务维度{insert\_element\_1\_}
        score_df = base_df.withColumn(
            "total_score",
            round(
                # 流量获取：点击率（已为百分比，直接加权）
                (col("click_rate") * weight["click_rate"]) +
                # 流量转化：转化率（原始值放大100倍转为百分比）
                (col("conversion_rate") * weight["conversion_rate"] * 100) +
                # 内容营销：内容互动率（已为百分比，直接加权）
                (col("content_interact_rate") * weight["content_interact_rate"]) +
                # 客户拉新：新用户占比（已为百分比，直接加权）
                (col("new_user_rate") * weight["new_user_rate"]) +
                # 服务质量：服务评分（5分制转为100分后加权）
                (col("service_rating") * weight["service_rating"] * 20),
                2
            )
        ).withColumn(
            # 4. 划分评分等级（85+为A，70-85为B，50-70为C，50以下为D）{insert\_element\_2\_}
            "score_level",
            when(col("total_score") >= 85, "A级")
            .when(col("total_score") >= 70, "B级")
            .when(col("total_score") >= 50, "C级")
            .otherwise("D级")
        )

        # 5. 计算各维度排名（用于单品竞争力对比）
        # 按统计日期分区，各维度降序排名{insert\_element\_3\_}
        window = Window.partitionBy("stat_date")
        ranked_df = score_df \
            .withColumn("traffic_rank", row_number().over(window.orderBy(col("click_rate").desc()))) \
            .withColumn("conversion_rank", row_number().over(window.orderBy(col("conversion_rate").desc()))) \
            .withColumn("content_rank", row_number().over(window.orderBy(col("content_interact_rate").desc()))) \
            .withColumn("new_user_rank", row_number().over(window.orderBy(col("new_user_rate").desc()))) \
            .withColumn("service_rank", row_number().over(window.orderBy(col("service_rating").desc()))) \
            .withColumn("competitiveness_index", round(col("total_score") * 0.8 + 20, 2))

        # 7. 创建ADS表（若不存在）
        spark.sql("""
            CREATE TABLE IF NOT EXISTS gd.ads_product_diagnosis (
                product_id STRING COMMENT '商品ID',
                product_name STRING COMMENT '商品名称',
                total_score DOUBLE COMMENT '综合评分（0-100）',
                score_level STRING COMMENT '评分等级（A/B/C/D级）',
                traffic_rank INT COMMENT '流量获取排名',
                conversion_rank INT COMMENT '流量转化排名',
                content_rank INT COMMENT '内容营销排名',
                new_user_rank INT COMMENT '客户拉新排名',
                service_rank INT COMMENT '服务质量排名',
                competitiveness_index DOUBLE COMMENT '竞争力指数（0-100）',
                stat_date DATE COMMENT '统计日期'
            )
            PARTITIONED BY (stat_date)
            STORED AS PARQUET
            LOCATION 'hdfs://cdh01:8020/user/hive/warehouse/gd.db/ads_product_diagnosis'
        """)

        # 8. 写入ADS表t
        ranked_df.select(
            "product_id", "product_name", "total_score", "score_level",
            "traffic_rank", "conversion_rank", "content_rank",
            "new_user_rank", "service_rank", "competitiveness_index", "stat_date"
        ).write \
            .partitionBy("stat_date") \
            .mode("overwrite") \
            .saveAsTable("gd.ads_product_diagnosis")

        # 9. 输出上线截图所需数据（满足验收标准）{insert\_element\_4\_}
        print("商品诊断看板上线截图数据：")
        spark.sql("""
            SELECT 
                product_id, product_name, total_score, score_level, 
                competitiveness_index, stat_date 
            FROM gd.ads_product_diagnosis 
            LIMIT 5
        """).show()

        print("ADS层数据处理完成")

    except Exception as e:
        print(f"ADS层处理失败：{e}")
    finally:
        # 关闭Spark会话
        spark.stop()

# 单独执行ADS层处理
if __name__ == "__main__":
    # 确保临时目录存在
    temp_dir = "D:/spark_temp"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    # 执行ADS层处理
    process_ads_data()