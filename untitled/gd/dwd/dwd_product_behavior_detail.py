from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, when, current_timestamp, round
import os

def process_dwd_data():
    spark = SparkSession.builder \
        .appName("商品诊断看板-DWD层处理") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh02:9083") \
        .config("spark.local.dir", "D:/spark_temp") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # 读取ODS数据
        ods_df = spark.table("gd.ods_product_behavior")
        print("成功读取ODS层数据")

        # 清洗并转换数据（重点处理长整型字段）
        dwd_df = ods_df \
            .filter("product_id is not null and product_id != ''") \
            .withColumn("traffic_cnt", when(
                col("traffic_cnt").cast("BIGINT").between(0, 9223372036854775807),
                col("traffic_cnt").cast("BIGINT")
            ).otherwise(0)) \
            .withColumn("click_cnt", when(
                col("click_cnt").cast("BIGINT").between(0, 9223372036854775807),
                col("click_cnt").cast("BIGINT")
            ).otherwise(0)) \
            .withColumn("sales_cnt", when(
                col("sales_cnt").cast("BIGINT").between(0, 9223372036854775807),
                col("sales_cnt").cast("BIGINT")
            ).otherwise(0)) \
            .withColumn("category", split(col("product_name"), "_")[0]) \
            .withColumn("click_rate",
                when(col("traffic_cnt") == 0, 0.0)
                .otherwise(round(col("click_cnt") / col("traffic_cnt"), 4))
            ) \
            .withColumn("avg_price",
                when(col("sales_cnt") == 0, 0.0)
                .otherwise(round(col("sales_amount") / col("sales_cnt"), 2))
            ) \
            .withColumn("etl_time", current_timestamp().cast("string")) \
            .select(
                "product_id", "product_name", "category",
                "traffic_cnt", "click_cnt", "click_rate",
                "conversion_rate", "content_interact_cnt",
                "new_user_cnt", "visitor_cnt", "service_rating",
                "sales_amount", "sales_cnt", "avg_price",
                "stat_date", "etl_time"
            )

        # 重新创建表（清除损坏结构）
        spark.sql("DROP TABLE IF EXISTS gd.dwd_product_behavior_detail")
        spark.sql("""
            CREATE TABLE gd.dwd_product_behavior_detail (
                product_id STRING, product_name STRING, category STRING,
                traffic_cnt BIGINT, click_cnt BIGINT, click_rate DOUBLE,
                conversion_rate DOUBLE, content_interact_cnt BIGINT,
                new_user_cnt BIGINT, visitor_cnt BIGINT, service_rating DOUBLE,
                sales_amount DECIMAL(16,2), sales_cnt BIGINT, avg_price DOUBLE,
                stat_date DATE, etl_time STRING
            )
            PARTITIONED BY (stat_date)
            STORED AS PARQUET
            LOCATION 'hdfs://cdh01:8020/user/hive/warehouse/gd.db/dwd_product_behavior_detail'
        """)

        # 禁用字典编码写入
        dwd_df.write \
            .option("parquet.enable.dictionary", "false") \
            .partitionBy("stat_date") \
            .mode("overwrite") \
            .saveAsTable("gd.dwd_product_behavior_detail")

        print("DWD层处理完成")
    except Exception as e:
        print(f"DWD层处理失败：{e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    temp_dir = "D:/spark_temp"
    os.makedirs(temp_dir, exist_ok=True)
    process_dwd_data()