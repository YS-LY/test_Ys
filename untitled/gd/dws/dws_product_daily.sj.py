from pyspark.sql import SparkSession

def process_dws_data():
    try:
        spark = SparkSession.builder \
            .appName("DWS层商品数据处理") \
            .config("hive.metastore.uris", "thrift://cdh02:9083") \
            .config("spark.sql.parquet.writeLegacyFormat", "true")  \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sql("USE gd")

        # 重新计算并写入数据（确保字段类型匹配）
        dws_df = spark.sql("""
            SELECT
                product_id,
                product_name,
                traffic_cnt,
                click_cnt,
                ROUND(CASE WHEN traffic_cnt=0 THEN 0 ELSE (click_cnt*100.0/traffic_cnt) END, 2) AS click_rate,
                conversion_rate,
                content_interact_cnt,
                ROUND(CASE WHEN traffic_cnt=0 THEN 0 ELSE (content_interact_cnt*100.0/traffic_cnt) END, 2) AS content_interact_rate,
                new_user_cnt,
                visitor_cnt,
                ROUND(CASE WHEN visitor_cnt=0 THEN 0 ELSE (new_user_cnt*100.0/visitor_cnt) END, 2) AS new_user_rate,
                service_rating,
                sales_amount,
                sales_cnt,
                ROUND(CASE WHEN sales_cnt=0 THEN 0 ELSE (sales_amount / sales_cnt) END, 2) AS avg_price,
                stat_date
            FROM gd.ods_product_behavior
        """)

        # 强制覆盖分区，避免旧数据干扰
        dws_df.write \
            .partitionBy("stat_date") \
            .mode("overwrite") \
            .option("parquet.enable.dictionary", "true") \
            .saveAsTable("gd.dws_product_daily")

        print("DWS层数据重新写入完成，符合商品诊断看板数据格式要求")
    except Exception as e:
        print(f"DWS层处理失败：{e}")
        spark.stop()
        exit(1)

if __name__ == "__main__":
    process_dws_data()