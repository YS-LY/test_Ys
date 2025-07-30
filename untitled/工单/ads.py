# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, sum, when, lit
#
# # 初始化SparkSession（需Spark 3.2及以上）
# spark = SparkSession.builder \
#     .appName("商品诊断看板-单品竞争力评分") \
#     .config("spark.sql.version", "3.2") \
#     .getOrCreate()
#
#
#
#
# # 1. 读取数据
# similar_df = spark.table("similar_item_metrics")  # 相似商品指标数据
# self_df = spark.table("self_item_metrics")        # 本品指标数据
#
# # 2. 计算相似商品各指标的平均值（基准值）
# competitor_avg_df = similar_df.agg(
#     avg("traffic_uv").alias("avg_traffic_uv"),
#     avg("traffic_ctr").alias("avg_traffic_ctr"),
#     avg("convert_pay_rate").alias("avg_convert_pay_rate"),
#     avg("content_exposure").alias("avg_content_exposure"),
#     avg("new_user_ratio").alias("avg_new_user_ratio"),
#     avg("service_rating_rate").alias("avg_service_rating_rate")
# )
#
# # 3. 关联本品数据与相似商品均值，计算各指标比例
# self_with_avg_df = self_df.crossJoin(competitor_avg_df) \
#     .withColumn("ratio_uv", col("traffic_uv") / col("avg_traffic_uv")) \
#     .withColumn("ratio_ctr", col("traffic_ctr") / col("avg_traffic_ctr")) \
#     .withColumn("ratio_convert", col("convert_pay_rate") / col("avg_convert_pay_rate")) \
#     .withColumn("ratio_content", col("content_exposure") / col("avg_content_exposure")) \
#     .withColumn("ratio_new_user", col("new_user_ratio") / col("avg_new_user_ratio")) \
#     .withColumn("ratio_service", col("service_rating_rate") / col("avg_service_rating_rate"))
#
# # 4. 计算各维度得分（每个维度取对应指标比例的得分均值）
# # 4.1 流量获取维度得分（uv、ctr两个指标）
# traffic_score_df = self_with_avg_df \
#     .withColumn("score_uv", when(col("ratio_uv") >= 1.2, 20)
#                 .when(col("ratio_uv") >= 1.0, 16)
#                 .when(col("ratio_uv") >= 0.8, 12)
#                 .when(col("ratio_uv") >= 0.6, 7)
#                 .otherwise(3)) \
#     .withColumn("score_ctr", when(col("ratio_ctr") >= 1.2, 20)
#                 .when(col("ratio_ctr") >= 1.0, 16)
#                 .when(col("ratio_ctr") >= 0.8, 12)
#                 .when(col("ratio_ctr") >= 0.6, 7)
#                 .otherwise(3)) \
#     .withColumn("traffic_score", (col("score_uv") + col("score_ctr")) / 2)  # 流量获取维度得分
#
# # 4.2 流量转化维度得分（仅支付转化率指标）
# convert_score_df = traffic_score_df \
#     .withColumn("convert_score", when(col("ratio_convert") >= 1.2, 20)
#                 .when(col("ratio_convert") >= 1.0, 16)
#                 .when(col("ratio_convert") >= 0.8, 12)
#                 .when(col("ratio_convert") >= 0.6, 7)
#                 .otherwise(3))
#
# # 4.3 内容营销维度得分（仅内容曝光量指标）
# content_score_df = convert_score_df \
#     .withColumn("content_score", when(col("ratio_content") >= 1.2, 20)
#                 .when(col("ratio_content") >= 1.0, 16)
#                 .when(col("ratio_content") >= 0.8, 12)
#                 .when(col("ratio_content") >= 0.6, 7)
#                 .otherwise(3))
#
# # 4.4 客户拉新维度得分（仅新客成交占比指标）
# new_user_score_df = content_score_df \
#     .withColumn("new_user_score", when(col("ratio_new_user") >= 1.2, 20)
#                 .when(col("ratio_new_user") >= 1.0, 16)
#                 .when(col("ratio_new_user") >= 0.8, 12)
#                 .when(col("ratio_new_user") >= 0.6, 7)
#                 .otherwise(3))
#
# # 4.5 服务质量维度得分（仅好评率指标）
# final_score_df = new_user_score_df \
#     .withColumn("service_score", when(col("ratio_service") >= 1.2, 20)
#                 .when(col("ratio_service") >= 1.0, 16)
#                 .when(col("ratio_service") >= 0.8, 12)
#                 .when(col("ratio_service") >= 0.6, 7)
#                 .otherwise(3)) \
#                 .withColumn("total_score", col("traffic_score") + col("convert_score") +
#                 col("content_score") + col("new_user_score") + col("service_score")) \
#                 .withColumn("level", when(col("total_score") >= 85, "A级")
#                 .when(col("total_score") >= 70, "B级")
#                 .when(col("total_score") >= 50, "C级")
#                 .otherwise("D级")) \
#                 .withColumn("weak_dimensions",
#                 when(col("traffic_score") < 10, "流量获取;").otherwise("") +
#                 when(col("convert_score") < 10, "流量转化;").otherwise("") +
#                 when(col("content_score") < 10, "内容营销;").otherwise("") +
#                 when(col("new_user_score") < 10, "客户拉新;").otherwise("") +
#                 when(col("service_score") < 10, "服务质量;").otherwise(""))
#
# # 5. 输出结果（含各维度得分、总得分、等级、劣势维度）
# final_score_df.select(
#     "item_id", "traffic_score", "convert_score", "content_score",
#     "new_user_score", "service_score", "total_score", "level", "weak_dimensions"
# ).show()
#
# # 代码注释：工单编号-大数据-电商数仓-07-商品主题商品诊断看板












# # 工单编号：大数据-电商数仓-07-商品主题商品诊断看板
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, when, round, lit
#
# spark = SparkSession.builder.appName("product_diagnosis_layered").getOrCreate()
#
# # 1. 读取DWD层数据
# dwd_base = spark.table("dwd.dwd_product_base")  # 商品基础信息
# dwd_traffic = spark.table("dwd.dwd_product_traffic")  # 流量数据
# dwd_conversion = spark.table("dwd.dwd_product_conversion")  # 转化数据
# dwd_content = spark.table("dwd.dwd_product_content")  # 内容数据
# dwd_acquisition = spark.table("dwd.dwd_product_acquisition")  # 拉新数据
# dwd_service = spark.table("dwd.dwd_product_service")  # 服务数据
#
# # 2. 计算流量获取评分（DWS层）
# # 2.1 计算品类平均流量
# cate_traffic_avg = dwd_traffic.groupBy("category_id") \
#     .agg(
#         avg("visitor_num").alias("cate_avg_visitor"),
#         avg("pv").alias("cate_avg_pv")
#     )
#
# # 2.2 计算本品流量得分
# dws_traffic_score = dwd_traffic.join(dwd_base, on="product_id", how="left") \
#     .join(cate_traffic_avg, on="category_id", how="left") \
#     .groupBy("product_id", "product_name", "stat_date") \
#     .agg(
#         avg("visitor_num").alias("avg_visitor"),
#         avg("pv").alias("avg_pv"),
#         avg("cate_avg_visitor").alias("cate_avg_visitor"),
#         avg("cate_avg_pv").alias("cate_avg_pv")
#     ) \
#     .withColumn("traffic_acquisition_score",
#         round(
#             (col("avg_visitor")/col("cate_avg_visitor")*0.4 + col("avg_pv")/col("cate_avg_pv")*0.6)*100,
#             2
#         )
#     ) \
#     .select("product_id", "product_name", "stat_date", "traffic_acquisition_score")
#
# # （其他维度得分计算类似，生成dws_conversion_score、dws_content_score等）
#
#
# # 步骤2：汇总到ADS层全品价值评估表
# ads_value = dws_traffic_score \
#     .join(dws_conversion_score, on=["product_id", "product_name", "stat_date"], how="left") \
#     .join(dws_content_score, on=["product_id", "product_name", "stat_date"], how="left") \
#     .join(dws_acquisition_score, on=["product_id", "product_name", "stat_date"], how="left") \
#     .join(dws_service_score, on=["product_id", "product_name", "stat_date"], how="left") \
#     .join(dwd_base.select("product_id", "price"), on="product_id", how="left") \
#     .join(dwd_conversion.select("product_id", "sales_num", "visitor_num"), on="product_id", how="left") \
#     .withColumn("total_score",
#         round(
#             col("traffic_acquisition_score")*0.2 +
#             col("traffic_conversion_score")*0.2 +
#             col("content_marketing_score")*0.2 +
#             col("customer_acquisition_score")*0.2 +
#             col("service_quality_score")*0.2,
#             2
#         )
#     )
#     .withColumn("level",
#         when(col("total_score") >= 85, "A")
#         .when(col("total_score") >= 70, "B")
#         .when(col("total_score") >= 50, "C")
#         .otherwise("D")
#     ) \
#     .withColumn("amount", round(col("price") * col("sales_num"), 2)) \
#     .withColumn("visitor_sales_ratio",
#         when(col("sales_num") == 0, lit(None))
#         .otherwise(round(col("visitor_num") / col("sales_num"), 2))
#     ) \
#     .select(

#         "product_id", "product_name", "stat_date",
#         "traffic_acquisition_score", "traffic_conversion_score",
#         "content_marketing_score", "customer_acquisition_score", "service_quality_score",
#         "total_score", "level", "amount", "price", "sales_num", "visitor_num", "visitor_sales_ratio"
#     )
#
# # 写入ADS层表
# ads_value.write.mode("overwrite").partitionBy("stat_date").saveAsTable("ads.ads_product_value_evaluation")