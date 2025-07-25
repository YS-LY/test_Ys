from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("dim_complex_full_process_fixed") \
    .enableHiveSupport() \
    .getOrCreate()

# 配置参数
database_name = "tms"
target_table = f"{database_name}.dim_complex_full"
table_path = "/warehouse/tms/dim/dim_complex_full"
current_dt = "20250712"  # 统计日期

# 1. 删除表（如果存在）
spark.sql(f"DROP TABLE IF EXISTS {target_table}")
print(f"已删除表 {target_table}（如果存在）")

# 2. 创建外部表（修复ORC格式与ROW FORMAT冲突）
spark.sql(f"""
CREATE EXTERNAL TABLE {target_table} (
    `id` bigint COMMENT '小区ID',
    `complex_name` string COMMENT '小区名称',
    `courier_emp_ids` array<string> COMMENT '负责快递员IDS',
    `province_id` bigint COMMENT '省份ID',
    `province_name` string COMMENT '省份名称',
    `city_id` bigint COMMENT '城市ID',
    `city_name` string COMMENT '城市名称',
    `district_id` bigint COMMENT '区（县）ID',
    `district_name` string COMMENT '区（县）名称'
) COMMENT '小区维度表'
PARTITIONED BY (`dt` string COMMENT '统计日期')
STORED AS ORC  -- ORC格式不需要指定ROW FORMAT
LOCATION '{table_path}'
TBLPROPERTIES ('orc.compress'='snappy')
""")
print(f"表 {target_table} 创建成功")

# 3. 读取源数据并处理
complex_info = spark.table("ods_base_complex") \
    .filter((F.col("ds") == current_dt) & (F.col("is_deleted") == "0")) \
    .select(
        F.col("id"),
        F.col("complex_name"),
        F.col("province_id"),
        F.col("city_id"),
        F.col("district_id"),
        F.col("district_name")
    )

dic_for_prov = spark.table("ods_base_region_info") \
    .filter((F.col("ds") == "20200623") & (F.col("is_deleted") == "0")) \
    .select(F.col("id"), F.col("name"))

dic_for_city = spark.table("ods_base_region_info") \
    .filter((F.col("ds") == "20200623") & (F.col("is_deleted") == "0")) \
    .select(F.col("id"), F.col("name"))

complex_courier = spark.table("ods_express_courier_complex") \
    .filter((F.col("ds") == "20250711") & (F.col("is_deleted") == "0")) \
    .groupBy("complex_id") \
    .agg(
        F.collect_set(F.col("courier_emp_id").cast("string")).alias("courier_emp_ids")
    )

# 4. 关联所有数据
result_df = complex_info \
    .join(dic_for_prov, complex_info["province_id"] == dic_for_prov["id"], "inner") \
    .withColumnRenamed("name", "province_name") \
    .join(dic_for_city, complex_info["city_id"] == dic_for_city["id"], "inner") \
    .withColumnRenamed("name", "city_name") \
    .join(complex_courier, complex_info["id"] == complex_courier["complex_id"], "left_outer") \
    .select(
        complex_info["id"],
        complex_info["complex_name"],
        F.col("courier_emp_ids"),
        complex_info["province_id"],
        F.col("province_name"),
        complex_info["city_id"],
        F.col("city_name"),
        complex_info["district_id"],
        complex_info["district_name"],
        F.lit(current_dt).alias("dt")
    )

# 5. 覆盖写入目标表
result_df.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .saveAsTable(target_table)

print(f"数据已成功写入 {target_table} 分区 dt={current_dt}")

# 6. 查询结果
print(f"\n查询 {target_table} 表数据：")
spark.table(target_table).show(truncate=False)

# 停止SparkSession
spark.stop()
