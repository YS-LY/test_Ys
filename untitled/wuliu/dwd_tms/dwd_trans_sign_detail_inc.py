from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, date_format, lit, concat, substring, cast,
    unix_timestamp, coalesce
)


def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("OrderSignETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def select_to_hive(jdbcDF: DataFrame, tableName: str):
    """动态适配目标表结构并写入Hive，复用成功经验"""
    try:
        spark = get_spark_session()
        target_table = f"tms.{tableName}"

        # 1. 读取目标表结构，用于字段对齐
        target_df = spark.table(target_table)
        target_columns = target_df.columns
        print(f"[INFO] 目标表字段: {target_columns}")

        # 2. 过滤并转换字段（仅保留目标表存在的字段，并转换类型）
        valid_columns = [c for c in jdbcDF.columns if c in target_columns]
        df_aligned = jdbcDF.select([
            col(c).cast(target_df.schema[c].dataType).alias(c)
            for c in target_columns
            if c in valid_columns
        ])

        # 3. 处理空值（根据字段类型填充）
        null_fill_values = {}
        for field in target_df.schema.fields:
            field_type = field.dataType.simpleString()
            if field_type.startswith(("int", "bigint", "double", "float", "decimal")):
                null_fill_values[field.name] = 0  # 数值型用0填充
            elif field_type == "string":
                null_fill_values[field.name] = ""  # 字符串用空串填充
            elif field_type in ("date", "timestamp"):
                null_fill_values[field.name] = "1970-01-01"  # 日期型用默认日期填充

        df_aligned = df_aligned.fillna(null_fill_values)

        # 4. 写入前校验数据
        print(f"[INFO] 待写入数据（适配后）样例:")
        df_aligned.show(5, truncate=False)
        print(f"[INFO] 待写入数据量: {df_aligned.count()}")

        # 5. 写入Hive表
        df_aligned.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {target_table}")

    except Exception as e:
        print(f"[ERROR] 写入Hive失败: {str(e)}")
        print("[ERROR] 原始数据样例:")
        jdbcDF.show(5, truncate=False)
        raise e  # 抛出异常，方便调度工具捕获


def process_order_sign(partition_date: str, tableName):
    """处理订单签收信息并写入Hive"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理订单签收信息，目标分区日期：{partition_date}")
    source_dt = "20250713"  # 源数据分区日期

    # 1. 读取源表数据
    ods_order_cargo = spark.table("tms.ods_order_cargo") \
        .filter((col("ds") == source_dt) & (col("is_deleted") == "0")) \
        .select(
        col("id"),
        col("order_id"),
        col("cargo_type"),
        # 字段名修正：volumn带u（适配目标表）
        col("volume_length").alias("volumn_length"),
        col("volume_width").alias("volumn_width"),
        col("volume_height").alias("volumn_height"),
        col("weight")
    ).alias("cargo")

    ods_order_info = spark.table("tms.ods_order_info") \
        .filter((col("ds") == source_dt) & (col("is_deleted") == "0")) \
        .filter(~col("status").isin(["60010", "60020", "60030", "60040", "60050", "60060", "60070", "60999"])) \
        .select(
        col("id"),
        col("order_no"),
        col("status"),
        col("collect_type"),
        col("user_id"),
        col("receiver_complex_id"),
        col("receiver_province_id"),
        col("receiver_city_id"),
        col("receiver_district_id"),
        concat(substring(col("receiver_name"), 1, 1), lit("*")).alias("receiver_name"),
        col("sender_complex_id"),
        col("sender_province_id"),
        col("sender_city_id"),
        col("sender_district_id"),
        concat(substring(col("sender_name"), 1, 1), lit("*")).alias("sender_name"),
        col("payment_type"),
        col("cargo_num"),
        col("amount"),
        # 日期类型直接格式化（适配目标表）
        date_format(col("estimate_arrive_time"), "yyyy-MM-dd HH:mm:ss").alias("estimate_arrive_time"),
        col("distance"),
        concat(substring(col("update_time"), 1, 10), lit(" "), substring(col("update_time"), 12, 8)).alias("sign_time")
    ).alias("info")

    ods_base_dic = spark.table("tms.ods_base_dic") \
        .filter((col("ds") == source_dt) & (col("is_deleted") == "0")) \
        .select(col("id"), col("name"))

    # 2. 创建字典表别名
    dic_for_cargo_type = ods_base_dic.alias("cargo_type_dic")
    dic_for_status = ods_base_dic.alias("status_dic")
    dic_for_collect_type = ods_base_dic.alias("collect_type_dic")
    dic_for_payment_type = ods_base_dic.alias("payment_type_dic")

    # 3. 构建主查询
    result_df = ods_order_cargo.join(
        ods_order_info,
        col("cargo.order_id") == col("info.id"),
        "inner"
    ).join(
        dic_for_cargo_type,
        col("cargo.cargo_type") == cast(col("cargo_type_dic.id"), "string"),
        "left"
    ).join(
        dic_for_status,
        col("info.status") == cast(col("status_dic.id"), "string"),
        "left"
    ).join(
        dic_for_collect_type,
        col("info.collect_type") == cast(col("collect_type_dic.id"), "string"),
        "left"
    ).join(
        dic_for_payment_type,
        col("info.payment_type") == cast(col("payment_type_dic.id"), "string"),
        "left"
    ).select(
        col("cargo.id"),
        col("cargo.order_id"),
        col("cargo.cargo_type"),
        col("cargo_type_dic.name").alias("cargo_type_name"),
        col("cargo.volumn_length"),
        col("cargo.volumn_width"),
        col("cargo.volumn_height"),
        col("cargo.weight"),
        col("info.sign_time"),
        col("info.order_no"),
        col("info.status"),
        col("status_dic.name").alias("status_name"),
        col("info.collect_type"),
        col("collect_type_dic.name").alias("collect_type_name"),
        col("info.user_id"),
        col("info.receiver_complex_id"),
        col("info.receiver_province_id"),
        col("info.receiver_city_id"),
        col("info.receiver_district_id"),
        col("info.receiver_name"),
        col("info.sender_complex_id"),
        col("info.sender_province_id"),
        col("info.sender_city_id"),
        col("info.sender_district_id"),
        col("info.sender_name"),
        col("info.payment_type"),
        col("payment_type_dic.name").alias("payment_type_name"),
        col("info.cargo_num"),
        col("info.amount"),
        col("info.estimate_arrive_time"),
        col("info.distance"),
        # 生成BIGINT类型的ts（适配目标表）
        (unix_timestamp() * 1000).cast("bigint").alias("ts"),
        # 分区字段dt
        lit(partition_date).alias("dt")
    )

    # 4. 补充目标表可能需要的其他字段（如果有）
    # 例如：如果目标表有额外字段，这里用默认值补充
    final_df = result_df

    # 5. 写入Hive（使用动态适配函数）
    select_to_hive(final_df, tableName)
    print(f"[INFO] 订单签收信息处理完成，目标分区：{partition_date}")


if __name__ == "__main__":
    target_table = 'dwd_trans_sign_detail_inc'  # 目标表名
    target_partition = '20250725'  # 目标分区字段值
    process_order_sign(target_partition, target_table)
