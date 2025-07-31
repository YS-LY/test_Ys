from pyspark.sql import SparkSession

# 初始化 SparkSession
spark = SparkSession.builder \
   .appName("InsertCSVDataToTable") \
   .enableHiveSupport() \
   .getOrCreate()

# 读取 CSV 文件，设置不读取表头
csv_data = spark.read.csv(
    "D:/2211A/workspace/工单/gd/ods/ods_product_behavior0.csv",
    header=False,
    inferSchema=True
)

# 将数据插入到 ods_product_behavior 表中
csv_data.write.mode("append").insertInto("ods_product_behavior")

print("CSV 数据已成功添加到 ods_product_behavior 表中。")

# 关闭 SparkSession
spark.stop()