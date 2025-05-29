from pyspark.sql import SparkSession

# 构建 SparkSession
# 如果你的代码中已经有 SparkSession 的创建，直接使用已有的 spark 变量
# 否则，添加这行：
spark = SparkSession.builder.appName("CheckMySparkMode").getOrCreate()

# 获取 SparkContext
sc = spark.sparkContext

# 打印 Master URL
print(f"DEBUG: Spark Master URL: {sc.master}")

# 判断是否为本地模式
if sc.isLocal:
    print("DEBUG: Spark is running in local mode.")
else:
    print("DEBUG: Spark is running in cluster mode.")

# 打印应用程序 ID
print(f"DEBUG: Spark Application ID: {sc.applicationId}")

# 打印 Spark UI URL
print(f"DEBUG: Spark UI URL: {sc.uiWebUrl}")

# 继续执行你原来的数据处理和可视化代码...

# 不要忘记在程序结束时停止 SparkSession
# spark.stop() # 如果程序逻辑后续需要继续使用 Spark，则不在这里停止