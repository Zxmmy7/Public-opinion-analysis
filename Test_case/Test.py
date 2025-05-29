import os
from pyspark.sql import SparkSession
from pyspark import SparkConf

# !!! 确保这里的路径与您的 Spark Master 实际安装路径完全一致 !!!
# 从您的日志中确认的路径是：/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13
os.environ['SPARK_HOME'] = '/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13'
os.environ['PYSPARK_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'

# 显式设置 PYSPARK_SUBMIT_ARGS 确保 PySpark 以 standalone 模式连接
# 如果您的 Spark Master 是 standalone 模式，这会很有帮助
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master spark://node01:7077 pyspark-shell'

print(f"SPARK_HOME set to: {os.environ.get('SPARK_HOME')}")
print(f"PYSPARK_PYTHON set to: {os.environ.get('PYSPARK_PYTHON')}")
print(f"PYSPARK_DRIVER_PYTHON set to: {os.environ.get('PYSPARK_DRIVER_PYTHON')}")
print(f"PYSPARK_SUBMIT_ARGS set to: {os.environ.get('PYSPARK_SUBMIT_ARGS')}")


conf = SparkConf() \
    .setAppName("MySparkApp") \
    .setMaster("spark://node01:7077") \
    .set("spark.driver.host", "192.168.88.11") # 替换为运行Test_case/T.py的机器的IP地址

# 尝试设置序列化器，虽然Java序列化是默认的，但显式指定可能有助于排除
# .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
# .set("spark.cleaner.referenceTracking.cleanWeakRefs", "true") # 这通常用于内存问题，这里可能不直接相关

spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("SparkSession created successfully!")
spark.stop()
print("SparkSession stopped.")