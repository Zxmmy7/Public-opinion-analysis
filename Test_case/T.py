import os
from pyspark.sql import SparkSession
import sys

# ... (你现有的 PYSPARK_PYTHON 和 PYSPARK_DRIVER_PYTHON 设置不变)
os.environ['PYSPARK_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'

# 核心步骤：设置 SPARK_HOME 指向你虚拟机上 Spark 3.2.0 (scala2.13) 的安装路径
os.environ['SPARK_HOME'] = '/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master spark://node01:7077 pyspark-shell'
# ... (如果你之前尝试了 PYSPARK_SUBMIT_ARGS，现在可以暂时移除或注释掉，先用直接 builder 方式)
# 例如，回到最初的 SparkSession.builder 写法
spark = SparkSession.builder \
    .appName("SparkClusterTest") \
    .master("spark://192.168.88.11:7077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

# 示例数据 (保持不变)
data = ["Hello Spark", "Spark is awesome", "Hello world", "Apache Spark"]
rdd = spark.sparkContext.parallelize(data)

# 单词计数逻辑 (保持不变)
words = rdd.flatMap(lambda line: line.split(" "))
word_pairs = words.map(lambda word: (word, 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# 打印结果 (保持不变)
print("=======================================")
print("Spark Cluster Test Results:")
for word, count in word_counts.collect():
    print(f"{word}: {count}")
print("=======================================")

# 停止 SparkSession (保持不变)
spark.stop()
print("SparkSession stopped.")