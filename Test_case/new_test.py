import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, count, date_format, lit, to_date, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType, DoubleType, IntegerType, StructType, StructField
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover, Word2Vec, CountVectorizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.clustering import LDA
import jieba
import os
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark import SparkContext

# --- 环境变量设置 (保持不变) ---
os.environ['PYSPARK_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['SPARK_HOME'] = '/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master spark://node01:7077 pyspark-shell'



# HDFS 相关配置
HDFS_HOST = "hdfs://node01:8020"  # 例如: "localhost" 或集群namenode IP
HDFS_PORT = 8020  # HDFS默认端口
PUBLIC_OPINION_DATA_PATH = "hdfs://node01:8020//user/spark/processed_data/part-00000-e051c3e7-5b1c-42cb-a351-927f90e05afe-c000.snappy.parquet".format(HDFS_HOST, HDFS_PORT)
SENTIMENT_DICT_PATH = "hdfs://node01:8020/user/spark/weibo_senti_100k.csv".format(HDFS_HOST, HDFS_PORT)  # 假设CSV

# 结果存储路径
OUTPUT_HDFS_PATH = "hdfs://node01:8020/user/spark/Machine_learning/".format(HDFS_HOST, HDFS_PORT)

# --- 1. 初始化SparkSession (保持不变) ---
spark = (SparkSession.builder \
    .appName("SentimentAnalysisWithExternalDictionary") \
    .master("spark://node01:7077") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://node01:8020") \
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeat.interval", "60s")
    .getOrCreate())
#由于性能问题增加网络超时和心跳间隔，提高稳定性

# 配置日志级别
spark.sparkContext.setLogLevel("WARN")



sc = SparkContext.getOrCreate()
delete_path = "hdfs://node01:8020/user/spark/	Machine_learning"
def delete_hdfs_path_recursively_if_exists(sc, path_to_delete):
    """
    检查HDFS路径是否存在，如果存在则递归删除它。
    这个函数会删除文件或目录及其所有内容。
    """
    try:
        # 通过 SparkContext 直接访问 JVM
        jvm = sc._jvm

        # 获取 Hadoop Configuration
        # Spark 通常会自动配置好 Hadoop，所以这里创建一个空的 Configuration 即可
        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration()

        # 创建HDFS路径对象
        hdfs_path_obj = jvm.org.apache.hadoop.fs.Path(path_to_delete)

        # 获取与该路径关联的 FileSystem 实例
        # 这种方式通常比 FileSystem.get(conf) 更可靠，因为它考虑了路径的 URI
        fs = hdfs_path_obj.getFileSystem(hadoop_conf)

        # 检查路径是否存在
        if fs.exists(hdfs_path_obj):
            print(f"HDFS路径 {path_to_delete} 已存在，正在递归删除...")
            # 递归删除路径 (True 表示递归删除所有内容)
            fs.delete(hdfs_path_obj, True)
            print(f"HDFS路径 {path_to_delete} 删除成功。")
        else:
            print(f"HDFS路径 {path_to_delete} 不存在，无需删除。")
    except Exception as e:
        print(f"删除HDFS路径 {path_to_delete} 时发生错误: {e}")


delete_hdfs_path_recursively_if_exists(sc, delete_path)
