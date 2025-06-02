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
spark = (SparkSession.builder
    .appName("SentimentAnalysisWithExternalDictionary")
    .master("spark://node01:7077")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.defaultFS", "hdfs://node01:8020")
    .config("spark.driver.memory", "512m")  # Driver内存限制
    .config("spark.executor.memory", "512m")  # 每个Executor内存
    .config("spark.executor.cores", "1")  # 每个Executor只使用1核
    .config("spark.memory.fraction", "0.5")  # 降低JVM堆内存使用比例
    .config("spark.memory.storageFraction", "0.2")  # 减少缓存内存占比
    .config("spark.sql.shuffle.partitions", "16")  # 减少shuffle分区数
    .config("spark.shuffle.spill.numElementsForceSpillThreshold", "1000")  # 更早溢出到磁盘
    .config("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=1")
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



print("SparkSession initialized.")

# --- 辅助函数：中文分词 ---
# 加载停用词（可以从HDFS加载或本地文件）
STOP_WORDS = ["的", "了", "是", "和", "就", "都", "而", "及", "给", "被", "也", "在", "与", "或", "不", "很", "对"]


@udf(ArrayType(StringType()))
def jieba_tokenize(text):
    if text is None:
        return []
    words = jieba.cut(text, cut_all=False)
    return [word for word in words if word.strip() and word.strip() not in STOP_WORDS]


# --- 1. 数据加载与预处理 ---

def load_and_preprocess_data():
    print("Loading public opinion data...")
    public_opinion_df = spark.read.parquet(PUBLIC_OPINION_DATA_PATH)
    public_opinion_df = public_opinion_df.withColumnRenamed("内容", "content_text") \
        .withColumnRenamed("创建日期时间", "created_at")

    public_opinion_df = public_opinion_df.withColumn(
        "created_timestamp",
        to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ssXXX")
    )
    public_opinion_df = public_opinion_df.withColumn("created_date", col("created_timestamp").cast("date"))

    public_opinion_df = public_opinion_df.withColumn("words", jieba_tokenize(col("content_text")))

    print("Public opinion data schema after tokenization:")
    public_opinion_df.printSchema()
    public_opinion_df.show(5)

    print("Loading sentiment dictionary...")
    sentiment_dict_df = spark.read.option("header", "true").option("inferSchema", "true").csv(SENTIMENT_DICT_PATH)
    sentiment_dict_df = sentiment_dict_df.withColumn("words", jieba_tokenize(col("review")))

    print("Sentiment dictionary schema after tokenization:")
    sentiment_dict_df.printSchema()
    sentiment_dict_df.show(5)

    return public_opinion_df, sentiment_dict_df


# --- 2. 情感分类器训练与应用 (朴素贝叶斯) ---

def train_and_apply_sentiment_model(public_opinion_df, sentiment_dict_df):
    print("Building TF-IDF model...")
    # TF-IDF 特征提取
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # 训练朴素贝叶斯模型
    print("Training NaiveBayes model...")
    nb_classifier = NaiveBayes(labelCol="label", featuresCol="features", modelType="multinomial")

    # 构建训练管道
    pipeline = Pipeline(stages=[hashingTF, idf, nb_classifier])

    # 检查sentiment_dict_df是否有足够的数据
    if sentiment_dict_df.count() == 0:
        raise ValueError("情感词典数据为空，无法训练分类器。请检查{}文件。")

    # --- 关键修改：首先从整个情感词典中随机抽取 10% 的内容 ---
    # fraction=0.1 表示大约抽取 10% 的行
    # withReplacement=False 表示无放回抽样
    # seed=42 确保可重现性
    sampled_sentiment_df = sentiment_dict_df.sample(withReplacement=False, fraction=0.02, seed=42)
    print(f"Sampled 10% of sentiment dictionary data. Sampled size: {sampled_sentiment_df.count()}")

    # --- 接着，在这 10% 的抽样数据中划分训练集和测试集 ---
    # 假设在这 10% 的数据中，我们按 80/20 比例划分训练集和测试集
    (trainingData, testData) = sampled_sentiment_df.randomSplit([0.8, 0.2], seed=42)

    print(f"Training data size (from 10% sample): {trainingData.count()}")
    print(f"Test data size (from 10% sample): {testData.count()}")

    # 训练模型
    model = pipeline.fit(trainingData)  # 使用 trainingData 来拟合模型
    print("NaiveBayes model trained.")

    # 应用模型到舆情数据
    print("Applying sentiment model to public opinion data...")
    sentiment_predicted_df = model.transform(public_opinion_df)

    sentiment_predicted_df = sentiment_predicted_df.withColumn("predicted_sentiment",
                                                               col("prediction").cast(IntegerType()))

    print("Sentiment analysis complete. Sample results:")
    sentiment_predicted_df.select("content_text", "predicted_sentiment").show(5, truncate=False)

    return sentiment_predicted_df, model, testData  # 返回测试集供后续评估使用


# --- 3. 算法评估 ---

# 评估函数现在直接接收测试集
def evaluate_sentiment_model(model, testData):
    print("Evaluating sentiment classification model on test data...")

    # 在测试集上进行预测
    predictions = model.transform(testData)

    # 评估器
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                            metricName="weightedPrecision")
    precision = evaluator_precision.evaluate(predictions)

    evaluator_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                         metricName="weightedRecall")
    recall = evaluator_recall.evaluate(predictions)

    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    f1_score = evaluator_f1.evaluate(predictions)

    print(f"Model Accuracy (on test data): {accuracy}")
    print(f"Model Precision (on test data): {precision}")
    print(f"Model Recall (on test data): {recall}")
    print(f"Model F1-Score (on test data): {f1_score}")

    evaluation_results = {
        "model_type": "NaiveBayes Classifier",
        "evaluation_dataset_size": testData.count(),  # 评估数据集大小是测试集大小
        "metrics": {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1_score
        }
    }
    return evaluation_results


# --- 4. 舆情趋势预测 (示例：基于每日情感分布的简单预测) ---

def predict_public_opinion_trend(sentiment_predicted_df):
    print("Predicting public opinion trend...")
    # 聚合每日情感数据
    daily_sentiment_summary = sentiment_predicted_df.groupBy("created_date", "predicted_sentiment") \
        .agg(count("*").alias("count")) \
        .orderBy("created_date", "predicted_sentiment")

    # 枢轴操作，将情感转换为列，方便时间序列分析
    daily_pivot_df = daily_sentiment_summary.groupBy("created_date") \
        .pivot("predicted_sentiment", [0, 1]) \
        .sum("count") \
        .na.fill(0) \
        .orderBy("created_date") \
        .cache()

    daily_pivot_df = daily_pivot_df.withColumnRenamed("0", "negative_count") \
        .withColumnRenamed("1", "positive_count")

    daily_pivot_df.show(5)

    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql.functions import when

    daily_pivot_df = daily_pivot_df.withColumn("id", monotonically_increasing_id())

    assembler = VectorAssembler(inputCols=["id"], outputCol="features")
    lr_data = assembler.transform(daily_pivot_df)

    lr_model = LinearRegression(featuresCol="features", labelCol="positive_count", regParam=0.01)  # 添加 regParam
    lr_model_fit = lr_model.fit(lr_data)

    print("Linear Regression model trained for positive sentiment trend.")

    max_date = daily_pivot_df.selectExpr("max(created_date)").collect()[0][0]

    from datetime import date, timedelta
    future_dates = []
    max_id = daily_pivot_df.selectExpr("max(id)").collect()[0][0]
    for i in range(1, 8):  # 预测未来7天
        future_dates.append((max_date + timedelta(days=i), float(i + max_id)))

    future_df = spark.createDataFrame(future_dates, ["created_date", "id"])
    future_df = assembler.transform(future_df)

    future_predictions = lr_model_fit.transform(future_df)

    future_predictions = future_predictions.withColumn(
        "prediction",
        when(col("prediction") < 0, 0).otherwise(col("prediction"))
    )

    print("Future trend predictions (positive sentiment count):")
    future_predictions.select("created_date", "prediction").show(truncate=False)

    trend_predictions = future_predictions.select(
        date_format(col("created_date"), "yyyy-MM-dd").alias("date"),
        col("prediction").alias("predicted_positive_count")
    ).toJSON().collect()

    return trend_predictions, lr_model_fit


# --- 5. 结果汇总与存储 ---

def store_results(sentiment_analysis_df, evaluation_results, trend_predictions, lr_model):
    print("Storing results to HDFS...")
    # 1. 舆情内容分析结果 (情感标签、主题等)
    analysis_content = sentiment_analysis_df.select(
        col("content_text").alias("content"),
        col("created_date").alias("date"),
        col("predicted_sentiment").alias("sentiment_label")
    )
    analysis_content.write.mode("overwrite").json(OUTPUT_HDFS_PATH + "public_opinion_analysis_content.json")
    print("Public opinion analysis content saved.")

    # 2. 算法评估结果
    eval_schema = StructType([
        StructField("model_type", StringType(), True),
        StructField("evaluation_dataset_size", IntegerType(), True),
        StructField("metrics", StructType([
            StructField("accuracy", DoubleType(), True),
            StructField("precision", DoubleType(), True),
            StructField("recall", DoubleType(), True),
            StructField("f1_score", DoubleType(), True)
        ]), True)
    ])
    eval_df = spark.createDataFrame([evaluation_results], schema=eval_schema)
    eval_df.write.mode("overwrite").json(OUTPUT_HDFS_PATH + "algorithm_evaluation_metrics.json")
    print("Algorithm evaluation metrics saved.")

    # 3. 舆情趋势预测结果 (已是JSON lines格式)
    trend_rdd = spark.sparkContext.parallelize(trend_predictions)
    trend_rdd.saveAsTextFile(OUTPUT_HDFS_PATH + "public_opinion_trend_predictions.json")
    print("Public opinion trend predictions saved.")


# --- 主执行流程 ---
if __name__ == "__main__":
    try:
        # 1. 数据加载与预处理
        public_opinion_df, sentiment_dict_df = load_and_preprocess_data()

        # 2. 情感分类器训练与应用
        # train_and_apply_sentiment_model 现在会返回测试集
        sentiment_predicted_df, nb_model, sentiment_test_data = train_and_apply_sentiment_model(public_opinion_df,
                                                                                                sentiment_dict_df)

        # 3. 算法评估
        # 评估函数现在使用返回的测试集进行评估
        evaluation_results = evaluate_sentiment_model(nb_model, sentiment_test_data)

        # 4. 舆情趋势预测
        trend_predictions, lr_model = predict_public_opinion_trend(sentiment_predicted_df)

        # 5. 结果汇总与存储
        store_results(sentiment_predicted_df, evaluation_results, trend_predictions, lr_model)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()
        print("SparkSession stopped.")