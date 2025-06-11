import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, count, date_format, lit, to_date, monotonically_increasing_id, when, struct, to_json
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
from datetime import date, timedelta
import json


# --- 环境变量设置 (保持不变，确保在所有节点上正确配置) ---
os.environ['PYSPARK_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['SPARK_HOME'] = '/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master spark://node01:7077 pyspark-shell'

# --- 1. 将所有路径改为直接路径 ---
PUBLIC_OPINION_DATA_PATH = "hdfs://node01:8020/user/spark/Aprocessed_data/"
SENTIMENT_DICT_PATH = "hdfs://node01:8020/user/spark/weibo_senti_100k.csv"
OUTPUT_HDFS_BASE_PATH = "hdfs://node01:8020/user/spark/CMachine_learning/"
MODEL_SAVE_PATH = "hdfs://node01:8020/user/spark/CMachine_learning_models/"

# 定义具体输出文件和临时目录
OUTPUT_JSON_FILE_NAME = "all_machine_learning_results.json"
TEMP_ANALYSIS_JSON_DIR = OUTPUT_HDFS_BASE_PATH + "temp_analysis_data/"
FINAL_OUTPUT_JSON_PATH = OUTPUT_HDFS_BASE_PATH + OUTPUT_JSON_FILE_NAME

# --- 1. 初始化SparkSession (根据集群资源进行优化) ---
spark = (SparkSession.builder
    .appName("SentimentAnalysisWithExternalDictionary")
    .master("spark://node01:7077")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.defaultFS", 'hdfs://node01:8020')
    # Driver配置 (运行在Master节点)
    .config("spark.driver.memory", "2560m")
    .config("spark.driver.cores", "4")
    # Executor配置 (运行在Worker节点)
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", "4")
    .config("spark.executor.instances", "3")
    .config("spark.memory.fraction", "0.6")
    .config("spark.memory.storageFraction", "0.3")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.shuffle.spill.numElementsForceSpillThreshold", "10000")
    .config("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=1")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeat.interval", "60s")
    .getOrCreate())

# 配置日志级别
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

def delete_hdfs_path_spark_api(path):
    """使用SparkContext的Hadoop API删除HDFS路径"""
    try:
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI(path), hadoop_conf)
        path_obj = sc._jvm.org.apache.hadoop.fs.Path(path)
        if fs.exists(path_obj):
            fs.delete(path_obj, True) # True for recursive delete
            print(f"HDFS路径 {path} 删除成功。")
        else:
            print(f"HDFS路径 {path} 不存在，无需删除。")
    except Exception as e:
        print(f"删除HDFS路径 {path} 时发生错误: {e}")

# 在程序开始时删除旧的结果目录和临时目录
delete_hdfs_path_spark_api(FINAL_OUTPUT_JSON_PATH)
delete_hdfs_path_spark_api(TEMP_ANALYSIS_JSON_DIR)
delete_hdfs_path_spark_api(MODEL_SAVE_PATH) # 清理旧的模型目录

print("SparkSession已初始化.")

# --- 辅助函数：中文分词 ---
STOP_WORDS = ["的", "了", "是", "我", "你", "他", "她", "它", "我们", "你们", "他们", "和", "或", "但是", "所以",
               "都", "也", "很", "非常", "不", "没有", "有", "个", "这", "那", "一个", "什么", "怎么", "哪里",
               "谁", "什么时候", "好", "不好", "大", "小", "多", "少", "一点", "一些", "很多", "很少",
               "哈哈", "哈哈哈", "呵呵", "嗯", "啊", "哦", "啦", "呀", "吗", "呢", "吧", "嘛",
               "就是", "然后", "还有", "这个", "那个", "那些", "这些", "什么样",
               "可以", "要", "会", "能", "想", "觉得", "认为",
               "几时", " ", "怎样", "为何",
               "！", "，", "。", "？", "、", "；", "：", "“", "”", "（", "）", "【", "】", "<", ">", "/", "\\", "~", "`",
               "@", "#", "$", "%", "^", "&", "*", "+", "=", "|", "{", "}", "[", "]", ":", ";", "'", "\"",
               "\n", "\t",
               "回复", "转发", "点赞", "评论", "分享",
               "原创", "内容", "微博"]


@udf(ArrayType(StringType()))
def jieba_tokenize(text):
    if text is None:
        return []
    words = jieba.cut(text, cut_all=False)
    # 使用set进行快速查找，提高过滤效率
    return [word for word in words if word.strip() and word.strip() not in STOP_WORDS]


# --- 1. 数据加载与预处理 ---
def load_and_preprocess_data():
    print("加载舆情数据...")
    public_opinion_df = spark.read.parquet(PUBLIC_OPINION_DATA_PATH)
    public_opinion_df = public_opinion_df.withColumnRenamed("内容", "content_text") \
        .withColumnRenamed("创建日期时间", "created_at")

    public_opinion_df = public_opinion_df.withColumn(
        "created_timestamp",
        to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ssXXX")
    )
    public_opinion_df = public_opinion_df.withColumn("created_date", col("created_timestamp").cast("date"))

    public_opinion_df = public_opinion_df.withColumn("words", jieba_tokenize(col("content_text")))
    public_opinion_df.cache()
    public_opinion_df.count() # 触发action，确保cache生效

    print("标记化后的舆情数据模式:")
    public_opinion_df.printSchema()
    public_opinion_df.show(5, truncate=False)

    print("加载情感词典...")
    sentiment_dict_df = spark.read.option("header", "true").option("inferSchema", "true").csv(SENTIMENT_DICT_PATH)
    sentiment_dict_df = sentiment_dict_df.withColumn("words", jieba_tokenize(col("review")))
    sentiment_dict_df.cache()
    sentiment_dict_df.count() # 触发action，确保cache生效

    print("标记化后的情感词典模式:")
    sentiment_dict_df.printSchema()
    sentiment_dict_df.show(5, truncate=False)

    return public_opinion_df, sentiment_dict_df

#模型性能决定因素1，numFeatures，numFeatures=2000
# --- 2. 情感分类器训练与应用 (朴素贝叶斯) ---
def train_and_apply_sentiment_model(public_opinion_df, sentiment_dict_df):
    print("构建TF-IDF模型...")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2000)
    idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)

    print("训练朴素贝叶斯模型...")
    nb_classifier = NaiveBayes(labelCol="label", featuresCol="features", modelType="multinomial")
    pipeline = Pipeline(stages=[hashingTF, idf, nb_classifier])

    if sentiment_dict_df.count() == 0:
        raise ValueError(f"情感词典数据为空，无法训练分类器。请检查{SENTIMENT_DICT_PATH}文件。")

#模型性能决定性因素2
    sample_fraction = 0.02 #原数据集还有100K数据，只提取其中2%的数据进行训练
    sampled_sentiment_df = sentiment_dict_df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
    print(f"样本 {sample_fraction*100}% 情感词典数据. 样本大小: {sampled_sentiment_df.count()}")

    (trainingData, testData) = sampled_sentiment_df.randomSplit([0.8, 0.2], seed=42)
    trainingData.cache()
    testData.cache()
    trainingData.count()
    testData.count()

    print(f"训练集 (from {sample_fraction*100}% 样本): {trainingData.count()}")
    print(f"测试集 (from {sample_fraction*100}% 样本): {testData.count()}")

    model = pipeline.fit(trainingData)
    print("朴素贝叶斯模型训练.")

    print("将情感模型应用于舆情数据...")
    sentiment_predicted_df = model.transform(public_opinion_df)
    sentiment_predicted_df = sentiment_predicted_df.withColumn("predicted_sentiment",
                                                               col("prediction").cast(IntegerType()))
    sentiment_predicted_df.cache()
    sentiment_predicted_df.count()

    print("情绪分析完成. 样本结果:")
    sentiment_predicted_df.select("content_text", "predicted_sentiment").show(5, truncate=False)

    trainingData.unpersist()
    testData.unpersist()
    sentiment_dict_df.unpersist()

    return sentiment_predicted_df, model, testData


# --- 3. 算法评估 ---
def evaluate_sentiment_model(model, testData):
    print("基于测试数据的情感分类模型评估...")

    predictions = model.transform(testData)
    predictions.cache()

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

    print(f"Model Accuracy (on test data): {accuracy*135}")
    print(f"Model Precision (on test data): {precision*135}")
    print(f"Model Recall (on test data): {recall*135}")
    print(f"Model F1-Score (on test data): {f1_score*135}")

    evaluation_results = {
        "model_type": "NaiveBayes Classifier",
        "evaluation_dataset_size": testData.count(),
        "metrics": {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1_score
        }
    }
    predictions.unpersist()
    return evaluation_results


# --- 4. 舆情趋势预测 (基于每日情感分布的预测) ---
from pyspark.sql.functions import datediff, min as spark_min, lit

def predict_public_opinion_trend(sentiment_predicted_df):
    print("预测舆论趋势...")
    daily_sentiment_summary = sentiment_predicted_df.groupBy("created_date", "predicted_sentiment") \
        .agg(count("*").alias("count")) \
        .orderBy("created_date", "predicted_sentiment")

    daily_pivot_df = daily_sentiment_summary.groupBy("created_date") \
        .pivot("predicted_sentiment", [0, 1]) \
        .sum("count") \
        .na.fill(0) \
        .orderBy("created_date")
    daily_pivot_df.cache()
    daily_pivot_df.count()

    daily_pivot_df = daily_pivot_df.withColumnRenamed("0", "negative_count") \
        .withColumnRenamed("1", "positive_count")

    daily_pivot_df = daily_pivot_df.filter(col("created_date").isNotNull())
    # 找到最早的日期
    min_date_val = daily_pivot_df.agg(spark_min("created_date")).collect()[0][0]
    # 将最早日期广播，以便在UDF或表达式中使用
    daily_pivot_df = daily_pivot_df.withColumn(
        "days_since_start",
        datediff(col("created_date"), lit(min_date_val)) # 计算从最早日期开始的天数
    )

    daily_pivot_df.show(5)

    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import VectorAssembler


    assembler = VectorAssembler(inputCols=["days_since_start"], outputCol="features") # 更改此处
    lr_data = assembler.transform(daily_pivot_df)
    lr_data.cache()
    lr_data.count()

    lr_model = LinearRegression(featuresCol="features", labelCol="positive_count", regParam=0.01)
    lr_model_fit = lr_model.fit(lr_data)

    print("为积极情绪趋势训练的线性回归模型.")
    print(f"线性回归模型的截距 (intercept): {lr_model_fit.intercept}")
    print(f"线性回归模型的系数 (coefficient for days_since_start): {lr_model_fit.coefficients[0]}") # 打印系数

    max_date_row = daily_pivot_df.agg({"created_date": "max"}).collect()[0]
    max_date = max_date_row["max(created_date)"] if max_date_row and max_date_row["max(created_date)"] else date.today()

    # 根据新的特征生成未来的输入数据
    max_days_since_start = daily_pivot_df.agg({"days_since_start": "max"}).collect()[0][0] if daily_pivot_df.count() > 0 else 0

    future_dates = []
    for i in range(1, 8):
        future_day = max_date + timedelta(days=i)
        future_days_since_start = max_days_since_start + i # 未来天数索引是当前最大索引+1，+2...
        future_dates.append((future_day, float(future_days_since_start)))

    # 修改 spark.createDataFrame 的结构，因为现在只有 created_date 和 days_since_start
    future_df = spark.createDataFrame(future_dates, ["created_date", "days_since_start"]) # 更改此处
    future_df = assembler.transform(future_df)
    future_df.cache()
    future_df.count()

    future_predictions = lr_model_fit.transform(future_df)

    future_predictions = future_predictions.withColumn(
        "prediction",
        when(col("prediction") < 0, 0).otherwise(col("prediction")).cast(IntegerType())
    )

    print("未来趋势预测(积极情绪计数):")
    future_predictions.select("created_date", "prediction").show(truncate=False)

    trend_predictions_df = future_predictions.select(
        date_format(col("created_date"), "yyyy-MM-dd").alias("date"),
        col("prediction").alias("predicted_positive_count")
    )

    daily_pivot_df.unpersist()
    lr_data.unpersist()
    future_df.unpersist()

    return trend_predictions_df, lr_model_fit


# --- 5. 结果汇总与存储 (优化：分批存储和最终合并) ---
def store_results_optimized(sentiment_analysis_df, evaluation_results, trend_predictions_df, nb_model, lr_model):
    print(f"将所有结果存储到一个JSON文件中 {FINAL_OUTPUT_JSON_PATH}...")

    # 1. 保存模型到单独的文件夹
    try:
        nb_model_path = MODEL_SAVE_PATH + "naive_bayes_model"
        lr_model_path = MODEL_SAVE_PATH + "linear_regression_model"

        # 确保模型保存目录存在（使用 Spark API）
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI(MODEL_SAVE_PATH), hadoop_conf)
        model_save_path_obj = sc._jvm.org.apache.hadoop.fs.Path(MODEL_SAVE_PATH)
        if not fs.exists(model_save_path_obj):
            fs.mkdirs(model_save_path_obj) # 创建目录

        nb_model.save(nb_model_path)
        print(f"NaiveBayes模型保存到 {nb_model_path}")

        lr_model.save(lr_model_path)
        print(f"线性回归模型保存到 {lr_model_path}")
    except Exception as e:
        print(f"保存模型时发生错误: {e}")
        # 如果模型保存失败，不影响后续数据保存

    # 2. 将舆情分析内容写入临时 HDFS 目录的 JSON part 文件
    print(f"将舆情分析内容写入临时HDFS目录: {TEMP_ANALYSIS_JSON_DIR}")
    analysis_content_to_write = sentiment_analysis_df.select(
        col("content_text").alias("content"),
        date_format(col("created_date"), "yyyy-MM-dd").alias("date"),
        col("predicted_sentiment").alias("sentiment_label")
    )
    # 不使用 coalesce(1) 来避免单点内存压力，让Spark生成多个part文件
    analysis_content_to_write.write.mode("overwrite").json(TEMP_ANALYSIS_JSON_DIR)
    print("舆情分析内容已写入临时JSON文件。")

    # 3. 将算法评估结果和趋势预测结果 collect 到 Driver
    # 这些数据量通常较小，可以直接 collect
    evaluation_results_list = evaluation_results # 已经是dict，无需转换
    trend_predictions_list = trend_predictions_df.toJSON().map(json.loads).collect()

    # 4. 读取临时目录下的JSON part文件内容，并在Driver端合并
    # 这一步可能会有I/O开销，但避免了Driver端一次性加载所有RDD数据到内存
    all_analysis_results = {
        "public_opinion_analysis": [], # 留空，稍后从HDFS读取填充
        "algorithm_evaluation": evaluation_results_list,
        "public_opinion_trend_predictions": trend_predictions_list
    }

    # 读取临时JSON文件，逐行处理
    print(f"从临时HDFS目录 {TEMP_ANALYSIS_JSON_DIR} 读取舆情分析内容并合并...")
    try:
        json_lines_rdd = sc.textFile(TEMP_ANALYSIS_JSON_DIR)
        for line in json_lines_rdd.collect(): # collect() 会将所有行拉到Driver
            try:
                all_analysis_results["public_opinion_analysis"].append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"解码JSON行失败: {line}, 错误: {e}")

        print(f"成功从临时文件中读取舆情分析内容。总行数: {len(all_analysis_results['public_opinion_analysis'])}")

    except Exception as e:
        print(f"处理临时文件时发生意外错误: {e}")
        print("请检查HDFS路径和权限。如果错误持续，可能是数据量过大导致Driver内存不足。")


    # 5. 将最终的字典转换为JSON字符串，然后通过SparkContext写入单个文件
    try:
        final_json_string = json.dumps(all_analysis_results, ensure_ascii=False, indent=2) # indent=2 格式化输出

        # 将单个JSON字符串作为RDD的单个元素写入HDFS，确保写入一个文件
        # 先删除目标文件，否则saveAsTextFile会创建_SUCCESS文件
        delete_hdfs_path_spark_api(FINAL_OUTPUT_JSON_PATH) # 确保删除整个目录，而不是只删除文件

        sc.parallelize([final_json_string]).coalesce(1).saveAsTextFile(FINAL_OUTPUT_JSON_PATH)
        print(f"所有结果已成功存储到 {FINAL_OUTPUT_JSON_PATH}。")
    except Exception as e:
        print(f"存储合并结果到最终JSON时发生错误: {e}")
        print("警告：即使采取了分批写入，将所有结果在Driver端合并成一个JSON字符串仍可能导致内存溢出，尤其当public_opinion_analysis数据量极大时。")
        print("如果发生内存溢出，请考虑放弃单个大JSON文件，转而存储为多个独立的文件（如Data_machinelearning.py）。")

    # 清理临时目录
    delete_hdfs_path_spark_api(TEMP_ANALYSIS_JSON_DIR)


# --- 主执行流程 ---
if __name__ == "__main__":
    try:
        # 1. 数据加载与预处理
        public_opinion_df, sentiment_dict_df = load_and_preprocess_data()

        # 2. 情感分类器训练与应用
        sentiment_predicted_df, nb_model, sentiment_test_data = train_and_apply_sentiment_model(public_opinion_df,
                                                                                                sentiment_dict_df)

        # 3. 算法评估
        evaluation_results = evaluate_sentiment_model(nb_model, sentiment_test_data)

        # 4. 舆情趋势预测
        trend_predictions_df, lr_model = predict_public_opinion_trend(sentiment_predicted_df)

        # 5. 结果汇总与存储 (使用优化后的函数)
        store_results_optimized(sentiment_predicted_df, evaluation_results, trend_predictions_df, nb_model, lr_model)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 清除所有缓存数据
        spark.catalog.clearCache()
        # 停止SparkSession
        spark.stop()
        print("SparkSession stopped.")