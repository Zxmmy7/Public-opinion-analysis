from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, size, array_contains, explode, count, when, pandas_udf, lit
from pyspark.sql.types import ArrayType, StringType, DoubleType, StructType, StructField, LongType
from pyspark.sql.pandas.functions import PandasUDFType
from pyspark.ml.feature import Tokenizer, Word2Vec, HashingTF, IDF, StopWordsRemover, VectorAssembler
from pyspark.ml.classification import LogisticRegression, NaiveBayes, LinearSVC
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.linalg import Vectors, VectorUDT
import numpy as np
import pandas as pd
import jieba # 用于中文分词，需要在集群上安装
import json # 用于保存评估结果
import os

# --- 环境变量设置 (保持不变) ---
os.environ['PYSPARK_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['SPARK_HOME'] = '/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master spark://node01:7077 pyspark-shell'

# --- 1. 初始化SparkSession (保持不变) ---
spark = (SparkSession.builder \
    .appName("SentimentAnalysisWithExternalDictionary") \
    .master("spark://node01:7077") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://node01:8020") \
    .getOrCreate())

# --- 2. 加载数据 ---

# 2.1 加载您的主分析数据 (Parquet 格式)
# 假设您的主分析数据位于 HDFS 的 /user/spark/analysis_results_final.parquet 目录下
# 并且该目录下可能包含多个 part-xxxx 文件，Spark 可以直接读取目录
analysis_data_path = "hdfs://node01:8020//user/spark/processed_data/part-00000-b3bdacee-b6aa-4323-91d6-7cd34965602e-c000.snappy.parquet"
try:
    # 读取 Parquet 文件，Spark 会自动推断 Schema
    df_analysis = spark.read.parquet(analysis_data_path)
    # 确保 '内容' 列存在，如果不存在，需要根据实际数据 Schema 调整
    if "内容" not in df_analysis.columns:
        print(f"Warning: '内容' column not found in {analysis_data_path}. Please check your Parquet file schema.")
        # 这里您可以选择抛出错误，或者尝试使用其他列
except Exception as e:
    print(f"Error loading analysis data from {analysis_data_path}: {e}")
    spark.stop()
    exit()

# 2.2 加载情感词典 (CSV 格式) (保持不变)
sentiment_dict_path = "hdfs://node01:8020/user/spark/weibo_senti_100k.csv" # 假设情感词典路径
df_sentiment_dict = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(sentiment_dict_path)

# 确保情感词典的列名与模型期望的匹配，并转换为 DoubleType
# 情感词典的review列对应主数据的'内容'，label列对应分类模型的'label'
df_sentiment_dict = df_sentiment_dict.select(
    col("review").alias("内容"),
    col("label").cast(DoubleType()) # 确保label是DoubleType
)

# 2.3 合并数据 (保持不变)
# 这里假设主分析数据可能没有label列，或者label列需要从情感词典中获取先验知识
# 如果您的主分析数据本来就带label，可以调整合并策略，例如只用词典增强特征或单独训练
# 这里我们采用简单合并，将词典数据作为带有明确标签的样本加入
# 首先，为主分析数据添加一个占位符label列（如果它没有真实标签）
# 如果df_analysis有真实标签，请根据您的业务逻辑处理合并冲突或优先使用哪一个标签
if "label" not in df_analysis.columns:
    df_analysis = df_analysis.withColumn("label", lit(None).cast(DoubleType())) # 暂时为空，稍后可能填充或在训练时跳过无标签数据

# 为了合并，确保两者的列一致 (至少包含 '内容' 和 'label')
# 如果df_analysis有更多列，这里只选择需要的列进行合并
df_combined_data = df_analysis.select("内容", "label").unionByName(df_sentiment_dict.select("内容", "label"))

# 过滤掉内容为空的行，以及label为None的行（如果df_analysis中存在无标签数据）
df_combined_data = df_combined_data.filter(col("内容").isNotNull() & col("label").isNotNull())


# 2.2 加载情感词典 (CSV 格式)
sentiment_dict_path = "hdfs://node01:8020/user/spark/weibo_senti_100k.csv" # 假设情感词典路径
df_sentiment_dict = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(sentiment_dict_path)

# 确保情感词典的列名与模型期望的匹配，并转换为 DoubleType
# 情感词典的review列对应主数据的'内容'，label列对应分类模型的'label'
df_sentiment_dict = df_sentiment_dict.select(
    col("review").alias("内容"),
    col("label").cast(DoubleType()) # 确保label是DoubleType
)

# 2.3 合并数据 (假设情感词典作为额外的训练数据)
# 这里假设主分析数据可能没有label列，或者label列需要从情感词典中获取先验知识
# 如果您的主分析数据本来就带label，可以调整合并策略，例如只用词典增强特征或单独训练
# 这里我们采用简单合并，将词典数据作为带有明确标签的样本加入
# 首先，为主分析数据添加一个占位符label列（如果它没有真实标签）
# 如果df_analysis有真实标签，请根据您的业务逻辑处理合并冲突或优先使用哪一个标签
if "label" not in df_analysis.columns:
    df_analysis = df_analysis.withColumn("label", lit(None).cast(DoubleType())) # 暂时为空，稍后可能填充或在训练时跳过无标签数据

# 为了合并，确保两者的列一致 (至少包含 '内容' 和 'label')
# 如果df_analysis有更多列，这里只选择需要的列进行合并
df_combined_data = df_analysis.select("内容", "label").unionByName(df_sentiment_dict.select("内容", "label"))

# 过滤掉内容为空的行，以及label为None的行（如果df_analysis中存在无标签数据）
df_combined_data = df_combined_data.filter(col("内容").isNotNull() & col("label").isNotNull())


# --- 3. 增强特征工程 ---

# 3.1. 中文分词 UDF (保持不变)
@udf(ArrayType(StringType()))
def jieba_tokenize_udf(text):
    if text is None:
        return []
    return list(jieba.cut(text))

# 应用分词到合并后的数据
df_features = df_combined_data.withColumn("words_jieba", jieba_tokenize_udf(col("内容")))

# 3.2. 移除停用词 (保持不变)
chinese_stopwords = ["的", "是", "了", "和", "就", "也", "都", "个", "等", "我", "你", "他", "她", "我们"]
stopwords_remover = StopWordsRemover(inputCol="words_jieba", outputCol="filtered_words", stopWords=chinese_stopwords)
df_features = stopwords_remover.transform(df_features)

# 3.3. Word2Vec 词嵌入 (保持不变)
word2Vec = Word2Vec(vectorSize=100, minCount=5, inputCol="filtered_words", outputCol="word_vectors_raw")
word2Vec_model = word2Vec.fit(df_features) # 注意这里用df_features来fit模型
df_features = word2Vec_model.transform(df_features)

# 定义UDF来聚合每个文档的词向量（例如求平均）
@udf(VectorUDT()) # 确保返回类型是 VectorUDT
def aggregate_word_vectors_udf(list_of_vectors):
    if not list_of_vectors or all(v is None for v in list_of_vectors):
        return Vectors.dense([0.0] * 100)

    valid_vectors = []
    for v in list_of_vectors:
        if v is not None:
            # Word2Vec输出的通常是DenseVector，直接用toArray()
            valid_vectors.append(v.toArray() if hasattr(v, 'toArray') else v)

    if not valid_vectors:
        return Vectors.dense([0.0] * 100)

    mean_vec = np.mean(valid_vectors, axis=0)
    return Vectors.dense(mean_vec)

df_features = df_features.withColumn("features", aggregate_word_vectors_udf(col("word_vectors_raw")))

# --- 4. 深度学习情感分类模型（通过Pandas UDF模拟） ---
# (这部分保持原样，因为它是一个模拟，不会直接用于主要MLlib模型训练)
INPUT_DIM = 100 # 对应Word2Vec的vectorSize
schema = StructType([
    StructField("prediction", DoubleType()),
    StructField("probability", ArrayType(DoubleType()))
])

@pandas_udf(schema, PandasUDFType.SCALAR_ITER)
def predict_sentiment_deep_learning(iterator: pd.Series) -> pd.DataFrame:
    results = []
    for pd_series in iterator:
        input_data = np.array([vec.toArray() for vec in pd_series if vec is not None])

        if input_data.size == 0:
            predicted_classes = np.zeros(len(pd_series))
            probabilities_list = [np.zeros(3).tolist()] * len(pd_series) # 假设3个类别
        else:
            simulated_predictions = np.random.rand(len(input_data), 3) # 模拟3个类别
            predicted_classes = np.argmax(simulated_predictions, axis=1)
            probabilities_list = [arr.tolist() for arr in simulated_predictions]

        results.append(pd.DataFrame({
            "prediction": predicted_classes.astype(float),
            "probability": probabilities_list
        }))
    return pd.concat(results)

# 如果需要，可以将模拟的深度学习预测应用到 df_features 上
# df_with_dl_predictions = df_features.withColumn("dl_prediction_result", predict_sentiment_deep_learning(col("features")))
# df_with_dl_predictions = df_with_dl_predictions.select(
#     "*",
#     col("dl_prediction_result.prediction").alias("dl_predicted_label"),
#     col("dl_prediction_result.probability").alias("dl_probabilities")
# )


# --- 5. PySpark MLlib分类模型训练与评估 ---

# 划分训练集和测试集
# 注意：这里在 df_features 上进行划分，它包含了合并后的数据
(trainingData, testData) = df_features.randomSplit([0.8, 0.2], seed=42)

# 构建分类器实例
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
nb = NaiveBayes(featuresCol="features", labelCol="label", smoothing=1.0)
svm = LinearSVC(featuresCol="features", labelCol="label", maxIter=10, rawPredictionCol="rawPredictionSVC") # 为SVC指定不同的rawPredictionCol

# 评估器 (多分类)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
binary_evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC") # 用于二分类，如果您的label是0,1

print("\n--- Logistic Regression Model ---")
lr_model = lr.fit(trainingData)
lr_predictions = lr_model.transform(testData)
accuracy_lr = evaluator.evaluate(lr_predictions)
f1_lr = evaluator.evaluate(lr_predictions, {evaluator.metricName: "f1"})
precision_lr = evaluator.evaluate(lr_predictions, {evaluator.metricName: "weightedPrecision"})
recall_lr = evaluator.evaluate(lr_predictions, {evaluator.metricName: "weightedRecall"})
print(f"LR 准确率: {accuracy_lr}")
print(f"LR F1-Score: {f1_lr}, Weighted Precision: {precision_lr}, Weighted Recall: {recall_lr}")

# 朴素贝叶斯模型 (如果数据是多分类，确保是适合的平滑参数)
print("\n--- Naive Bayes Model ---")
nb_model = nb.fit(trainingData)
nb_predictions = nb_model.transform(testData)
accuracy_nb = evaluator.evaluate(nb_predictions)
f1_nb = evaluator.evaluate(nb_predictions, {evaluator.metricName: "f1"})
print(f"NB 准确率: {accuracy_nb}")
print(f"NB F1-Score: {f1_nb}")

# 线性SVC模型 (注意：LinearSVC的rawPredictionCol默认与LogisticRegression冲突，已修改)
print("\n--- Linear SVC Model ---")
svm_model = svm.fit(trainingData)
svm_predictions = svm_model.transform(testData)
accuracy_svm = evaluator.evaluate(svm_predictions)
f1_svm = evaluator.evaluate(svm_predictions, {evaluator.metricName: "f1"})
print(f"SVM 准确率: {accuracy_svm}")
print(f"SVM F1-Score: {f1_svm}")


# --- 6. 算法评估与超参数调优 (以Logistic Regression为例) ---

# 构建包含LR的Pipeline
ml_pipeline = Pipeline(stages=[lr])

# 定义参数网格
paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

# 创建交叉验证器
crossval = CrossValidator(estimator=ml_pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(labelCol="label"),
                          numFolds=3)

# 运行交叉验证 (在全部有标签的数据上)
print("\n--- Running Cross Validation for Logistic Regression ---")
cvModel = crossval.fit(df_features) # 在df_features上进行交叉验证，因为它包含了所有带标签的数据
best_lr_model = cvModel.bestModel
print("最佳LR模型参数:")
# 打印最佳模型中最后一个阶段（LR模型）的参数
print({param.name: best_lr_model.stages[-1].getOrDefault(param) for param in best_lr_model.stages[-1].params})

# 获取最佳模型在测试集上的性能
best_lr_predictions = best_lr_model.transform(testData)
accuracy_best_lr = evaluator.evaluate(best_lr_predictions)
f1_best_lr = evaluator.evaluate(best_lr_predictions, {evaluator.metricName: "f1"})
precision_best_lr = evaluator.evaluate(best_lr_predictions, {evaluator.metricName: "weightedPrecision"})
recall_best_lr = evaluator.evaluate(best_lr_predictions, {evaluator.metricName: "weightedRecall"})
print(f"最佳LR模型在测试集上的准确率: {accuracy_best_lr}")
print(f"最佳LR模型F1-Score: {f1_best_lr}, Weighted Precision: {precision_best_lr}, Weighted Recall: {recall_best_lr}")


# --- 7. 结果保存与前端展示数据准备 ---
evaluation_results = {
    "LogisticRegression": {
        "accuracy": accuracy_lr,
        "f1_score": f1_lr,
        "precision": precision_lr,
        "recall": recall_lr
    },
    "NaiveBayes": {
        "accuracy": accuracy_nb,
        "f1_score": f1_nb
    },
    "LinearSVC": {
        "accuracy": accuracy_svm,
        "f1_score": f1_svm
    },
    "BestLogisticRegression_CV": {
        "accuracy": accuracy_best_lr,
        "f1_score": f1_best_lr,
        "precision": precision_best_lr,
        "recall": recall_best_lr,
        "best_params": {param.name: str(best_lr_model.stages[-1].getOrDefault(param)) for param in best_lr_model.stages[-1].params}
    }
}

# 将结果写入本地文件
output_json_path = "enhanced_sentiment_analysis_results.json"
with open(output_json_path, "w", encoding="utf-8") as f:
    json.dump(evaluation_results, f, ensure_ascii=False, indent=4)
print(f"\n评估结果已保存到 {output_json_path}")

# --- 停止SparkSession (保持不变) ---
spark.stop()