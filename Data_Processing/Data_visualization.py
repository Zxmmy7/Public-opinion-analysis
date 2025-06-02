from pyspark.sql.types import StringType, StructType, StructField, LongType
import json
import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_unixtime, hour, dayofmonth, month, year, sum, count, avg, round, desc, asc, lit, explode, split,round as spark_round,when
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField, ArrayType,DoubleType
import json
import pandas as pd # 用于处理情感词典，在驱动节点上使用
import jieba # 用于中文分词
import builtins
from py4j.java_gateway import JavaGateway # 导入JavaGateway，用于访问Java对象
from pyspark import SparkContext

# ... (你现有的 PYSPARK_PYTHON 和 PYSPARK_DRIVER_PYTHON 设置不变)
os.environ['PYSPARK_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'

# 核心步骤：设置 SPARK_HOME 指向你虚拟机上 Spark 3.2.0 (scala2.13) 的安装路径
os.environ['SPARK_HOME'] = '/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master spark://node01:7077 pyspark-shell'




spark = (SparkSession.builder \
    .appName("DataVisualization") \
    .master("spark://node01:7077") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://node01:8020") \
    .getOrCreate())

# 设置日志级别，减少不必要的输出
spark.sparkContext.setLogLevel("WARN")

# HDFS上的Parquet文件路径
hdfs_input_path = "hdfs://node01:8020/user/spark/processed_data"




#两者in和out用处有差异
sc = SparkContext.getOrCreate()

# 定义您的HDFS输出目录路径
output_hdfs_path = "hdfs://node01:8020/user/spark/analysis_results_final"


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


delete_hdfs_path_recursively_if_exists(sc, output_hdfs_path)


#程序正式开始

try:
    df = spark.read.parquet(hdfs_input_path)
    print(f"Data loaded from {hdfs_input_path}. Schema:")
    df.printSchema()
    df.show(5)
except Exception as e:
    print(f"Error loading data from HDFS: {e}")
    spark.stop()
    exit()

# --- 关键修正部分：先重命名列，再进行类型转换 ---
# 将列名统一，方便后续操作
df = df.withColumnRenamed("内容", "content") \
    .withColumnRenamed("创建日期时间", "created_at") \
    .withColumnRenamed("点赞数", "likes") \
    .withColumnRenamed("评论数", "comments") \
    .withColumnRenamed("分享数", "shares") \
    .withColumnRenamed("IP属地", "ip_location") \
    .withColumnRenamed("性别", "gender")  # 你的数据没有“昵称”列，所以这里移除了

# 数据类型转换，确保数值列为数值类型
# created_at 列在你的Schema中已经是 string，我们假设它能被直接转换为 timestamp
df = df.withColumn("likes", col("likes").cast(IntegerType())) \
    .withColumn("comments", col("comments").cast(IntegerType())) \
    .withColumn("shares", col("shares").cast(IntegerType()))

print("\nSchema after renaming and type casting:")
df.printSchema()

# --- 情感词典加载和处理 ---
# 加载情感词典
sentiment_words_path = "weibo_senti_100k.csv"  # 请确保这个文件存在于你的PySpark作业运行的机器上

positive_words = set()
negative_words = set()

try:
    sentiment_df_pd = pd.read_csv(sentiment_words_path)

    # 提取积极词汇 (label=1)
    positive_reviews = sentiment_df_pd[sentiment_df_pd['label'] == 1]['review'].tolist()
    for review in positive_reviews:
        # 使用jieba进行分词，并添加到正面词典
        positive_words.update(jieba.cut(str(review)))

    # 提取消极词汇 (label=0)
    negative_reviews = sentiment_df_pd[sentiment_df_pd['label'] == 0]['review'].tolist()
    for review in negative_reviews:
        # 使用jieba进行分词，并添加到负面词典
        negative_words.update(jieba.cut(str(review)))

    # 移除通用词或停用词，避免中性词进入情感词典
    common_words = set(
        ["的", "了", "是", "我", "你", "他", "她", "它", "我们", "你们", "他们", "和", "或", "但是", "所以", "都", "也",
         "很", "非常", "不", "没有", "有", "个", "这", "那", "一个", "什么", "怎么", "哪里", "谁", "什么时候", "好",
         "不好", "大", "小", "多", "少", "一点", "一些", "很多", "很少"])  # 示例停用词
    positive_words = positive_words - common_words
    negative_words = negative_words - common_words

    # 避免词语同时出现在积极和消极词典中
    common_in_senti = positive_words.intersection(negative_words)
    positive_words = positive_words - common_in_senti
    negative_words = negative_words - common_in_senti

    print(
        f"Loaded {len(positive_words)} positive words and {len(negative_words)} negative words from {sentiment_words_path}.")

except Exception as e:
    print(f"Error loading sentiment words from local file: {e}")
    print("Please ensure 'weibo_senti_100k.csv' exists and is accessible. Using empty sentiment dictionaries.")
    positive_words = set()
    negative_words = set()


# 定义情感分析UDF (User-Defined Function)
def analyze_sentiment(text):
    if text is None:
        return "neutral"

    import jieba

    text_segmented = list(jieba.cut(str(text).lower()))
    positive_count = 0
    negative_count = 0

    for word in text_segmented:
        if word in positive_words:
            positive_count += 1
        if word in negative_words:
            negative_count += 1

    if positive_count > negative_count:
        return "positive"
    elif negative_count > positive_count:
        return "negative"
    else:
        return "neutral"


sentiment_udf = udf(analyze_sentiment, StringType())

# 应用情感分析UDF
df = df.withColumn("sentiment", sentiment_udf(col("content")))
df.cache()  # 缓存DataFrame，因为后续会多次使用
print("\nDataFrame with sentiment column:")
df.show(5)

# --- 执行探索性分析并生成JSON数据 (此部分与之前基本一致，但已修正列名) ---
results = {}  # 存储所有分析结果的字典


# 辅助函数：将Spark DataFrame转换为适合ECharts的JSON格式
def df_to_echarts_json(spark_df, x_axis_col, y_axis_col, series_name=None, value_cols=None):
    if value_cols is None:
        value_cols = [y_axis_col]

    pandas_df = spark_df.toPandas()

    echarts_data = []

    if series_name and len(value_cols) == 1:
        echarts_data = {
            'x_data': pandas_df[x_axis_col].astype(str).tolist(),
            'series_data': pandas_df[value_cols[0]].tolist(),
            'series_name': series_name
        }
    elif len(value_cols) > 1:
        echarts_data = {
            'x_data': pandas_df[x_axis_col].astype(str).tolist(),
            'series': []
        }
        for col_name in value_cols:
            echarts_data['series'].append({
                'name': col_name,
                'type': 'bar',
                'data': pandas_df[col_name].tolist()
            })
    else:
        echarts_data = pandas_df.to_dict(orient='records')

    return echarts_data


# 1. 使用情感词典分析正面信息总量、负面信息总量和中性信息总量
print("\n--- 1. 情感信息总量分析 ---")
sentiment_counts = df.groupBy("sentiment").count().collect()
sentiment_results = {row["sentiment"]: row["count"] for row in sentiment_counts}
results["sentiment_overall_counts"] = sentiment_results
print(sentiment_results)

# 2. 分析热门信息正面信息总量、负面信息总量和中性信息总量（点赞数>500即为热门信息）
print("\n--- 2. 热门信息情感分析 ---")
hot_posts_df = df.filter(col("likes") > 500)
hot_sentiment_counts = hot_posts_df.groupBy("sentiment").count().collect()
hot_sentiment_results = {row["sentiment"]: row["count"] for row in hot_sentiment_counts}
results["hot_posts_sentiment_counts"] = hot_sentiment_results
print(hot_sentiment_results)

# 3. 计算点赞、评论、分享数的99分位数，识别异常高互动内容
print("\n--- 3. 互动量99分位数及异常高互动内容 ---")
percentiles = {}
for col_name in ["likes", "comments", "shares"]:
    percentile_value = df.approxQuantile(col_name, [0.99], 0.01)[0]
    percentiles[col_name] = builtins.round(percentile_value, 2) # 使用Python内置的round函数
results["interaction_percentiles"] = percentiles
print(f"99th Percentiles: {percentiles}")

# 识别点赞数>10000的内容
extremely_high_likes_df = df.filter(col("likes") > 10000)
extremely_high_likes_count = extremely_high_likes_df.count()
results["extremely_high_likes_count"] = extremely_high_likes_count
print(f"Content with >10000 likes count: {extremely_high_likes_count}")



# 4. 按小时/天统计发帖量，识别流量高峰时段
def df_to_echarts_json(df_spark, x_axis_col, y_axis_col, series_name):
    """
    将PySpark DataFrame转换为ECharts所需的JSON格式。
    """
    data = df_spark.select(x_axis_col, y_axis_col).collect()
    x_data = [row[x_axis_col] for row in data]
    series_data = [row[y_axis_col] for row in data]

    return {
        "x_data": x_data,
        "series_data": series_data,
        "series_name": series_name
    }


print("\n--- 4. 发帖量按小时/天统计 ---")
# 假设 'created_at' 是可以被转换为时间戳的字符串，例如 'YYYY-MM-DD HH:MM:SS'
df_with_time = df.withColumn("timestamp", col("created_at").cast("timestamp")) \
    .withColumn("post_hour", hour(col("timestamp"))) \
    .withColumn("post_day", dayofmonth(col("timestamp"))) \
    .withColumn("post_month", month(col("timestamp"))) \
    .withColumn("post_year", year(col("timestamp")))

# 按小时统计
posts_per_hour = df_with_time.groupBy("post_hour").count().orderBy("post_hour").collect()

# 定义posts_per_hour的Schema
# count()函数通常返回LongType
posts_per_hour_schema = StructType([
    StructField("post_hour", IntegerType(), True),
    StructField("count", LongType(), True)
])

posts_per_hour_data = df_to_echarts_json(
    spark.createDataFrame(posts_per_hour, schema=posts_per_hour_schema), # 明确指定Schema
    x_axis_col="post_hour",
    y_axis_col="count",
    series_name="Posts per Hour"
)
results["posts_per_hour"] = posts_per_hour_data
print("Posts per hour:")
print(json.dumps(posts_per_hour_data, indent=2, ensure_ascii=False))

# 按天统计
posts_per_day = df_with_time.groupBy("post_year", "post_month", "post_day") \
    .count() \
    .orderBy("post_year", "post_month", "post_day") \
    .withColumn("date",
                col("post_year").cast(StringType()) +
                lit("-") +
                col("post_month").cast(StringType()) +
                lit("-") +
                col("post_day").cast(StringType())) \
    .select("date", "count") \
    .collect()

# 定义posts_per_day的Schema
# count()函数通常返回LongType
posts_per_day_schema = StructType([
    StructField("date", StringType(), True),
    StructField("count", LongType(), True)
])

posts_per_day_data = df_to_echarts_json(
    spark.createDataFrame(posts_per_day, schema=posts_per_day_schema), # 明确指定Schema
    x_axis_col="date",
    y_axis_col="count",
    series_name="Posts per Day"
)
results["posts_per_day"] = posts_per_day_data
print("Posts per day:")
print(json.dumps(posts_per_day_data, indent=2, ensure_ascii=False))

# 5. 标记舆情事件的引发期、高潮期、平息期阶段
print("\n--- 5. 舆情事件阶段标记 (简化版) ---")
posts_per_day_df = df_with_time.groupBy("post_year", "post_month", "post_day") \
    .count() \
    .orderBy("post_year", "post_month", "post_day") \
    .withColumn("date_str",
                col("post_year").cast(StringType()) +
                lit("-") +
                col("post_month").cast(StringType()) +
                lit("-") +
                col("post_day").cast(StringType()))

posts_per_day_pandas = posts_per_day_df.toPandas()
posts_per_day_pandas['date'] = pd.to_datetime(posts_per_day_pandas['date_str'])
posts_per_day_pandas = posts_per_day_pandas.set_index('date').sort_index()

if not posts_per_day_pandas.empty:
    max_posts_row = posts_per_day_pandas.loc[posts_per_day_pandas['count'].idxmax()]
    if isinstance(max_posts_row, pd.DataFrame):
        max_posts_row = max_posts_row.iloc[0] # Take the first row if it's a DataFrame
    peak_date = max_posts_row['date_str'] # This should now be a scalar string
    peak_count = max_posts_row['count']   # This should now be a scalar numeric type

    peak_date = str(peak_date)
    peak_count = int(peak_count)

    event_stages = {
        "peak_date": peak_date,
        "peak_count": peak_count,
        "stages_description": "此为简化识别，真实事件阶段标记需更复杂的算法。通常引发期是发帖量开始增长的时期，高潮期是发帖量达到峰值的时期，平息期是发帖量开始下降的时期。"
    }
else:
    event_stages = {"stages_description": "No data for event stage analysis."}

results["event_stages"] = event_stages
print(json.dumps(event_stages, indent=2, ensure_ascii=False))



# 6. 统计各IP属地的数量
print("\n--- 6. IP属地统计 ---")
ip_location_counts = df.groupBy("ip_location").count().orderBy(desc("count")).collect()
ip_location_data = df_to_echarts_json(
    spark.createDataFrame(ip_location_counts),
    x_axis_col="ip_location",
    y_axis_col="count",
    series_name="Posts by IP Location"
)
results["ip_location_counts"] = ip_location_data
print("IP Location Counts:")
print(json.dumps(ip_location_data, indent=2, ensure_ascii=False))


# 7. 对比不同性别用户的数量比例、平均发帖量、互动参与度、情感倾向差异
print("\n--- 7. 不同性别用户分析 ---")
# 数量比例
gender_counts = df.groupBy("gender").count().collect()
gender_ratio = {row["gender"]: row["count"] for row in gender_counts}
results["gender_ratio"] = gender_ratio
print(f"Gender Ratio: {gender_ratio}")

# 平均发帖量 (需要先统计每个用户的发帖量)
# 由于你的数据没有昵称（nickname）列，这里需要修正，我们将跳过此项或假定某种用户ID
# 鉴于你只提供了性别，我们将按性别统计发帖量
# 如果你实际有用户ID，请将其作为group by的依据
# 这里为了能运行，我们跳过按nickname统计发帖量，因为它不存在
# 如果你需要计算平均发帖量，需要有用户ID来区分不同的用户
results["avg_posts_per_gender"] = "Analysis skipped due to missing 'nickname' column for user-specific post count."
print(results["avg_posts_per_gender"])

# 互动参与度 (平均点赞、评论、分享)
# 假设 'likes', 'comments', 'shares' 存在且为数值类型，如果不是，需要先转换
df_with_numeric_interactions = df.withColumn("likes_numeric", col("likes").cast(DoubleType())) \
                                 .withColumn("comments_numeric", col("comments").cast(DoubleType())) \
                                 .withColumn("shares_numeric", col("shares").cast(DoubleType())) # 新增对 shares 的类型转换

interaction_per_gender = df_with_numeric_interactions.groupBy("gender").agg(
    spark_round(avg("likes_numeric"), 2).alias("avg_likes"), # 在 PySpark 层面四舍五入
    spark_round(avg("comments_numeric"), 2).alias("avg_comments"), # 在 PySpark 层面四舍五入
    spark_round(avg("shares_numeric"), 2).alias("avg_shares") # 在 PySpark 层面四舍五入
).collect()

interaction_gender_data = {
    row["gender"]: {
        "avg_likes": row["avg_likes"], # 直接使用已经四舍五入的值
        "avg_comments": row["avg_comments"], # 直接使用已经四舍五入的值
        "avg_shares": row["avg_shares"] # 直接使用已经四舍五入的值
    } for row in interaction_per_gender}
results["interaction_per_gender"] = interaction_gender_data
print(f"Interaction per Gender: {interaction_gender_data}")

# 情感倾向差异
sentiment_per_gender = df.groupBy("gender", "sentiment").count() \
    .withColumnRenamed("count", "sentiment_count") \
    .groupBy("gender") \
    .pivot("sentiment", ["positive", "negative", "neutral"]) \
    .sum("sentiment_count") \
    .fillna(0) \
    .collect()

sentiment_gender_data = {}
for row in sentiment_per_gender:
    gender = row["gender"]
    sentiment_gender_data[gender] = {
        "positive": row["positive"],
        "negative": row["negative"],
        "neutral": row["neutral"]
    }
results["sentiment_per_gender"] = sentiment_gender_data
print(f"Sentiment per Gender: {sentiment_gender_data}")



# 8. 统计IP属地间的内容分享数，绘制地域传播网络图
print("\n--- 8. 地域传播网络图 (简化版) ---")
shares_by_ip = df.groupBy("ip_location").agg(sum("shares").alias("total_shares")).orderBy(
    desc("total_shares")).collect()
shares_by_ip_data = df_to_echarts_json(
    spark.createDataFrame(shares_by_ip),
    x_axis_col="ip_location",
    y_axis_col="total_shares",
    series_name="Total Shares by IP Location"
)
results["shares_by_ip_location"] = shares_by_ip_data
print("Shares by IP Location (simplified for network visualization):")
print(json.dumps(shares_by_ip_data, indent=2, ensure_ascii=False))

# 9. 计算分享转化率（分享数/总互动量），分析高转化率内容的语义特征
print("\n--- 9. 分享转化率及高转化率内容语义特征 ---")

# Ensure 'likes', 'comments', 'shares' columns are numeric.
# If they might be strings in your actual data, uncomment and adjust these lines:
# from pyspark.sql.types import IntegerType # Or LongType, DoubleType
# df = df.withColumn("likes", col("likes").cast(IntegerType()))
# df = df.withColumn("comments", col("comments").cast(IntegerType()))
# df = df.withColumn("shares", col("shares").cast(IntegerType()))

df_with_total_interaction = df.withColumn("total_interaction", col("likes") + col("comments") + col("shares"))

# Calculate share_conversion_rate, handling division by zero
df_with_conversion_rate = df_with_total_interaction.withColumn(
    "share_conversion_rate",
    when(col("total_interaction") > 0, col("shares") / col("total_interaction")).otherwise(0.0)
)

# Filter out rows where total_interaction is not positive if desired,
# though 'when' clause above already handles the 0 case.
# df_with_conversion_rate = df_with_conversion_rate.filter(col("total_interaction") > 0)


avg_conversion_rate = df_with_conversion_rate.agg(avg("share_conversion_rate").alias("average_rate")).collect()[0][
    "average_rate"]

# The fix: Explicitly call Python's built-in round.
# If there's a conflict, you can be super explicit by using __builtins__.round
results["average_share_conversion_rate"] = __builtins__.round(avg_conversion_rate, 4)
print(f"Average Share Conversion Rate: {results['average_share_conversion_rate']}")

high_conversion_content = df_with_conversion_rate.orderBy(desc("share_conversion_rate")).limit(10).select("content",
                                                                                                          "sentiment",
                                                                                                          "share_conversion_rate").collect()
high_conversion_content_data = [row.asDict() for row in high_conversion_content]
results["high_conversion_content_examples"] = high_conversion_content_data
print("Top 10 High Conversion Content Examples:")
print(json.dumps(high_conversion_content_data, indent=2, ensure_ascii=False))

# 10. 分析不同地域用户对同一主题的情感差异
print("\n--- 10. 不同地域用户情感差异 ---")
sentiment_per_ip_location = df.groupBy("ip_location", "sentiment").count() \
    .withColumnRenamed("count", "sentiment_count") \
    .groupBy("ip_location") \
    .pivot("sentiment", ["positive", "negative", "neutral"]) \
    .sum("sentiment_count") \
    .fillna(0) \
    .collect()

sentiment_ip_data = {}
for row in sentiment_per_ip_location:
    location = row["ip_location"]
    sentiment_ip_data[location] = {
        "positive": row["positive"],
        "negative": row["negative"],
        "neutral": row["neutral"]
    }
results["sentiment_per_ip_location"] = sentiment_ip_data
print("Sentiment per IP Location:")
print(json.dumps(sentiment_ip_data, indent=2, ensure_ascii=False))

# 11. 基于历史数据训练回归模型，预测未来24小时的内容互动量趋势
print("\n--- 11. 内容互动量趋势预测 (概念性) ---")
results[
    "interaction_prediction_model"] = "This is a placeholder. Implementing a regression model for interaction prediction requires: 1. Feature Engineering (time, content features). 2. Model Selection (e.g., ARIMA, Prophet, or Supervised Learning with time-based features). 3. Model Training, Evaluation, and Prediction. This goes beyond basic Spark SQL analysis."
print(results["interaction_prediction_model"])

# 12. 结合时间分析舆情发展趋势
print("\n--- 12. 舆情发展趋势 (情感随时间变化) ---")
sentiment_over_time = df_with_time.groupBy("post_year", "post_month", "post_day", "sentiment") \
    .count() \
    .orderBy("post_year", "post_month", "post_day") \
    .withColumn("date_str",
                col("post_year").cast(StringType()) +
                lit("-") +
                col("post_month").cast(StringType()) +
                lit("-") +
                col("post_day").cast(StringType())) \
    .select("date_str", "sentiment", "count") \
    .collect()

sentiment_time_data_echarts = {
    'x_data': sorted(list(set([row["date_str"] for row in sentiment_over_time]))),
    'series': []
}

sentiment_types = ["positive", "negative", "neutral"]
for s_type in sentiment_types:
    data_points = []
    for date in sentiment_time_data_echarts['x_data']:
        found_count = next(
            (row["count"] for row in sentiment_over_time if row["date_str"] == date and row["sentiment"] == s_type), 0)
        data_points.append(found_count)
    sentiment_time_data_echarts['series'].append({
        'name': s_type,
        'type': 'line',
        'stack': 'Total',
        'areaStyle': {},
        'data': data_points
    })

results["sentiment_over_time"] = sentiment_time_data_echarts
print("Sentiment Over Time:")
print(json.dumps(sentiment_time_data_echarts, indent=2, ensure_ascii=False))

# 13. 分析频繁被使用的词组，将它们分为正面和负面
print("\n--- 13. 频繁词组分析 ---")
# 定义分词UDF
def segment_text_udf(text):
    if text is None:
        return []
    import jieba
    return list(jieba.cut(str(text)))


segment_udf = udf(segment_text_udf, ArrayType(StringType()))

# 应用分词UDF并炸开词语
df_words = df.withColumn("word", explode(segment_udf(col("content"))))

# 过滤空词语，并统计词频
word_counts = df_words.filter(col("word") != "").groupBy("word").count().orderBy(desc("count")).limit(50)
frequent_words = word_counts.collect()

# 结合情感词典对高频词进行情感分类
frequent_words_sentiment = []
for row in frequent_words:
    word = row["word"]
    count = row["count"]
    sentiment = "neutral"
    if word in positive_words:
        sentiment = "positive"
    elif word in negative_words:
        sentiment = "negative"
    frequent_words_sentiment.append({"word": word, "count": count, "sentiment": sentiment})

results["frequent_words_sentiment"] = frequent_words_sentiment
print("Frequent Words with Sentiment:")
print(json.dumps(frequent_words_sentiment, indent=2, ensure_ascii=False))

# 6. (原问题中重复的6) 按用户总互动量（点赞+评论+分享）筛选Top 100意见领袖
# 鉴于你的数据中没有“昵称”列，这里无法按昵称筛选。
# 如果你有一个唯一的用户ID列，可以替换“nickname”。
# 如果没有唯一用户ID，此分析项无法实现，或者只能通过某种聚合代理
results[
    "top_100_opinion_leaders"] = "Analysis skipped due to missing 'nickname' or other unique user identifier column."
print(results["top_100_opinion_leaders"])

# 最终将所有结果写入HDFS
output_hdfs_path = "hdfs://node01:8020/user/spark/analysis_results_final"
try:
    json_output_string = json.dumps(results, indent=2, ensure_ascii=False)

    # 为了确保只有一个输出文件，将结果合并为一个RDD分区
    rdd = spark.sparkContext.parallelize([json_output_string], 1)
    rdd.saveAsTextFile(output_hdfs_path)

    print(f"\nAnalysis results saved to {output_hdfs_path}")
except Exception as e:
    print(f"Error saving results to HDFS: {e}")

spark.stop()
print("SparkSession stopped.")