from pyspark.sql import SparkSession
import os

# --- 配置部分 ---
csv_file_path_on_node01 = "/Axiangmu/tongbu/Visual_data/merged_AB.csv"

# 原始列名和翻译后的列名映射
column_translation_map = {
    "create_date_time": "创建日期时间",
    "content": "内容",
    "comments_count": "评论数",
    "liked_count": "点赞数",
    "shared_count": "分享数",
    "ip_location": "IP属地",
    "gender": "性别"
}



# 处理后的数据在HDFS上的保存路径
# *** HDFS 的端口是 8020 ***
hdfs_output_path = "hdfs://node01:8020/user/spark/processed_data"
output_format = "parquet"  # 使用parquet格式，因为它高效且支持列式存储
# --- 配置部分结束 ---

def process_data_with_spark(csv_input_path, column_map, hdfs_output_path, output_fmt):
    """
    使用Spark处理数据：翻译列名，删除列，删除空值行，并保存到HDFS。

    Args:
        csv_input_path (str): CSV文件在node01上的路径。
        column_map (dict): 原始列名到新列名的映射字典。
        hdfs_output_path (str): 处理后的数据在HDFS上的保存路径。
        output_fmt (str): 输出文件格式（例如："parquet", "csv"）。
    """
    # 1. 初始化 SparkSession
    print("正在初始化 SparkSession...")
    spark = SparkSession.builder \
        .appName("CSV Data Processor") \
        .getOrCreate()
    print("SparkSession 初始化成功。")

    try:
        # 2. 从 CSV 文件加载数据
        print(f"正在从 {csv_input_path} 加载数据...")
        # header=True 表示第一行是列名，inferSchema=True 表示自动推断数据类型
        df = spark.read.csv(csv_input_path, header=True, inferSchema=True)
        print("数据加载成功。")
        print("原始数据 Schema:")
        df.printSchema()
        print(f"原始数据行数: {df.count()}")

        # 3. 对 CSV 文件的列名进行翻译
        print("正在翻译列名...")
        for old_name, new_name in column_map.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
                print(f"  - 列名 '{old_name}' 已翻译为 '{new_name}'")
            else:
                print(f"  - 警告: 原始列 '{old_name}' 不存在于数据中，跳过翻译。")
        print("列名翻译完成。")
        print("翻译后数据 Schema:")
        df.printSchema()



        # 5. 删除含有空值的行
        print("正在删除含有空值的行...")
        original_rows = df.count()
        df = df.na.drop()
        rows_after_drop_na = df.count()
        print(f"  - 已删除 {original_rows - rows_after_drop_na} 行含有空值的数据。")
        print(f"删除空值行后数据行数: {rows_after_drop_na}")

        # 6. 将处理后的数据上传至 HDFS
        print(f"正在将处理后的数据以 {output_fmt} 格式写入 HDFS 路径: {hdfs_output_path}...")
        # mode("overwrite") 如果路径存在则覆盖，mode("append") 则追加
        # 如果路径不存在，Spark会自动创建
        df.write.format(output_fmt).mode("overwrite").save(hdfs_output_path)
        print("数据成功写入 HDFS。")

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
    finally:
        # 停止 SparkSession
        print("正在停止 SparkSession...")
        spark.stop()
        print("SparkSession 已停止。")

if __name__ == "__main__":
    print("开始数据处理...")
    process_data_with_spark(
        csv_file_path_on_node01,
        column_translation_map,
        hdfs_output_path,
        output_format
    )
    print("数据处理执行完毕。")