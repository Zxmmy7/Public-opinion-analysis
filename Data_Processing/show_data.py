from pyspark.sql import SparkSession

# --- 配置部分 ---
# 处理后的数据在HDFS上的路径
hdfs_processed_data_path = "hdfs://node01:8020/user/spark/Aprocessed_data"
# --- 配置部分结束 ---

def analyze_processed_data(input_path):
    """
    从HDFS读取处理后的数据并进行分析。

    Args:
        input_path (str): HDFS上处理后的数据路径。
    """
    print("正在初始化 SparkSession...")
    spark = SparkSession.builder \
        .appName("Analyze Processed Data") \
        .getOrCreate()
    print("SparkSession 初始化成功。")

    try:
        print(f"正在从 HDFS 路径 {input_path} 加载处理后的数据...")
        # 从HDFS读取Parquet格式的数据
        df_processed = spark.read.parquet(input_path)
        print("数据加载成功。")
        print("处理后数据的 Schema:")
        df_processed.printSchema()
        print(f"处理后数据的行数: {df_processed.count()}")

        # --- 在这里进行您后续的数据分析或操作 ---
        # 示例：显示前几行数据
        print("\n处理后数据的前5行:")
        df_processed.show(50)


    except Exception as e:
        print(f"分析过程中发生错误: {e}")
    finally:
        print("正在停止 SparkSession...")
        spark.stop()
        print("SparkSession 已停止。")

if __name__ == "__main__":
    print("开始分析处理后的数据脚本...")
    analyze_processed_data(hdfs_processed_data_path)
    print("分析处理后的数据脚本执行完毕。")