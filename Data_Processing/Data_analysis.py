from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import os



# 使用spark前先将一级评论和二级评论上传至hdfs中
#
# 指令：
#
# # 上传一级评论数据
# hdfs dfs -put /Axiangmu/tongbu/Visual_data/蜜雪冰城数据一级评论.csv /user/spark/new_data/蜜雪冰城数据一级评论.csv
#
# # 上传二级评论数据
# hdfs dfs -put /Axiangmu/tongbu/Visual_data/蜜雪冰城二级评论.csv /user/spark/new_data/蜜雪冰城二级评论.csv
#
# # 验证文件是否成功上传
# hdfs dfs -ls /user/spark/new_data/



# (你现有的 PYSPARK_PYTHON 和 PYSPARK_DRIVER_PYTHON 设置不变)
os.environ['PYSPARK_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Axiangmu/huanjing/myenv/bin/python3.9'
# 核心步骤：设置 SPARK_HOME 指向你虚拟机上 Spark 3.2.0 (scala2.13) 的安装路径
os.environ['SPARK_HOME'] = '/Axiangmu/software/spark-3.2.0-bin-hadoop3.2-scala2.13'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master spark://node01:7077 pyspark-shell'


# --- 配置部分 ---
# HDFS 输入路径 - 确保这里指向你上传的 HDFS 路径
# HDFS 的端口通常是 8020
hdfs_input_base_path = "hdfs://node01:8020/user/spark/new_data"
input_csv_level1_hdfs = f"{hdfs_input_base_path}/蜜雪冰城数据一级评论.csv"
input_csv_level2_hdfs = f"{hdfs_input_base_path}/蜜雪冰城二级评论.csv"

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

# 处理后的数据在HDFS上的保存基础路径
# 你可以根据需要修改这个基础路径
hdfs_output_base_path = "hdfs://node01:8020/user/spark/processed_data"

output_format = "parquet"  # 推荐使用 parquet 格式，高效且支持列式存储




# --- 主处理函数 ---
def run_spark_data_pipeline(
        level1_input_path,
        level2_input_path,
        final_column_map,
        hdfs_base_output_path,
        output_fmt,
        custom_output_file_name=None  # 用于自定义最终输出的目录名
):
    """
    整合的Spark数据处理管道：
    1. 从HDFS加载一级和二级评论数据。
    2. 清洗、翻译列名、删除空值。
    3. 合并数据。
    4. 将最终处理后的数据保存到HDFS，强制为单个 Parquet 文件。
    """
    print("开始执行整合的 Spark 数据处理管道...")
    print("正在初始化 SparkSession...")
    # 使用 .master("spark://node01:7077") 连接到 Spark 集群 Master
    spark = SparkSession.builder \
        .appName("Integrated Michel Comments Data Pipeline") \
        .master("spark://node01:7077") \
        .getOrCreate()
    print("SparkSession 初始化成功。")

    try:
        # --- 阶段 1: 一级评论数据处理 ---
        print("\n--- 阶段 1: 正在处理一级评论数据 ---")
        print(f"正在从 HDFS ({level1_input_path}) 加载一级评论数据...")
        df_level1 = spark.read.csv(level1_input_path, header=True, inferSchema=True)
        print("一级评论数据加载成功。原始 Schema:")
        df_level1.printSchema()
        print(f"原始一级评论行数: {df_level1.count()}")

        columns_to_delete_level1 = [
            'comment_id', 'create_time', 'note_id', 'last_modify_ts',
            'parent_comment_id', 'user_id', 'nickname', 'profile_url',
            'avatar', 'source_keyword', "note_url"
        ]

        existing_cols_level1 = [c for c in columns_to_delete_level1 if c in df_level1.columns]
        if existing_cols_level1:
            print(f"正在从一级评论数据中删除列: {', '.join(existing_cols_level1)}...")
            df_level1_processed = df_level1.drop(*existing_cols_level1)
            print("一级评论列删除完成。")
        else:
            print("一级评论数据中没有找到要删除的列。")
            df_level1_processed = df_level1.alias("df_level1_processed")

        original_rows_level1 = df_level1_processed.count()
        df_level1_processed = df_level1_processed.na.drop()
        rows_after_drop_na_level1 = df_level1_processed.count()
        print(f"  - 已从一级评论数据中删除 {original_rows_level1 - rows_after_drop_na_level1} 行含有空值的数据。")
        print(f"一级评论数据处理后行数: {rows_after_drop_na_level1}")
        df_level1_processed.printSchema()

        # --- 阶段 2: 二级评论数据处理 ---
        print("\n--- 阶段 2: 正在处理二级评论数据 ---")
        print(f"正在从 HDFS ({level2_input_path}) 加载二级评论数据...")
        df_level2 = spark.read.csv(level2_input_path, header=True, inferSchema=True)
        print("二级评论数据加载成功。原始 Schema:")
        df_level2.printSchema()
        print(f"原始二级评论行数: {df_level2.count()}")

        columns_to_delete_level2 = [
            'comment_id', 'create_time', 'note_id', 'last_modify_ts',
            'parent_comment_id', 'user_id', 'nickname', 'profile_url', 'avatar'
        ]

        existing_cols_level2 = [c for c in columns_to_delete_level2 if c in df_level2.columns]
        if existing_cols_level2:
            print(f"正在从二级评论数据中删除列: {', '.join(existing_cols_level2)}...")
            df_level2_processed = df_level2.drop(*existing_cols_level2)
            print("二级评论列删除完成。")
        else:
            print("二级评论数据中没有找到要删除的列。")
            df_level2_processed = df_level2.alias("df_level2_processed")

        original_rows_level2 = df_level2_processed.count()
        df_level2_processed = df_level2_processed.na.drop()
        rows_after_drop_na_level2 = df_level2_processed.count()
        print(f"  - 已从二级评论数据中删除 {original_rows_level2 - rows_after_drop_na_level2} 行含有空值的数据。")
        print(f"二级评论数据处理后行数: {rows_after_drop_na_level2}")
        df_level2_processed.printSchema()

        # --- 阶段 3: 合并数据 (Spark) ---
        print("\n--- 阶段 3: 正在合并一级和二级评论数据 ---")

        # 3.1. 重命名二级评论 DataFrame 中的列
        print("重命名二级评论 DataFrame 中的列以匹配一级评论...")
        df_level2_for_merge = df_level2_processed.withColumnRenamed('comment_like_count', 'liked_count') \
            .withColumnRenamed('sub_comment_count', 'comments_count')
        df_level2_for_merge.printSchema()

        # 3.2. 为二级评论添加 'shared_count' 列并赋值为 0
        print("为二级评论 DataFrame 添加 'shared_count' 列并赋值为 0...")
        df_level2_for_merge = df_level2_for_merge.withColumn('shared_count', lit(0))
        df_level2_for_merge.printSchema()

        # 3.3. 统一列顺序：确保两个 DataFrame 具有相同的列且顺序一致
        final_merged_columns_order = df_level1_processed.columns
        print(f"最终合并的列顺序将是: {final_merged_columns_order}")

        df_level2_aligned = df_level2_for_merge.select(
            [col(c) for c in final_merged_columns_order if c in df_level2_for_merge.columns])

        for c in final_merged_columns_order:
            if c not in df_level2_aligned.columns:
                df_level2_aligned = df_level2_aligned.withColumn(c, lit(None))

        df_level2_aligned = df_level2_aligned.select(final_merged_columns_order)
        print("二级评论 DataFrame 列已对齐。")
        df_level2_aligned.printSchema()

        # 3.4. 合并两个 DataFrame
        print("正在合并一级和二级评论 DataFrame...")
        merged_df = df_level1_processed.unionByName(df_level2_aligned)
        print("数据合并成功。合并后 Schema:")
        merged_df.printSchema()
        print(f"合并后总行数: {merged_df.count()}")

        # --- 阶段 4: 最终数据分析和 HDFS 上传 ---
        print("\n--- 阶段 4: 最终数据分析和上传 HDFS ---")
        df_final = merged_df

        # 4.1. 翻译列名
        print("正在翻译最终 DataFrame 的列名...")
        for old_name, new_name in final_column_map.items():
            if old_name in df_final.columns:
                df_final = df_final.withColumnRenamed(old_name, new_name)
                print(f"  - 列名 '{old_name}' 已翻译为 '{new_name}'")
            else:
                print(f"  - 警告: 原始列 '{old_name}' 不存在于最终数据中，跳过翻译。")
        print("列名翻译完成。")
        print("翻译后最终数据 Schema:")
        df_final.printSchema()

        # 4.2. 删除含有空值的行
        print("正在删除最终数据中含有空值的行...")
        original_rows_final = df_final.count()
        df_final = df_final.na.drop()
        rows_after_drop_na_final = df_final.count()
        print(f"  - 已从最终数据中删除 {original_rows_final - rows_after_drop_na_final} 行含有空值的数据。")
        print(f"删除空值行后最终数据行数: {rows_after_drop_na_final}")

        # 4.3. 构建最终的输出路径 (支持自定义文件名)
        final_output_hdfs_path = hdfs_base_output_path
        if custom_output_file_name:
            final_output_hdfs_path = f"{hdfs_base_output_path}/{custom_output_file_name}"
            print(f"自定义输出目录为: '{custom_output_file_name}'。")
        else:
            print("未指定自定义输出目录名，Spark 将生成一个默认名称。")

        print(f"最终数据输出路径将是: {final_output_hdfs_path}...")

        # 4.4. 将处理后的数据写入 HDFS，强制写入单个文件
        print(f"正在将处理后的最终数据以 {output_fmt} 格式写入 HDFS 路径: {final_output_hdfs_path}...")
        # 核心：使用 repartition(1) 来强制 Spark 将所有数据写入一个分区，从而生成一个文件
        df_final.repartition(1).write.format(output_fmt).mode("overwrite").save(final_output_hdfs_path)
        print("数据成功写入 HDFS。")

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
    finally:
        print("正在停止 SparkSession...")
        spark.stop()
        print("SparkSession 已停止。")


if __name__ == "__main__":
    run_spark_data_pipeline(
        level1_input_path=input_csv_level1_hdfs,
        level2_input_path=input_csv_level2_hdfs,
        final_column_map=column_translation_map,
        hdfs_base_output_path=hdfs_output_base_path,
        output_fmt=output_format,
    )
    print("整合的 Spark 数据处理管道执行完毕。")