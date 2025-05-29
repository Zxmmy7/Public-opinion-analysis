import pandas as pd

def delete_columns_from_csv(input_csv_file='A.csv', output_csv_file='A_processed.csv'):
    """
    从指定的 CSV 文件中删除不需要的列。

    Args:
        input_csv_file (str): 输入 CSV 文件的名称。默认为 'A.csv'。
        output_csv_file (str): 处理后数据保存的 CSV 文件名称。默认为 'A_processed.csv'。
                                如果设置为与 input_csv_file 相同，将覆盖原文件。
    """
    try:
        # 1. 加载 CSV 文件
        print(f"正在加载 CSV 文件: {input_csv_file}...")
        df = pd.read_csv(input_csv_file)
        print("CSV 文件加载成功。")

        # 2. 指定要删除的列
        columns_to_delete = [
            'comment_id',  # 假设这个列是你想删除的，尽管你最初提供的A.csv列名中没有
            'create_time',
            'note_id',
            'last_modify_ts',
            'parent_comment_id', # 假设这个列是你想删除的，尽管你最初提供的A.csv列名中没有
            'user_id',
            'nickname',
            'profile_url',
            'avatar'
        ]

        # 过滤掉实际不存在于DataFrame中的列，以避免KeyError
        existing_columns_to_delete = [col for col in columns_to_delete if col in df.columns]
        non_existing_columns = [col for col in columns_to_delete if col not in df.columns]

        if non_existing_columns:
            print(f"注意：以下列在文件中不存在，将被跳过删除: {', '.join(non_existing_columns)}")

        # 3. 删除列
        if existing_columns_to_delete:
            print(f"正在删除列: {', '.join(existing_columns_to_delete)}...")
            df_processed = df.drop(columns=existing_columns_to_delete, axis=1)
            print("列删除成功。")
        else:
            print("没有找到要删除的列（或所有指定列都不存在）。")
            df_processed = df.copy() # 如果没有列要删除，则保持DataFrame不变


        # 4. 保存处理后的数据
        print(f"正在将处理后的数据保存到: {output_csv_file}...")
        df_processed.to_csv(output_csv_file, index=False) # index=False 避免将 DataFrame 索引写入 CSV
        print("数据保存成功。")
        print(f"处理完成！处理后的文件已保存为 {output_csv_file}")

    except FileNotFoundError:
        print(f"错误：文件 '{input_csv_file}' 未找到。请确保文件在正确的路径下。")
    except Exception as e:
        print(f"发生了一个错误: {e}")

# 调用函数执行删除操作
if __name__ == "__main__":
    # 你可以修改这里的文件名
    delete_columns_from_csv(input_csv_file='蜜雪冰城数据一级评论.csv', output_csv_file='A_processed.csv')
    # 如果你想直接覆盖原文件A.csv，可以这样调用：
    # delete_columns_from_csv(input_csv_file='A.csv', output_csv_file='A.csv')