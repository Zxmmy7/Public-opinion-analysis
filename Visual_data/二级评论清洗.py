import pandas as pd

def process_csv_and_delete_columns(input_csv_file='A.csv', output_csv_file='A_processed.csv'):
    """
    从指定的 CSV 文件中删除不需要的列，并以 UTF-8 with BOM 编码保存处理后的数据。

    Args:
        input_csv_file (str): 输入 CSV 文件的名称。默认为 'A.csv'。
        output_csv_file (str): 处理后数据保存的 CSV 文件名称。默认为 'A_processed.csv'。
                                如果设置为与 input_csv_file 相同，将覆盖原文件。
    """
    try:
        # 1. 加载 CSV 文件
        print(f"正在加载 CSV 文件: {input_csv_file}...")
        # 默认尝试以 UTF-8 读取。如果原始文件编码不是 UTF-8，你可能需要在此处指定：
        # 例如：df = pd.read_csv(input_csv_file, encoding='GBK')
        df = pd.read_csv(input_csv_file)
        print("CSV 文件加载成功。")

        # 2. 指定要删除的列
        # 根据你最新提供的A.csv列名和要删除的列进行更新
        columns_to_delete = [
            'comment_id',
            'create_time',
            'note_id',
            'last_modify_ts',
            'parent_comment_id',
            'user_id',
            'nickname',
            'profile_url',
            'avatar'
        ]

        # 过滤掉实际不存在于 DataFrame 中的列，以避免 KeyError
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
            df_processed = df.copy() # 如果没有列要删除，则保持 DataFrame 不变

        # 4. 保存处理后的数据
        print(f"正在将处理后的数据保存到: {output_csv_file}...")
        # 使用 'utf-8-sig' 编码，它会添加 BOM，帮助 Excel 正确识别 UTF-8
        df_processed.to_csv(output_csv_file, index=False, encoding='utf-8-sig')
        print("数据保存成功。")
        print(f"处理完成！处理后的文件已保存为 {output_csv_file}")

    except FileNotFoundError:
        print(f"错误：文件 '{input_csv_file}' 未找到。请确保文件在正确的路径下。")
    except UnicodeDecodeError:
        print(f"错误：读取文件 '{input_csv_file}' 时发生编码错误。")
        print("尝试手动指定文件的原始编码，例如：pd.read_csv(input_csv_file, encoding='GBK') 或 encoding='latin1'")
    except Exception as e:
        print(f"发生了一个错误: {e}")

# 调用函数执行删除操作
if __name__ == "__main__":
    # 你可以修改这里的文件名
    # 文件将在虚拟机上生成 A_processed.csv
    process_csv_and_delete_columns(input_csv_file='蜜雪冰城二级评论.csv', output_csv_file='B_processed.csv')

