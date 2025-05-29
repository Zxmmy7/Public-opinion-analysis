import pandas as pd

def merge_csv_files(file_a='A_processed.csv', file_b='B_processed.csv', output_file='merged_processed.csv'):
    """
    合并两个 CSV 文件 (A_processed.csv 和 B_processed.csv)，
    处理列名冲突和特殊赋值，并以 UTF-8 with BOM 编码保存结果。

    Args:
        file_a (str): 第一个 CSV 文件的名称 (A_processed.csv)。
        file_b (str): 第二个 CSV 文件的名称 (B_processed.csv)。
        output_file (str): 合并后数据保存的 CSV 文件名称。
    """
    try:
        print(f"正在加载文件: {file_a} 和 {file_b}...")
        # 加载 A_processed.csv
        # 默认尝试以 UTF-8 读取。如果原始文件编码不是 UTF-8，你可能需要在此处指定：
        # 例如：df_a = pd.read_csv(file_a, encoding='GBK')
        df_a = pd.read_csv(file_a)
        print(f"文件 {file_a} 加载成功。")

        # 加载 B_processed.csv
        # 同上，如果原始文件编码不是 UTF-8，可能需要指定 encoding
        df_b = pd.read_csv(file_b)
        print(f"文件 {file_b} 加载成功。")

        print("开始处理列名和赋值...")

        # --- 处理 B_processed DataFrame ---
        # 1. 重命名列
        print("重命名 B_processed 中的列...")
        df_b = df_b.rename(columns={
            'comment_like_count': 'liked_count',
            'sub_comment_count': 'comments_count'
        })

        # 2. 为 B_processed 添加 shared_count 列并赋值为 0
        print("为 B_processed 添加 'shared_count' 列并赋值为 0...")
        df_b['shared_count'] = 0

        # --- 确保两个 DataFrame 具有相同的列并保持顺序 ---
        # 获取 A_processed 的所有列，作为最终合并的列顺序
        final_columns_order = df_a.columns.tolist()

        # 确保 B_processed 拥有 A_processed 的所有列，并按相同顺序排列
        # 对于 B_processed 中缺失但 A_processed 中有的列，Pandas 会自动填充 NaN
        # 但由于我们已经手动添加了 shared_count，这里主要是为了保证顺序
        df_b_processed_for_merge = df_b[final_columns_order]

        print("列处理完成，准备合并...")

        # --- 合并两个 DataFrame ---
        # 使用 concat 垂直合并，忽略索引
        merged_df = pd.concat([df_a, df_b_processed_for_merge], ignore_index=True)
        print("文件合并成功。")

        # --- 保存合并后的数据 ---
        print(f"正在将合并后的数据保存到: {output_file}...")
        # 使用 'utf-8-sig' 编码，它会添加 BOM，帮助 Excel 正确识别 UTF-8
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print("数据保存成功。")
        print(f"合并完成！合并后的文件已保存为 {output_file}")

    except FileNotFoundError as e:
        print(f"错误：文件 '{e.filename}' 未找到。请确保文件在正确的路径下。")
    except KeyError as e:
        print(f"错误：DataFrame 中缺少指定的列: {e}. 请检查您的 CSV 文件是否包含所需的所有列。")
    except UnicodeDecodeError:
        print(f"错误：读取文件时发生编码错误。")
        print("尝试手动指定原始文件的编码，例如：pd.read_csv(file_a, encoding='GBK')")
    except Exception as e:
        print(f"发生了一个未知错误: {e}")

# 调用函数执行合并操作
if __name__ == "__main__":
    # 你可以修改这里的文件名
    merge_csv_files(file_a='A_processed.csv', file_b='B_processed.csv', output_file='merged_AB.csv')