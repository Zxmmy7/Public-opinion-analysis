import pandas as pd
import os
#将一个目录下的所有CSV文件合并成一个
def merge_csv_files(folder_path, output_file_name="merged_output.csv"):
    """
    合并指定文件夹内所有CSV文件到一个文件。

    Args:
        folder_path (str): 包含CSV文件的文件夹路径。
        output_file_name (str): 合并后输出的CSV文件名。
    """
    # 1. 初始化一个空的DataFrame来存储所有合并的数据
    all_data = pd.DataFrame()

    # 2. 获取文件夹内所有CSV文件的路径
    csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]

    if not csv_files:
        print(f"在 '{folder_path}' 文件夹中没有找到任何CSV文件。")
        return

    print(f"找到以下CSV文件准备合并：")
    for csv_file in csv_files:
        print(f"- {os.path.basename(csv_file)}")

    # 3. 循环读取并合并每个CSV文件
    for i, csv_file in enumerate(csv_files):
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_file)
            print(f"已读取文件: {os.path.basename(csv_file)} (行数: {len(df)})")

            # 将当前CSV文件的数据添加到总数据中
            if all_data.empty:
                all_data = df
            else:
                # 使用 concat 进行行堆叠，由于列名相同，它们会自动对齐
                all_data = pd.concat([all_data, df], ignore_index=True)
            print(f"当前合并数据总行数: {len(all_data)}")

        except Exception as e:
            print(f"读取文件 '{os.path.basename(csv_file)}' 时发生错误: {e}")
            continue # 跳过当前文件，继续处理下一个

    # 4. 保存合并后的文件
    output_path = os.path.join(folder_path, output_file_name)
    try:
        all_data.to_csv(output_path, index=False)
        print(f"\n所有CSV文件已成功合并到 '{output_path}'")
        print(f"最终合并文件总行数: {len(all_data)}")
    except Exception as e:
        print(f"保存合并文件时发生错误: {e}")

# --- 如何使用 ---
if __name__ == "__main__":
    # 请将 'your_folder_path_here' 替换为您实际的文件夹路径
    # 例如: folder_to_merge = "C:/Users/YourUser/Documents/my_csv_files"
    # 或者对于相对路径: folder_to_merge = "./data"
    folder_to_merge = "/Axiangmu/tongbu/Visual_data/二级评论csv" # <--- 在这里替换为您的文件夹路径！
    output_merged_file = "/Axiangmu/tongbu/Visual_data/小米二级评论.csv"

    # 调用函数执行合并
    merge_csv_files(folder_to_merge, output_merged_file)

    print("\n-----------------------------------------------------")
    print("重要提示：请将代码中的 'your_folder_path_here' 替换为实际的文件夹路径！")
    print("例如：如果您希望合并当前脚本所在文件夹下的 'data' 文件夹中的CSV，可以设置为 './data'")
    print("如果您在Windows系统，路径可能类似 'C:\\Users\\YourName\\Documents\\my_csv_files' (注意双反斜杠或使用正斜杠)。")
    print("-----------------------------------------------------")