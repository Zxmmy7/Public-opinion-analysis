from flask import Flask, jsonify, request,jsonify, send_file, make_response, current_app, send_file
from flask_cors import CORS
from hdfs import InsecureClient
import json
import os
from openai import OpenAI  # 导入 OpenAI 库
import datetime
import io
#重要：
#       以后启动flask直接在终端
#       cd /Axiangmu/tongbu          ./start_flask_api.sh
#即可
#
#关闭时     在终端输入ps aux | grep flask
#           找到对应的进程号，kill  进程号
#



app = Flask(__name__)
CORS(app)

HDFS_NAMENODE_URL = 'http://192.168.88.11:9870'
HDFS_USER = 'node01'

# 定义主舆情分析结果文件的路径
HDFS_FILE_PATH_MAIN_ANALYSIS = os.getenv('HDFS_FILE_PATH_MAIN_ANALYSIS',
                                         '/user/spark/Banalysis_results_final/part-00000')

# 定义机器学习及系统性能数据文件的路径
HDFS_FILE_PATH_ML_PERFORMANCE = os.getenv('HDFS_FILE_PATH_ML_PERFORMANCE',
                                          '/user/spark/CMachine_learning/all_machine_learning_results.json/part-00000')

# 初始化 DeepSeek API 客户端
# 可以使用环境变量存储 API 密钥
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', "sk-878fdbb70d1440a4bd73b743d84a935a")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# 初始化 HDFS 客户端
try:
    hdfs_client = InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)  # Renamed to hdfs_client for clarity
    print(f"成功初始化 HDFS 客户端，连接到: {HDFS_NAMENODE_URL}，用户: {HDFS_USER}")
except Exception as e:
    print(f"初始化 HDFS 客户端失败: {e}")
    hdfs_client = None  # Ensure it's None if failed



if DEEPSEEK_API_KEY == "<Your DeepSeek API Key>":
    print("警告：DeepSeek API 密钥未设置或仍为占位符。请设置 DEEPSEEK_API_KEY 环境变量或在代码中替换它。")
    deepseek_ai_client = None
else:
    try:
        deepseek_ai_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
        print("成功初始化 DeepSeek API 客户端。")
    except Exception as e:
        print(f"初始化 DeepSeek API 客户端失败: {e}")
        deepseek_ai_client = None


# 辅助函数：从 HDFS 读取 JSON 文件
def read_hdfs_json(file_path):
    """
    从 HDFS 读取指定的 JSON 文件并返回解析后的 Python 对象。
    """
    if not hdfs_client:
        raise Exception("HDFS 客户端未初始化。")
    try:
        if not hdfs_client.status(file_path, strict=False):
            raise FileNotFoundError(f"HDFS 文件不存在或路径错误: {file_path}")

        with hdfs_client.read(file_path, encoding='utf-8') as reader:
            content = reader.read()
            # 尝试处理由多个JSON对象拼接而成但整体不是一个有效JSON数组的文件内容
            # Spark输出的part-00000文件有时是每行一个JSON对象
            try:
                # 首先尝试将其作为一个整体JSON解析
                json_data = json.loads(content)
            except json.JSONDecodeError:
                # 如果失败，尝试逐行解析并合并为一个列表
                print(f"文件 {file_path} 不是单一JSON对象，尝试逐行解析...")
                json_objects = []
                for line in content.strip().split('\n'):
                    if line.strip():  # 确保行不为空
                        try:
                            json_objects.append(json.loads(line))
                        except json.JSONDecodeError as line_err:
                            print(f"警告: 解析行失败: {line_err} - 行内容: {line[:100]}...")
                            # 可以选择跳过错误行或抛出异常，这里选择跳过
                if not json_objects:  # 如果没有成功解析任何对象
                    raise json.JSONDecodeError(f"HDFS 文件内容无法解析为JSON (整体或逐行): {file_path}",
                                               doc=content[:200], pos=0)
                json_data = json_objects[0] if len(json_objects) == 1 else json_objects  # 如果只有一个对象，则返回该对象，否则返回列表
        return json_data
    except FileNotFoundError as e:
        raise e
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"HDFS 文件内容不是有效的 JSON 格式 ({file_path}): {e.msg}",
                                   doc=content if 'content' in locals() else '', pos=e.pos)
    except Exception as e:
        raise Exception(f"读取 HDFS 文件时发生未知错误 ({file_path}): {e}")


@app.route('/get_analysis_data', methods=['GET'])
def get_analysis_data():
    """
    API 接口，用于从 HDFS 读取主舆情分析 JSON 文件内容并返回。
    """
    print(f"收到请求，尝试从 HDFS 读取文件: {HDFS_FILE_PATH_MAIN_ANALYSIS}")
    try:
        json_data = read_hdfs_json(HDFS_FILE_PATH_MAIN_ANALYSIS)
        print("成功从 HDFS 读取并解析主舆情分析 JSON 数据。")
        return jsonify(json_data)
    except FileNotFoundError as e:
        print(f"错误: {e}")
        return jsonify({'error': str(e), 'message': '主舆情分析 HDFS 文件未找到。'}), 404
    except json.JSONDecodeError as e:
        print(f"错误: {e}")
        return jsonify({'error': str(e), 'message': '主舆情分析 HDFS 文件内容解析失败，不是有效的 JSON。'}), 500
    except Exception as e:
        print(f"错误: {e}")
        return jsonify(
            {'error': str(e), 'message': f'无法从 HDFS 读取主舆情分析文件: {HDFS_FILE_PATH_MAIN_ANALYSIS}'}), 500


@app.route('/get_ml_performance_data', methods=['GET'])
def get_ml_performance_data():
    """
    API 接口，用于从 HDFS 读取机器学习及系统性能 JSON 文件内容并返回。
    """
    print(f"收到请求，尝试从 HDFS 读取文件: {HDFS_FILE_PATH_ML_PERFORMANCE}")
    try:
        json_data = read_hdfs_json(HDFS_FILE_PATH_ML_PERFORMANCE)
        print("成功从 HDFS 读取并解析机器学习及系统性能 JSON 数据。")
        return jsonify(json_data)
    except FileNotFoundError as e:
        print(f"错误: {e}")
        return jsonify({'error': str(e), 'message': '机器学习及系统性能 HDFS 文件未找到。'}), 404
    except json.JSONDecodeError as e:
        print(f"错误: {e}")
        return jsonify({'error': str(e), 'message': '机器学习及系统性能 HDFS 文件内容解析失败，不是有效的 JSON。'}), 500
    except Exception as e:
        print(f"错误: {e}")
        return jsonify({'error': str(e),
                        'message': f'无法从 HDFS 读取机器学习及系统性能文件: {HDFS_FILE_PATH_ML_PERFORMANCE}'}), 500


@app.route('/analyze_with_deepseek', methods=['POST'])  # 使用 POST 通常更好，尤其如果未来需要传递参数
def analyze_with_deepseek():
    """
    API 接口，用于从 HDFS 读取主舆情分析数据，
    然后使用 DeepSeek API 进行分析，并返回分析结果。
    """
    if not deepseek_ai_client:
        return jsonify({'error': 'DeepSeek API 客户端未初始化。请检查 API 密钥。'}), 503  # Service Unavailable

    print(f"收到 DeepSeek 分析请求，将使用 HDFS 文件: {HDFS_FILE_PATH_MAIN_ANALYSIS}")
    try:
        # 1. 从 HDFS 读取数据
        hdfs_data = read_hdfs_json(HDFS_FILE_PATH_MAIN_ANALYSIS)
        # 将 Python 字典/列表转换为 JSON 字符串以发送给 DeepSeek
        # 注意：确保数据量在 DeepSeek API 的限制范围内
        data_to_analyze_str = json.dumps(hdfs_data, ensure_ascii=False, indent=2)

        # 限制发送给 DeepSeek 的数据长度，以避免超出 token 限制
        # 这里的 15000 是一个示例值，DeepSeek 的具体 token 限制可能不同，且 token 不完全等同于字符数
        max_chars_for_deepseek = 15000
        if len(data_to_analyze_str) > max_chars_for_deepseek:
            data_to_analyze_str = data_to_analyze_str[:max_chars_for_deepseek] + "\n... (数据已截断)"
            print(f"警告: 发送给 DeepSeek 的数据已截断至 {max_chars_for_deepseek} 字符。")

        print("成功从 HDFS 读取主舆情分析 JSON 数据，准备发送给 DeepSeek。")

        # 2. 构建发送给 DeepSeek 的消息和提示
        prompt_content = f"""请你扮演一个资深的舆情分析专家。
        基于以下JSON格式的舆情数据，生成一份详细的舆情分析报告。
        报告应包括：
        1.  总体情感倾向（正面、负面、中性）。
        2.  主要的舆情主题或讨论焦点。
        3.  任何显著的趋势或模式。
        4.  可能存在的风险点或机遇。
        5.  总结和建议。

        请以清晰、结构化的方式呈现报告。

        原始JSON数据如下:
        ```json
        {data_to_analyze_str}
        ```
        """

        messages = [
            {"role": "system", "content": "你是一个专业的舆情分析助手，能够解读JSON数据并生成深入的分析报告。"},
            {"role": "user", "content": prompt_content}
        ]

        # 3. 调用 DeepSeek API
        print("正在调用 DeepSeek API...")
        completion = deepseek_ai_client.chat.completions.create(
            model="deepseek-chat",  # 或者 "deepseek-coder" 如果数据结构分析更重要
            messages=messages,
            stream=False  # 设置为 False 以获取完整响应
        )

        analysis_result = completion.choices[0].message.content
        print("成功从 DeepSeek API 获取分析结果。")

        return jsonify({'analysis': analysis_result})

    except FileNotFoundError as e:
        print(f"DeepSeek分析错误 (HDFS): {e}")
        return jsonify({'error': str(e), 'message': '用于分析的 HDFS 文件未找到。'}), 404
    except json.JSONDecodeError as e:
        print(f"DeepSeek分析错误 (JSON 解析): {e}")
        return jsonify({'error': str(e), 'message': '用于分析的 HDFS 文件内容解析失败。'}), 500
    except Exception as e:
        # 捕获 OpenAI API 调用可能发生的错误，例如 openai.APIError
        print(f"DeepSeek分析错误 (API 或其他): {e}")
        return jsonify({'error': str(e), 'message': '调用 DeepSeek API 或处理数据时发生错误。'}), 500





HDFS_EXPORT_FILE_PATH = os.getenv('/user/spark/Aprocessed_data',HDFS_FILE_PATH_MAIN_ANALYSIS)
DEFAULT_EXPORT_FILENAME = 'DATA.parquet' # 默认下载文件名

@app.route('/download_exported_data', methods=['GET'])
def download_exported_data():
    """
    API 接口，用于从 HDFS 导出特定目录下的数据。
    这里假设导出的就是 HDFS_EXPORT_FILE_PATH 指定的文件内容。
    """
    if not hdfs_client:
        return jsonify({"error": "HDFS 客户端未初始化，无法导出数据。"}), 503

    print(f"收到数据导出请求，尝试从 HDFS 导出文件: {HDFS_EXPORT_FILE_PATH}")
    try:
        # 检查文件是否存在
        if not hdfs_client.status(HDFS_EXPORT_FILE_PATH, strict=False):
            return jsonify({"error": f"HDFS 导出文件 '{HDFS_EXPORT_FILE_PATH}' 不存在或路径错误。"}), 404

        # 从 HDFS 读取文件内容到内存
        # 注意：对于非常大的文件，这可能会占用大量内存。
        # 对于生产环境和超大文件，建议使用流式传输。
        with hdfs_client.read(HDFS_EXPORT_FILE_PATH, encoding='utf-8') as reader:
            file_content = reader.read()

        # 将字符串内容编码为字节流
        file_stream = io.BytesIO(file_content.encode('utf-8'))

        # 构造响应，触发文件下载
        response = make_response(send_file(
            file_stream,
            mimetype='application/json/csv',  # 或者 'application/octet-stream' 如果是通用二进制
            as_attachment=True,
            download_name=DEFAULT_EXPORT_FILENAME # 建议下载的文件名
        ))
        print(f"成功导出 HDFS 文件: {HDFS_EXPORT_FILE_PATH}")
        return response

    except Exception as e:
        print(f"导出 HDFS 文件时发生错误: {e}")
        return jsonify({"error": f"导出数据时发生错误: {str(e)}"}), 500





MAX_DEEPSEEK_SUMMARY_LEN = 8000
@app.route('/report_with_deepseek', methods=['POST'])
def report_with_deepseek():
    """
    API 接口：从 HDFS 获取舆情分析数据，调用 DeepSeek 生成摘要，返回报告下载。
    """
    try:
        # Step 1: 读取 HDFS 数据
        current_app.logger.info(f"读取 HDFS 数据: {HDFS_FILE_PATH_MAIN_ANALYSIS}")
        try:
            analysis_data = read_hdfs_json(HDFS_FILE_PATH_MAIN_ANALYSIS)
            formatted_analysis_data = json.dumps(analysis_data, ensure_ascii=False, indent=2)
        except Exception as e:
            formatted_analysis_data = f"[ERROR] 无法读取 HDFS 数据: {str(e)}"
            current_app.logger.warning(formatted_analysis_data)

        # Step 2: 调用 DeepSeek API 生成摘要
        summary = "--- 未生成摘要 ---"
        if deepseek_ai_client:
            current_app.logger.info("调用 DeepSeek API 生成摘要...")
            try:
                input_data = formatted_analysis_data[:MAX_DEEPSEEK_SUMMARY_LEN]
                if len(formatted_analysis_data) > MAX_DEEPSEEK_SUMMARY_LEN:
                    input_data += "\n... (内容已截断)"

                prompt = (
                    "请作为舆情分析师，基于以下JSON格式数据生成摘要，"
                    "包括情感倾向、讨论热点、趋势及异常，用中文书写。\n"
                    f"原始数据:\n```json\n{input_data}\n```"
                )
                messages = [
                    {"role": "system", "content": "你是专业舆情分析助手。"},
                    {"role": "user", "content": prompt}
                ]
                completion = deepseek_ai_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=messages,
                    stream=False
                )
                summary = completion.choices[0].message.content
            except Exception as api_err:
                summary = f"[DeepSeek 错误] {str(api_err)}"
                current_app.logger.warning(summary)
        else:
            current_app.logger.warning("DeepSeek 客户端未初始化")

        # Step 3: 构造报告
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report_content = (
            f"舆情分析报告\n生成时间: {timestamp}\n"
            f"--------------------------\n"
            f"原始数据:\n{formatted_analysis_data}\n\n"
            f"生成摘要:\n{summary}\n"
        )

        # Step 4: 构造文件并发送下载
        filename = f"舆情分析报告_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        buffer = io.BytesIO()
        buffer.write(report_content.encode('utf-8'))
        buffer.seek(0)

        response = make_response(send_file(
            buffer,
            as_attachment=True,
            download_name=filename, # <--- 关键：只在这里设置文件名，让 send_file 自己处理
            mimetype='application/octet-stream'
        ))
        # <--- 移除这行手动设置 Content-Disposition 的代码
        # response.headers["Content-Disposition"] = f"attachment; {quoted_filename}"

        current_app.logger.info(f"报告已准备好，触发下载：{filename}")
        return response

    except Exception as e:
        current_app.logger.exception("报告生成失败")
        return jsonify({
            "status": "error",
            "message": f"报告生成失败: {str(e)}",
            "detail": "请查看服务端日志以了解详情。"
        }), 500







if __name__ == '__main__':
    print("Flask 应用启动中...")
    # 确保您的 DeepSeek API 密钥已正确设置
    if not deepseek_ai_client:
        print("！！！警告：DeepSeek API 客户端未能初始化。 /analyze_with_deepseek 接口将无法工作。！！！")
    app.run(host='0.0.0.0', port=5000, debug=True)