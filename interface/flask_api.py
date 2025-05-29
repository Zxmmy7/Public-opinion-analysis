
from flask import Flask, jsonify
from flask_cors import CORS # 导入 CORS，用于处理跨域请求
from hdfs import InsecureClient # 导入 HDFS 客户端
import json
import os # 用于获取环境变量，提高配置灵活性
from flask_cors import CORS


#重要：
#       以后启动flask直接在终端
#       cd /Axiangmu/tongbu          ./start_flask_api.sh
#即可
#
#
#
app = Flask(__name__)
CORS(app) # 允许所有来源的跨域请求，方便开发
# -----------------------------------------------------------------------------
# CORS 配置：允许前端跨域访问
# 在开发阶段，我们允许所有来源访问。在生产环境中，您应该限制为特定域名。
# -----------------------------------------------------------------------------



HDFS_NAMENODE_URL = 'http://192.168.88.11:9870' # 确保这里是NameNode的WebHDFS端口，通常是50070
HDFS_USER = 'node01' # 根据您保存文件的用户来设置
HDFS_FILE_PATH = os.getenv('HDFS_FILE_PATH', '/user/spark/analysis_results_final.json/part-00000')



# 初始化 HDFS 客户端
try:
    client = InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)
    print(f"成功初始化 HDFS 客户端，连接到: {HDFS_NAMENODE_URL}，用户: {HDFS_USER}")
except Exception as e:
    print(f"初始化 HDFS 客户端失败: {e}")
    # 在实际应用中，如果 HDFS 连接失败，您可能需要更健壮的错误处理，
    # 例如退出应用或提供一个默认的错误页面。

# -----------------------------------------------------------------------------
# API 路由定义
# -----------------------------------------------------------------------------
@app.route('/get_analysis_data', methods=['GET'])
def get_analysis_data():
    """
    API 接口，用于从 HDFS 读取指定 JSON 文件内容并返回。
    """
    print(f"收到请求，尝试从 HDFS 读取文件: {HDFS_FILE_PATH}")
    try:
        # 使用 HDFS 客户端读取文件
        with client.read(HDFS_FILE_PATH, encoding='utf-8') as reader:
            data = reader.read()
            json_data = json.loads(data) # 将读取到的字符串解析为 JSON 对象

        print("成功从 HDFS 读取并解析 JSON 数据。")
        return jsonify(json_data) # 将 JSON 对象作为 HTTP 响应返回
    except FileNotFoundError:
        error_msg = f"错误: HDFS 文件不存在或路径错误: {HDFS_FILE_PATH}"
        print(error_msg)
        return jsonify({'error': error_msg, 'message': 'HDFS 文件未找到。'}), 404
    except json.JSONDecodeError:
        error_msg = f"错误: HDFS 文件内容不是有效的 JSON 格式: {HDFS_FILE_PATH}"
        print(error_msg)
        return jsonify({'error': error_msg, 'message': 'HDFS 文件内容解析失败，不是有效的 JSON。'}), 500
    except Exception as e:
        error_msg = f"读取 HDFS 文件时发生未知错误: {e}"
        print(error_msg)
        return jsonify({'error': error_msg, 'message': f'无法从 HDFS 读取文件: {HDFS_FILE_PATH}'}), 500

# -----------------------------------------------------------------------------
# 应用运行配置
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    # host='0.0.0.0' 表示 Flask 应用将监听所有可用的网络接口，
    # 这样您的 Windows 机器可以通过虚拟机的 IP 地址访问它。
    # port=5000 是服务监听的端口。您可以根据需要修改。
    # debug=True 在开发阶段很有用，它会在代码修改后自动重载，并提供详细的错误信息。
    # 在生产环境中，应将 debug 设置为 False。
    print("Flask 应用启动中...")
    app.run(host='0.0.0.0', port=5000, debug=True)