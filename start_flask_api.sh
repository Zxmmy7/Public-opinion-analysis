#!/bin/bash

# --- 配置部分 ---
# 虚拟环境路径
VENV_PATH="/Axiangmu/huanjing/myenv"
# Flask 应用主文件路径，相对于当前脚本运行的目录
FLASK_APP_FILE="interface/flask_api.py"
# Flask 应用监听的端口
FLASK_PORT=5000
# Flask 应用监听的IP地址
FLASK_HOST="0.0.0.0"
# 日志文件路径
LOG_FILE="/Axiangmu/software/flask_api_logs/flask_api.log"
# 错误日志文件路径
ERROR_LOG_FILE="/Axiangmu/software/flask_api_logs/flask_api_error.log"

# --- 脚本逻辑 ---

# 确保日志目录存在
mkdir -p $(dirname "$LOG_FILE")
mkdir -p $(dirname "$ERROR_LOG_FILE")

echo "--- $(date) ---" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"
echo "尝试启动 Flask API..." | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"

# 激活虚拟环境
echo "激活虚拟环境: $VENV_PATH/bin/activate" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"
source "$VENV_PATH/bin/activate"

if [ $? -ne 0 ]; then
    echo "错误: 无法激活虚拟环境。请检查 VENV_PATH 是否正确。" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"
    exit 1
fi

# 设置 FLASK_APP 环境变量
echo "设置 FLASK_APP=$FLASK_APP_FILE" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"
export FLASK_APP="$FLASK_APP_FILE"

# 使用 nohup 在后台启动 Flask 应用
# --no-reload 避免开发模式下的自动重启，这在后台运行可能导致问题
# --with-threads 可以在生产环境使用多线程来处理请求
echo "执行命令: nohup flask run --host=$FLASK_HOST --port=$FLASK_PORT --no-reload --with-threads > $LOG_FILE 2> $ERROR_LOG_FILE &" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"

nohup flask run --host="$FLASK_HOST" --port="$FLASK_PORT" --no-reload --with-threads > "$LOG_FILE" 2> "$ERROR_LOG_FILE" &

# 获取 Flask 进程的 PID
FLASK_PID=$!
echo "Flask API 已在后台启动，PID: $FLASK_PID" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"
echo "输出日志: $LOG_FILE" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"
echo "错误日志: $ERROR_LOG_FILE" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"
echo "--- 启动尝试结束 ---" | tee -a "$LOG_FILE" "$ERROR_LOG_FILE"

# 退出虚拟环境（避免污染当前终端环境）
deactivate
