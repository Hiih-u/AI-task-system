# services/workers/deepseek/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# 1. 安装依赖
COPY requirements_linux.txt .
RUN pip install --no-cache-dir -r requirements_linux.txt

# 2. 复制公共代码 (Common + Core)
COPY common/ ./common/
COPY services/workers/core/ ./services/workers/core/

# 3. 复制 DeepSeek 业务代码
COPY services/workers/deepseek/ ./services/workers/deepseek/

# 4. 设置环境变量
ENV PYTHONPATH=/app
ENV DEEPSEEK_WORKER_ID=deepseek-docker-default

# 5. 启动命令
CMD ["python", "-u", "services/workers/deepseek/deepseek_worker.py"]