# api-gateway/Dockerfile

# 使用官方 Python 3.10 轻量版镜像 (与你的 requirements 兼容)
FROM python:3.10-slim

# 设置容器内的工作目录
WORKDIR /app

# --- 1. 安装依赖 ---
# 复制根目录下的 requirements.txt
COPY requirements.txt .

# 安装依赖
# --no-cache-dir 减小镜像体积
RUN pip install --no-cache-dir -r requirements.txt

# --- 2. 复制源代码 ---
# 关键：必须复制 shared 文件夹，因为 main.py 依赖它
COPY shared/ ./shared/

# 复制 api-gateway 代码
COPY api-gateway/ ./api-gateway/

# --- 3. 配置启动 ---
# 暴露端口
EXPOSE 8000

# 启动命令
# 注意：因为 WORKDIR 是 /app，我们使用模块路径 "api-gateway.main:app" 来启动
CMD ["uvicorn", "api-gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]