FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建数据目录
RUN mkdir -p /app/data/historical /app/data/cache /app/logs

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV TRADING_MODE=paper

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["python", "main.py"]
