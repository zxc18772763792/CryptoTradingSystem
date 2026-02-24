"""
启动脚本 - 启动Web服务
"""
import subprocess
import sys
from pathlib import Path

project_path = Path(__file__).parent.parent
sys.path.insert(0, str(project_path))

print("=" * 60)
print("Crypto Trading System - Web服务启动")
print("=" * 60)
print()
print("访问地址: http://localhost:8000")
print("API文档: http://localhost:8000/docs")
print()
print("按 Ctrl+C 停止服务")
print("=" * 60)

subprocess.run([
    sys.executable, "-m", "uvicorn",
    "web.main:app",
    "--host", "0.0.0.0",
    "--port", "8000",
    "--reload"
], cwd=str(project_path))
