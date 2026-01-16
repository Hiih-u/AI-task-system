import json
from datetime import datetime
from logging import DEBUG

import redis
import requests
import os
import time
from sqlalchemy.orm import Session
from shared import models, database

# 配置
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# 指向你的 Gemini Service (就是你上传的 server.py 运行的服务)
# 假设它运行在 localhost:8000
GEMINI_SERVICE_URL = os.getenv("GEMINI_SERVICE_URL", "http://192.168.202.155:61028/v1/chat/completions")

redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

def debug_log(message: str, level: str = "INFO"):
    """统一的 debug 日志输出"""
    if DEBUG:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        emoji_map = {
            "INFO": "ℹ️", "SUCCESS": "✅", "ERROR": "❌",
            "WARNING": "⚠️", "DEBUG": "🔍", "REQUEST": "📝",
            "RESPONSE": "📤", "IMAGE": "🖼️", "FILE": "📎", "CHAT": "💬"
        }
        emoji = emoji_map.get(level, "•")
        print(f"[{timestamp}] {emoji} {message}")

def process_tasks():
    debug_log("=" * 40, "INFO")
    debug_log(f"Worker 启动，监听队列: gemini_tasks", "INFO")
    debug_log(f"下游服务地址: {GEMINI_SERVICE_URL}", "INFO")
    debug_log("=" * 40, "INFO")

    while True:
        try:
            # 1. 阻塞获取任务
            result = redis.brpop(["gemini_tasks"], timeout=5)
            if not result:
                continue

            queue, data = result
            task_data = json.loads(data)

            task_id = task_data['task_id']
            conversation_id = task_data['conversation_id']
            prompt = task_data['prompt']
            model = task_data['model']

            debug_log(f"📥 收到任务: {task_id}", "REQUEST")
            debug_log(f"会话: {conversation_id} | 模型: {model}", "CHAT")

            db = database.SessionLocal()
            try:
                # 2. 构造请求发送给 Gemini Service
                # 注意：我们不需要在这里 build_chat_history，因为 Gemini Service 会根据 conversation_id 自动加载
                payload = {
                    "model": model,
                    "conversation_id": conversation_id,  # 透传 ID，实现上下文复用
                    "messages": [
                        {"role": "user", "content": prompt}  # 只发最新的一句
                    ]
                }

                start_time = time.time()
                debug_log(f"正在调用下游服务...", "DEBUG")

                # 调用接口
                response = requests.post(GEMINI_SERVICE_URL, json=payload, timeout=120)

                if response.status_code == 200:
                    res_json = response.json()
                    # 提取 AI 回复内容
                    ai_text = res_json['choices'][0]['message']['content']

                    # 3. 更新数据库状态为 SUCCESS
                    task_record = db.query(models.Task).filter(models.Task.task_id == task_id).first()
                    if task_record:
                        task_record.response_text = ai_text
                        task_record.status = "SUCCESS"
                        task_record.cost_time = round(time.time() - start_time, 2)

                        # 更新会话最后活跃时间
                        conv = db.query(models.Conversation).filter(
                            models.Conversation.conversation_id == conversation_id).first()
                        if conv:
                            conv.updated_at = models.datetime.now()

                        db.commit()
                        debug_log(f"任务完成: {task_id} (耗时: {task_record.cost_time:.2f}s)", "SUCCESS")
                else:
                    # 处理 API 报错
                    error_detail = response.text
                    debug_log(f"Gemini Service 报错: {response.status_code}", "ERROR")
                    debug_log(f"详情: {error_detail}", "ERROR")
                    _mark_failed(db, task_id, f"Service Error: {response.status_code}")

            except requests.exceptions.RequestException as e:
                debug_log(f"连接 Gemini Service 失败: {e}", "ERROR")
                _mark_failed(db, task_id, "Service Unreachable")
            except Exception as e:
                debug_log(f"Worker 内部错误: {e}", "ERROR")
                _mark_failed(db, task_id, str(e))
            finally:
                db.close()

        except Exception as e:
            debug_log(f"Redis 循环错误: {e}", "ERROR")
            time.sleep(5)


def _mark_failed(db, task_id, msg):
    try:
        task = db.query(models.Task).filter(models.Task.task_id == task_id).first()
        if task:
            task.status = "FAILED"
            task.error_msg = msg
            db.commit()
            debug_log(f"任务 {task_id} 已标记为失败", "WARNING")
    except:
        db.rollback()


if __name__ == "__main__":
    process_tasks()