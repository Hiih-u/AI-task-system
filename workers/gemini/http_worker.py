import json
from datetime import datetime
from logging import DEBUG

import redis
import requests
import os
import time
from sqlalchemy.orm import Session
from shared import models, database
from shared.models import TaskStatus
from shared.utils import log_error

# 配置
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# 指向你的 Gemini Service (就是你上传的 server.py 运行的服务)
# 假设它运行在 localhost:8000
GEMINI_SERVICE_URL = os.getenv("GEMINI_SERVICE_URL", "http://localhost:61080/v1/chat/completions")

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
            try:
                task_data = json.loads(data)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                # ✅ 同时捕获 "格式错误" 和 "编码错误"
                error_msg = f"Redis 数据异常 (无法解析): {data}"
                debug_log(error_msg, "ERROR")
                log_error("Worker-Gemini", error_msg, None, e)
                continue  # 跳过这条脏数据，处理下一条

            task_id = task_data.get('task_id')
            if not task_id:
                log_error("Worker-Gemini", f"任务缺少 task_id: {data}")
                continue

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
                        task_record.status = TaskStatus.SUCCESS
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
                    error_msg = f"Gemini Service Error: {response.status_code}"
                    debug_log(error_msg, "ERROR")
                    log_error(
                        source="Worker-Gemini",
                        message=f"API调用失败: {error_detail[:200]}...",  # 截取一部分防止太长
                        task_id=task_id,
                        error=Exception(f"HTTP {response.status_code}: {error_detail}")
                    )
                    _mark_failed(db, task_id, error_msg)

            except requests.exceptions.RequestException as e:
                debug_log(f"连接 Gemini Service 失败: {e}", "ERROR")
                log_error("Worker-Gemini", "无法连接下游服务", task_id, e)
                _mark_failed(db, task_id, "Service Unreachable")
            except Exception as e:
                debug_log(f"Worker 内部错误: {e}", "ERROR")
                log_error("Worker-Gemini", "Worker 内部逻辑异常", task_id, e)
                _mark_failed(db, task_id, str(e))
            finally:
                db.close()

        except Exception as e:
            debug_log(f"Redis 循环错误: {e}", "ERROR")
            log_error("Worker-Loop", "Redis 监听循环异常", None, e)
            time.sleep(5)  # 防止死循环刷屏


def _mark_failed(db, task_id, msg):
    try:
        task = db.query(models.Task).filter(models.Task.task_id == task_id).first()
        if task:
            task.status = TaskStatus.FAILED
            task.error_msg = msg
            db.commit()
            debug_log(f"任务 {task_id} 已标记为失败", "WARNING")
    except Exception as e:
        db.rollback()
        print(f"⚠️ 致命错误：无法更新任务失败状态! {e}")


if __name__ == "__main__":
    process_tasks()