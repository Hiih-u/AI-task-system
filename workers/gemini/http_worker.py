import json
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
GEMINI_SERVICE_URL = os.getenv("GEMINI_SERVICE_URL", "http://localhost:61080/v1/chat/completions")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def process_tasks():
    print(f"Worker 启动，监听队列: gemini_tasks")
    print(f"下游服务地址: {GEMINI_SERVICE_URL}")

    while True:
        try:
            # 1. 阻塞获取任务
            result = r.brpop(["gemini_tasks"], timeout=5)
            if not result:
                continue

            queue, data = result
            task_data = json.loads(data)

            task_id = task_data['task_id']
            conversation_id = task_data['conversation_id']
            prompt = task_data['prompt']
            model = task_data['model']

            print(f"处理任务: {task_id} | 会话: {conversation_id}")

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
                        task_record.cost_time = time.time() - start_time

                        # 更新会话最后活跃时间
                        conv = db.query(models.Conversation).filter(
                            models.Conversation.conversation_id == conversation_id).first()
                        if conv:
                            conv.updated_at = models.datetime.now()

                        db.commit()
                        print(f"✅ 任务完成: {task_id}")
                else:
                    # 处理 API 报错
                    error_detail = response.text
                    print(f"❌ Gemini Service 报错: {response.status_code} - {error_detail}")
                    _mark_failed(db, task_id, f"Service Error: {response.status_code}")

            except requests.exceptions.RequestException as e:
                print(f"❌ 连接 Gemini Service 失败: {e}")
                _mark_failed(db, task_id, "Service Unreachable")
            except Exception as e:
                print(f"❌ Worker 内部错误: {e}")
                _mark_failed(db, task_id, str(e))
            finally:
                db.close()

        except Exception as e:
            print(f"Redis 错误: {e}")
            time.sleep(5)


def _mark_failed(db, task_id, msg):
    try:
        task = db.query(models.Task).filter(models.Task.task_id == task_id).first()
        if task:
            task.status = "FAILED"
            task.error_msg = msg
            db.commit()
    except:
        db.rollback()


if __name__ == "__main__":
    process_tasks()