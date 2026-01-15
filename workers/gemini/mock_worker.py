# workers/gemini/mock_worker.py
import time
import json
import redis
import os  # 新增
from shared import database, models
from shared.database import SessionLocal

# 读取环境变量
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# 连接 Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# 定义需要监听的所有队列列表
LISTEN_QUEUES = ["gemini_tasks", "sd_tasks", "deepseek_tasks", "image_tasks"]


def process_tasks():
    print(f"👷 Mock Worker 正在运行，监听队列: {LISTEN_QUEUES} ...")
    while True:
        # brpop 可以同时监听多个队列
        # 只要其中任何一个有新消息，就会立即返回
        task = r.brpop(LISTEN_QUEUES, timeout=5)

        if task:
            # task 是一个元组: (b'queue_name', b'data')
            queue_name_bytes, data = task
            queue_name = queue_name_bytes.decode('utf-8')

            payload = json.loads(data)
            print(f"📥 从 [{queue_name}] 收到任务: {payload}")

            task_id = payload['task_id']
            task_type = payload.get('type', 'IMAGE')
            prompt = payload['prompt']
            conversation_id = payload.get('conversation_id')

            # 模拟处理时间
            time.sleep(2)

            # 更新数据库状态
            db = SessionLocal()
            try:
                task_record = db.query(models.Task).filter(models.Task.task_id == task_id).first()
                if not task_record:
                    print(f"❌ 数据库中未找到任务 {task_id}")
                    continue

                if task_type == "TEXT":
                    task_record.response_text = f"【AI回复】针对 '{prompt}' 的回答 (来自 {queue_name})"
                    task_record.status = "SUCCESS"
                    print(f"✅ 文本任务 {task_id} 完成")

                    if conversation_id:
                        conv = db.query(models.Conversation).filter(
                            models.Conversation.conversation_id == conversation_id).first()
                        if conv:
                            conv.updated_at = models.datetime.now()

                elif task_type == "IMAGE":
                    task_record.result_url = f"http://localhost:8000/static/images/{task_id}.png"
                    task_record.status = "SUCCESS"
                    print(f"✅ 图片任务 {task_id} 完成")

                db.commit()
            except Exception as e:
                print(f"❌ 处理出错: {e}")
                db.rollback()
            finally:
                db.close()


if __name__ == "__main__":
    process_tasks()