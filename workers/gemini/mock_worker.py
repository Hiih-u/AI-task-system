import time
import json
import redis
from shared import database, models
from shared.database import SessionLocal

# 连接 Redis
r = redis.Redis(host='127.0.0.1', port=6379, db=0)

QUEUE_NAME = "gemini_tasks"

def process_tasks():
    print("👷 Mock Worker 正在运行，等待任务...")
    while True:
        # 阻塞式读取队列 'ai_tasks'
        # brpop 返回元组 (queue_name, data)
        task = r.brpop("ai_tasks", timeout=5)

        if task:
            queue_name, data = task
            payload = json.loads(data)
            print(f"📥 收到任务: {payload}")

            task_id = payload['task_id']
            task_type = payload.get('type', 'IMAGE')  # 默认为 IMAGE 以兼容旧数据
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
                    # 模拟文本回复
                    task_record.response_text = f"【AI回复】针对你说的 '{prompt}'，这是我的回答... (模拟)"
                    task_record.status = "SUCCESS"
                    print(f"✅ 文本任务 {task_id} 完成")

                    # 同时也应该更新 Conversation 的 session_metadata (模拟)
                    if conversation_id:
                        conv = db.query(models.Conversation).filter(
                            models.Conversation.conversation_id == conversation_id).first()
                        if conv:
                            conv.updated_at = models.datetime.now()
                            # conv.session_metadata = {...}

                elif task_type == "IMAGE":
                    # 模拟图片生成
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