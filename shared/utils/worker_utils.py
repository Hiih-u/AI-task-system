import json
from datetime import time, datetime

import redis
from requests import Session
from shared import models
from .logger import debug_log, log_error
from shared.models import TaskStatus
from ..database import SessionLocal

# --- 1. 死信队列逻辑 (新增) ---
DLQ_STREAM_KEY = "sys_dead_letters"

def send_to_dlq(redis_client, message_id, raw_payload, error_msg, source="Unknown"):
    """
    💀 将烂消息移入死信队列，并 ACK 丢弃
    """
    try:
        # 确保 message_id 是字符串
        if isinstance(message_id, bytes):
            message_id = message_id.decode()

        # 确保 payload 是字符串
        payload_str = "None"
        if raw_payload:
            payload_str = raw_payload.decode('utf-8', errors='ignore') if isinstance(raw_payload, bytes) else str(
                raw_payload)

        dead_msg = {
            "original_id": message_id,
            "error": str(error_msg),
            "source_worker": source,
            "failed_at": str(int(time.time())),
            "raw_payload": payload_str
        }

        # 1. 入死信
        redis_client.xadd(DLQ_STREAM_KEY, dead_msg, maxlen=10000)
        debug_log(f"💀 已移入死信队列: {message_id}", "WARNING")

    except Exception as e:
        debug_log(f"写入死信队列失败: {e}", "ERROR")


# --- 2. 安全解析逻辑 (新增) ---
def parse_and_validate(redis_client, stream_key, group_name, message_id, message_data, consumer_name):
    """
    🛡️ 通用解析函数：
    - 如果解析成功，返回 task_data (dict)
    - 如果解析失败（JSON错误/空消息），自动入死信 + ACK，并返回 None
    """
    payload_bytes = message_data.get(b'payload')

    # 1. 检查空消息
    if not payload_bytes:
        send_to_dlq(redis_client, message_id, b"", "Empty Payload", consumer_name)
        redis_client.xack(stream_key, group_name, message_id)
        return None

    try:
        # 2. 尝试解析 JSON
        task_data = json.loads(payload_bytes)
        return task_data

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # 3. 解析失败 -> 自动处理后事 (DLQ + ACK)
        debug_log(f"数据解析失败: {e}", "ERROR")
        send_to_dlq(redis_client, message_id, payload_bytes, f"JSON Error: {e}", consumer_name)
        redis_client.xack(stream_key, group_name, message_id)
        return None


def mark_task_failed(db, task_id, error_msg):
    """
    通用任务失败处理逻辑
    :param db: 数据库 Session 对象
    :param task_id: 任务 ID
    :param error_msg: 错误信息字符串
    """
    try:
        if task_id and task_id != "UNKNOWN":
            task = db.query(models.Task).filter(models.Task.task_id == task_id).first()
            if task:
                task.status = TaskStatus.FAILED
                task.error_msg = str(error_msg)
                db.commit()
                debug_log(f"💾 任务已标记为失败: {task_id} - {error_msg}", "WARNING")
            else:
                debug_log(f"⚠️ 标记失败时未找到任务: {task_id}", "WARNING")
    except Exception as e:
        db.rollback()
        log_error("TaskHelper", f"更新任务失败状态时数据库错误: {e}", task_id)


def claim_task(db: Session, task_id: str) -> bool:
    """
    🔥 核心幂等性函数：尝试认领任务
    原理：利用数据库原子更新 (UPDATE ... WHERE status=PENDING)

    :param db: 数据库会话
    :param task_id: 任务ID
    :return: True(抢占成功，可以执行), False(已被抢占或已完成，跳过)
    """
    try:
        # 执行原子更新：只有当前是 PENDING 时才更新为 PROCESSING
        # synchronize_session=False 能提高性能，防止 SQLAlchemy 尝试更新内存对象
        result = db.query(models.Task).filter(
            models.Task.task_id == task_id,
            models.Task.status == TaskStatus.PENDING
        ).update(
            {"status": TaskStatus.PROCESSING},
            synchronize_session=False
        )

        db.commit()

        if result == 1:
            debug_log(f"🔒 成功锁定任务: {task_id} -> PROCESSING", "INFO")
            return True
        else:
            # result == 0 说明找不到符合条件(ID匹配且状态为PENDING)的记录
            # 这意味着任务可能正在被别人处理(PROCESSING)或者已经完成(SUCCESS/FAILED)
            debug_log(f"✋ 任务抢占失败 (已被处理): {task_id}", "WARNING")
            return False

    except Exception as e:
        db.rollback()
        log_error("TaskHelper", f"抢占任务时发生数据库错误: {e}", task_id)
        return False


def recover_pending_tasks(
        redis_client: redis.Redis,
        stream_key: str,
        group_name: str,
        consumer_name: str,
        process_callback
):
    try:
        # 获取所有已认领但未 ACK 的消息 (Start from '0')
        response = redis_client.xreadgroup(
            group_name, consumer_name, {stream_key: '0'}, count=50, block=None
        )

        if response:
            stream_name, messages = response[0]
            if messages:
                debug_log(f"♻️  [{consumer_name}] 正在恢复 {len(messages)} 个挂起任务...", "WARNING")

                # 获取数据库会话，用于批量修复状态
                db = SessionLocal()

                try:
                    for message_id, message_data in messages:
                        # --- 1. 尝试解析并修复僵尸状态 ---
                        try:
                            # Redis 的 message_id (如 "1678888888888-0") 前半部分是时间戳(毫秒)
                            msg_timestamp = int(message_id.decode().split('-')[0])
                            current_time = int(time.time() * 1000)

                            # 如果消息超过 60 秒（即时聊天的容忍度），直接丢弃
                            if current_time - msg_timestamp > 60000:
                                print(f"⏰ 丢弃过期任务: {message_id} (超时 > 60s)")
                                redis_client.xack(stream_key, group_name, message_id)
                                continue  # 跳过，不执行

                            payload_bytes = message_data.get(b'payload')
                            if payload_bytes:
                                task_data = json.loads(payload_bytes)
                                task_id = task_data.get('task_id')

                                # 🔥 关键修复：如果任务状态是 PROCESSING，说明是上次崩溃留下的
                                # 必须强制重置为 PENDING，否则后续 claim_task 会抢占失败
                                if task_id:
                                    result = db.query(models.Task).filter(
                                        models.Task.task_id == task_id,
                                        models.Task.status == TaskStatus.PROCESSING
                                    ).update(
                                        {"status": TaskStatus.PENDING},
                                        synchronize_session=False
                                    )
                                    if result > 0:
                                        db.commit()
                                        debug_log(f"🔧 [自愈] 修复僵尸任务: {task_id} PROCESSING -> PENDING", "INFO")

                        except Exception as e:
                            debug_log(f"预检查解析失败 (将由 Worker 自动处理): {e}", "WARNING")
                            # 解析都失败了，通常建议直接 ACK 跳过，防止死循环
                            # redis_client.xack(stream_key, group_name, message_id)
                            # continue

                        # --- 2. 调用具体的 Worker 逻辑进行处理 ---
                        # check_idempotency=True 依然重要，防止处理那些其实已经 SUCCESS 但没 ACK 的任务
                        process_callback(message_id, message_data, check_idempotency=True)

                finally:
                    db.close()

                debug_log("✅ 挂起任务处理完毕", "INFO")

    except Exception as e:
        debug_log(f"❌ 恢复 Pending 任务流程失败: {e}", "ERROR")


def finish_task_success(db, task_id, response_text, cost_time, conversation_id=None):
    """
    ✅ 通用任务成功处理逻辑
    1. 查询任务 (懒加载)
    2. 更新状态、结果、耗时
    3. 更新会话时间
    4. 提交事务
    """
    try:
        # 1. 查询任务
        task = db.query(models.Task).filter(models.Task.task_id == task_id).first()

        if task:
            # 2. 更新任务字段
            task.response_text = response_text
            task.status = TaskStatus.SUCCESS
            task.cost_time = cost_time
            task.updated_at = datetime.now()

            # 3. 更新会话最后活跃时间 (如果有)
            if conversation_id:
                conv = db.query(models.Conversation).filter(
                    models.Conversation.conversation_id == conversation_id
                ).first()
                if conv:
                    conv.updated_at = datetime.now()

            # 4. 提交
            db.commit()
            debug_log(f"✅ 任务完成: {task_id} (耗时: {cost_time}s)", "SUCCESS")
            return True
        else:
            debug_log(f"⚠️ 保存结果时未找到任务: {task_id}", "WARNING")
            return False

    except Exception as e:
        db.rollback()
        log_error("WorkerUtils", f"保存任务结果失败: {e}", task_id)
        return False


def process_ai_result(db, task_id, ai_text, cost_time, conversation_id=None, refusal_keywords=None):
    """
    ⚖️ 通用 AI 结果处理函数 (终审法官)

    1. 软拒绝检测 (Soft Rejection Check): 检查内容是否包含拒绝关键词
    2. 如果命中 -> 自动标记为失败 (FAILED)
    3. 如果通过 -> 自动标记为成功 (SUCCESS) 并保存

    :param refusal_keywords: 拒绝词列表 (List[str])，如果不传则不检查
    :return: True(成功保存), False(被拒绝或出错)
    """
    try:
        # --- 1. 软拒绝检测 ---
        if refusal_keywords:
            # 检查是否包含任意一个关键词
            is_refusal = any(keyword in ai_text for keyword in refusal_keywords)

            if is_refusal:
                error_msg = f"AI 拒绝生成: {ai_text[:100]}..."  # 只截取前100字避免日志过长
                debug_log(f"🛑 捕获到软拒绝: {error_msg}", "WARNING")

                # 直接调用同文件的失败处理函数
                mark_task_failed(db, task_id, f"生成失败: {ai_text}")
                return False

        # --- 2. 审核通过，保存结果 ---
        # 直接调用上一轮我们封装好的成功处理函数
        return finish_task_success(db, task_id, ai_text, cost_time, conversation_id)

    except Exception as e:
        debug_log(f"处理 AI 结果时发生异常: {e}", "ERROR")
        return False