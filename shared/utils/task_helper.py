import json

import redis
from requests import Session
from shared import models
from .logger import debug_log, log_error
from shared.models import TaskStatus
from ..database import SessionLocal


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
    """
    🔥 通用恢复逻辑：处理 Worker 崩溃后遗留的 Pending 任务

    核心功能：
    1. 从 Redis PEL 读取未确认消息
    2. 关键修复：将数据库中卡在 PROCESSING 的状态重置为 PENDING
    3. 调用传入的 process_callback 函数重新执行任务

    :param redis_client: Redis 客户端实例
    :param stream_key: 队列名称 (如 gemini_stream)
    :param group_name: 消费者组名称
    :param consumer_name: 消费者名称
    :param process_callback: 具体的业务处理函数，签名需为 func(msg_id, msg_data, check_idempotency)
    """
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
                            debug_log(f"解析恢复消息失败: {e}", "ERROR")
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