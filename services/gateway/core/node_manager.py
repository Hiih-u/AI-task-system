import time
from datetime import datetime, timedelta
from sqlalchemy import update

from common.database import SessionLocal
from common.models import GeminiServiceNode
from common.logger import debug_log


def mark_inactive_nodes_offline(db, timeout_seconds: int = 30) -> int:
    """
    💓 心跳检测与熔断：
    检查所有节点，如果 last_heartbeat 超过 timeout_seconds (默认30秒) 没有更新，
    则将其状态强制置为 'OFFLINE'。

    :param db: 数据库 Session
    :param timeout_seconds: 超时阈值，默认 30 秒
    :return: 被标记为下线的节点数量
    """
    try:
        # 计算截止时间：当前时间 - 30秒
        deadline = datetime.now() - timedelta(seconds=timeout_seconds)

        # 构造批量更新语句
        # 逻辑：把 last_heartbeat < deadline 且当前状态还不是 OFFLINE 的节点，更新为 OFFLINE
        stmt = (
            update(GeminiServiceNode)
            .where(GeminiServiceNode.last_heartbeat < deadline)
            .where(GeminiServiceNode.status != "OFFLINE")  # 避免重复更新已下线的节点
            .values(
                status="OFFLINE",
                dispatched_tasks=0,  # 可选：下线同时清空预订数，防止任务卡死
                current_tasks=0  # 可选：重置当前任务数
            )
        )

        result = db.execute(stmt)
        db.commit()

        affected_rows = result.rowcount
        if affected_rows > 0:
            debug_log(f"📉 心跳检测: 已将 {affected_rows} 个超时节点标记为 OFFLINE", "WARNING")

        return affected_rows

    except Exception as e:
        db.rollback()
        debug_log(f"⚠️ 心跳检测执行失败: {e}", "ERROR")
        return 0


def start_heartbeat_monitor():
    """每隔 10 秒执行一次数据库检查"""
    while True:
        try:
            # 手动创建 Session，用完即关
            db = SessionLocal()
            mark_inactive_nodes_offline(db, timeout_seconds=30)
            db.close()
        except Exception as e:
            print(f"Monitor Loop Error: {e}")

        time.sleep(20)  # 检查间隔可以比超时时间短，例如10秒查一次