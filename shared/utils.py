# shared/utils.py (新建)
import traceback
from shared.database import SessionLocal
from shared.models import SystemLog

def log_error(source: str, message: str, task_id: str = None, error: Exception = None):
    """
    通用错误记录函数
    :param source: 来源 (如 "Worker", "API")
    :param message: 简短描述
    :param task_id: 关联的任务ID (可选)
    :param error: 捕获的 Exception 对象 (可选)
    """
    db = SessionLocal()
    try:
        stack_trace = None
        if error:
            # 获取完整的堆栈字符串
            stack_trace = "".join(traceback.format_exception(type(error), error, error.__traceback__))
            # 如果 message 没填，可以用 error 的字符串
            if not message:
                message = str(error)

        log = SystemLog(
            level="ERROR",
            source=source,
            task_id=task_id,
            message=message,
            stack_trace=stack_trace
        )
        db.add(log)
        db.commit()
        print(f"❌ [已记录日志到数据库] {message}")
    except Exception as e:
        print(f"⚠️ 严重：日志记录失败! {e}")
    finally:
        db.close()