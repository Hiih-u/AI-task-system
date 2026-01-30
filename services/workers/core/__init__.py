# services/workers/core/__init__.py

# 1. 导出 IO 模块
from .io.message_io import parse_and_validate, recover_pending_tasks
from .io.upload_file import upload_files_to_downstream

# 2. 导出 Data 模块
from .data.task_state import claim_task, mark_task_failed, update_node_load
from .data.context_loader import build_conversation_context
from .data.auditor import process_ai_result

# 3. 导出 Dispatch 模块
from .dispatch.node_manager import acquire_node_with_retry, release_node_safe
from .dispatch.router import get_database_target_url

# 4. 导出 Core Runner
from .runner import run_chat_task

# 定义 __all__ 让 IDE 提示更友好
__all__ = [
    "parse_and_validate",
    "upload_files_to_downstream",
    "claim_task",
    "mark_task_failed",
    "process_ai_result",
    "update_node_load",
    "build_conversation_context",
    "recover_pending_tasks",
    "acquire_node_with_retry",
    "release_node_safe",
    "get_database_target_url",
    "run_chat_task"
]