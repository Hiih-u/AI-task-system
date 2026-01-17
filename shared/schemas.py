# schemas.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, Any

class TaskSubmitResponse(BaseModel):
    task_id: str
    status: int
    conversation_id: str  # 返回会话ID，方便前端下次使用
    message: str = "请求成功"

# 任务状态响应
class TaskQueryResponse(BaseModel):
    task_id: str
    conversation_id: Optional[str]
    status: int
    task_type: str  # 新增：让前端知道是文本还是图片
    prompt: str
    created_at: datetime
    cost_time: Optional[float] = None
    # 结果字段
    response_text: Optional[str] = None

    model: str

    class Config:
        from_attributes = True

# 新增：会话详情响应（用于查看历史）
class ConversationResponse(BaseModel):
    conversation_id: str
    title: Optional[str]
    created_at: datetime
    updated_at: datetime
    # 可以在这里嵌套 tasks 列表来返回历史记录，视需求而定

    class Config:
        from_attributes = True

# === 新增：文本对话请求 ===
class ChatRequest(BaseModel):
    prompt: str
    model: str = "gemini-2.5-flash"
    conversation_id: Optional[str] = None