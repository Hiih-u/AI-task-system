# models.py
from sqlalchemy import Column, String, DateTime, Text, ForeignKey, JSON, Float, Integer, BigInteger
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime
from shared.database import Base
from enum import IntEnum


class SystemLog(Base):
    """
    新增：系统日志表
    用于记录详细的报错堆栈，方便开发者排查问题
    """
    __tablename__ = "sys_logs"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)

    # 日志级别: INFO, ERROR, WARNING
    level = Column(String, default="INFO", index=True)

    # 来源: "API-Gateway", "Worker-Gemini", "Worker-SD"
    source = Column(String, index=True)

    # 关联的任务ID (如果是某个任务出错的话)
    task_id = Column(String, index=True, nullable=True)

    # 简短错误信息
    message = Column(Text)

    # 完整堆栈信息 (关键！把 traceback.format_exc() 存进去)
    stack_trace = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.now)


class Conversation(Base):
    """
    新增：会话表
    用于存储对话的上下文状态，实现对话复用
    """
    __tablename__ = "ai_conversations"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    conversation_id = Column(String, unique=True, index=True, default=lambda: str(uuid.uuid4()))
    title = Column(String, nullable=True)

    # 关键字段：存储下游 Worker 需要的会话状态 (如 Gemini 的 metadata, history tokens)
    # 对应 server.py 中的 chat.metadata
    session_metadata = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 建立关系
    tasks = relationship("Task", back_populates="conversation")

class TaskStatus(IntEnum):
    PENDING = 0
    SUCCESS = 1
    FAILED = 2

class Task(Base):
    __tablename__ = "ai_tasks"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    task_id = Column(String, unique=True, index=True, default=lambda: str(uuid.uuid4()))
    conversation_id = Column(String, ForeignKey("ai_conversations.conversation_id"), nullable=True)

    # === 新增字段 ===
    task_type = Column(String, default="TEXT")  # 枚举: "IMAGE" 或 "TEXT"
    response_text = Column(Text, nullable=True)  # 用于存储 AI 的文本回复
    # ================

    status = Column(Integer, default=TaskStatus.PENDING, index=True)
    prompt = Column(Text)

    @property
    def model(self):
        return self.model_name
    model_name = Column(String)

    role = Column(String, default="user")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    cost_time = Column(Float, nullable=True)

    conversation = relationship("Conversation", back_populates="tasks")
    error_msg = Column(Text, nullable=True)