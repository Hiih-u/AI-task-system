# shared/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# 优先读取环境变量，本地开发才用默认值
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Hi8899")
DB_HOST = os.getenv("DB_HOST", "192.168.202.155") # Docker里会变成 "db"
DB_PORT = os.getenv("DB_PORT", "61020")          # Docker里通常是 "5432"
DB_NAME = os.getenv("POSTGRES_DB", "gemini")

SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()