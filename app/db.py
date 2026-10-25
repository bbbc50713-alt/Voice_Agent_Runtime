"""数据库装配:engine + SessionLocal + get_db 依赖 + init_db。

默认 SQLite(项目根 voice.db,check_same_thread=False 供 WS 协程跨线程用);
DATABASE_URL 可切 PostgreSQL,字段类型保持方言中立。
"""

from __future__ import annotations

from sqlalchemy import JSON, DateTime, create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings


def _make_engine():
    url = get_settings().database_url
    kwargs: dict = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)


engine = _make_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    from app import models  # noqa: F401 — 注册全部表

    models.Base.metadata.create_all(engine)
    _ensure_compat_columns()


def _ensure_compat_columns() -> None:
    """补齐既有数据库缺失的轻量字段。

    该服务没有引入迁移框架；create_all 不会给已有表追加列，因此这里逐条幂等
    补列：speaker_events.created_at(隐私清理线程依赖)、voice_tasks.delivery/
    dedupe_key(2026-10-14 播报策略+聚合交付契约)。
    """
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    specs = (
        ("speaker_events", "created_at", str(DateTime().compile(dialect=engine.dialect))),
        ("voice_tasks", "delivery", str(JSON().compile(dialect=engine.dialect))),
        ("voice_tasks", "dedupe_key", "VARCHAR(64)"),
    )
    for table, column, column_type in specs:
        if table not in tables:
            continue
        columns = {col["name"] for col in inspector.get_columns(table)}
        if column in columns:
            continue
        with engine.begin() as connection:
            connection.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}"))
            if table == "speaker_events" and column == "created_at":
                connection.execute(
                    text("UPDATE speaker_events SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL")
                )
