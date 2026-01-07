"""
채팅 서버용 PostgreSQL 데이터베이스 모듈
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

# 설정
_config = None
_log_callback = None


def init_db(config: dict, log_callback=None):
    """DB 초기화"""
    global _config, _log_callback
    _config = config
    _log_callback = log_callback


def log(message: str):
    """로그 출력"""
    if _log_callback:
        _log_callback(message)
    else:
        print(f"[Chat DB] {message}")


def get_connection():
    """DB 연결 생성"""
    if not _config:
        raise Exception("DB not initialized. Call init_db() first.")

    return psycopg2.connect(
        host=_config.get('host', 'localhost'),
        port=_config.get('port', 5432),
        user=_config.get('user', ''),
        password=_config.get('password', ''),
        database=_config.get('database', ''),
        cursor_factory=RealDictCursor
    )


@contextmanager
def get_cursor():
    """커서 컨텍스트 매니저"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def execute(sql: str, params: tuple = None) -> int:
    """SQL 실행 (INSERT, UPDATE, DELETE)"""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


def query(sql: str, params: tuple = None) -> list:
    """SELECT 쿼리 실행 - 여러 행"""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_one(sql: str, params: tuple = None) -> dict | None:
    """SELECT 쿼리 실행 - 단일 행"""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def test_connection() -> bool:
    """연결 테스트"""
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception as e:
        log(f"Connection failed: {e}")
        return False


def ensure_tables():
    """필요한 테이블 생성 확인"""
    # chat_sessions 테이블 생성 (없으면)
    execute("""
        CREATE TABLE IF NOT EXISTS chat_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES chat_users(id) ON DELETE CASCADE,
            token VARCHAR(64) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP NOT NULL,
            last_active_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # 인덱스 생성
    execute("""
        CREATE INDEX IF NOT EXISTS idx_chat_sessions_token ON chat_sessions(token)
    """)
    execute("""
        CREATE INDEX IF NOT EXISTS idx_chat_sessions_user_id ON chat_sessions(user_id)
    """)

    log("Tables ensured")
