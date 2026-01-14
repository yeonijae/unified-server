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

    # 1. chat_users 테이블
    execute("""
        CREATE TABLE IF NOT EXISTS chat_users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            portal_user_id VARCHAR(100) UNIQUE,
            email VARCHAR(255) UNIQUE,
            password_hash VARCHAR(255),
            display_name VARCHAR(100) NOT NULL,
            avatar_url TEXT,
            avatar_color VARCHAR(7),
            status VARCHAR(20) DEFAULT 'offline',
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)

    # portal_user_id 컬럼 추가 (기존 테이블용)
    try:
        execute("ALTER TABLE chat_users ADD COLUMN IF NOT EXISTS portal_user_id VARCHAR(100) UNIQUE")
    except:
        pass

    # email 컬럼 NOT NULL 제약조건 제거 (포털 인증에서는 email 불필요)
    try:
        execute("ALTER TABLE chat_users ALTER COLUMN email DROP NOT NULL")
    except:
        pass

    # 2. chat_sessions 테이블
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

    # 3. chat_channels 테이블
    execute("""
        CREATE TABLE IF NOT EXISTS chat_channels (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            type VARCHAR(20) NOT NULL DEFAULT 'group',
            name VARCHAR(100),
            description TEXT,
            avatar_url TEXT,
            is_private BOOLEAN DEFAULT false,
            created_by UUID REFERENCES chat_users(id),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            deleted_at TIMESTAMP WITH TIME ZONE
        )
    """)

    # 4. chat_channel_members 테이블
    execute("""
        CREATE TABLE IF NOT EXISTS chat_channel_members (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            channel_id UUID NOT NULL REFERENCES chat_channels(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES chat_users(id) ON DELETE CASCADE,
            role VARCHAR(20) DEFAULT 'member',
            last_read_message_id UUID,
            last_read_at TIMESTAMP WITH TIME ZONE,
            is_muted BOOLEAN DEFAULT false,
            joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(channel_id, user_id)
        )
    """)

    # 5. chat_messages 테이블
    execute("""
        CREATE TABLE IF NOT EXISTS chat_messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            channel_id UUID NOT NULL REFERENCES chat_channels(id) ON DELETE CASCADE,
            sender_id UUID REFERENCES chat_users(id) ON DELETE SET NULL,
            parent_id UUID REFERENCES chat_messages(id) ON DELETE CASCADE,
            thread_count INTEGER DEFAULT 0,
            content TEXT NOT NULL,
            type VARCHAR(20) DEFAULT 'text',
            metadata JSONB DEFAULT '{}',
            is_edited BOOLEAN DEFAULT false,
            is_pinned BOOLEAN DEFAULT false,
            deleted_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)

    # 6. chat_user_preferences 테이블 (레이아웃 저장용)
    execute("""
        CREATE TABLE IF NOT EXISTS chat_user_preferences (
            user_id UUID PRIMARY KEY REFERENCES chat_users(id) ON DELETE CASCADE,
            layout_data JSONB DEFAULT '{}',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)

    # 7. chat_message_reactions 테이블
    execute("""
        CREATE TABLE IF NOT EXISTS chat_message_reactions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            message_id UUID NOT NULL REFERENCES chat_messages(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES chat_users(id) ON DELETE CASCADE,
            emoji VARCHAR(50) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(message_id, user_id, emoji)
        )
    """)

    # 8. chat_user_settings 테이블
    execute("""
        CREATE TABLE IF NOT EXISTS chat_user_settings (
            user_id UUID PRIMARY KEY REFERENCES chat_users(id) ON DELETE CASCADE,
            notification_enabled BOOLEAN DEFAULT true,
            notification_sound BOOLEAN DEFAULT true,
            theme VARCHAR(20) DEFAULT 'system',
            language VARCHAR(10) DEFAULT 'ko',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)

    # 인덱스 생성
    execute("CREATE INDEX IF NOT EXISTS idx_chat_sessions_token ON chat_sessions(token)")
    execute("CREATE INDEX IF NOT EXISTS idx_chat_sessions_user_id ON chat_sessions(user_id)")
    execute("CREATE INDEX IF NOT EXISTS idx_chat_messages_channel_id ON chat_messages(channel_id)")
    execute("CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON chat_messages(created_at DESC)")
    execute("CREATE INDEX IF NOT EXISTS idx_chat_channel_members_user_id ON chat_channel_members(user_id)")

    # 기존 테이블에 누락된 컬럼 추가
    try:
        execute("ALTER TABLE chat_channels ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITH TIME ZONE")
        execute("ALTER TABLE chat_users ADD COLUMN IF NOT EXISTS status_message VARCHAR(255)")
        execute("ALTER TABLE chat_users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP WITH TIME ZONE")
        execute("ALTER TABLE chat_channel_members ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN DEFAULT false")
        execute("CREATE INDEX IF NOT EXISTS idx_chat_message_reactions_message_id ON chat_message_reactions(message_id)")
    except:
        pass  # 컬럼이 이미 있거나 에러 무시

    log("Tables ensured")
