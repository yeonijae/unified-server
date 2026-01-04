"""
PostgreSQL 데이터베이스 연결 모듈
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import threading
from datetime import datetime
from config import load_config

# 전역 변수
_config = None

# 로그 콜백 (GUI에서 설정)
log_callback = None

# SQL 로깅 활성화 여부
sql_logging_enabled = False


def log(message, force=False):
    """PostgreSQL 로그 출력"""
    if not force and not sql_logging_enabled:
        return

    timestamp = datetime.now().strftime("%H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(f"[PostgreSQL] {log_msg}")
    if log_callback:
        log_callback(log_msg)


def set_sql_logging(enabled: bool):
    """SQL 로깅 활성화/비활성화"""
    global sql_logging_enabled
    sql_logging_enabled = enabled
    log(f"SQL 로깅 {'활성화' if enabled else '비활성화'}됨", force=True)


def is_sql_logging_enabled():
    """SQL 로깅 상태 확인"""
    return sql_logging_enabled


def get_db_config():
    """PostgreSQL 설정 로드"""
    global _config
    if _config is None:
        config = load_config()
        _config = config.get('postgres', {})
    return _config


def reload_config():
    """설정 다시 로드"""
    global _config
    _config = None
    return get_db_config()


def get_connection():
    """PostgreSQL 연결 생성"""
    config = get_db_config()

    if not config:
        raise Exception("PostgreSQL config not found")

    conn = psycopg2.connect(
        host=config.get('host', 'localhost'),
        port=config.get('port', 5432),
        user=config.get('user', ''),
        password=config.get('password', ''),
        database=config.get('database', ''),
        connect_timeout=10
    )
    return conn


def get_dict_connection():
    """딕셔너리 형태로 결과 반환하는 연결"""
    config = get_db_config()

    if not config:
        raise Exception("PostgreSQL config not found")

    conn = psycopg2.connect(
        host=config.get('host', 'localhost'),
        port=config.get('port', 5432),
        user=config.get('user', ''),
        password=config.get('password', ''),
        database=config.get('database', ''),
        connect_timeout=10,
        cursor_factory=RealDictCursor
    )
    return conn


def test_connection():
    """PostgreSQL 연결 테스트"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        log("연결 테스트 성공", force=True)
        return {"success": True}
    except Exception as e:
        log(f"연결 테스트 실패: {e}", force=True)
        return {"success": False, "error": str(e)}


def execute_query(query, params=None, fetch=True):
    """쿼리 실행

    Args:
        query: SQL 쿼리
        params: 쿼리 파라미터 (튜플 또는 딕셔너리)
        fetch: True면 결과 반환, False면 commit만

    Returns:
        fetch=True: 결과 리스트 (딕셔너리)
        fetch=False: affected rows 수
    """
    conn = None
    try:
        conn = get_dict_connection()
        cur = conn.cursor()

        log(f"Query: {query[:100]}...")
        if params:
            log(f"Params: {params}")

        cur.execute(query, params)

        if fetch:
            results = cur.fetchall()
            # RealDictRow를 일반 dict로 변환
            results = [dict(row) for row in results]
            cur.close()
            conn.close()
            log(f"Results: {len(results)} rows")
            return results
        else:
            affected = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            log(f"Affected rows: {affected}")
            return affected

    except Exception as e:
        log(f"Query error: {e}", force=True)
        if conn:
            conn.rollback()
            conn.close()
        raise e


def execute_many(query, params_list):
    """여러 행 삽입/업데이트

    Args:
        query: SQL 쿼리
        params_list: 파라미터 리스트

    Returns:
        affected rows 수
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        log(f"Execute many: {query[:100]}... ({len(params_list)} rows)")

        cur.executemany(query, params_list)
        affected = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()

        log(f"Affected rows: {affected}")
        return affected

    except Exception as e:
        log(f"Execute many error: {e}", force=True)
        if conn:
            conn.rollback()
            conn.close()
        raise e


def get_tables():
    """테이블 목록 조회"""
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """
    return execute_query(query)


def get_table_columns(table_name):
    """테이블 컬럼 정보 조회"""
    query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """
    return execute_query(query, (table_name,))
