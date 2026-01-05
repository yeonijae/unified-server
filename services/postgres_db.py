"""
PostgreSQL 데이터베이스 연결 모듈 (Connection Pooling 지원)
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import threading
from datetime import datetime
from config import load_config

# 전역 변수
_config = None
_connection_pool = None
_pool_lock = threading.Lock()

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
    global _config, _connection_pool
    _config = None
    # 풀도 리셋
    close_pool()
    return get_db_config()


def init_pool(min_conn=5, max_conn=20):
    """연결 풀 초기화"""
    global _connection_pool

    with _pool_lock:
        if _connection_pool is not None:
            return _connection_pool

        config = get_db_config()
        if not config:
            raise Exception("PostgreSQL config not found")

        try:
            _connection_pool = pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=config.get('host', 'localhost'),
                port=config.get('port', 5432),
                user=config.get('user', ''),
                password=config.get('password', ''),
                database=config.get('database', ''),
                connect_timeout=10
            )
            log(f"연결 풀 초기화 완료 (min={min_conn}, max={max_conn})", force=True)
            return _connection_pool
        except Exception as e:
            log(f"연결 풀 초기화 실패: {e}", force=True)
            raise e


def close_pool():
    """연결 풀 종료"""
    global _connection_pool

    with _pool_lock:
        if _connection_pool is not None:
            _connection_pool.closeall()
            _connection_pool = None
            log("연결 풀 종료", force=True)


def get_pool():
    """연결 풀 가져오기 (없으면 생성)"""
    global _connection_pool

    if _connection_pool is None:
        init_pool()
    return _connection_pool


def get_connection():
    """연결 풀에서 연결 가져오기"""
    try:
        p = get_pool()
        conn = p.getconn()
        return conn
    except Exception as e:
        log(f"연결 풀에서 연결 가져오기 실패: {e}", force=True)
        # 풀 실패 시 직접 연결 시도
        config = get_db_config()
        return psycopg2.connect(
            host=config.get('host', 'localhost'),
            port=config.get('port', 5432),
            user=config.get('user', ''),
            password=config.get('password', ''),
            database=config.get('database', ''),
            connect_timeout=10
        )


def put_connection(conn):
    """연결을 풀에 반환"""
    try:
        p = get_pool()
        p.putconn(conn)
    except Exception as e:
        log(f"연결 반환 실패: {e}")
        try:
            conn.close()
        except:
            pass


def get_dict_connection():
    """딕셔너리 형태로 결과 반환하는 연결 (풀 사용)"""
    conn = get_connection()
    # RealDictCursor는 cursor 생성 시 지정
    return conn


def test_connection():
    """PostgreSQL 연결 테스트"""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        put_connection(conn)
        log("연결 테스트 성공", force=True)
        return {"success": True}
    except Exception as e:
        log(f"연결 테스트 실패: {e}", force=True)
        if conn:
            put_connection(conn)
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
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        log(f"Query: {query[:100]}...")
        if params:
            log(f"Params: {params}")

        cur.execute(query, params)

        if fetch:
            results = cur.fetchall()
            # RealDictRow를 일반 dict로 변환
            results = [dict(row) for row in results]
            cur.close()
            put_connection(conn)
            log(f"Results: {len(results)} rows")
            return results
        else:
            affected = cur.rowcount
            conn.commit()
            cur.close()
            put_connection(conn)
            log(f"Affected rows: {affected}")
            return affected

    except Exception as e:
        log(f"Query error: {e}", force=True)
        if conn:
            conn.rollback()
            put_connection(conn)
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
        put_connection(conn)

        log(f"Affected rows: {affected}")
        return affected

    except Exception as e:
        log(f"Execute many error: {e}", force=True)
        if conn:
            conn.rollback()
            put_connection(conn)
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
