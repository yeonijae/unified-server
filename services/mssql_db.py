"""
MSSQL 데이터베이스 연결 모듈
"""

from datetime import datetime
from config import load_config

# 로그 콜백 (GUI에서 설정)
log_callback = None


def log(message):
    """MSSQL 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(f"[MSSQL] {log_msg}")
    if log_callback:
        log_callback(log_msg)


def get_connection():
    """MSSQL 연결 생성"""
    try:
        import pymssql
        config = load_config()
        mssql_config = config.get('mssql', {})

        conn = pymssql.connect(
            server=mssql_config.get('server', '192.168.0.173'),
            user=mssql_config.get('user', 'members'),
            password=mssql_config.get('password', 'msp1234'),
            port=mssql_config.get('port', 55555),
            database=mssql_config.get('database', 'MasterDB'),
            charset='utf8'
        )
        return conn
    except Exception as e:
        log(f"연결 오류: {e}")
        return None


def test_connection():
    """MSSQL 연결 테스트"""
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Customer")
            count = cursor.fetchone()[0]
            conn.close()
            return {"success": True, "count": count}
        return {"success": False, "error": "연결 실패"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def format_referral_source(suggest, cust_url, suggcustnamesn):
    """유입경로 조합"""
    if not suggest:
        return None

    if '소개' in suggest:
        if suggcustnamesn:
            return f"{suggest} ({suggcustnamesn})"
        elif cust_url:
            return f"{suggest} ({cust_url})"
        return suggest

    if suggest in ['네이버', '인터넷', '지도검색', '홈페이지']:
        if cust_url:
            return f"{suggest} - {cust_url}"
        return suggest

    return suggest
