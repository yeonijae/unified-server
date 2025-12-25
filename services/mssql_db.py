"""
MSSQL 데이터베이스 연결 모듈
- Connection Pool 지원
- 자동 재연결
- 연결 상태 모니터링
- 기존 코드 호환 (conn.close() 호출 시 풀에 반환)
"""

import threading
import queue
import time
from datetime import datetime
from config import load_config

# 로그 콜백 (GUI에서 설정)
log_callback = None

# Connection Pool 설정
POOL_SIZE = 5
POOL_TIMEOUT = 30  # 연결 대기 시간 (초)
CONNECTION_MAX_AGE = 300  # 연결 최대 수명 (5분)
HEALTH_CHECK_INTERVAL = 60  # 상태 체크 간격 (초)


class PooledConnection:
    """
    Connection Pool 래퍼
    - 기존 pymssql 연결처럼 사용 가능
    - close() 호출 시 풀에 반환
    """

    def __init__(self, conn, pool, created_at=None):
        self._conn = conn
        self._pool = pool
        self._created_at = created_at or time.time()
        self._last_used = time.time()
        self._use_count = 0
        self._closed = False

    def cursor(self, as_dict=False):
        """커서 생성"""
        self._last_used = time.time()
        self._use_count += 1
        return self._conn.cursor(as_dict=as_dict)

    def commit(self):
        """커밋"""
        return self._conn.commit()

    def rollback(self):
        """롤백"""
        return self._conn.rollback()

    def close(self):
        """연결 닫기 - 실제로는 풀에 반환"""
        if self._closed:
            return
        self._closed = True
        if self._pool:
            self._pool._return_connection(self)
        else:
            # 풀이 없으면 실제로 닫기
            try:
                self._conn.close()
            except:
                pass

    def _force_close(self):
        """강제 연결 종료 (풀에서 제거할 때)"""
        try:
            self._conn.close()
        except:
            pass

    def is_expired(self):
        """연결 수명 초과 여부"""
        return time.time() - self._created_at > CONNECTION_MAX_AGE

    def is_valid(self):
        """연결 유효성 검사"""
        if self._conn is None or self._closed:
            return False
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class ConnectionPool:
    """MSSQL Connection Pool"""

    def __init__(self, size=POOL_SIZE):
        self.size = size
        self.pool = queue.Queue(maxsize=size)
        self.lock = threading.Lock()
        self.created_count = 0
        self.active_count = 0
        self._initialized = False
        self._config = None

    def initialize(self, config=None):
        """풀 초기화"""
        if config:
            self._config = config
        else:
            full_config = load_config()
            self._config = full_config.get('mssql', {})

        # 기존 연결 정리
        self._clear_pool()

        # 초기 연결 생성 (지연 로딩으로 변경 - 첫 요청 시 생성)
        self._initialized = True
        log(f"Connection Pool 초기화 완료 (size={self.size}, lazy loading)")

    def _clear_pool(self):
        """풀 비우기"""
        while not self.pool.empty():
            try:
                pooled = self.pool.get_nowait()
                if pooled:
                    pooled._force_close()
            except queue.Empty:
                break
        self.created_count = 0
        self.active_count = 0

    def _create_connection(self):
        """새 연결 생성"""
        try:
            import pymssql
            conn = pymssql.connect(
                server=self._config.get('server', '192.168.0.173'),
                user=self._config.get('user', 'members'),
                password=self._config.get('password', 'msp1234'),
                port=self._config.get('port', 55555),
                database=self._config.get('database', 'MasterDB'),
                charset='utf8',
                login_timeout=10,
                timeout=30
            )
            self.created_count += 1
            return PooledConnection(conn, self)
        except Exception as e:
            log(f"연결 생성 실패: {e}")
            return None

    def get_connection(self, database=None):
        """풀에서 연결 획득"""
        if not self._initialized:
            self.initialize()

        pooled = None

        # 1. 풀에서 기존 연결 시도
        try:
            pooled = self.pool.get_nowait()

            # 만료되었거나 유효하지 않은 연결은 폐기
            if pooled.is_expired() or not pooled.is_valid():
                pooled._force_close()
                pooled = None
        except queue.Empty:
            pass

        # 2. 새 연결 생성
        if pooled is None:
            with self.lock:
                pooled = self._create_connection()

        if pooled is None:
            log("연결 획득 실패: 연결 생성 실패")
            return None

        # 연결 상태 리셋
        pooled._closed = False
        pooled._last_used = time.time()

        # 데이터베이스 변경 필요 시
        if database and database != self._config.get('database', 'MasterDB'):
            try:
                cursor = pooled._conn.cursor()
                cursor.execute(f"USE [{database}]")
                cursor.close()
            except Exception as e:
                log(f"데이터베이스 변경 실패: {e}")

        self.active_count += 1
        return pooled

    def _return_connection(self, pooled):
        """연결을 풀에 반환 (PooledConnection.close()에서 호출)"""
        if pooled is None:
            return

        self.active_count = max(0, self.active_count - 1)

        # 만료된 연결은 폐기
        if pooled.is_expired():
            pooled._force_close()
            return

        # 풀에 반환
        try:
            pooled._closed = False  # 재사용 가능하도록
            self.pool.put_nowait(pooled)
        except queue.Full:
            # 풀이 가득 차면 연결 폐기
            pooled._force_close()

    def get_stats(self):
        """풀 상태 반환"""
        return {
            "pool_size": self.size,
            "available": self.pool.qsize(),
            "active": self.active_count,
            "total_created": self.created_count,
            "initialized": self._initialized
        }

    def health_check(self):
        """풀 상태 점검"""
        stats = self.get_stats()

        # 테스트 연결
        try:
            pooled = self.get_connection()
            if pooled and pooled.is_valid():
                pooled.close()  # 풀에 반환
                stats["healthy"] = True
                stats["message"] = "OK"
            else:
                stats["healthy"] = False
                stats["message"] = "연결 테스트 실패"
        except Exception as e:
            stats["healthy"] = False
            stats["message"] = str(e)

        return stats


# 전역 Connection Pool 인스턴스
_pool = ConnectionPool()


def log(message):
    """MSSQL 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(f"[MSSQL] {log_msg}")
    if log_callback:
        log_callback(log_msg)


def initialize_pool(config=None):
    """풀 초기화 (서버 시작 시 호출)"""
    _pool.initialize(config)


def get_connection(database=None):
    """MSSQL 연결 획득 (풀 사용)

    Args:
        database: 데이터베이스 이름 (기본값: config의 database 또는 MasterDB)

    Returns:
        PooledConnection (pymssql 연결처럼 사용 가능, close() 호출 시 풀에 반환)
    """
    return _pool.get_connection(database)


def get_pool_stats():
    """풀 상태 조회"""
    return _pool.get_stats()


def health_check():
    """상태 점검"""
    return _pool.health_check()


def test_connection():
    """MSSQL 연결 테스트"""
    conn = None
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Customer")
            count = cursor.fetchone()[0]
            cursor.close()
            return {"success": True, "count": count, "pool": get_pool_stats()}
        return {"success": False, "error": "연결 실패", "pool": get_pool_stats()}
    except Exception as e:
        return {"success": False, "error": str(e), "pool": get_pool_stats()}
    finally:
        if conn:
            conn.close()  # 풀에 반환


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
