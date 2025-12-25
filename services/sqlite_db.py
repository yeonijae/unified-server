"""
SQLite 데이터베이스 연결 및 백업 모듈
"""

import sqlite3
import shutil
import threading
import schedule
import time
from datetime import datetime
from pathlib import Path
from config import load_config, save_config

# 전역 변수
db_path = None
backup_thread = None

# 로그 콜백 (GUI에서 설정)
log_callback = None

# SQL 로깅 활성화 여부 (기본: 비활성화 - 성능 향상)
sql_logging_enabled = False


def log(message, force=False):
    """SQLite 로그 출력

    Args:
        message: 로그 메시지
        force: True이면 sql_logging_enabled와 관계없이 항상 출력 (백업, 에러 등)
    """
    if not force and not sql_logging_enabled:
        return

    timestamp = datetime.now().strftime("%H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(f"[SQLite] {log_msg}")
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


def set_db_path(path):
    """DB 경로 설정"""
    global db_path
    db_path = path


def get_db_path():
    """현재 DB 경로 반환"""
    return db_path


def get_connection():
    """SQLite 연결 생성 (WAL 모드 + 타임아웃)"""
    if not db_path:
        raise Exception("SQLite DB path not configured")
    # timeout=30: 30초 동안 락 대기 (기본 5초)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL 모드: 동시 읽기/쓰기 지원
    conn.execute("PRAGMA journal_mode=WAL")
    # busy_timeout 추가 (밀리초)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def test_connection():
    """SQLite 연결 테스트"""
    try:
        if not db_path:
            return {"success": False, "error": "DB path not set"}
        conn = get_connection()
        conn.execute("SELECT 1")
        conn.close()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============ 백업 기능 ============

def do_backup():
    """백업 실행"""
    config = load_config()
    if not config.get("backup_enabled") or not config.get("backup_folder") or not db_path:
        return False

    backup_folder = Path(config["backup_folder"])
    if not backup_folder.exists():
        try:
            backup_folder.mkdir(parents=True)
        except:
            return False

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_name = Path(db_path).stem
    backup_file = backup_folder / f"{db_name}_backup_{timestamp}.sqlite"

    try:
        shutil.copy2(db_path, backup_file)
        config["last_backup"] = datetime.now().isoformat()
        save_config(config)
        log(f"백업 완료: {backup_file.name}", force=True)
        return True
    except Exception as e:
        log(f"백업 실패: {e}", force=True)
        return False


def backup_scheduler():
    """백업 스케줄러 루프"""
    while True:
        schedule.run_pending()
        time.sleep(60)


def start_backup_scheduler():
    """백업 스케줄러 시작"""
    global backup_thread
    config = load_config()

    schedule.clear()

    if config.get("backup_enabled") and config.get("backup_interval_hours", 0) > 0:
        interval = config["backup_interval_hours"]
        schedule.every(interval).hours.do(do_backup)

        if backup_thread is None or not backup_thread.is_alive():
            backup_thread = threading.Thread(target=backup_scheduler, daemon=True)
            backup_thread.start()
            log(f"백업 스케줄러 시작 (간격: {interval}시간)", force=True)
