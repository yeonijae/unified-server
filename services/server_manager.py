"""
서버 관리 모듈
- Waitress 기반 Production 서버
- Health Check
- 자동 재시작 스케줄러
"""

import threading
import time
import schedule
from datetime import datetime
from typing import Optional, Callable

# 로그 콜백
log_callback = None


def log(message, server_type="SERVER"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(f"[{server_type}] {log_msg}")
    if log_callback:
        log_callback(log_msg)


class ProductionServer:
    """Waitress 기반 Production 서버"""

    def __init__(self, name: str, app, host: str = '0.0.0.0', port: int = 3100):
        self.name = name
        self.app = app
        self.host = host
        self.port = port
        self._server = None
        self._thread = None
        self._running = False
        self._log_callback = None

    def set_log_callback(self, callback: Callable):
        """로그 콜백 설정"""
        self._log_callback = callback

    def _log(self, message):
        """내부 로그"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        print(f"[{self.name}] {log_msg}")
        if self._log_callback:
            self._log_callback(log_msg)

    def start(self):
        """서버 시작"""
        if self._running:
            self._log("이미 실행 중입니다.")
            return False

        def run():
            try:
                from waitress import serve
                self._running = True
                self._log(f"Waitress 서버 시작 (포트: {self.port})")
                # Waitress는 블로킹 호출
                serve(
                    self.app,
                    host=self.host,
                    port=self.port,
                    threads=8,
                    connection_limit=100,
                    channel_timeout=120,
                    recv_bytes=8192,
                    send_bytes=18000,
                    expose_tracebacks=False,
                    ident='Haniwon-Server'
                )
            except ImportError:
                # Waitress가 없으면 Flask 개발 서버 사용
                self._log("Waitress 미설치 - Flask 개발 서버 사용")
                self._running = True
                self.app.run(host=self.host, port=self.port, threaded=True, use_reloader=False)
            except Exception as e:
                self._log(f"서버 오류: {e}")
                self._running = False

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        return True

    def stop(self):
        """서버 중지 (Waitress는 graceful shutdown 지원 제한적)"""
        if not self._running:
            return

        self._running = False
        self._log("서버 중지 요청됨")
        # Waitress는 스레드 기반이라 완전 중지가 어려움
        # 프로세스 재시작이 필요할 수 있음

    def is_running(self):
        """실행 상태 확인"""
        return self._running


class HealthMonitor:
    """Health Check 및 자동 재시작 모니터"""

    def __init__(self):
        self._running = False
        self._thread = None
        self._check_interval = 60  # 초
        self._restart_time = "04:00"  # 새벽 4시 자동 재시작
        self._health_checks = {}  # name -> check_function
        self._restart_callbacks = {}  # name -> restart_function
        self._log_callback = None
        self._scheduler_thread = None

    def set_log_callback(self, callback: Callable):
        """로그 콜백 설정"""
        self._log_callback = callback

    def _log(self, message):
        """내부 로그"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        print(f"[HEALTH] {log_msg}")
        if self._log_callback:
            self._log_callback(log_msg)

    def register_service(self, name: str, health_check: Callable, restart_callback: Callable):
        """서비스 등록"""
        self._health_checks[name] = health_check
        self._restart_callbacks[name] = restart_callback
        self._log(f"서비스 등록: {name}")

    def set_restart_time(self, time_str: str):
        """자동 재시작 시간 설정 (HH:MM 형식)"""
        self._restart_time = time_str
        self._log(f"자동 재시작 시간: {time_str}")

    def check_health(self, name: str = None):
        """상태 확인"""
        results = {}

        if name:
            # 특정 서비스만 확인
            if name in self._health_checks:
                try:
                    results[name] = self._health_checks[name]()
                except Exception as e:
                    results[name] = {"healthy": False, "error": str(e)}
        else:
            # 모든 서비스 확인
            for svc_name, check_fn in self._health_checks.items():
                try:
                    results[svc_name] = check_fn()
                except Exception as e:
                    results[svc_name] = {"healthy": False, "error": str(e)}

        return results

    def _do_scheduled_restart(self):
        """예약된 재시작 실행"""
        self._log("예약된 서버 재시작 시작...")
        for name, restart_fn in self._restart_callbacks.items():
            try:
                self._log(f"{name} 재시작 중...")
                restart_fn()
                self._log(f"{name} 재시작 완료")
            except Exception as e:
                self._log(f"{name} 재시작 실패: {e}")

    def _run_scheduler(self):
        """스케줄러 실행"""
        # 매일 지정된 시간에 재시작
        schedule.every().day.at(self._restart_time).do(self._do_scheduled_restart)
        self._log(f"자동 재시작 스케줄 등록: 매일 {self._restart_time}")

        while self._running:
            schedule.run_pending()
            time.sleep(1)

    def _run_health_monitor(self):
        """상태 모니터링 루프"""
        while self._running:
            time.sleep(self._check_interval)
            if not self._running:
                break

            # 상태 확인
            for name, check_fn in self._health_checks.items():
                try:
                    result = check_fn()
                    if not result.get("healthy", False):
                        self._log(f"{name} 상태 이상 감지: {result.get('message', 'Unknown')}")
                        # 자동 재시작 시도
                        if name in self._restart_callbacks:
                            self._log(f"{name} 자동 재시작 시도...")
                            try:
                                self._restart_callbacks[name]()
                                self._log(f"{name} 재시작 성공")
                            except Exception as e:
                                self._log(f"{name} 재시작 실패: {e}")
                except Exception as e:
                    self._log(f"{name} 상태 확인 실패: {e}")

    def start(self):
        """모니터링 시작"""
        if self._running:
            return

        self._running = True

        # 스케줄러 스레드
        self._scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self._scheduler_thread.start()

        # 상태 모니터링 스레드
        self._thread = threading.Thread(target=self._run_health_monitor, daemon=True)
        self._thread.start()

        self._log("상태 모니터링 시작")

    def stop(self):
        """모니터링 중지"""
        self._running = False
        schedule.clear()
        self._log("상태 모니터링 중지")


# 전역 인스턴스
health_monitor = HealthMonitor()


def get_health_monitor():
    """Health Monitor 인스턴스 반환"""
    return health_monitor
