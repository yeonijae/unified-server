"""
Git 및 Bun 빌드 모듈
"""

import os
import sys
import subprocess
import threading
import time
import hmac
import hashlib
from pathlib import Path
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from config import APP_DIR, load_config, save_config

# 경로 설정
BUN_EXE = APP_DIR / "bun.exe"
GIT_DIR = APP_DIR / "mingit"
GIT_EXE = GIT_DIR / "cmd" / "git.exe"

# 전역 변수
observer = None
is_building = False
build_lock = threading.Lock()
last_webhook_time = None

# 로그 콜백 (GUI에서 설정)
log_callback = None


def log(message):
    """Static 서버 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(f"[Static] {log_msg}")
    if log_callback:
        log_callback(log_msg)


def bun_exists():
    """Bun 실행파일 존재 여부"""
    return BUN_EXE.exists()


def git_exists():
    """Git 실행파일 존재 여부"""
    return GIT_EXE.exists()


# ============ Git 명령어 ============

def get_git_command():
    """내장 Git 또는 시스템 Git 경로 반환"""
    if GIT_EXE.exists():
        return str(GIT_EXE)
    return "git"


def get_git_env():
    """Git 실행에 필요한 환경변수"""
    env = os.environ.copy()
    if GIT_DIR.exists():
        mingw_bin = GIT_DIR / "mingw64" / "bin"
        usr_bin = GIT_DIR / "usr" / "bin"
        env["PATH"] = f"{mingw_bin};{usr_bin};{GIT_DIR / 'cmd'};{env.get('PATH', '')}"
    return env


def run_git_clone(repo_url, target_path, callback=None):
    """git clone 실행"""
    try:
        if callback:
            callback(None, f"Cloning {repo_url}...")

        git_cmd = get_git_command()
        git_env = get_git_env()

        result = subprocess.run(
            [git_cmd, "clone", repo_url, target_path],
            capture_output=True,
            text=True,
            env=git_env,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )

        if result.returncode == 0:
            if callback:
                callback(True, "Clone successful")
            log(f"Clone 성공: {target_path}")
            return True
        else:
            if callback:
                callback(False, f"Clone failed: {result.stderr[:200]}")
            log(f"Clone 실패: {result.stderr[:100]}")
            return False
    except FileNotFoundError:
        if callback:
            callback(False, "Git not found!")
        return False
    except Exception as e:
        if callback:
            callback(False, f"Git error: {e}")
        return False


def run_git_pull(project_path, callback=None):
    """git pull 실행"""
    try:
        if callback:
            callback(None, "Running git pull...")

        git_cmd = get_git_command()
        git_env = get_git_env()

        result = subprocess.run(
            [git_cmd, "pull"],
            cwd=project_path,
            capture_output=True,
            text=True,
            env=git_env,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )

        output = result.stdout.strip()

        if result.returncode == 0:
            if "Already up to date" in output or "Already up-to-date" in output:
                if callback:
                    callback(True, "Already up to date")
                log("Git pull: 변경 없음")
                return True, False  # 성공, 변경 없음
            else:
                if callback:
                    callback(True, f"Pull successful: {output[:100]}")
                log("Git pull: 변경 있음")
                return True, True  # 성공, 변경 있음
        else:
            if callback:
                callback(False, f"Git pull failed: {result.stderr}")
            log(f"Git pull 실패: {result.stderr[:100]}")
            return False, False
    except FileNotFoundError:
        if callback:
            callback(False, "Git not found!")
        return False, False
    except Exception as e:
        if callback:
            callback(False, f"Git error: {e}")
        return False, False


# ============ Bun 빌드 ============

def run_bun_install(project_path, callback=None):
    """bun install 실행"""
    global is_building

    if not BUN_EXE.exists():
        if callback:
            callback(False, "bun.exe not found")
        return False

    try:
        is_building = True
        if callback:
            callback(None, "Installing dependencies...")
        log("bun install 시작...")

        result = subprocess.run(
            [str(BUN_EXE), "install"],
            cwd=project_path,
            capture_output=True,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )

        if result.returncode == 0:
            if callback:
                callback(True, "Dependencies installed")
            log("bun install 완료")
            return True
        else:
            if callback:
                callback(False, f"Install failed: {result.stderr}")
            log(f"bun install 실패: {result.stderr[:100]}")
            return False
    except Exception as e:
        if callback:
            callback(False, f"Install error: {e}")
        return False
    finally:
        is_building = False


def run_bun_build(project_path, callback=None):
    """bun run build 실행"""
    global is_building

    with build_lock:
        if is_building:
            return False
        is_building = True

    if not BUN_EXE.exists():
        if callback:
            callback(False, "bun.exe not found")
        is_building = False
        return False

    try:
        if callback:
            callback(None, "Building...")
        log("bun run build 시작...")

        result = subprocess.run(
            [str(BUN_EXE), "run", "build"],
            cwd=project_path,
            capture_output=True,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )

        if result.returncode == 0:
            if callback:
                callback(True, "Build successful")
            log("빌드 완료")
            return True
        else:
            if callback:
                callback(False, f"Build failed: {result.stderr[:500]}")
            log(f"빌드 실패: {result.stderr[:100]}")
            return False
    except Exception as e:
        if callback:
            callback(False, f"Build error: {e}")
        return False
    finally:
        is_building = False


def get_is_building():
    """빌드 중인지 여부"""
    return is_building


# ============ Webhook 처리 ============

def verify_github_signature(payload_body, signature_header, secret):
    """GitHub webhook 서명 검증"""
    if not signature_header:
        return False

    try:
        hash_algorithm, github_signature = signature_header.split('=')
    except ValueError:
        return False

    if hash_algorithm != 'sha256':
        return False

    expected_signature = hmac.new(
        secret.encode('utf-8'),
        payload_body,
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(expected_signature, github_signature)


def handle_webhook(project_path, callback=None):
    """Webhook 수신 시 git pull + build 실행"""
    global last_webhook_time

    last_webhook_time = time.strftime('%Y-%m-%d %H:%M:%S')

    if callback:
        callback(None, f"Webhook received at {last_webhook_time}")
    log(f"Webhook 수신: {last_webhook_time}")

    # git pull 실행
    success, has_changes = run_git_pull(project_path, callback)

    if success and has_changes:
        # 변경이 있으면 빌드
        run_bun_build(project_path, callback)
    elif success:
        if callback:
            callback(True, "No changes to build")


def get_last_webhook_time():
    """마지막 웹훅 수신 시간"""
    return last_webhook_time


# ============ Self Update (unified-server 자체 업데이트) ============

last_self_update_time = None

def handle_self_update(callback=None):
    """unified-server 자체 업데이트: git pull 후 재시작"""
    global last_self_update_time

    last_self_update_time = time.strftime('%Y-%m-%d %H:%M:%S')

    if callback:
        callback(None, f"Self-update webhook received at {last_self_update_time}")
    log(f"Self-update 수신: {last_self_update_time}")

    # unified-server 폴더에서 git pull
    server_path = APP_DIR
    success, has_changes = run_git_pull(str(server_path), callback)

    if success and has_changes:
        if callback:
            callback(None, "Changes detected. Restarting server in 3 seconds...")
        log("변경사항 감지. 3초 후 서버 재시작...")

        # 3초 후 재시작 (응답 반환 시간 확보)
        def restart_server():
            time.sleep(3)
            log("서버 재시작 중...")
            # Python 스크립트 재실행
            python = sys.executable
            os.execl(python, python, *sys.argv)

        threading.Thread(target=restart_server, daemon=True).start()
        return True, "Restarting..."
    elif success:
        if callback:
            callback(True, "No changes detected")
        return True, "No changes"
    else:
        if callback:
            callback(False, "Git pull failed")
        return False, "Failed"


def get_last_self_update_time():
    """마지막 self-update 시간"""
    return last_self_update_time


# ============ 파일 감시 (Auto Build) ============

class SourceFileHandler(FileSystemEventHandler):
    def __init__(self, project_path, callback=None):
        self.project_path = project_path
        self.callback = callback
        self.last_build_time = 0
        self.debounce_seconds = 2

    def on_any_event(self, event):
        if event.is_directory:
            return

        watch_extensions = {'.tsx', '.ts', '.jsx', '.js', '.css', '.scss', '.html', '.json'}
        ignore_folders = {'node_modules', 'dist', '.git', 'build'}

        file_path = Path(event.src_path)

        for folder in ignore_folders:
            if folder in file_path.parts:
                return

        if file_path.suffix.lower() not in watch_extensions:
            return

        current_time = time.time()
        if current_time - self.last_build_time < self.debounce_seconds:
            return
        self.last_build_time = current_time

        if self.callback:
            self.callback(None, f"File changed: {file_path.name}")
        log(f"파일 변경 감지: {file_path.name}")

        threading.Thread(
            target=run_bun_build,
            args=(self.project_path, self.callback),
            daemon=True
        ).start()


def start_file_watcher(project_path, callback=None):
    """파일 감시 시작"""
    global observer
    stop_file_watcher()

    src_path = Path(project_path) / "src"
    if not src_path.exists():
        src_path = Path(project_path)

    event_handler = SourceFileHandler(project_path, callback)
    observer = Observer()
    observer.schedule(event_handler, str(src_path), recursive=True)
    observer.start()

    if callback:
        callback(None, f"Watching: {src_path}")
    log(f"파일 감시 시작: {src_path}")


def stop_file_watcher():
    """파일 감시 중지"""
    global observer
    if observer:
        observer.stop()
        observer.join(timeout=1)
        observer = None
