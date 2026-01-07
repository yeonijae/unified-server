"""
Haniwon Unified Server GUI
- 3개 서버 (Static, MSSQL, PostgreSQL) 관리
- 각 서버별 Auto Start, Log
- Windows 시작프로그램 등록
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import webbrowser
from pathlib import Path

from config import (
    APP_VERSION, VERSION, APP_NAME, APP_DIR,
    load_config, save_config, is_startup_enabled, set_startup_enabled
)
from services import mssql_db, postgres_db, git_build
from services.server_manager import HealthMonitor, get_health_monitor


class UnifiedServerGUI:
    def __init__(self):
        self.config = load_config()
        self.root = tk.Tk()
        self.root.title(f"{APP_NAME} v{VERSION}")
        self.root.geometry("580x700")
        self.root.resizable(False, False)

        # 서버 상태
        self.static_running = False
        self.mssql_running = False
        self.postgres_running = False
        self.chat_running = False

        # Flask 앱 참조
        self.static_app = None
        self.mssql_app = None
        self.postgres_app = None
        self.chat_app = None
        self.chat_socketio = None

        # 로그 텍스트 위젯 (나중에 생성)
        self.static_log = None
        self.mssql_log = None
        self.postgres_log = None
        self.chat_log = None

        # MSSQL → PostgreSQL 대기열 동기화 스레드
        self.waiting_sync_running = False
        self.waiting_sync_thread = None
        self.sync_count = 0
        self.last_sync_time = None

        # 요일별 스케줄 기본값 (월~일)
        self.default_schedule = {
            "mon": {"enabled": True, "start": "08:30", "end": "18:30"},
            "tue": {"enabled": True, "start": "08:30", "end": "18:30"},
            "wed": {"enabled": True, "start": "08:30", "end": "18:30"},
            "thu": {"enabled": True, "start": "08:30", "end": "18:30"},
            "fri": {"enabled": True, "start": "08:30", "end": "18:30"},
            "sat": {"enabled": True, "start": "08:30", "end": "13:00"},
            "sun": {"enabled": False, "start": "09:00", "end": "12:00"},
        }

        self._setup_styles()
        self._create_widgets()
        self._setup_tray()
        self._setup_log_callbacks()

        # 자동 시작 체크 (개별 서버 설정에 따라)
        self.root.after(500, self._auto_start_servers)

    def _setup_styles(self):
        style = ttk.Style()
        style.configure('TLabel', padding=2)
        style.configure('TButton', padding=2)
        style.configure('TNotebook.Tab', padding=[10, 5])

    def _create_widgets(self):
        # 상단: Windows 시작 옵션
        top_frame = ttk.Frame(self.root, padding=(10, 5))
        top_frame.pack(fill=tk.X)

        self.startup_var = tk.BooleanVar(value=is_startup_enabled())
        ttk.Checkbutton(
            top_frame,
            text="Start with Windows",
            variable=self.startup_var,
            command=self._toggle_startup
        ).pack(anchor=tk.W)

        ttk.Separator(self.root, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=10, pady=3)

        # 메인 노트북 (탭)
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 5))

        # 탭 생성
        self._create_static_tab(notebook)
        self._create_mssql_tab(notebook)
        self._create_postgres_tab(notebook)
        self._create_chat_tab(notebook)
        self._create_sync_tab(notebook)
        self._create_upload_tab(notebook)
        self._create_webhook_tab(notebook)
        self._create_apikey_tab(notebook)

        # 하단 버전 (EXE 버전만 표시, API 버전은 MSSQL 탭에서 표시)
        ttk.Label(self.root, text=f"Server v{APP_VERSION}", foreground="gray").pack(side=tk.BOTTOM, pady=3)

    # ============ Static Server 탭 ============
    def _create_static_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" Static ")

        # 서버 상태 + 포트 + 버튼 (한 줄)
        server_frame = ttk.LabelFrame(tab, text="Static File Server", padding=8)
        server_frame.pack(fill=tk.X, pady=(0, 8))

        row = ttk.Frame(server_frame)
        row.pack(fill=tk.X)

        self.static_status_var = tk.StringVar(value="Stopped")
        self.static_status_label = ttk.Label(row, textvariable=self.static_status_var, font=('Segoe UI', 10, 'bold'), foreground="red", width=8)
        self.static_status_label.pack(side=tk.LEFT)

        ttk.Label(row, text="Port:").pack(side=tk.LEFT, padx=(10, 2))
        self.static_port_var = tk.IntVar(value=self.config.get("static_port", 11111))
        ttk.Entry(row, textvariable=self.static_port_var, width=6).pack(side=tk.LEFT)

        ttk.Button(row, text="Console", command=lambda: webbrowser.open(f"http://localhost:{self.static_port_var.get()}/console")).pack(side=tk.RIGHT, padx=2)
        self.static_stop_btn = ttk.Button(row, text="Stop", command=self._stop_static, state=tk.DISABLED)
        self.static_stop_btn.pack(side=tk.RIGHT, padx=2)
        self.static_start_btn = ttk.Button(row, text="Start", command=self._start_static)
        self.static_start_btn.pack(side=tk.RIGHT, padx=2)

        # WWW 폴더
        folder_frame = ttk.LabelFrame(tab, text="Project Folder", padding=8)
        folder_frame.pack(fill=tk.X, pady=(0, 8))

        folder_row = ttk.Frame(folder_frame)
        folder_row.pack(fill=tk.X)
        self.www_folder_var = tk.StringVar(value=self.config.get("www_folder", ""))
        ttk.Entry(folder_row, textvariable=self.www_folder_var, width=45).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(folder_row, text="Browse", command=self._browse_www_folder).pack(side=tk.LEFT, padx=(5, 0))

        # 빌드 옵션
        build_frame = ttk.LabelFrame(tab, text="Build", padding=8)
        build_frame.pack(fill=tk.X, pady=(0, 8))

        build_row = ttk.Frame(build_frame)
        build_row.pack(fill=tk.X)
        ttk.Button(build_row, text="Git Clone", command=self._git_clone).pack(side=tk.LEFT, padx=2)
        ttk.Button(build_row, text="Git Pull", command=self._git_pull).pack(side=tk.LEFT, padx=2)
        ttk.Button(build_row, text="Install", command=self._bun_install).pack(side=tk.LEFT, padx=2)
        ttk.Button(build_row, text="Build", command=self._bun_build).pack(side=tk.LEFT, padx=2)

        self.auto_build_var = tk.BooleanVar(value=self.config.get("auto_build", False))
        ttk.Checkbutton(build_row, text="Auto Build", variable=self.auto_build_var).pack(side=tk.LEFT, padx=(15, 0))

        bun_ok = git_build.bun_exists()
        git_ok = git_build.git_exists()
        ttk.Label(build_row, text=f"Bun:{'OK' if bun_ok else 'X'}", foreground="green" if bun_ok else "red").pack(side=tk.RIGHT, padx=5)
        ttk.Label(build_row, text=f"Git:{'OK' if git_ok else 'X'}", foreground="green" if git_ok else "red").pack(side=tk.RIGHT)

        # 옵션
        self.static_auto_start_var = tk.BooleanVar(value=self.config.get("static_auto_start", False))
        ttk.Checkbutton(tab, text="Auto start on startup", variable=self.static_auto_start_var).pack(anchor=tk.W)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        self.static_log = tk.Text(log_frame, height=5, font=('Consolas', 9), bg='#1e1e1e', fg='#f59e0b', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.static_log.yview)
        self.static_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.static_log.pack(fill=tk.BOTH, expand=True)

        log_btn_row = ttk.Frame(log_frame)
        log_btn_row.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(log_btn_row, text="Clear", command=lambda: self._clear_log(self.static_log)).pack(side=tk.RIGHT)
        ttk.Button(log_btn_row, text="Save Settings", command=self._save_static_settings).pack(side=tk.RIGHT, padx=5)

    # ============ MSSQL 탭 ============
    def _create_mssql_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" MSSQL ")

        # 서버 상태 + 포트 + 버튼 (한 줄)
        server_frame = ttk.LabelFrame(tab, text="MSSQL API Server", padding=8)
        server_frame.pack(fill=tk.X, pady=(0, 8))

        row = ttk.Frame(server_frame)
        row.pack(fill=tk.X)

        self.mssql_status_var = tk.StringVar(value="Stopped")
        self.mssql_status_label = ttk.Label(row, textvariable=self.mssql_status_var, font=('Segoe UI', 10, 'bold'), foreground="red", width=8)
        self.mssql_status_label.pack(side=tk.LEFT)

        ttk.Label(row, text="Port:").pack(side=tk.LEFT, padx=(10, 2))
        self.mssql_port_var = tk.IntVar(value=self.config.get("mssql_api_port", 3100))
        ttk.Entry(row, textvariable=self.mssql_port_var, width=6).pack(side=tk.LEFT)

        ttk.Button(row, text="Console", command=lambda: webbrowser.open(f"http://localhost:{self.mssql_port_var.get()}")).pack(side=tk.RIGHT, padx=2)
        self.mssql_stop_btn = ttk.Button(row, text="Stop", command=self._stop_mssql, state=tk.DISABLED)
        self.mssql_stop_btn.pack(side=tk.RIGHT, padx=2)
        self.mssql_start_btn = ttk.Button(row, text="Start", command=self._start_mssql)
        self.mssql_start_btn.pack(side=tk.RIGHT, padx=2)

        # 연결 정보
        conn_frame = ttk.LabelFrame(tab, text="Connection", padding=8)
        conn_frame.pack(fill=tk.X, pady=(0, 8))

        mssql_config = self.config.get('mssql', {})
        self.mssql_server_var = tk.StringVar(value=mssql_config.get('server', '192.168.0.173'))
        self.mssql_conn_port_var = tk.IntVar(value=mssql_config.get('port', 55555))
        self.mssql_user_var = tk.StringVar(value=mssql_config.get('user', 'members'))
        self.mssql_pass_var = tk.StringVar(value=mssql_config.get('password', 'msp1234'))
        self.mssql_db_var = tk.StringVar(value=mssql_config.get('database', 'MasterDB'))

        row1 = ttk.Frame(conn_frame)
        row1.pack(fill=tk.X, pady=2)
        ttk.Label(row1, text="Server:").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.mssql_server_var, width=15).pack(side=tk.LEFT, padx=(2, 10))
        ttk.Label(row1, text="Port:").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.mssql_conn_port_var, width=6).pack(side=tk.LEFT, padx=(2, 10))
        ttk.Label(row1, text="DB:").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.mssql_db_var, width=12).pack(side=tk.LEFT, padx=2)

        row2 = ttk.Frame(conn_frame)
        row2.pack(fill=tk.X, pady=2)
        ttk.Label(row2, text="User:").pack(side=tk.LEFT)
        ttk.Entry(row2, textvariable=self.mssql_user_var, width=12).pack(side=tk.LEFT, padx=(2, 10))
        ttk.Label(row2, text="Pass:").pack(side=tk.LEFT)
        ttk.Entry(row2, textvariable=self.mssql_pass_var, width=12, show="*").pack(side=tk.LEFT, padx=2)

        # 연결 테스트 (한 줄)
        self.mssql_test_var = tk.StringVar(value="Not tested")
        self.mssql_test_label = ttk.Label(row2, textvariable=self.mssql_test_var, foreground="gray")
        self.mssql_test_label.pack(side=tk.RIGHT)
        ttk.Button(row2, text="Test", command=self._test_mssql).pack(side=tk.RIGHT, padx=(10, 5))

        # API 버전 표시
        api_frame = ttk.LabelFrame(tab, text="API Routes", padding=8)
        api_frame.pack(fill=tk.X, pady=(0, 8))

        api_row = ttk.Frame(api_frame)
        api_row.pack(fill=tk.X)

        # 현재 enc 파일 버전 확인
        self.mssql_api_version_var = tk.StringVar(value="unknown")
        self._update_api_version()

        ttk.Label(api_row, text="API Version:").pack(side=tk.LEFT)
        self.mssql_api_version_label = ttk.Label(api_row, textvariable=self.mssql_api_version_var, font=('Segoe UI', 9, 'bold'), foreground="blue")
        self.mssql_api_version_label.pack(side=tk.LEFT, padx=(5, 15))

        self.mssql_use_builtin_var = tk.BooleanVar(value=self.config.get("mssql_use_builtin", False))
        ttk.Checkbutton(api_row, text="Use Built-in Routes", variable=self.mssql_use_builtin_var).pack(side=tk.LEFT)

        ttk.Button(api_row, text="Hot Reload", command=self._hot_reload_mssql).pack(side=tk.RIGHT)
        ttk.Button(api_row, text="Update API", command=self._download_enc_and_reload).pack(side=tk.RIGHT, padx=(0, 5))

        # 옵션
        options_frame = ttk.Frame(tab)
        options_frame.pack(fill=tk.X, pady=(5, 0))

        self.mssql_auto_start_var = tk.BooleanVar(value=self.config.get("mssql_auto_start", False))
        ttk.Checkbutton(options_frame, text="Auto start on startup", variable=self.mssql_auto_start_var).pack(side=tk.LEFT)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        self.mssql_log = tk.Text(log_frame, height=5, font=('Consolas', 9), bg='#1e1e1e', fg='#00d9ff', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.mssql_log.yview)
        self.mssql_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.mssql_log.pack(fill=tk.BOTH, expand=True)

        log_btn_row = ttk.Frame(log_frame)
        log_btn_row.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(log_btn_row, text="Clear", command=lambda: self._clear_log(self.mssql_log)).pack(side=tk.RIGHT)
        ttk.Button(log_btn_row, text="Save Settings", command=self._save_mssql_settings).pack(side=tk.RIGHT, padx=5)

    # ============ PostgreSQL 탭 ============
    def _create_postgres_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" PostgreSQL ")

        # 서버 상태 + 포트 + 버튼 (한 줄)
        server_frame = ttk.LabelFrame(tab, text="PostgreSQL API Server", padding=8)
        server_frame.pack(fill=tk.X, pady=(0, 8))

        row = ttk.Frame(server_frame)
        row.pack(fill=tk.X)

        self.postgres_status_var = tk.StringVar(value="Stopped")
        self.postgres_status_label = ttk.Label(row, textvariable=self.postgres_status_var, font=('Segoe UI', 10, 'bold'), foreground="red", width=8)
        self.postgres_status_label.pack(side=tk.LEFT)

        ttk.Label(row, text="Port:").pack(side=tk.LEFT, padx=(10, 2))
        self.postgres_port_var = tk.IntVar(value=self.config.get("postgres_api_port", 3200))
        ttk.Entry(row, textvariable=self.postgres_port_var, width=6).pack(side=tk.LEFT)

        ttk.Button(row, text="Console", command=lambda: webbrowser.open(f"http://localhost:{self.postgres_port_var.get()}")).pack(side=tk.RIGHT, padx=2)
        self.postgres_stop_btn = ttk.Button(row, text="Stop", command=self._stop_postgres, state=tk.DISABLED)
        self.postgres_stop_btn.pack(side=tk.RIGHT, padx=2)
        self.postgres_start_btn = ttk.Button(row, text="Start", command=self._start_postgres)
        self.postgres_start_btn.pack(side=tk.RIGHT, padx=2)

        # 데이터베이스 연결 정보
        db_frame = ttk.LabelFrame(tab, text="Database Connection", padding=8)
        db_frame.pack(fill=tk.X, pady=(0, 8))

        pg_config = self.config.get("postgres", {})

        row1 = ttk.Frame(db_frame)
        row1.pack(fill=tk.X, pady=2)
        ttk.Label(row1, text="Host:", width=10).pack(side=tk.LEFT)
        self.pg_host_var = tk.StringVar(value=pg_config.get("host", "192.168.0.173"))
        ttk.Entry(row1, textvariable=self.pg_host_var, width=20).pack(side=tk.LEFT)
        ttk.Label(row1, text="Port:").pack(side=tk.LEFT, padx=(10, 2))
        self.pg_port_var = tk.IntVar(value=pg_config.get("port", 5432))
        ttk.Entry(row1, textvariable=self.pg_port_var, width=6).pack(side=tk.LEFT)

        row2 = ttk.Frame(db_frame)
        row2.pack(fill=tk.X, pady=2)
        ttk.Label(row2, text="Database:", width=10).pack(side=tk.LEFT)
        self.pg_database_var = tk.StringVar(value=pg_config.get("database", "haniwon"))
        ttk.Entry(row2, textvariable=self.pg_database_var, width=20).pack(side=tk.LEFT)

        row3 = ttk.Frame(db_frame)
        row3.pack(fill=tk.X, pady=2)
        ttk.Label(row3, text="User:", width=10).pack(side=tk.LEFT)
        self.pg_user_var = tk.StringVar(value=pg_config.get("user", "haniwon_user"))
        ttk.Entry(row3, textvariable=self.pg_user_var, width=20).pack(side=tk.LEFT)
        ttk.Label(row3, text="Password:").pack(side=tk.LEFT, padx=(10, 2))
        self.pg_password_var = tk.StringVar(value=pg_config.get("password", ""))
        ttk.Entry(row3, textvariable=self.pg_password_var, width=15, show="*").pack(side=tk.LEFT)

        row4 = ttk.Frame(db_frame)
        row4.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(row4, text="Test Connection", command=self._test_postgres_connection).pack(side=tk.LEFT)
        ttk.Button(row4, text="Save Connection", command=self._save_postgres_connection).pack(side=tk.LEFT, padx=5)

        # 옵션
        self.postgres_auto_start_var = tk.BooleanVar(value=self.config.get("postgres_auto_start", False))
        ttk.Checkbutton(tab, text="Auto start on startup", variable=self.postgres_auto_start_var).pack(anchor=tk.W)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        self.postgres_log = tk.Text(log_frame, height=5, font=('Consolas', 9), bg='#1e1e1e', fg='#3b82f6', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.postgres_log.yview)
        self.postgres_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.postgres_log.pack(fill=tk.BOTH, expand=True)

        log_btn_row = ttk.Frame(log_frame)
        log_btn_row.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(log_btn_row, text="Clear", command=lambda: self._clear_log(self.postgres_log)).pack(side=tk.RIGHT)
        ttk.Button(log_btn_row, text="Save Settings", command=self._save_postgres_settings).pack(side=tk.RIGHT, padx=5)


    # ============ Chat Server 탭 ============
    def _create_chat_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" Chat ")

        # 서버 상태 + 포트 + 버튼 (한 줄)
        server_frame = ttk.LabelFrame(tab, text="Chat Server (Socket.io)", padding=8)
        server_frame.pack(fill=tk.X, pady=(0, 8))

        row = ttk.Frame(server_frame)
        row.pack(fill=tk.X)

        self.chat_status_var = tk.StringVar(value="Stopped")
        self.chat_status_label = ttk.Label(row, textvariable=self.chat_status_var, font=('Segoe UI', 10, 'bold'), foreground="red", width=8)
        self.chat_status_label.pack(side=tk.LEFT)

        ttk.Label(row, text="Port:").pack(side=tk.LEFT, padx=(10, 2))
        self.chat_port_var = tk.IntVar(value=self.config.get("chat_port", 3300))
        ttk.Entry(row, textvariable=self.chat_port_var, width=6).pack(side=tk.LEFT)

        self.chat_stop_btn = ttk.Button(row, text="Stop", command=self._stop_chat, state=tk.DISABLED)
        self.chat_stop_btn.pack(side=tk.RIGHT, padx=2)
        self.chat_start_btn = ttk.Button(row, text="Start", command=self._start_chat)
        self.chat_start_btn.pack(side=tk.RIGHT, padx=2)

        # DB 설정
        db_frame = ttk.LabelFrame(tab, text="PostgreSQL Database", padding=8)
        db_frame.pack(fill=tk.X, pady=(0, 8))

        chat_config = self.config.get("chat", {})

        row1 = ttk.Frame(db_frame)
        row1.pack(fill=tk.X, pady=2)
        ttk.Label(row1, text="Host:", width=8).pack(side=tk.LEFT)
        self.chat_db_host_var = tk.StringVar(value=chat_config.get("host", "192.168.0.173"))
        ttk.Entry(row1, textvariable=self.chat_db_host_var, width=20).pack(side=tk.LEFT)
        ttk.Label(row1, text="Port:").pack(side=tk.LEFT, padx=(10, 2))
        self.chat_db_port_var = tk.IntVar(value=chat_config.get("port", 5432))
        ttk.Entry(row1, textvariable=self.chat_db_port_var, width=6).pack(side=tk.LEFT)

        row2 = ttk.Frame(db_frame)
        row2.pack(fill=tk.X, pady=2)
        ttk.Label(row2, text="Database:", width=8).pack(side=tk.LEFT)
        self.chat_db_name_var = tk.StringVar(value=chat_config.get("database", "haniwon"))
        ttk.Entry(row2, textvariable=self.chat_db_name_var, width=15).pack(side=tk.LEFT)
        ttk.Label(row2, text="User:").pack(side=tk.LEFT, padx=(10, 2))
        self.chat_db_user_var = tk.StringVar(value=chat_config.get("user", "haniwon_user"))
        ttk.Entry(row2, textvariable=self.chat_db_user_var, width=15).pack(side=tk.LEFT)

        row3 = ttk.Frame(db_frame)
        row3.pack(fill=tk.X, pady=2)
        ttk.Label(row3, text="Password:", width=8).pack(side=tk.LEFT)
        self.chat_db_pass_var = tk.StringVar(value=chat_config.get("password", ""))
        ttk.Entry(row3, textvariable=self.chat_db_pass_var, width=20, show="*").pack(side=tk.LEFT)
        ttk.Button(row3, text="Test", command=self._test_chat_db).pack(side=tk.LEFT, padx=(10, 0))

        # 옵션
        self.chat_auto_start_var = tk.BooleanVar(value=self.config.get("chat_auto_start", False))
        ttk.Checkbutton(tab, text="Auto start on startup", variable=self.chat_auto_start_var).pack(anchor=tk.W)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        self.chat_log = tk.Text(log_frame, height=5, font=('Consolas', 9), bg='#1e1e1e', fg='#22c55e', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.chat_log.yview)
        self.chat_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.chat_log.pack(fill=tk.BOTH, expand=True)

        log_btn_row = ttk.Frame(log_frame)
        log_btn_row.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(log_btn_row, text="Clear", command=lambda: self._clear_log(self.chat_log)).pack(side=tk.RIGHT)
        ttk.Button(log_btn_row, text="Save Settings", command=self._save_chat_settings).pack(side=tk.RIGHT, padx=5)


    # ============ Sync 탭 (MSSQL → PostgreSQL 동기화) ============
    def _create_sync_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" Sync ")

        # 동기화 상태
        status_frame = ttk.LabelFrame(tab, text="MSSQL → PostgreSQL 대기열 동기화", padding=8)
        status_frame.pack(fill=tk.X, pady=(0, 10))

        row1 = ttk.Frame(status_frame)
        row1.pack(fill=tk.X, pady=2)

        self.sync_status_var = tk.StringVar(value="Stopped")
        self.sync_status_label = ttk.Label(row1, textvariable=self.sync_status_var, font=('Segoe UI', 10, 'bold'), foreground="red", width=10)
        self.sync_status_label.pack(side=tk.LEFT)

        self.sync_start_btn = ttk.Button(row1, text="Start", command=self._manual_start_sync)
        self.sync_start_btn.pack(side=tk.LEFT, padx=5)
        self.sync_stop_btn = ttk.Button(row1, text="Stop", command=self._manual_stop_sync, state=tk.DISABLED)
        self.sync_stop_btn.pack(side=tk.LEFT)
        ttk.Button(row1, text="Sync Now", command=self._manual_sync_once).pack(side=tk.LEFT, padx=10)

        # 설정
        settings_frame = ttk.LabelFrame(tab, text="Settings", padding=8)
        settings_frame.pack(fill=tk.X, pady=(0, 10))

        row2 = ttk.Frame(settings_frame)
        row2.pack(fill=tk.X, pady=2)

        ttk.Label(row2, text="Sync Interval (seconds):").pack(side=tk.LEFT)
        self.sync_interval_var = tk.IntVar(value=self.config.get("sync_interval", 5))
        ttk.Spinbox(row2, from_=1, to=60, textvariable=self.sync_interval_var, width=5).pack(side=tk.LEFT, padx=5)

        self.sync_auto_start_var = tk.BooleanVar(value=self.config.get("sync_auto_start", True))
        ttk.Checkbutton(row2, text="Auto start when both servers running", variable=self.sync_auto_start_var).pack(side=tk.LEFT, padx=15)

        # 통계
        stats_frame = ttk.LabelFrame(tab, text="Statistics", padding=8)
        stats_frame.pack(fill=tk.X, pady=(0, 10))

        row3 = ttk.Frame(stats_frame)
        row3.pack(fill=tk.X, pady=2)

        ttk.Label(row3, text="Last Sync:").pack(side=tk.LEFT)
        self.last_sync_var = tk.StringVar(value="-")
        ttk.Label(row3, textvariable=self.last_sync_var, foreground="blue").pack(side=tk.LEFT, padx=5)

        ttk.Label(row3, text="Total Synced:").pack(side=tk.LEFT, padx=(20, 0))
        self.sync_count_var = tk.StringVar(value="0")
        ttk.Label(row3, textvariable=self.sync_count_var, foreground="green").pack(side=tk.LEFT, padx=5)

        # 서버 상태
        server_frame = ttk.LabelFrame(tab, text="Server Status", padding=8)
        server_frame.pack(fill=tk.X, pady=(0, 10))

        row4 = ttk.Frame(server_frame)
        row4.pack(fill=tk.X, pady=2)

        ttk.Label(row4, text="MSSQL:").pack(side=tk.LEFT)
        self.sync_mssql_status_var = tk.StringVar(value="Stopped")
        self.sync_mssql_label = ttk.Label(row4, textvariable=self.sync_mssql_status_var, foreground="red")
        self.sync_mssql_label.pack(side=tk.LEFT, padx=(5, 20))

        ttk.Label(row4, text="PostgreSQL:").pack(side=tk.LEFT)
        self.sync_postgres_status_var = tk.StringVar(value="Stopped")
        self.sync_postgres_label = ttk.Label(row4, textvariable=self.sync_postgres_status_var, foreground="red")
        self.sync_postgres_label.pack(side=tk.LEFT, padx=5)

        # 요일별 스케줄 설정
        schedule_frame = ttk.LabelFrame(tab, text="Weekly Schedule (동기화 시간)", padding=8)
        schedule_frame.pack(fill=tk.X, pady=(0, 10))

        # 스케줄 변수 초기화
        schedule_config = self.config.get("sync_schedule", self.default_schedule)
        self.schedule_vars = {}

        days = [
            ("mon", "월"), ("tue", "화"), ("wed", "수"), ("thu", "목"),
            ("fri", "금"), ("sat", "토"), ("sun", "일")
        ]

        # 헤더
        header_row = ttk.Frame(schedule_frame)
        header_row.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(header_row, text="요일", width=4).pack(side=tk.LEFT)
        ttk.Label(header_row, text="사용", width=5).pack(side=tk.LEFT)
        ttk.Label(header_row, text="시작", width=8).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Label(header_row, text="종료", width=8).pack(side=tk.LEFT, padx=(5, 0))

        for day_key, day_name in days:
            day_config = schedule_config.get(day_key, self.default_schedule[day_key])

            row = ttk.Frame(schedule_frame)
            row.pack(fill=tk.X, pady=1)

            ttk.Label(row, text=day_name, width=4).pack(side=tk.LEFT)

            enabled_var = tk.BooleanVar(value=day_config.get("enabled", True))
            ttk.Checkbutton(row, variable=enabled_var, width=3).pack(side=tk.LEFT)

            start_var = tk.StringVar(value=day_config.get("start", "08:30"))
            start_entry = ttk.Entry(row, textvariable=start_var, width=6)
            start_entry.pack(side=tk.LEFT, padx=(5, 0))

            ttk.Label(row, text="~").pack(side=tk.LEFT, padx=2)

            end_var = tk.StringVar(value=day_config.get("end", "18:30"))
            end_entry = ttk.Entry(row, textvariable=end_var, width=6)
            end_entry.pack(side=tk.LEFT)

            self.schedule_vars[day_key] = {
                "enabled": enabled_var,
                "start": start_var,
                "end": end_var
            }

        # 저장 버튼
        ttk.Button(tab, text="Save Sync Settings", command=self._save_sync_settings).pack(pady=15)

        # 상태 업데이트 타이머
        self._update_sync_status()

    def _update_sync_status(self):
        """동기화 탭 상태 업데이트"""
        # MSSQL 상태
        if self.mssql_running:
            self.sync_mssql_status_var.set("Running")
            self.sync_mssql_label.configure(foreground="green")
        else:
            self.sync_mssql_status_var.set("Stopped")
            self.sync_mssql_label.configure(foreground="red")

        # PostgreSQL 상태
        if self.postgres_running:
            self.sync_postgres_status_var.set("Running")
            self.sync_postgres_label.configure(foreground="green")
        else:
            self.sync_postgres_status_var.set("Stopped")
            self.sync_postgres_label.configure(foreground="red")

        # 동기화 상태
        if self.waiting_sync_running:
            self.sync_status_var.set("Running")
            self.sync_status_label.configure(foreground="green")
            self.sync_start_btn.configure(state=tk.DISABLED)
            self.sync_stop_btn.configure(state=tk.NORMAL)
        else:
            self.sync_status_var.set("Stopped")
            self.sync_status_label.configure(foreground="red")
            self.sync_start_btn.configure(state=tk.NORMAL)
            self.sync_stop_btn.configure(state=tk.DISABLED)

        # 통계 업데이트
        self.sync_count_var.set(str(self.sync_count))
        if self.last_sync_time:
            self.last_sync_var.set(self.last_sync_time.strftime("%H:%M:%S"))

        # 1초마다 업데이트
        self.root.after(1000, self._update_sync_status)

    def _manual_start_sync(self):
        """수동 동기화 시작"""
        if not self.mssql_running or not self.postgres_running:
            messagebox.showwarning("Warning", "MSSQL과 PostgreSQL 서버가 모두 실행 중이어야 합니다.")
            return
        self._start_waiting_sync()

    def _manual_stop_sync(self):
        """수동 동기화 중지"""
        self._stop_waiting_sync()

    def _manual_sync_once(self):
        """즉시 1회 동기화"""
        if not self.mssql_running or not self.postgres_running:
            messagebox.showwarning("Warning", "MSSQL과 PostgreSQL 서버가 모두 실행 중이어야 합니다.")
            return

        def do_sync():
            import requests
            from datetime import datetime

            try:
                mssql_port = self.mssql_port_var.get()
                postgres_port = self.postgres_port_var.get()

                mssql_res = requests.get(f"http://localhost:{mssql_port}/api/queue/status", timeout=5)
                if mssql_res.status_code != 200:
                    self.root.after(0, lambda: messagebox.showerror("Error", "MSSQL API 오류"))
                    return

                queue_data = mssql_res.json()
                treating_list = queue_data.get('treating', [])

                for t in treating_list:
                    if 'treating_since' in t and 'waiting_since' not in t:
                        t['waiting_since'] = t['treating_since']

                if not treating_list:
                    self.root.after(0, lambda: messagebox.showinfo("Info", "동기화할 환자가 없습니다."))
                    return

                sync_res = requests.post(
                    f"http://localhost:{postgres_port}/api/treatments/sync",
                    json={"waiting": treating_list},
                    timeout=5
                )

                if sync_res.status_code == 200:
                    result = sync_res.json()
                    added = result.get('added', 0)
                    updated = result.get('updated', 0)
                    skipped = result.get('skipped', 0)
                    self.sync_count += added
                    self.last_sync_time = datetime.now()
                    self.root.after(0, lambda: messagebox.showinfo("Success", f"동기화 완료: 추가 {added}, 업데이트 {updated}, 스킵 {skipped}"))
                else:
                    self.root.after(0, lambda: messagebox.showerror("Error", "PostgreSQL API 오류"))

            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"동기화 오류: {str(e)}"))

        threading.Thread(target=do_sync, daemon=True).start()

    def _save_sync_settings(self):
        """동기화 설정 저장"""
        self.config["sync_interval"] = self.sync_interval_var.get()
        self.config["sync_auto_start"] = self.sync_auto_start_var.get()

        # 스케줄 저장
        schedule = {}
        for day_key, vars in self.schedule_vars.items():
            schedule[day_key] = {
                "enabled": vars["enabled"].get(),
                "start": vars["start"].get(),
                "end": vars["end"].get()
            }
        self.config["sync_schedule"] = schedule

        save_config(self.config)
        messagebox.showinfo("Success", "Sync settings saved")


    # ============ Upload 탭 ============
    def _create_upload_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" Upload ")

        # Upload 폴더 설정
        upload_frame = ttk.LabelFrame(tab, text="Upload Folder", padding=8)
        upload_frame.pack(fill=tk.X, pady=(0, 10))

        upload_row = ttk.Frame(upload_frame)
        upload_row.pack(fill=tk.X)
        self.upload_folder_var = tk.StringVar(value=self.config.get("upload_folder", "C:/haniwon_data/uploads"))
        ttk.Entry(upload_row, textvariable=self.upload_folder_var, width=45).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(upload_row, text="Browse", command=self._browse_upload_folder).pack(side=tk.LEFT)

        ttk.Label(upload_frame, text="검사결과 이미지, PDF 등이 저장되는 폴더", foreground="gray").pack(anchor=tk.W, pady=(5, 0))

        # Thumbnail 폴더 설정
        thumb_frame = ttk.LabelFrame(tab, text="Thumbnail Folder", padding=8)
        thumb_frame.pack(fill=tk.X, pady=(0, 10))

        thumb_row = ttk.Frame(thumb_frame)
        thumb_row.pack(fill=tk.X)
        self.thumbnail_folder_var = tk.StringVar(value=self.config.get("thumbnail_folder", "C:/haniwon_data/thumbnails"))
        ttk.Entry(thumb_row, textvariable=self.thumbnail_folder_var, width=45).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(thumb_row, text="Browse", command=self._browse_thumbnail_folder).pack(side=tk.LEFT)

        ttk.Label(thumb_frame, text="이미지 썸네일이 저장되는 폴더", foreground="gray").pack(anchor=tk.W, pady=(5, 0))

        # 파일 설정
        file_settings_frame = ttk.LabelFrame(tab, text="File Settings", padding=8)
        file_settings_frame.pack(fill=tk.X, pady=(0, 10))

        # 최대 파일 크기
        size_row = ttk.Frame(file_settings_frame)
        size_row.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(size_row, text="Max File Size (MB):").pack(side=tk.LEFT)
        self.max_file_size_var = tk.IntVar(value=self.config.get("max_file_size_mb", 20))
        ttk.Spinbox(size_row, from_=1, to=100, textvariable=self.max_file_size_var, width=10).pack(side=tk.LEFT, padx=5)

        # 허용 확장자
        ext_row = ttk.Frame(file_settings_frame)
        ext_row.pack(fill=tk.X)
        ttk.Label(ext_row, text="Allowed Extensions:").pack(side=tk.LEFT)
        default_ext = self.config.get("allowed_extensions", "jpg,jpeg,png,gif,pdf,bmp,tiff,tif")
        self.allowed_ext_var = tk.StringVar(value=default_ext)
        ttk.Entry(ext_row, textvariable=self.allowed_ext_var, width=40).pack(side=tk.LEFT, padx=5)

        # API 정보
        api_frame = ttk.LabelFrame(tab, text="File API Endpoints (PostgreSQL Server)", padding=8)
        api_frame.pack(fill=tk.X, pady=(0, 10))

        port = self.config.get("postgres_api_port", 3200)
        endpoints = [
            ("POST", f"/api/files/upload", "파일 업로드"),
            ("GET", f"/api/files/<path>", "파일 다운로드"),
            ("DELETE", f"/api/files/<path>", "파일 삭제"),
            ("GET", f"/api/files/list/<path>", "폴더 목록"),
        ]
        for method, path, desc in endpoints:
            row = ttk.Frame(api_frame)
            row.pack(fill=tk.X, pady=1)
            ttk.Label(row, text=method, width=8, foreground="green").pack(side=tk.LEFT)
            ttk.Label(row, text=path, foreground="blue").pack(side=tk.LEFT, padx=5)
            ttk.Label(row, text=f"- {desc}", foreground="gray").pack(side=tk.LEFT)

        # 저장 버튼
        ttk.Button(tab, text="Save Upload Settings", command=self._save_upload_settings).pack(pady=20)

    def _browse_upload_folder(self):
        folder = filedialog.askdirectory(initialdir=self.upload_folder_var.get())
        if folder:
            self.upload_folder_var.set(folder)

    def _browse_thumbnail_folder(self):
        folder = filedialog.askdirectory(initialdir=self.thumbnail_folder_var.get())
        if folder:
            self.thumbnail_folder_var.set(folder)

    def _save_upload_settings(self):
        self.config["upload_folder"] = self.upload_folder_var.get()
        self.config["thumbnail_folder"] = self.thumbnail_folder_var.get()
        self.config["max_file_size_mb"] = self.max_file_size_var.get()
        self.config["allowed_extensions"] = self.allowed_ext_var.get()
        save_config(self.config)
        messagebox.showinfo("Saved", "Upload settings saved!")

    # ============ Webhook 탭 ============
    def _create_webhook_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" Webhook ")

        # 공통 Secret
        secret_frame = ttk.LabelFrame(tab, text="Common Secret", padding=8)
        secret_frame.pack(fill=tk.X, pady=(0, 10))

        secret_row = ttk.Frame(secret_frame)
        secret_row.pack(fill=tk.X)
        ttk.Label(secret_row, text="Secret:").pack(side=tk.LEFT)
        self.webhook_secret_var = tk.StringVar(value=self.config.get("webhook_secret", ""))
        self.webhook_secret_entry = ttk.Entry(secret_row, textvariable=self.webhook_secret_var, width=35, show="*")
        self.webhook_secret_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(secret_row, text="Show", command=self._toggle_webhook_secret).pack(side=tk.LEFT)

        # Static Server Webhook
        static_wh_frame = ttk.LabelFrame(tab, text="Static Server (Build on Push)", padding=8)
        static_wh_frame.pack(fill=tk.X, pady=(0, 10))

        self.webhook_enabled_var = tk.BooleanVar(value=self.config.get("webhook_enabled", False))
        ttk.Checkbutton(static_wh_frame, text="Enable", variable=self.webhook_enabled_var).pack(anchor=tk.W)

        static_url_row = ttk.Frame(static_wh_frame)
        static_url_row.pack(fill=tk.X, pady=(5, 0))
        ttk.Label(static_url_row, text="URL:").pack(side=tk.LEFT)
        static_url = f"http://<server-ip>:{self.config.get('static_port', 11111)}/webhook"
        ttk.Label(static_url_row, text=static_url, foreground="blue").pack(side=tk.LEFT, padx=5)

        # MSSQL Server Webhook
        mssql_wh_frame = ttk.LabelFrame(tab, text="MSSQL Server (Self Update)", padding=8)
        mssql_wh_frame.pack(fill=tk.X, pady=(0, 10))

        self.mssql_webhook_enabled_var = tk.BooleanVar(value=self.config.get("mssql_webhook_enabled", True))
        ttk.Checkbutton(mssql_wh_frame, text="Enable", variable=self.mssql_webhook_enabled_var).pack(anchor=tk.W)

        mssql_url_row = ttk.Frame(mssql_wh_frame)
        mssql_url_row.pack(fill=tk.X, pady=(5, 0))
        ttk.Label(mssql_url_row, text="URL:").pack(side=tk.LEFT)
        mssql_url = f"http://<server-ip>:{self.config.get('mssql_api_port', 3100)}/api/self-update"
        ttk.Label(mssql_url_row, text=mssql_url, foreground="blue").pack(side=tk.LEFT, padx=5)

        ttk.Label(mssql_wh_frame, text="Downloads mssql_routes.enc from GitHub and restarts", foreground="gray").pack(anchor=tk.W, pady=(5, 0))

        # 저장 버튼
        ttk.Button(tab, text="Save Webhook Settings", command=self._save_webhook_settings).pack(pady=20)

    # ============ API Keys 탭 ============
    def _create_apikey_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" API Keys ")

        # 설명
        desc_label = ttk.Label(tab, text="AI 서비스 API 키 설정 (암호화하여 저장됩니다)", foreground="gray")
        desc_label.pack(anchor=tk.W, pady=(0, 10))

        # OpenAI (GPT) API Key
        gpt_frame = ttk.LabelFrame(tab, text="OpenAI (GPT / Whisper)", padding=8)
        gpt_frame.pack(fill=tk.X, pady=(0, 10))

        gpt_row = ttk.Frame(gpt_frame)
        gpt_row.pack(fill=tk.X)
        ttk.Label(gpt_row, text="API Key:").pack(side=tk.LEFT)
        self.openai_key_var = tk.StringVar(value=self.config.get("openai_api_key", ""))
        self.openai_key_entry = ttk.Entry(gpt_row, textvariable=self.openai_key_var, width=45, show="*")
        self.openai_key_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(gpt_row, text="Show", command=lambda: self._toggle_key_visibility(self.openai_key_entry)).pack(side=tk.LEFT)

        gpt_hint = ttk.Label(gpt_frame, text="sk-... 또는 sk-proj-... 형식", foreground="gray")
        gpt_hint.pack(anchor=tk.W, pady=(5, 0))

        # Google Gemini API Key
        gemini_frame = ttk.LabelFrame(tab, text="Google Gemini", padding=8)
        gemini_frame.pack(fill=tk.X, pady=(0, 10))

        gemini_row = ttk.Frame(gemini_frame)
        gemini_row.pack(fill=tk.X)
        ttk.Label(gemini_row, text="API Key:").pack(side=tk.LEFT)
        self.gemini_key_var = tk.StringVar(value=self.config.get("gemini_api_key", ""))
        self.gemini_key_entry = ttk.Entry(gemini_row, textvariable=self.gemini_key_var, width=45, show="*")
        self.gemini_key_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(gemini_row, text="Show", command=lambda: self._toggle_key_visibility(self.gemini_key_entry)).pack(side=tk.LEFT)

        gemini_hint = ttk.Label(gemini_frame, text="AIza... 형식 (Google AI Studio에서 발급)", foreground="gray")
        gemini_hint.pack(anchor=tk.W, pady=(5, 0))

        # 저장 버튼
        btn_frame = ttk.Frame(tab)
        btn_frame.pack(pady=20)
        ttk.Button(btn_frame, text="Save API Keys", command=self._save_api_keys).pack()

        # 상태 표시
        self.apikey_status_label = ttk.Label(tab, text="", foreground="green")
        self.apikey_status_label.pack()

    def _toggle_key_visibility(self, entry_widget):
        """API 키 표시/숨김 토글"""
        if entry_widget.cget("show") == "*":
            entry_widget.configure(show="")
        else:
            entry_widget.configure(show="*")

    def _save_api_keys(self):
        """API 키 저장"""
        self.config["openai_api_key"] = self.openai_key_var.get().strip()
        self.config["gemini_api_key"] = self.gemini_key_var.get().strip()
        save_config(self.config)
        self.apikey_status_label.configure(text="저장 완료!", foreground="green")
        self.root.after(3000, lambda: self.apikey_status_label.configure(text=""))

    # ============ 로그 콜백 설정 ============
    def _setup_log_callbacks(self):
        def make_log_callback(log_widget):
            def callback(msg):
                self.root.after(0, lambda: self._append_log(log_widget, msg))
            return callback

        git_build.log_callback = make_log_callback(self.static_log)
        git_build.mssql_log_callback = make_log_callback(self.mssql_log)  # MSSQL self-update 로그
        mssql_db.log_callback = make_log_callback(self.mssql_log)
        postgres_db.log_callback = make_log_callback(self.postgres_log)

    def _append_log(self, log_widget, message):
        log_widget.configure(state=tk.NORMAL)
        log_widget.insert(tk.END, message + "\n")
        log_widget.see(tk.END)
        log_widget.configure(state=tk.DISABLED)

    def _clear_log(self, log_widget):
        log_widget.configure(state=tk.NORMAL)
        log_widget.delete(1.0, tk.END)
        log_widget.configure(state=tk.DISABLED)

    # ============ 시스템 트레이 ============
    def _setup_tray(self):
        try:
            import pystray
            from PIL import Image, ImageDraw

            def create_icon():
                img = Image.new('RGB', (64, 64), color=(26, 26, 46))
                draw = ImageDraw.Draw(img)
                draw.ellipse([16, 16, 48, 48], fill=(0, 217, 255))
                return img

            def on_show(icon, item):
                self.root.after(0, self.root.deiconify)

            def on_quit(icon, item):
                icon.stop()
                self.root.after(0, self.root.destroy)

            icon = pystray.Icon(
                APP_NAME,
                create_icon(),
                APP_NAME,
                menu=pystray.Menu(
                    pystray.MenuItem("Show", on_show, default=True),
                    pystray.MenuItem("Static Console", lambda: webbrowser.open(f"http://localhost:{self.static_port_var.get()}/console")),
                    pystray.MenuItem("MSSQL Console", lambda: webbrowser.open(f"http://localhost:{self.mssql_port_var.get()}")),
                    pystray.MenuItem("PostgreSQL Console", lambda: webbrowser.open(f"http://localhost:{self.postgres_port_var.get()}")),
                    pystray.Menu.SEPARATOR,
                    pystray.MenuItem("Quit", on_quit)
                )
            )

            threading.Thread(target=icon.run, daemon=True).start()

            def on_minimize():
                self.root.withdraw()

            self.root.protocol("WM_DELETE_WINDOW", on_minimize)

        except Exception as e:
            print(f"Tray setup failed: {e}")
            self.root.protocol("WM_DELETE_WINDOW", self.root.destroy)

    # ============ 이벤트 핸들러 ============
    def _toggle_startup(self):
        set_startup_enabled(self.startup_var.get())

    def _browse_www_folder(self):
        path = filedialog.askdirectory(title="Select Project Folder")
        if path:
            self.www_folder_var.set(path)

    def _toggle_webhook_secret(self):
        current = self.webhook_secret_entry.cget('show')
        self.webhook_secret_entry.configure(show="" if current == "*" else "*")

    def _download_enc_and_reload(self):
        """GitHub에서 enc 파일 다운로드 후 핫 리로드"""
        def do_download():
            try:
                # 다운로드
                mssql_db.log("enc 파일 다운로드 시작...")
                success, has_changes = git_build.download_enc_from_github_http()

                if not success:
                    self.root.after(0, lambda: mssql_db.log("다운로드 실패"))
                    self.root.after(0, lambda: messagebox.showerror("Error", "enc 파일 다운로드 실패"))
                    return

                if not has_changes:
                    self.root.after(0, lambda: mssql_db.log("변경사항 없음"))
                    self.root.after(0, lambda: messagebox.showinfo("Info", "이미 최신 버전입니다."))
                    return

                # 다운로드 성공, 핫 리로드 시도
                self.root.after(0, lambda: mssql_db.log("다운로드 완료, 핫 리로드 중..."))

                if self.mssql_running:
                    from services.mssql_loader import hot_reload_routes
                    reload_success, new_version, message = hot_reload_routes()

                    if reload_success:
                        self.root.after(0, lambda: self._on_mssql_reload(new_version))
                        self.root.after(0, lambda: mssql_db.log(f"핫 리로드 완료: v{new_version}"))
                        self.root.after(0, lambda: messagebox.showinfo("Success", f"API 업데이트 완료: v{new_version}"))
                    else:
                        self.root.after(0, lambda: mssql_db.log(f"핫 리로드 실패: {message}"))
                        self.root.after(0, lambda: messagebox.showwarning("Warning", f"다운로드 완료, 핫 리로드 실패:\n{message}\n\nMSSQL 서버 재시작 필요"))
                else:
                    # MSSQL 서버가 실행 중이 아니면 버전만 업데이트
                    self.root.after(0, lambda: self._update_api_version())
                    self.root.after(0, lambda: mssql_db.log("다운로드 완료 (서버 미실행)"))
                    self.root.after(0, lambda: messagebox.showinfo("Success", "enc 파일 다운로드 완료.\nMSSQL 서버 시작 시 적용됩니다."))

            except Exception as e:
                self.root.after(0, lambda: mssql_db.log(f"오류: {str(e)}"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"오류 발생:\n{str(e)}"))

        threading.Thread(target=do_download, daemon=True).start()

    # ============ 서버 제어: Static ============
    def _start_static(self):
        from flask import Flask
        from flask_cors import CORS
        from routes.static_routes import static_bp, set_www_folder

        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return

        if not Path(www_folder).exists():
            messagebox.showerror("Error", f"폴더가 존재하지 않습니다:\n{www_folder}")
            return

        port = self.static_port_var.get()
        set_www_folder(www_folder)

        self.static_app = Flask(__name__)
        CORS(self.static_app)
        self.static_app.register_blueprint(static_bp)

        # HTTP 서버 (PC용)
        def run_http():
            self.static_running = True
            try:
                from waitress import serve
                git_build.log(f"HTTP 서버 시작 (포트: {port})")
                serve(
                    self.static_app,
                    host='0.0.0.0',
                    port=port,
                    threads=4,
                    connection_limit=50,
                    channel_timeout=120,
                    expose_tracebacks=False,
                    ident='Haniwon-Static'
                )
            except ImportError:
                git_build.log(f"Flask HTTP 서버 시작 (포트: {port})")
                self.static_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run_http, daemon=True).start()

        self.static_status_var.set("Running")
        self.static_status_label.configure(foreground="green")
        self.static_start_btn.configure(state=tk.DISABLED)
        self.static_stop_btn.configure(state=tk.NORMAL)

        if self.auto_build_var.get():
            git_build.start_file_watcher(www_folder)

    def _stop_static(self):
        self.static_running = False
        git_build.stop_file_watcher()
        git_build.log("서버 중지됨")
        self.static_status_var.set("Stopped")
        self.static_status_label.configure(foreground="orange")
        self.static_start_btn.configure(state=tk.NORMAL)
        self.static_stop_btn.configure(state=tk.DISABLED)

    def _git_clone(self):
        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return

        # .git 폴더가 이미 있으면 경고
        git_dir = Path(www_folder) / ".git"
        if git_dir.exists():
            messagebox.showinfo("Info", "이미 Git 저장소입니다. Git Pull을 사용하세요.")
            return

        # 저장된 repo_url 사용 또는 입력 받기
        repo_url = self.config.get("repo_url", "")
        if not repo_url:
            repo_url = "https://github.com/yeonijae/haniwon.git"

        # 간단한 입력 다이얼로그
        from tkinter import simpledialog
        repo_url = simpledialog.askstring(
            "Git Clone",
            "Repository URL:",
            initialvalue=repo_url,
            parent=self.root
        )

        if not repo_url:
            return

        # repo_url 저장
        self.config["repo_url"] = repo_url
        save_config(self.config)

        # 폴더가 비어있지 않으면 경고
        target_path = Path(www_folder)
        if target_path.exists() and any(target_path.iterdir()):
            if not messagebox.askyesno("Warning", f"폴더가 비어있지 않습니다.\n{www_folder}\n\n폴더를 삭제하고 Clone할까요?"):
                return
            # 폴더 삭제
            import shutil
            try:
                shutil.rmtree(www_folder)
                git_build.log(f"폴더 삭제: {www_folder}")
            except Exception as e:
                messagebox.showerror("Error", f"폴더 삭제 실패: {e}")
                return

        # Clone 실행
        def do_clone():
            git_build.log(f"Cloning {repo_url}...")
            success = git_build.run_git_clone(repo_url, www_folder)
            if success:
                git_build.log("Clone 완료!")
            else:
                git_build.log("Clone 실패")

        threading.Thread(target=do_clone, daemon=True).start()

    def _git_pull(self):
        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return
        threading.Thread(target=lambda: git_build.run_git_pull(www_folder), daemon=True).start()

    def _bun_install(self):
        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return
        threading.Thread(target=lambda: git_build.run_bun_install(www_folder), daemon=True).start()

    def _bun_build(self):
        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return
        threading.Thread(target=lambda: git_build.run_bun_build(www_folder), daemon=True).start()

    def _save_static_settings(self):
        self.config["static_port"] = self.static_port_var.get()
        self.config["www_folder"] = self.www_folder_var.get()
        self.config["auto_build"] = self.auto_build_var.get()
        self.config["static_auto_start"] = self.static_auto_start_var.get()
        save_config(self.config)
        messagebox.showinfo("Success", "Static settings saved")

    # ============ 서버 제어: MSSQL ============
    def _update_api_version(self):
        """mssql_routes.enc 파일의 API 버전 확인"""
        try:
            from services.crypto_loader import get_module_version
            enc_path = APP_DIR / "mssql_routes.enc"
            if enc_path.exists():
                version = get_module_version(str(enc_path))
                self.mssql_api_version_var.set(f"v{version}")
            else:
                self.mssql_api_version_var.set("(enc 없음)")
        except Exception as e:
            self.mssql_api_version_var.set(f"error: {e}")

    def _hot_reload_mssql(self):
        """MSSQL 라우트 핫 리로드 (서버 재시작 없이 모듈 교체)"""
        if not self.mssql_running:
            messagebox.showwarning("Warning", "MSSQL 서버가 실행 중이 아닙니다.")
            return

        mssql_db.log("핫 리로드 시작...")

        def do_reload():
            try:
                from services.mssql_loader import hot_reload_routes
                success, new_version, message = hot_reload_routes()

                if success:
                    self.root.after(0, lambda: self._on_mssql_reload(new_version))
                    self.root.after(0, lambda: mssql_db.log(f"핫 리로드 성공: v{new_version}"))
                else:
                    self.root.after(0, lambda: mssql_db.log(f"핫 리로드 실패: {message}"))
                    self.root.after(0, lambda: messagebox.showerror("Error", f"핫 리로드 실패:\n{message}"))
            except Exception as e:
                self.root.after(0, lambda: mssql_db.log(f"핫 리로드 오류: {e}"))
                self.root.after(0, lambda: messagebox.showerror("Error", f"핫 리로드 오류:\n{e}"))

        threading.Thread(target=do_reload, daemon=True).start()

    def _start_mssql(self):
        from flask import Flask
        from flask_cors import CORS
        from services.mssql_loader import load_mssql_routes, set_flask_app

        self.config["mssql"] = {
            "server": self.mssql_server_var.get(),
            "port": self.mssql_conn_port_var.get(),
            "user": self.mssql_user_var.get(),
            "password": self.mssql_pass_var.get(),
            "database": self.mssql_db_var.get()
        }
        save_config(self.config)

        port = self.mssql_port_var.get()
        use_builtin = self.mssql_use_builtin_var.get()

        # Connection Pool 초기화
        mssql_db.initialize_pool(self.config.get("mssql", {}))

        mssql_bp, routes_version, source = load_mssql_routes(use_builtin=use_builtin)
        self.mssql_routes_version = routes_version
        self.mssql_routes_source = source
        self.mssql_api_version_var.set(f"v{routes_version}")
        mssql_db.log(f"Routes 로드: v{routes_version} ({source})")

        self.mssql_app = Flask(__name__)
        CORS(self.mssql_app)
        self.mssql_app.register_blueprint(mssql_bp)

        # 핫 리로드를 위한 Flask 앱 참조 설정
        def on_reload(new_version):
            self.root.after(0, lambda: self._on_mssql_reload(new_version))
        set_flask_app(self.mssql_app, on_reload)

        def run():
            self.mssql_running = True
            try:
                from waitress import serve
                mssql_db.log(f"Waitress 서버 시작 (포트: {port}, API: v{routes_version})")
                serve(
                    self.mssql_app,
                    host='0.0.0.0',
                    port=port,
                    threads=8,
                    connection_limit=100,
                    channel_timeout=120,
                    expose_tracebacks=False,
                    ident='Haniwon-MSSQL'
                )
            except ImportError:
                mssql_db.log(f"Flask 서버 시작 (포트: {port}, API: v{routes_version}) - Waitress 미설치")
                self.mssql_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run, daemon=True).start()

        # Health Monitor 등록
        health_monitor = get_health_monitor()
        health_monitor.register_service(
            "mssql",
            lambda: mssql_db.health_check(),
            lambda: self._restart_mssql()
        )

        self.mssql_status_var.set("Running")
        self.mssql_status_label.configure(foreground="green")
        self.mssql_start_btn.configure(state=tk.DISABLED)
        self.mssql_stop_btn.configure(state=tk.NORMAL)

        # 대기열 동기화 체크
        self._check_and_start_waiting_sync()

    def _on_mssql_reload(self, new_version):
        """핫 리로드 후 GUI 업데이트"""
        self.mssql_api_version_var.set(f"v{new_version}")
        self.mssql_routes_version = new_version
        mssql_db.log(f"핫 리로드 완료: v{new_version}")

    def _restart_mssql(self):
        """MSSQL 서버 재시작"""
        mssql_db.log("서버 재시작 중...")
        self._stop_mssql()
        import time
        time.sleep(1)
        self.root.after(0, self._start_mssql)

    def _stop_mssql(self):
        self.mssql_running = False
        mssql_db.log("서버 중지됨")
        self.mssql_status_var.set("Stopped")
        self.mssql_status_label.configure(foreground="orange")
        self.mssql_start_btn.configure(state=tk.NORMAL)
        self.mssql_stop_btn.configure(state=tk.DISABLED)

        # 대기열 동기화 체크
        self._check_and_start_waiting_sync()

    def _test_mssql(self):
        self.mssql_test_var.set("Testing...")
        self.mssql_test_label.configure(foreground="gray")
        self.root.update()

        self.config["mssql"] = {
            "server": self.mssql_server_var.get(),
            "port": self.mssql_conn_port_var.get(),
            "user": self.mssql_user_var.get(),
            "password": self.mssql_pass_var.get(),
            "database": self.mssql_db_var.get()
        }
        save_config(self.config)

        result = mssql_db.test_connection()
        if result['success']:
            self.mssql_test_var.set(f"OK ({result['count']:,})")
            self.mssql_test_label.configure(foreground="green")
        else:
            self.mssql_test_var.set("Failed")
            self.mssql_test_label.configure(foreground="red")

    def _save_mssql_settings(self):
        self.config["mssql"] = {
            "server": self.mssql_server_var.get(),
            "port": self.mssql_conn_port_var.get(),
            "user": self.mssql_user_var.get(),
            "password": self.mssql_pass_var.get(),
            "database": self.mssql_db_var.get()
        }
        self.config["mssql_api_port"] = self.mssql_port_var.get()
        self.config["mssql_auto_start"] = self.mssql_auto_start_var.get()
        self.config["mssql_use_builtin"] = self.mssql_use_builtin_var.get()
        save_config(self.config)
        messagebox.showinfo("Success", "MSSQL settings saved")

    # ============ 서버 제어: PostgreSQL ============
    def _start_postgres(self):
        from flask import Flask
        from flask_cors import CORS
        from routes.postgres_routes import postgres_bp
        from routes.file_routes import file_bp

        # 연결 테스트
        result = postgres_db.test_connection()
        if not result.get('success'):
            messagebox.showerror("Error", f"PostgreSQL 연결 실패:\n{result.get('error', 'Unknown error')}")
            return

        port = self.postgres_port_var.get()

        self.postgres_app = Flask(__name__)
        CORS(self.postgres_app, resources={
            r"/api/*": {
                "origins": "*",
                "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                "allow_headers": ["Content-Type", "Authorization", "X-Requested-With"]
            }
        })
        self.postgres_app.register_blueprint(postgres_bp)
        self.postgres_app.register_blueprint(file_bp)

        def run():
            self.postgres_running = True
            config = postgres_db.get_db_config()
            postgres_db.log(f"DB: {config.get('host')}:{config.get('port')}/{config.get('database')}", force=True)
            try:
                from waitress import serve
                postgres_db.log(f"Waitress 서버 시작 (포트: {port})", force=True)
                serve(
                    self.postgres_app,
                    host='0.0.0.0',
                    port=port,
                    threads=64,
                    connection_limit=200,
                    channel_timeout=120,
                    expose_tracebacks=False,
                    ident='Haniwon-PostgreSQL'
                )
            except ImportError:
                postgres_db.log(f"Flask 서버 시작 (포트: {port}) - Waitress 미설치", force=True)
                self.postgres_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run, daemon=True).start()

        self.postgres_status_var.set("Running")
        self.postgres_status_label.configure(foreground="green")
        self.postgres_start_btn.configure(state=tk.DISABLED)
        self.postgres_stop_btn.configure(state=tk.NORMAL)

        # 대기열 동기화 체크
        self._check_and_start_waiting_sync()

    def _stop_postgres(self):
        self.postgres_running = False
        postgres_db.log("서버 중지됨", force=True)
        self.postgres_status_var.set("Stopped")
        self.postgres_status_label.configure(foreground="orange")
        self.postgres_start_btn.configure(state=tk.NORMAL)
        self.postgres_stop_btn.configure(state=tk.DISABLED)

        # 대기열 동기화 중지
        self._check_and_start_waiting_sync()

    def _test_postgres_connection(self):
        """PostgreSQL 연결 테스트"""
        # 설정값을 임시로 업데이트
        self._save_postgres_connection(show_message=False)
        postgres_db.reload_config()

        result = postgres_db.test_connection()
        if result.get('success'):
            messagebox.showinfo("Success", "PostgreSQL 연결 성공!")
        else:
            messagebox.showerror("Error", f"연결 실패:\n{result.get('error', 'Unknown error')}")

    def _save_postgres_connection(self, show_message=True):
        """PostgreSQL 연결 설정 저장"""
        self.config["postgres"] = {
            "host": self.pg_host_var.get(),
            "port": self.pg_port_var.get(),
            "database": self.pg_database_var.get(),
            "user": self.pg_user_var.get(),
            "password": self.pg_password_var.get()
        }
        save_config(self.config)
        postgres_db.reload_config()
        if show_message:
            messagebox.showinfo("Success", "PostgreSQL 연결 설정 저장됨")

    def _save_postgres_settings(self):
        """PostgreSQL 서버 설정 저장"""
        self.config["postgres_api_port"] = self.postgres_port_var.get()
        self.config["postgres_auto_start"] = self.postgres_auto_start_var.get()
        self._save_postgres_connection(show_message=False)
        messagebox.showinfo("Success", "PostgreSQL settings saved")

    # ============ Chat Server ============
    def _start_chat(self):
        """Chat 서버 시작"""
        if self.chat_running:
            return

        port = self.chat_port_var.get()
        db_config = {
            'host': self.chat_db_host_var.get(),
            'port': self.chat_db_port_var.get(),
            'database': self.chat_db_name_var.get(),
            'user': self.chat_db_user_var.get(),
            'password': self.chat_db_pass_var.get()
        }

        def chat_log(msg):
            self._append_log(self.chat_log, msg)

        def run_chat():
            try:
                from chat.server import create_chat_app
                self.chat_app, self.chat_socketio = create_chat_app(db_config, chat_log)
                chat_log(f"Starting on port {port}...")
                self.chat_socketio.run(
                    self.chat_app,
                    host='0.0.0.0',
                    port=port,
                    debug=False,
                    use_reloader=False
                )
            except Exception as e:
                chat_log(f"Error: {e}")
                self.chat_running = False
                self.root.after(0, lambda: self.chat_status_var.set("Error"))
                self.root.after(0, lambda: self.chat_status_label.configure(foreground="red"))

        self.chat_running = True
        threading.Thread(target=run_chat, daemon=True).start()

        chat_log(f"Chat server starting on port {port}...")
        self.chat_status_var.set("Running")
        self.chat_status_label.configure(foreground="green")
        self.chat_start_btn.configure(state=tk.DISABLED)
        self.chat_stop_btn.configure(state=tk.NORMAL)

    def _stop_chat(self):
        """Chat 서버 중지"""
        self.chat_running = False
        self._append_log(self.chat_log, "서버 중지됨")
        self.chat_status_var.set("Stopped")
        self.chat_status_label.configure(foreground="orange")
        self.chat_start_btn.configure(state=tk.NORMAL)
        self.chat_stop_btn.configure(state=tk.DISABLED)

    def _test_chat_db(self):
        """Chat DB 연결 테스트"""
        import psycopg2
        try:
            conn = psycopg2.connect(
                host=self.chat_db_host_var.get(),
                port=self.chat_db_port_var.get(),
                database=self.chat_db_name_var.get(),
                user=self.chat_db_user_var.get(),
                password=self.chat_db_pass_var.get(),
                connect_timeout=5
            )
            conn.close()
            messagebox.showinfo("Success", "PostgreSQL 연결 성공!")
        except Exception as e:
            messagebox.showerror("Error", f"연결 실패:\n{e}")

    def _save_chat_settings(self):
        """Chat 서버 설정 저장"""
        self.config["chat_port"] = self.chat_port_var.get()
        self.config["chat_auto_start"] = self.chat_auto_start_var.get()
        self.config["chat"] = {
            "host": self.chat_db_host_var.get(),
            "port": self.chat_db_port_var.get(),
            "database": self.chat_db_name_var.get(),
            "user": self.chat_db_user_var.get(),
            "password": self.chat_db_pass_var.get()
        }
        save_config(self.config)
        messagebox.showinfo("Success", "Chat settings saved")

    # ============ MSSQL → PostgreSQL 대기열 동기화 ============
    def _start_waiting_sync(self):
        """MSSQL Waiting → PostgreSQL waiting_queue 백그라운드 동기화 시작"""
        if self.waiting_sync_running:
            return

        self.waiting_sync_running = True
        self.waiting_sync_thread = threading.Thread(target=self._waiting_sync_loop, daemon=True)
        self.waiting_sync_thread.start()
        interval = self.config.get("sync_interval", 5)
        postgres_db.log(f"대기열 동기화 시작 ({interval}초 간격)", force=True)

    def _stop_waiting_sync(self):
        """동기화 중지"""
        self.waiting_sync_running = False
        if self.waiting_sync_thread:
            self.waiting_sync_thread = None


    def _is_within_schedule(self):
        """현재 시간이 스케줄 내에 있는지 확인"""
        from datetime import datetime

        now = datetime.now()
        day_map = {0: "mon", 1: "tue", 2: "wed", 3: "thu", 4: "fri", 5: "sat", 6: "sun"}
        day_key = day_map[now.weekday()]

        schedule = self.config.get("sync_schedule", self.default_schedule)
        day_schedule = schedule.get(day_key, self.default_schedule[day_key])

        if not day_schedule.get("enabled", True):
            return False

        try:
            start_str = day_schedule.get("start", "08:30")
            end_str = day_schedule.get("end", "18:30")

            start_time = datetime.strptime(start_str, "%H:%M").time()
            end_time = datetime.strptime(end_str, "%H:%M").time()
            current_time = now.time()

            return start_time <= current_time <= end_time
        except:
            return True  # 파싱 오류 시 동기화 허용

    def _waiting_sync_loop(self):
        """설정된 간격으로 MSSQL Treating(치료실)을 PostgreSQL waiting_queue에 동기화

        - MSSQL Treating → PostgreSQL waiting_queue (queue_type='treatment')
        - MSSQL Waiting(대기실)은 동기화하지 않음 (CS관리에서 수동 처리)
        - 요일별 스케줄에 따라 동기화 실행
        """
        import time
        import requests
        from datetime import datetime

        while self.waiting_sync_running:
            interval = self.config.get("sync_interval", 5)

            try:
                # 두 서버가 모두 실행 중인지 확인
                if not self.mssql_running or not self.postgres_running:
                    time.sleep(interval)
                    continue

                # 스케줄 체크
                if not self._is_within_schedule():
                    time.sleep(interval)
                    continue

                mssql_port = self.mssql_port_var.get()
                postgres_port = self.postgres_port_var.get()

                # 1. MSSQL에서 치료실(Treating) 목록만 가져오기
                try:
                    mssql_res = requests.get(
                        f"http://localhost:{mssql_port}/api/queue/status",
                        timeout=5
                    )
                    if mssql_res.status_code != 200:
                        time.sleep(interval)
                        continue

                    queue_data = mssql_res.json()
                    treating_list = queue_data.get('treating', [])

                    # treating 데이터를 waiting 형식에 맞게 변환
                    for t in treating_list:
                        if 'treating_since' in t and 'waiting_since' not in t:
                            t['waiting_since'] = t['treating_since']

                    sync_list = treating_list

                except requests.exceptions.RequestException:
                    time.sleep(interval)
                    continue

                # 목록이 비어있으면 동기화 스킵
                if not sync_list:
                    time.sleep(interval)
                    continue

                # 2. PostgreSQL에 동기화 (daily_treatment_records)
                try:
                    sync_res = requests.post(
                        f"http://localhost:{postgres_port}/api/treatments/sync",
                        json={"waiting": sync_list},
                        timeout=5
                    )

                    if sync_res.status_code == 200:
                        result = sync_res.json()
                        added = result.get('added', 0)
                        updated = result.get('updated', 0)
                        skipped = result.get('skipped', 0)
                        self.last_sync_time = datetime.now()
                        if added > 0:
                            self.sync_count += added
                            postgres_db.log(f"치료기록 동기화: 추가 {added}, 업데이트 {updated}, 스킵 {skipped}", force=True)

                except requests.exceptions.RequestException:
                    pass

            except Exception as e:
                postgres_db.log(f"대기열 동기화 오류: {e}")

            time.sleep(interval)

    def _check_and_start_waiting_sync(self):
        """두 서버가 모두 실행 중이고 auto_start가 활성화되어 있으면 동기화 시작"""
        if self.mssql_running and self.postgres_running:
            if self.config.get("sync_auto_start", True):
                self._start_waiting_sync()
        else:
            self._stop_waiting_sync()

    def _save_webhook_settings(self):
        self.config["webhook_secret"] = self.webhook_secret_var.get()
        self.config["webhook_enabled"] = self.webhook_enabled_var.get()
        self.config["mssql_webhook_enabled"] = self.mssql_webhook_enabled_var.get()
        save_config(self.config)
        messagebox.showinfo("Success", "Webhook settings saved")

    # ============ 자동 시작 ============
    def _auto_start_servers(self):
        any_auto_started = False

        if self.static_auto_start_var.get() and self.www_folder_var.get():
            git_build.log("자동 시작")
            self._start_static()
            any_auto_started = True

        if self.mssql_auto_start_var.get():
            mssql_db.log("자동 시작")
            self._start_mssql()
            any_auto_started = True

        if self.postgres_auto_start_var.get():
            postgres_db.log("자동 시작", force=True)
            self._start_postgres()
            any_auto_started = True

        if self.chat_auto_start_var.get():
            self._append_log(self.chat_log, "자동 시작")
            self._start_chat()
            any_auto_started = True

        # Health Monitor 시작 (자동 재시작: 매일 새벽 4시)
        if any_auto_started:
            health_monitor = get_health_monitor()
            health_monitor.set_restart_time("04:00")
            health_monitor.start()

        if any_auto_started:
            self.root.withdraw()

    def run(self):
        self.root.mainloop()


def run_gui():
    app = UnifiedServerGUI()
    app.run()


if __name__ == "__main__":
    run_gui()
