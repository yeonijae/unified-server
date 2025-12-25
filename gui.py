"""
Haniwon Unified Server GUI
- 3개 서버 (Static, MSSQL, SQLite) 관리
- 각 서버별 Auto Start, Log
- Windows 시작프로그램 등록
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import webbrowser
import sqlite3
from pathlib import Path

from config import (
    APP_VERSION, VERSION, APP_NAME, APP_DIR, DEFAULT_DB_FILE,
    load_config, save_config, is_startup_enabled, set_startup_enabled
)
from services import mssql_db, sqlite_db, git_build
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
        self.sqlite_running = False

        # Flask 앱 참조
        self.static_app = None
        self.mssql_app = None
        self.sqlite_app = None

        # 로그 텍스트 위젯 (나중에 생성)
        self.static_log = None
        self.mssql_log = None
        self.sqlite_log = None

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
        self._create_sqlite_tab(notebook)
        self._create_webhook_tab(notebook)

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

    # ============ SQLite 탭 ============
    def _create_sqlite_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=10)
        notebook.add(tab, text=" SQLite ")

        # 서버 상태 + 포트 + 버튼 (한 줄)
        server_frame = ttk.LabelFrame(tab, text="SQLite API Server", padding=8)
        server_frame.pack(fill=tk.X, pady=(0, 8))

        row = ttk.Frame(server_frame)
        row.pack(fill=tk.X)

        self.sqlite_status_var = tk.StringVar(value="Stopped")
        self.sqlite_status_label = ttk.Label(row, textvariable=self.sqlite_status_var, font=('Segoe UI', 10, 'bold'), foreground="red", width=8)
        self.sqlite_status_label.pack(side=tk.LEFT)

        ttk.Label(row, text="Port:").pack(side=tk.LEFT, padx=(10, 2))
        self.sqlite_port_var = tk.IntVar(value=self.config.get("sqlite_api_port", 3200))
        ttk.Entry(row, textvariable=self.sqlite_port_var, width=6).pack(side=tk.LEFT)

        ttk.Button(row, text="Console", command=lambda: webbrowser.open(f"http://localhost:{self.sqlite_port_var.get()}")).pack(side=tk.RIGHT, padx=2)
        self.sqlite_stop_btn = ttk.Button(row, text="Stop", command=self._stop_sqlite, state=tk.DISABLED)
        self.sqlite_stop_btn.pack(side=tk.RIGHT, padx=2)
        self.sqlite_start_btn = ttk.Button(row, text="Start", command=self._start_sqlite)
        self.sqlite_start_btn.pack(side=tk.RIGHT, padx=2)

        # 데이터베이스 파일
        db_frame = ttk.LabelFrame(tab, text="Database File", padding=8)
        db_frame.pack(fill=tk.X, pady=(0, 8))

        db_row = ttk.Frame(db_frame)
        db_row.pack(fill=tk.X)
        self.sqlite_path_var = tk.StringVar(value=self.config.get("sqlite_db_path", ""))
        ttk.Entry(db_row, textvariable=self.sqlite_path_var, width=40).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(db_row, text="Browse", command=self._browse_sqlite_db).pack(side=tk.LEFT, padx=(5, 0))

        db_btn_row = ttk.Frame(db_frame)
        db_btn_row.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(db_btn_row, text="Create New", command=self._create_sqlite_db).pack(side=tk.LEFT, padx=2)
        ttk.Button(db_btn_row, text="Run SQL...", command=self._run_sql_file).pack(side=tk.LEFT, padx=2)

        # 백업 설정
        backup_frame = ttk.LabelFrame(tab, text="Backup", padding=8)
        backup_frame.pack(fill=tk.X, pady=(0, 8))

        backup_row1 = ttk.Frame(backup_frame)
        backup_row1.pack(fill=tk.X)
        self.backup_enabled_var = tk.BooleanVar(value=self.config.get("backup_enabled", False))
        ttk.Checkbutton(backup_row1, text="Auto Backup", variable=self.backup_enabled_var).pack(side=tk.LEFT)
        self.backup_interval_var = tk.IntVar(value=self.config.get("backup_interval_hours", 24))
        ttk.Label(backup_row1, text="Every").pack(side=tk.LEFT, padx=(10, 2))
        ttk.Entry(backup_row1, textvariable=self.backup_interval_var, width=4).pack(side=tk.LEFT)
        ttk.Label(backup_row1, text="hours").pack(side=tk.LEFT, padx=2)
        ttk.Button(backup_row1, text="Backup Now", command=self._manual_backup).pack(side=tk.RIGHT)

        backup_row2 = ttk.Frame(backup_frame)
        backup_row2.pack(fill=tk.X, pady=(5, 0))
        ttk.Label(backup_row2, text="Folder:").pack(side=tk.LEFT)
        self.backup_folder_var = tk.StringVar(value=self.config.get("backup_folder", ""))
        ttk.Entry(backup_row2, textvariable=self.backup_folder_var, width=35).pack(side=tk.LEFT, padx=2, fill=tk.X, expand=True)
        ttk.Button(backup_row2, text="Browse", command=self._browse_backup_folder).pack(side=tk.LEFT, padx=(2, 0))

        # 옵션
        self.sqlite_auto_start_var = tk.BooleanVar(value=self.config.get("sqlite_auto_start", False))
        ttk.Checkbutton(tab, text="Auto start on startup", variable=self.sqlite_auto_start_var).pack(anchor=tk.W)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        self.sqlite_log = tk.Text(log_frame, height=5, font=('Consolas', 9), bg='#1e1e1e', fg='#10b981', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.sqlite_log.yview)
        self.sqlite_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.sqlite_log.pack(fill=tk.BOTH, expand=True)

        log_btn_row = ttk.Frame(log_frame)
        log_btn_row.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(log_btn_row, text="Clear", command=lambda: self._clear_log(self.sqlite_log)).pack(side=tk.RIGHT)
        ttk.Button(log_btn_row, text="Save Settings", command=self._save_sqlite_settings).pack(side=tk.RIGHT, padx=5)

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

    # ============ 로그 콜백 설정 ============
    def _setup_log_callbacks(self):
        def make_log_callback(log_widget):
            def callback(msg):
                self.root.after(0, lambda: self._append_log(log_widget, msg))
            return callback

        git_build.log_callback = make_log_callback(self.static_log)
        git_build.mssql_log_callback = make_log_callback(self.mssql_log)  # MSSQL self-update 로그
        mssql_db.log_callback = make_log_callback(self.mssql_log)
        sqlite_db.log_callback = make_log_callback(self.sqlite_log)

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
                    pystray.MenuItem("SQLite Console", lambda: webbrowser.open(f"http://localhost:{self.sqlite_port_var.get()}")),
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

    def _browse_sqlite_db(self):
        path = filedialog.askopenfilename(
            title="Select SQLite Database",
            filetypes=[("SQLite files", "*.sqlite *.db"), ("All files", "*.*")]
        )
        if path:
            self.sqlite_path_var.set(path)

    def _browse_backup_folder(self):
        path = filedialog.askdirectory(title="Select Backup Folder")
        if path:
            self.backup_folder_var.set(path)

    def _create_sqlite_db(self):
        path = filedialog.asksaveasfilename(
            title="Create New Database",
            defaultextension=".sqlite",
            filetypes=[("SQLite files", "*.sqlite"), ("All files", "*.*")]
        )
        if path:
            conn = sqlite3.connect(path)
            conn.close()
            self.sqlite_path_var.set(path)
            messagebox.showinfo("Success", f"Database created:\n{path}")

    def _toggle_webhook_secret(self):
        current = self.webhook_secret_entry.cget('show')
        self.webhook_secret_entry.configure(show="" if current == "*" else "*")

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

        def run():
            self.static_running = True
            try:
                from waitress import serve
                git_build.log(f"Waitress 서버 시작 (포트: {port})")
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
                git_build.log(f"Flask 서버 시작 (포트: {port}) - Waitress 미설치")
                self.static_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run, daemon=True).start()

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

    # ============ 서버 제어: SQLite ============
    def _start_sqlite(self):
        from flask import Flask
        from flask_cors import CORS
        from routes.sqlite_routes import sqlite_bp

        db_path = self.sqlite_path_var.get()
        if not db_path:
            messagebox.showerror("Error", "SQLite DB 파일을 선택해주세요.")
            return

        if not Path(db_path).exists():
            if messagebox.askyesno("Create?", f"파일이 없습니다. 생성할까요?\n{db_path}"):
                conn = sqlite3.connect(db_path)
                conn.close()
            else:
                return

        sqlite_db.set_db_path(db_path)
        port = self.sqlite_port_var.get()

        self.sqlite_app = Flask(__name__)
        CORS(self.sqlite_app, resources={
            r"/api/*": {
                "origins": "*",
                "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                "allow_headers": ["Content-Type", "Authorization", "X-Requested-With"]
            }
        })
        self.sqlite_app.register_blueprint(sqlite_bp)

        def run():
            self.sqlite_running = True
            sqlite_db.log(f"DB: {db_path}")
            try:
                from waitress import serve
                sqlite_db.log(f"Waitress 서버 시작 (포트: {port})")
                serve(
                    self.sqlite_app,
                    host='0.0.0.0',
                    port=port,
                    threads=4,
                    connection_limit=50,
                    channel_timeout=120,
                    expose_tracebacks=False,
                    ident='Haniwon-SQLite'
                )
            except ImportError:
                sqlite_db.log(f"Flask 서버 시작 (포트: {port}) - Waitress 미설치")
                self.sqlite_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run, daemon=True).start()

        self.sqlite_status_var.set("Running")
        self.sqlite_status_label.configure(foreground="green")
        self.sqlite_start_btn.configure(state=tk.DISABLED)
        self.sqlite_stop_btn.configure(state=tk.NORMAL)

        sqlite_db.start_backup_scheduler()

    def _stop_sqlite(self):
        self.sqlite_running = False
        sqlite_db.log("서버 중지됨")
        self.sqlite_status_var.set("Stopped")
        self.sqlite_status_label.configure(foreground="orange")
        self.sqlite_start_btn.configure(state=tk.NORMAL)
        self.sqlite_stop_btn.configure(state=tk.DISABLED)

    def _manual_backup(self):
        if not sqlite_db.get_db_path():
            messagebox.showerror("Error", "서버가 실행 중이 아니거나 DB가 설정되지 않았습니다.")
            return

        result = sqlite_db.do_backup()
        if result:
            messagebox.showinfo("Success", "백업이 완료되었습니다.")
        else:
            messagebox.showerror("Error", "백업 실패 또는 설정되지 않음")

    def _run_sql_file(self):
        db_path = self.sqlite_path_var.get()
        if not db_path or not Path(db_path).exists():
            messagebox.showerror("Error", "SQLite DB 파일을 먼저 선택해주세요.")
            return

        sql_file = filedialog.askopenfilename(
            title="Select SQL File",
            filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
        )
        if not sql_file:
            return

        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
            statements = [s.strip() for s in content.split(';') if s.strip() and not s.strip().startswith('--')]
            stmt_count = len(statements)
        except Exception as e:
            messagebox.showerror("Error", f"파일 읽기 실패: {e}")
            return

        if not messagebox.askyesno("Confirm", f"SQL 파일을 실행하시겠습니까?\n\n파일: {Path(sql_file).name}\n문장 수: {stmt_count}개"):
            return

        sqlite_db.log(f"SQL 실행 시작: {stmt_count}개 문장")

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            success_count = 0
            error_count = 0

            for stmt in statements:
                try:
                    cursor.execute(stmt)
                    success_count += 1
                except:
                    error_count += 1

            conn.commit()
            conn.close()

            sqlite_db.log(f"완료: 성공 {success_count}, 실패 {error_count}")
            messagebox.showinfo("SQL 실행 완료", f"성공: {success_count}개\n실패: {error_count}개")

        except Exception as e:
            sqlite_db.log(f"[ERROR] {str(e)}")
            messagebox.showerror("Error", str(e))

    def _save_sqlite_settings(self):
        self.config["sqlite_db_path"] = self.sqlite_path_var.get()
        self.config["sqlite_api_port"] = self.sqlite_port_var.get()
        self.config["sqlite_auto_start"] = self.sqlite_auto_start_var.get()
        self.config["backup_enabled"] = self.backup_enabled_var.get()
        self.config["backup_folder"] = self.backup_folder_var.get()
        self.config["backup_interval_hours"] = self.backup_interval_var.get()
        save_config(self.config)
        sqlite_db.start_backup_scheduler()
        messagebox.showinfo("Success", "SQLite settings saved")

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

        if self.sqlite_auto_start_var.get():
            if DEFAULT_DB_FILE.exists() and not self.sqlite_path_var.get():
                self.sqlite_path_var.set(str(DEFAULT_DB_FILE))

            if self.sqlite_path_var.get():
                sqlite_db.log("자동 시작")
                self._start_sqlite()
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
