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
    VERSION, APP_NAME, APP_DIR, DEFAULT_DB_FILE,
    load_config, save_config, is_startup_enabled, set_startup_enabled
)
from services import mssql_db, sqlite_db, git_build


class UnifiedServerGUI:
    def __init__(self):
        self.config = load_config()
        self.root = tk.Tk()
        self.root.title(f"{APP_NAME} v{VERSION}")
        self.root.geometry("580x820")
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

        # 자동 시작 체크
        if is_startup_enabled():
            self.root.after(500, self._auto_start_servers)

    def _setup_styles(self):
        style = ttk.Style()
        style.configure('TLabel', padding=3)
        style.configure('TButton', padding=3)
        style.configure('TNotebook.Tab', padding=[12, 6])

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

        ttk.Separator(self.root, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=10, pady=5)

        # 메인 노트북 (탭)
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        # 탭 생성
        self._create_static_tab(notebook)
        self._create_mssql_tab(notebook)
        self._create_sqlite_tab(notebook)

        # 하단 버전
        ttk.Label(self.root, text=f"Version {VERSION}", foreground="gray").pack(side=tk.BOTTOM, pady=5)

    # ============ Static Server 탭 ============
    def _create_static_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=15)
        notebook.add(tab, text="  Static Server  ")

        # 서버 상태
        server_frame = ttk.LabelFrame(tab, text="Static File Server", padding=10)
        server_frame.pack(fill=tk.X, pady=(0, 10))

        self.static_status_var = tk.StringVar(value="Stopped")
        self.static_status_label = ttk.Label(
            server_frame,
            textvariable=self.static_status_var,
            font=('Segoe UI', 12, 'bold'),
            foreground="red"
        )
        self.static_status_label.pack()

        # 포트
        port_frame = ttk.Frame(server_frame)
        port_frame.pack(pady=5)
        ttk.Label(port_frame, text="Port:").pack(side=tk.LEFT)
        self.static_port_var = tk.IntVar(value=self.config.get("static_port", 11111))
        ttk.Entry(port_frame, textvariable=self.static_port_var, width=8).pack(side=tk.LEFT, padx=5)

        # 버튼
        btn_frame = ttk.Frame(server_frame)
        btn_frame.pack(pady=5)
        self.static_start_btn = ttk.Button(btn_frame, text="Start", command=self._start_static)
        self.static_start_btn.pack(side=tk.LEFT, padx=3)
        self.static_stop_btn = ttk.Button(btn_frame, text="Stop", command=self._stop_static, state=tk.DISABLED)
        self.static_stop_btn.pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="Console", command=lambda: webbrowser.open(f"http://localhost:{self.static_port_var.get()}/console")).pack(side=tk.LEFT, padx=3)

        # WWW 폴더
        folder_frame = ttk.LabelFrame(tab, text="Project Folder", padding=10)
        folder_frame.pack(fill=tk.X, pady=(0, 10))

        self.www_folder_var = tk.StringVar(value=self.config.get("www_folder", ""))
        ttk.Label(folder_frame, text="Folder:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(folder_frame, textvariable=self.www_folder_var, width=40).grid(row=0, column=1, padx=5)
        ttk.Button(folder_frame, text="Browse...", command=self._browse_www_folder).grid(row=0, column=2)

        # 빌드 옵션
        build_frame = ttk.LabelFrame(tab, text="Build Options", padding=10)
        build_frame.pack(fill=tk.X, pady=(0, 10))

        build_btn_frame = ttk.Frame(build_frame)
        build_btn_frame.pack(fill=tk.X)
        ttk.Button(build_btn_frame, text="Git Pull", command=self._git_pull).pack(side=tk.LEFT, padx=3)
        ttk.Button(build_btn_frame, text="Install", command=self._bun_install).pack(side=tk.LEFT, padx=3)
        ttk.Button(build_btn_frame, text="Build", command=self._bun_build).pack(side=tk.LEFT, padx=3)

        self.auto_build_var = tk.BooleanVar(value=self.config.get("auto_build", False))
        ttk.Checkbutton(build_btn_frame, text="Auto Build on Change", variable=self.auto_build_var).pack(side=tk.LEFT, padx=20)

        # 도구 상태
        status_frame = ttk.Frame(build_frame)
        status_frame.pack(fill=tk.X, pady=(5, 0))

        bun_ok = git_build.bun_exists()
        git_ok = git_build.git_exists()
        ttk.Label(status_frame, text=f"Bun: {'Ready' if bun_ok else 'Not found'}", foreground="green" if bun_ok else "red").pack(side=tk.LEFT, padx=(0, 20))
        ttk.Label(status_frame, text=f"Git: {'Ready' if git_ok else 'Not found'}", foreground="green" if git_ok else "red").pack(side=tk.LEFT)

        # Webhook 설정
        webhook_frame = ttk.LabelFrame(tab, text="GitHub Webhook", padding=10)
        webhook_frame.pack(fill=tk.X, pady=(0, 10))

        self.webhook_enabled_var = tk.BooleanVar(value=self.config.get("webhook_enabled", False))
        ttk.Checkbutton(webhook_frame, text="Enable Webhook (auto build on push)", variable=self.webhook_enabled_var).grid(row=0, column=0, columnspan=3, sticky=tk.W, pady=3)

        ttk.Label(webhook_frame, text="Secret:").grid(row=1, column=0, sticky=tk.W, pady=3)
        self.webhook_secret_var = tk.StringVar(value=self.config.get("webhook_secret", ""))
        self.webhook_secret_entry = ttk.Entry(webhook_frame, textvariable=self.webhook_secret_var, width=30, show="*")
        self.webhook_secret_entry.grid(row=1, column=1, padx=5, pady=3)
        ttk.Button(webhook_frame, text="Show", command=self._toggle_webhook_secret).grid(row=1, column=2, padx=5)

        webhook_url_frame = ttk.Frame(webhook_frame)
        webhook_url_frame.grid(row=2, column=0, columnspan=3, sticky=tk.W, pady=3)
        ttk.Label(webhook_url_frame, text="URL:").pack(side=tk.LEFT)
        self.webhook_url_label = ttk.Label(webhook_url_frame, text=f"http://<server-ip>:{self.static_port_var.get()}/webhook", foreground="blue")
        self.webhook_url_label.pack(side=tk.LEFT, padx=5)

        # 옵션
        opt_frame = ttk.LabelFrame(tab, text="Options", padding=10)
        opt_frame.pack(fill=tk.X, pady=(0, 10))
        self.static_auto_start_var = tk.BooleanVar(value=self.config.get("static_auto_start", False))
        ttk.Checkbutton(opt_frame, text="Auto start Static server on startup", variable=self.static_auto_start_var).pack(anchor=tk.W)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.static_log = tk.Text(log_frame, height=6, font=('Consolas', 9), bg='#1e1e1e', fg='#f59e0b', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.static_log.yview)
        self.static_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.static_log.pack(fill=tk.BOTH, expand=True)

        ttk.Button(log_frame, text="Clear", command=lambda: self._clear_log(self.static_log)).pack(anchor=tk.E, pady=(5, 0))

        # 저장 버튼
        ttk.Button(tab, text="Save Settings", command=self._save_static_settings).pack(pady=5)

    # ============ MSSQL 탭 ============
    def _create_mssql_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=15)
        notebook.add(tab, text="  MSSQL (차트DB)  ")

        # 서버 상태
        server_frame = ttk.LabelFrame(tab, text="MSSQL API Server", padding=10)
        server_frame.pack(fill=tk.X, pady=(0, 10))

        self.mssql_status_var = tk.StringVar(value="Stopped")
        self.mssql_status_label = ttk.Label(
            server_frame,
            textvariable=self.mssql_status_var,
            font=('Segoe UI', 12, 'bold'),
            foreground="red"
        )
        self.mssql_status_label.pack()

        port_frame = ttk.Frame(server_frame)
        port_frame.pack(pady=5)
        ttk.Label(port_frame, text="API Port:").pack(side=tk.LEFT)
        self.mssql_port_var = tk.IntVar(value=self.config.get("mssql_api_port", 3100))
        ttk.Entry(port_frame, textvariable=self.mssql_port_var, width=8).pack(side=tk.LEFT, padx=5)

        btn_frame = ttk.Frame(server_frame)
        btn_frame.pack(pady=5)
        self.mssql_start_btn = ttk.Button(btn_frame, text="Start", command=self._start_mssql)
        self.mssql_start_btn.pack(side=tk.LEFT, padx=3)
        self.mssql_stop_btn = ttk.Button(btn_frame, text="Stop", command=self._stop_mssql, state=tk.DISABLED)
        self.mssql_stop_btn.pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="Console", command=lambda: webbrowser.open(f"http://localhost:{self.mssql_port_var.get()}")).pack(side=tk.LEFT, padx=3)

        # 연결 정보
        conn_frame = ttk.LabelFrame(tab, text="Connection Settings", padding=10)
        conn_frame.pack(fill=tk.X, pady=(0, 10))

        mssql_config = self.config.get('mssql', {})
        self.mssql_server_var = tk.StringVar(value=mssql_config.get('server', '192.168.0.173'))
        self.mssql_conn_port_var = tk.IntVar(value=mssql_config.get('port', 55555))
        self.mssql_user_var = tk.StringVar(value=mssql_config.get('user', 'members'))
        self.mssql_pass_var = tk.StringVar(value=mssql_config.get('password', 'msp1234'))
        self.mssql_db_var = tk.StringVar(value=mssql_config.get('database', 'MasterDB'))

        ttk.Label(conn_frame, text="Server:").grid(row=0, column=0, sticky=tk.W, pady=2)
        ttk.Entry(conn_frame, textvariable=self.mssql_server_var, width=20).grid(row=0, column=1, padx=5, pady=2)
        ttk.Label(conn_frame, text="DB Port:").grid(row=0, column=2, sticky=tk.W, pady=2)
        ttk.Entry(conn_frame, textvariable=self.mssql_conn_port_var, width=8).grid(row=0, column=3, padx=5, pady=2)

        ttk.Label(conn_frame, text="Database:").grid(row=1, column=0, sticky=tk.W, pady=2)
        ttk.Entry(conn_frame, textvariable=self.mssql_db_var, width=20).grid(row=1, column=1, padx=5, pady=2)

        ttk.Label(conn_frame, text="User:").grid(row=2, column=0, sticky=tk.W, pady=2)
        ttk.Entry(conn_frame, textvariable=self.mssql_user_var, width=15).grid(row=2, column=1, padx=5, pady=2, sticky=tk.W)
        ttk.Label(conn_frame, text="Password:").grid(row=2, column=2, sticky=tk.W, pady=2)
        ttk.Entry(conn_frame, textvariable=self.mssql_pass_var, width=12, show="*").grid(row=2, column=3, padx=5, pady=2)

        # 연결 테스트
        test_frame = ttk.LabelFrame(tab, text="Connection Test", padding=10)
        test_frame.pack(fill=tk.X, pady=(0, 10))

        self.mssql_test_var = tk.StringVar(value="Not tested")
        self.mssql_test_label = ttk.Label(test_frame, textvariable=self.mssql_test_var)
        self.mssql_test_label.pack()
        ttk.Button(test_frame, text="Test Connection", command=self._test_mssql).pack(pady=5)

        # 옵션
        opt_frame = ttk.LabelFrame(tab, text="Options", padding=10)
        opt_frame.pack(fill=tk.X, pady=(0, 10))
        self.mssql_auto_start_var = tk.BooleanVar(value=self.config.get("mssql_auto_start", False))
        ttk.Checkbutton(opt_frame, text="Auto start MSSQL server on startup", variable=self.mssql_auto_start_var).pack(anchor=tk.W)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.mssql_log = tk.Text(log_frame, height=6, font=('Consolas', 9), bg='#1e1e1e', fg='#00d9ff', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.mssql_log.yview)
        self.mssql_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.mssql_log.pack(fill=tk.BOTH, expand=True)

        ttk.Button(log_frame, text="Clear", command=lambda: self._clear_log(self.mssql_log)).pack(anchor=tk.E, pady=(5, 0))

        # 저장 버튼
        ttk.Button(tab, text="Save Settings", command=self._save_mssql_settings).pack(pady=5)

    # ============ SQLite 탭 ============
    def _create_sqlite_tab(self, notebook):
        tab = ttk.Frame(notebook, padding=15)
        notebook.add(tab, text="  SQLite  ")

        # 서버 상태
        server_frame = ttk.LabelFrame(tab, text="SQLite API Server", padding=10)
        server_frame.pack(fill=tk.X, pady=(0, 10))

        self.sqlite_status_var = tk.StringVar(value="Stopped")
        self.sqlite_status_label = ttk.Label(
            server_frame,
            textvariable=self.sqlite_status_var,
            font=('Segoe UI', 12, 'bold'),
            foreground="red"
        )
        self.sqlite_status_label.pack()

        port_frame = ttk.Frame(server_frame)
        port_frame.pack(pady=5)
        ttk.Label(port_frame, text="API Port:").pack(side=tk.LEFT)
        self.sqlite_port_var = tk.IntVar(value=self.config.get("sqlite_api_port", 3200))
        ttk.Entry(port_frame, textvariable=self.sqlite_port_var, width=8).pack(side=tk.LEFT, padx=5)

        btn_frame = ttk.Frame(server_frame)
        btn_frame.pack(pady=5)
        self.sqlite_start_btn = ttk.Button(btn_frame, text="Start", command=self._start_sqlite)
        self.sqlite_start_btn.pack(side=tk.LEFT, padx=3)
        self.sqlite_stop_btn = ttk.Button(btn_frame, text="Stop", command=self._stop_sqlite, state=tk.DISABLED)
        self.sqlite_stop_btn.pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="Console", command=lambda: webbrowser.open(f"http://localhost:{self.sqlite_port_var.get()}")).pack(side=tk.LEFT, padx=3)

        # 데이터베이스 파일
        db_frame = ttk.LabelFrame(tab, text="Database File", padding=10)
        db_frame.pack(fill=tk.X, pady=(0, 10))

        self.sqlite_path_var = tk.StringVar(value=self.config.get("sqlite_db_path", ""))
        ttk.Label(db_frame, text="DB Path:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(db_frame, textvariable=self.sqlite_path_var, width=38).grid(row=0, column=1, padx=5)
        ttk.Button(db_frame, text="Browse...", command=self._browse_sqlite_db).grid(row=0, column=2)

        btn_db_frame = ttk.Frame(db_frame)
        btn_db_frame.grid(row=1, column=0, columnspan=3, pady=5)
        ttk.Button(btn_db_frame, text="Create New DB", command=self._create_sqlite_db).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_db_frame, text="Run SQL File...", command=self._run_sql_file).pack(side=tk.LEFT, padx=5)

        # 백업 설정
        backup_frame = ttk.LabelFrame(tab, text="Backup Settings", padding=10)
        backup_frame.pack(fill=tk.X, pady=(0, 10))

        self.backup_enabled_var = tk.BooleanVar(value=self.config.get("backup_enabled", False))
        ttk.Checkbutton(backup_frame, text="Enable Auto Backup", variable=self.backup_enabled_var).grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=3)

        self.backup_folder_var = tk.StringVar(value=self.config.get("backup_folder", ""))
        ttk.Label(backup_frame, text="Backup Folder:").grid(row=1, column=0, sticky=tk.W, pady=3)
        ttk.Entry(backup_frame, textvariable=self.backup_folder_var, width=28).grid(row=1, column=1, padx=5, pady=3)
        ttk.Button(backup_frame, text="Browse...", command=self._browse_backup_folder).grid(row=1, column=2, padx=5)

        self.backup_interval_var = tk.IntVar(value=self.config.get("backup_interval_hours", 24))
        ttk.Label(backup_frame, text="Interval (hours):").grid(row=2, column=0, sticky=tk.W, pady=3)
        ttk.Entry(backup_frame, textvariable=self.backup_interval_var, width=8).grid(row=2, column=1, sticky=tk.W, padx=5, pady=3)

        ttk.Button(backup_frame, text="Backup Now", command=self._manual_backup).grid(row=3, column=0, columnspan=2, pady=5)

        # 옵션
        opt_frame = ttk.LabelFrame(tab, text="Options", padding=10)
        opt_frame.pack(fill=tk.X, pady=(0, 10))
        self.sqlite_auto_start_var = tk.BooleanVar(value=self.config.get("sqlite_auto_start", False))
        ttk.Checkbutton(opt_frame, text="Auto start SQLite server on startup", variable=self.sqlite_auto_start_var).pack(anchor=tk.W)

        # 로그
        log_frame = ttk.LabelFrame(tab, text="Log", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.sqlite_log = tk.Text(log_frame, height=6, font=('Consolas', 9), bg='#1e1e1e', fg='#10b981', state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.sqlite_log.yview)
        self.sqlite_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.sqlite_log.pack(fill=tk.BOTH, expand=True)

        ttk.Button(log_frame, text="Clear", command=lambda: self._clear_log(self.sqlite_log)).pack(anchor=tk.E, pady=(5, 0))

        # 저장 버튼
        ttk.Button(tab, text="Save Settings", command=self._save_sqlite_settings).pack(pady=5)

    # ============ 로그 콜백 설정 ============
    def _setup_log_callbacks(self):
        def make_log_callback(log_widget):
            def callback(msg):
                self.root.after(0, lambda: self._append_log(log_widget, msg))
            return callback

        git_build.log_callback = make_log_callback(self.static_log)
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
            git_build.log(f"서버 시작 (포트: {port})")
            self.static_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run, daemon=True).start()

        self.static_status_var.set(f"Running on port {port}")
        self.static_status_label.configure(foreground="green")
        self.static_start_btn.configure(state=tk.DISABLED)
        self.static_stop_btn.configure(state=tk.NORMAL)

        # Auto build
        if self.auto_build_var.get():
            git_build.start_file_watcher(www_folder)

    def _stop_static(self):
        self.static_running = False
        git_build.stop_file_watcher()
        git_build.log("서버 중지됨")
        self.static_status_var.set("Stopped (restart app)")
        self.static_status_label.configure(foreground="orange")
        self.static_start_btn.configure(state=tk.NORMAL)
        self.static_stop_btn.configure(state=tk.DISABLED)

    def _git_pull(self):
        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return

        def run():
            git_build.run_git_pull(www_folder)

        threading.Thread(target=run, daemon=True).start()

    def _bun_install(self):
        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return

        def run():
            git_build.run_bun_install(www_folder)

        threading.Thread(target=run, daemon=True).start()

    def _bun_build(self):
        www_folder = self.www_folder_var.get()
        if not www_folder:
            messagebox.showerror("Error", "WWW 폴더를 선택해주세요.")
            return

        def run():
            git_build.run_bun_build(www_folder)

        threading.Thread(target=run, daemon=True).start()

    def _toggle_webhook_secret(self):
        """Webhook secret 표시/숨김 토글"""
        current = self.webhook_secret_entry.cget('show')
        self.webhook_secret_entry.configure(show="" if current == "*" else "*")

    def _save_static_settings(self):
        self.config["static_port"] = self.static_port_var.get()
        self.config["www_folder"] = self.www_folder_var.get()
        self.config["auto_build"] = self.auto_build_var.get()
        self.config["static_auto_start"] = self.static_auto_start_var.get()
        self.config["webhook_enabled"] = self.webhook_enabled_var.get()
        self.config["webhook_secret"] = self.webhook_secret_var.get()
        save_config(self.config)
        messagebox.showinfo("Success", "Static settings saved")

    # ============ 서버 제어: MSSQL ============
    def _start_mssql(self):
        from flask import Flask
        from flask_cors import CORS
        from services.mssql_loader import load_mssql_routes

        # 설정 저장
        self.config["mssql"] = {
            "server": self.mssql_server_var.get(),
            "port": self.mssql_conn_port_var.get(),
            "user": self.mssql_user_var.get(),
            "password": self.mssql_pass_var.get(),
            "database": self.mssql_db_var.get()
        }
        save_config(self.config)

        port = self.mssql_port_var.get()

        # MSSQL Routes 로드 (외부 암호화 파일 우선)
        mssql_bp, routes_version, source = load_mssql_routes()
        self.mssql_routes_version = routes_version
        self.mssql_routes_source = source
        mssql_db.log(f"Routes 로드: v{routes_version} ({source})")

        self.mssql_app = Flask(__name__)
        CORS(self.mssql_app)
        self.mssql_app.register_blueprint(mssql_bp)

        def run():
            self.mssql_running = True
            mssql_db.log(f"서버 시작 (포트: {port}, API: v{routes_version})")
            self.mssql_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run, daemon=True).start()

        self.mssql_status_var.set(f"Running on port {port}")
        self.mssql_status_label.configure(foreground="green")
        self.mssql_start_btn.configure(state=tk.DISABLED)
        self.mssql_stop_btn.configure(state=tk.NORMAL)

    def _stop_mssql(self):
        self.mssql_running = False
        mssql_db.log("서버 중지됨")
        self.mssql_status_var.set("Stopped (restart app)")
        self.mssql_status_label.configure(foreground="orange")
        self.mssql_start_btn.configure(state=tk.NORMAL)
        self.mssql_stop_btn.configure(state=tk.DISABLED)

    def _test_mssql(self):
        self.mssql_test_var.set("Testing...")
        self.mssql_test_label.configure(foreground="gray")
        self.root.update()

        # 설정 저장 먼저
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
            self.mssql_test_var.set(f"Connected! ({result['count']:,} patients)")
            self.mssql_test_label.configure(foreground="green")
        else:
            self.mssql_test_var.set(f"Failed: {result['error'][:40]}")
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
            sqlite_db.log(f"서버 시작 (포트: {port})")
            sqlite_db.log(f"DB: {db_path}")
            self.sqlite_app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

        threading.Thread(target=run, daemon=True).start()

        self.sqlite_status_var.set(f"Running on port {port}")
        self.sqlite_status_label.configure(foreground="green")
        self.sqlite_start_btn.configure(state=tk.DISABLED)
        self.sqlite_stop_btn.configure(state=tk.NORMAL)

        # 백업 스케줄러 시작
        sqlite_db.start_backup_scheduler()

    def _stop_sqlite(self):
        self.sqlite_running = False
        sqlite_db.log("서버 중지됨")
        self.sqlite_status_var.set("Stopped (restart app)")
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
        """SQL 파일을 불러와서 순차적으로 실행"""
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

        # 파일 읽기 및 문장 수 확인
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
            statements = [s.strip() for s in content.split(';') if s.strip() and not s.strip().startswith('--')]
            stmt_count = len(statements)
        except Exception as e:
            messagebox.showerror("Error", f"파일 읽기 실패: {e}")
            return

        # 확인 대화상자
        if not messagebox.askyesno("Confirm", f"SQL 파일을 실행하시겠습니까?\n\n파일: {Path(sql_file).name}\n문장 수: {stmt_count}개\n대상 DB: {Path(db_path).name}"):
            return

        sqlite_db.log(f"SQL 실행 시작: {stmt_count}개 문장")

        # 동기 실행 (간단하고 안정적)
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            success_count = 0
            error_count = 0
            error_msgs = []

            for stmt in statements:
                if not stmt or stmt.startswith('--'):
                    continue
                try:
                    cursor.execute(stmt)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    if len(error_msgs) < 3:
                        error_msgs.append(str(e)[:50])

            conn.commit()
            conn.close()

            # 결과 로그
            sqlite_db.log(f"완료: 성공 {success_count}, 실패 {error_count}")
            if error_msgs:
                for msg in error_msgs:
                    sqlite_db.log(f"  - {msg}")

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

    # ============ 자동 시작 ============
    def _auto_start_servers(self):
        # Static 서버
        if self.static_auto_start_var.get() and self.www_folder_var.get():
            git_build.log("자동 시작 활성화됨")
            self._start_static()

        # MSSQL 서버
        if self.mssql_auto_start_var.get():
            mssql_db.log("자동 시작 활성화됨")
            self._start_mssql()

        # SQLite 서버
        if self.sqlite_auto_start_var.get():
            # main.sqlite가 같은 폴더에 있으면 자동으로 설정
            if DEFAULT_DB_FILE.exists() and not self.sqlite_path_var.get():
                self.sqlite_path_var.set(str(DEFAULT_DB_FILE))

            if self.sqlite_path_var.get():
                sqlite_db.log("자동 시작 활성화됨")
                self._start_sqlite()

        # 창 최소화 (트레이로)
        self.root.withdraw()

    def run(self):
        self.root.mainloop()


def run_gui():
    app = UnifiedServerGUI()
    app.run()
