"""
Haniwon Unified Server

통합 서버:
- Static File Server (React/Vite) - 포트 11111
- MSSQL API (차트프로그램 DB) - 포트 3100
- PostgreSQL API - 포트 3200

Features:
- Windows 시작프로그램 등록
- 서버별 Auto Start
- 시스템 트레이 아이콘
- 로그 창
"""

from gui import run_gui

if __name__ == "__main__":
    run_gui()
