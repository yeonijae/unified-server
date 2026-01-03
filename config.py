"""
설정 관리 모듈
- config.json 로드/저장
- Windows 시작프로그램 등록
"""

import os
import sys
import json
import winreg
from pathlib import Path

APP_VERSION = "3.1.6"  # HTTPS 별도 Flask 앱 분리
APP_NAME = "Haniwon Unified Server"

# 하위 호환성 (기존 코드에서 VERSION 사용 시)
VERSION = APP_VERSION

# 실행 파일 경로
if getattr(sys, 'frozen', False):
    APP_DIR = Path(sys.executable).parent
else:
    APP_DIR = Path(__file__).parent

CONFIG_FILE = APP_DIR / "config.json"
CONFIG_ENC_FILE = APP_DIR / "config.enc"
DEFAULT_DB_FILE = APP_DIR / "main.sqlite"

# 기본 설정
DEFAULT_CONFIG = {
    # Static File Server
    "static_port": 11111,
    "static_auto_start": False,
    "www_folder": "",
    "auto_build": False,
    "webhook_enabled": False,
    "webhook_secret": "",
    "repo_url": "",

    # MSSQL API
    "mssql_api_port": 3100,
    "mssql_auto_start": False,
    "mssql": {
        "server": "192.168.0.173",
        "user": "members",
        "password": "msp1234",
        "port": 55555,
        "database": "MasterDB"
    },

    # SQLite API
    "sqlite_api_port": 3200,
    "sqlite_auto_start": False,
    "sqlite_db_path": "",
    "backup_enabled": False,
    "backup_folder": "",
    "backup_interval_hours": 24,
    "last_backup": "",

    # Windows
    "start_with_windows": False,

    # File Upload
    "upload_folder": "C:/haniwon_data/uploads",
    "thumbnail_folder": "C:/haniwon_data/thumbnails",

    # AI API Keys
    "openai_api_key": "",
    "gemini_api_key": ""
}


def load_config():
    """설정 파일 로드 (config.enc 우선, config.json fallback)"""
    try:
        # 1. 암호화된 config.enc 우선 시도
        try:
            from services.secure_config import load_secure_config, is_encrypted_file
            if CONFIG_ENC_FILE.exists():
                config = load_secure_config(str(CONFIG_FILE))
            elif CONFIG_FILE.exists():
                # .json 파일이 암호화된 경우도 처리
                config = load_secure_config(str(CONFIG_FILE))
            else:
                config = DEFAULT_CONFIG.copy()
        except ImportError:
            # secure_config 모듈이 없으면 일반 로드
            if CONFIG_FILE.exists():
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            else:
                config = DEFAULT_CONFIG.copy()

        # 누락된 키 추가
        for key, value in DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = value
        # mssql 하위 키 확인
        if 'mssql' not in config:
            config['mssql'] = DEFAULT_CONFIG['mssql']
        else:
            for k, v in DEFAULT_CONFIG['mssql'].items():
                if k not in config['mssql']:
                    config['mssql'][k] = v
        return config
    except:
        pass
    return DEFAULT_CONFIG.copy()


def save_config(config, encrypt=True):
    """설정 파일 저장

    Args:
        config: 설정 딕셔너리
        encrypt: True면 config.enc로 암호화 저장
    """
    if encrypt:
        try:
            from services.secure_config import save_secure_config
            save_secure_config(config, str(CONFIG_FILE))
            return
        except ImportError:
            pass  # secure_config 모듈이 없으면 일반 저장

    # 일반 저장 (암호화 실패 시 fallback)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


# ============ Windows 시작프로그램 등록 ============

def get_startup_registry_key():
    return r"Software\Microsoft\Windows\CurrentVersion\Run"


def is_startup_enabled():
    """시작프로그램 등록 여부 확인"""
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, get_startup_registry_key(), 0, winreg.KEY_READ)
        try:
            winreg.QueryValueEx(key, APP_NAME)
            return True
        except WindowsError:
            return False
        finally:
            winreg.CloseKey(key)
    except:
        return False


def set_startup_enabled(enabled):
    """시작프로그램 등록/해제"""
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, get_startup_registry_key(), 0, winreg.KEY_SET_VALUE)
        if enabled:
            exe_path = sys.executable if getattr(sys, 'frozen', False) else f'"{sys.executable}" "{__file__}"'
            winreg.SetValueEx(key, APP_NAME, 0, winreg.REG_SZ, exe_path)
        else:
            try:
                winreg.DeleteValue(key, APP_NAME)
            except WindowsError:
                pass
        winreg.CloseKey(key)
        return True
    except Exception as e:
        print(f"Startup registry error: {e}")
        return False
