"""
MSSQL Routes 로더
- 외부 암호화 파일 또는 내장 모듈 로드
- 버전 관리
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# 로그 콜백
log_callback = None

def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_msg = f"[{timestamp}] [MSSQLLoader] {message}"
    print(log_msg)
    if log_callback:
        log_callback(log_msg)


def get_external_routes_path():
    """외부 mssql_routes 파일 경로 (암호화/비암호화)"""
    # exe와 같은 폴더에서 찾기
    if getattr(sys, 'frozen', False):
        base_dir = Path(sys.executable).parent
    else:
        base_dir = Path(__file__).parent.parent

    # 우선순위: .enc > .py
    enc_path = base_dir / "mssql_routes.enc"
    py_path = base_dir / "mssql_routes.py"

    if enc_path.exists():
        return enc_path, "encrypted"
    elif py_path.exists():
        return py_path, "plain"
    else:
        return None, None


def load_mssql_routes():
    """MSSQL Routes 로드 (외부 파일 우선, 없으면 내장)

    Returns:
        tuple: (mssql_bp, version, source)
    """
    ext_path, file_type = get_external_routes_path()

    # 외부 암호화 파일
    if file_type == "encrypted":
        try:
            from services.crypto_loader import decrypt_and_load
            module, version = decrypt_and_load(str(ext_path), "mssql_routes_ext")
            log(f"외부 암호화 모듈 로드: v{version}")
            return module.mssql_bp, version, "external_encrypted"
        except Exception as e:
            log(f"암호화 모듈 로드 실패: {e}")
            # fallback to built-in

    # 외부 일반 파일 (개발용)
    elif file_type == "plain":
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("mssql_routes_ext", ext_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # 버전 추출 (파일 내 VERSION 변수 또는 기본값)
            version = getattr(module, 'MODULE_VERSION', 'dev')
            log(f"외부 모듈 로드: v{version}")
            return module.mssql_bp, version, "external_plain"
        except Exception as e:
            log(f"외부 모듈 로드 실패: {e}")
            # fallback to built-in

    # 내장 모듈
    try:
        from routes.mssql_routes import mssql_bp
        from config import VERSION
        log(f"내장 모듈 사용: v{VERSION}")
        return mssql_bp, VERSION, "builtin"
    except Exception as e:
        log(f"내장 모듈 로드 실패: {e}")
        raise RuntimeError("MSSQL Routes 모듈을 로드할 수 없습니다")


def get_routes_info():
    """현재 로드 가능한 routes 정보"""
    ext_path, file_type = get_external_routes_path()

    info = {
        "external_path": str(ext_path) if ext_path else None,
        "external_type": file_type,
        "external_exists": ext_path is not None,
    }

    if file_type == "encrypted":
        try:
            from services.crypto_loader import get_module_version
            info["external_version"] = get_module_version(str(ext_path))
        except:
            info["external_version"] = "unknown"
    elif file_type == "plain":
        info["external_version"] = "dev"

    return info
