"""
MSSQL Routes 로더
- 외부 암호화 파일 또는 내장 모듈 로드
- 버전 관리
- 핫 리로드 지원 (서버 재시작 없이 모듈 교체)
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# 로그 콜백
log_callback = None

# 현재 로드된 모듈 및 Flask 앱 참조 (핫 리로드용)
_current_module = None
_current_version = None
_flask_app = None
_reload_callback = None  # GUI 콜백

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


def load_mssql_routes(use_builtin=False):
    """MSSQL Routes 로드 (외부 파일 우선, 옵션으로 내장 모듈 사용)

    Args:
        use_builtin: True면 내장 모듈 강제 사용

    Returns:
        tuple: (mssql_bp, version, source)
    """
    # 1. 내장 모듈 강제 사용 옵션
    if use_builtin:
        try:
            from routes.mssql_routes import mssql_bp, MODULE_VERSION
            log(f"내장 모듈 사용 (강제): v{MODULE_VERSION}")
            return mssql_bp, MODULE_VERSION, "builtin"
        except Exception as e:
            log(f"내장 모듈 로드 실패: {e}")
            raise RuntimeError("내장 MSSQL Routes 모듈을 로드할 수 없습니다")

    # 2. 외부 파일 우선
    ext_path, file_type = get_external_routes_path()

    # 외부 암호화 파일
    if file_type == "encrypted":
        try:
            from services.crypto_loader import decrypt_and_load
            module, version = decrypt_and_load(str(ext_path), "mssql_routes_ext")
            log(f"외부 암호화 모듈 로드: v{version}")
            return module.mssql_bp, version, "external_encrypted"
        except Exception as e:
            log(f"암호화 모듈 로드 실패: {e}, 내장 모듈로 fallback...")

    # 외부 일반 파일 (개발용)
    elif file_type == "plain":
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("mssql_routes_ext", ext_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            version = getattr(module, 'MODULE_VERSION', 'dev')
            log(f"외부 모듈 로드: v{version}")
            return module.mssql_bp, version, "external_plain"
        except Exception as e:
            log(f"외부 모듈 로드 실패: {e}, 내장 모듈로 fallback...")

    # 3. 내장 모듈 fallback
    try:
        from routes.mssql_routes import mssql_bp, MODULE_VERSION
        log(f"내장 모듈 사용 (fallback): v{MODULE_VERSION}")
        return mssql_bp, MODULE_VERSION, "builtin"
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


# ============ 핫 리로드 기능 ============

def set_flask_app(app, reload_callback=None):
    """Flask 앱 참조 설정 (핫 리로드용)"""
    global _flask_app, _reload_callback
    _flask_app = app
    _reload_callback = reload_callback


def hot_reload_routes():
    """서버 재시작 없이 mssql_routes 모듈 핫 리로드

    Returns:
        tuple: (success, new_version, message)
    """
    global _current_module, _current_version, _flask_app

    if _flask_app is None:
        log("Flask 앱이 설정되지 않음")
        return False, None, "Flask app not set"

    try:
        # 1. 새 모듈 로드
        ext_path, file_type = get_external_routes_path()

        if file_type != "encrypted":
            log("암호화된 enc 파일을 찾을 수 없음")
            return False, None, "No encrypted file found"

        from services.crypto_loader import decrypt_and_load

        # 고유한 모듈 이름으로 로드 (캐시 우회)
        import time
        module_name = f"mssql_routes_hot_{int(time.time())}"
        new_module, new_version = decrypt_and_load(str(ext_path), module_name)

        log(f"새 모듈 로드 완료: v{new_version}")

        # 2. 기존 Blueprint 제거
        old_bp_name = 'mssql'
        if old_bp_name in _flask_app.blueprints:
            # Flask는 Blueprint 제거를 직접 지원하지 않으므로
            # url_map에서 해당 규칙들을 제거
            rules_to_remove = []
            for rule in _flask_app.url_map.iter_rules():
                if rule.endpoint.startswith(f"{old_bp_name}."):
                    rules_to_remove.append(rule)

            for rule in rules_to_remove:
                _flask_app.url_map._rules.remove(rule)
                if rule.endpoint in _flask_app.url_map._rules_by_endpoint:
                    del _flask_app.url_map._rules_by_endpoint[rule.endpoint]

            # blueprints에서도 제거
            del _flask_app.blueprints[old_bp_name]

            # view_functions에서 제거
            endpoints_to_remove = [ep for ep in _flask_app.view_functions
                                   if ep.startswith(f"{old_bp_name}.")]
            for ep in endpoints_to_remove:
                del _flask_app.view_functions[ep]

            log("기존 Blueprint 제거 완료")

        # 3. 새 Blueprint 등록 (Flask의 _got_first_request 체크 우회)
        # Flask 2.x 이상에서는 첫 요청 후 blueprint 등록이 막혀있음
        # 임시로 플래그를 해제하고 등록 후 복원
        had_first_request = getattr(_flask_app, '_got_first_request', False)
        if had_first_request:
            _flask_app._got_first_request = False

        try:
            _flask_app.register_blueprint(new_module.mssql_bp)
        finally:
            if had_first_request:
                _flask_app._got_first_request = True

        # 4. url_map 내부 캐시 무효화 (중요!)
        # Werkzeug Map 객체의 _rules_lock과 _remap 플래그를 통해 캐시 갱신
        if hasattr(_flask_app.url_map, '_remap'):
            _flask_app.url_map._remap = True
        if hasattr(_flask_app.url_map, '_remap_lock'):
            with _flask_app.url_map._remap_lock:
                _flask_app.url_map._remap = True

        log(f"새 Blueprint 등록 완료: v{new_version}")

        # 4. 상태 업데이트
        _current_module = new_module
        _current_version = new_version

        # 5. GUI 콜백 호출
        if _reload_callback:
            _reload_callback(new_version)

        return True, new_version, f"Hot reload successful: v{new_version}"

    except Exception as e:
        log(f"핫 리로드 실패: {e}")
        import traceback
        traceback.print_exc()
        return False, None, f"Hot reload failed: {e}"


def get_current_version():
    """현재 로드된 모듈 버전"""
    return _current_version
