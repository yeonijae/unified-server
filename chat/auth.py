"""
세션 기반 인증 모듈
- 로그인 시 세션 토큰 발급
- DB에서 세션 검증
"""

import hashlib
import secrets
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify, g

# DB 연결은 server.py에서 주입
_db = None


def init_auth(db_module):
    """DB 모듈 초기화"""
    global _db
    _db = db_module


def hash_password(password: str) -> str:
    """비밀번호 해시 (SHA256)"""
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    """비밀번호 검증"""
    return hash_password(password) == hashed


def generate_session_token() -> str:
    """세션 토큰 생성 (64자 hex)"""
    return secrets.token_hex(32)


def create_session(user_id: str, expires_hours: int = 24 * 7) -> str:
    """세션 생성 및 DB 저장"""
    token = generate_session_token()
    expires_at = datetime.now() + timedelta(hours=expires_hours)

    _db.execute(
        """
        INSERT INTO chat_sessions (user_id, token, expires_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (token) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            expires_at = EXCLUDED.expires_at,
            last_active_at = NOW()
        """,
        (user_id, token, expires_at)
    )
    return token


def validate_session(token: str) -> dict | None:
    """세션 검증 및 사용자 정보 반환"""
    if not token:
        return None

    result = _db.query_one(
        """
        SELECT s.user_id, s.expires_at, u.display_name, u.avatar_url, u.avatar_color
        FROM chat_sessions s
        JOIN chat_users u ON s.user_id = u.id
        WHERE s.token = %s AND s.expires_at > NOW()
        """,
        (token,)
    )

    if result:
        # 마지막 활동 시간 업데이트
        _db.execute(
            "UPDATE chat_sessions SET last_active_at = NOW() WHERE token = %s",
            (token,)
        )
        return {
            'id': str(result['user_id']),
            'display_name': result['display_name'],
            'avatar_url': result.get('avatar_url'),
            'avatar_color': result.get('avatar_color')
        }
    return None


def delete_session(token: str) -> bool:
    """세션 삭제 (로그아웃)"""
    _db.execute("DELETE FROM chat_sessions WHERE token = %s", (token,))
    return True


def cleanup_expired_sessions():
    """만료된 세션 정리"""
    _db.execute("DELETE FROM chat_sessions WHERE expires_at < NOW()")


def get_token_from_request() -> str | None:
    """요청에서 토큰 추출"""
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        return auth_header[7:]
    return request.args.get('token')


def require_auth(f):
    """인증 필수 데코레이터"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_from_request()
        user = validate_session(token)

        if not user:
            return jsonify({'error': 'Unauthorized', 'message': '로그인이 필요합니다'}), 401

        g.current_user = user
        return f(*args, **kwargs)
    return decorated


def get_current_user() -> dict | None:
    """현재 로그인된 사용자 반환"""
    return getattr(g, 'current_user', None)
