"""
채팅 REST API 라우트
"""

from flask import Blueprint, request, jsonify, g
from . import db
from .auth import (
    require_auth, get_current_user, hash_password, verify_password,
    create_session, delete_session, get_token_from_request, validate_session
)
import bleach

chat_bp = Blueprint('chat', __name__, url_prefix='/api/v1')


# 전역 CORS 핸들러 - OPTIONS 요청 처리
@chat_bp.before_request
def handle_preflight():
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'ok'})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response.headers['Access-Control-Max-Age'] = '86400'
        return response


# 모든 응답에 CORS 헤더 추가
@chat_bp.after_request
def after_request_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


def cors_preflight():
    response = jsonify({'status': 'ok'})
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


def json_response(data, status=200):
    response = jsonify(data)
    response.status_code = status
    return response  # after_request_cors가 자동으로 CORS 헤더 추가


# HTML Sanitization
ALLOWED_TAGS = ['p', 'br', 'strong', 'em', 'u', 'a', 'code', 'pre', 'ul', 'ol', 'li']
ALLOWED_ATTRS = {'a': ['href', 'target', 'rel']}


def sanitize_html(content: str) -> str:
    return bleach.clean(content, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS)


# ============ Auth Routes ============

@chat_bp.route('/auth/register', methods=['POST', 'OPTIONS'])
def register():
    """회원가입 (deprecated - 포털 로그인 사용)"""
    if request.method == 'OPTIONS':
        return cors_preflight()
    return json_response({'error': '이 기능은 더 이상 사용되지 않습니다. 포털 로그인을 사용해주세요.'}, 410)


@chat_bp.route('/auth/login', methods=['POST', 'OPTIONS'])
def login():
    """로그인 (deprecated - 포털 로그인 사용)"""
    if request.method == 'OPTIONS':
        return cors_preflight()
    return json_response({'error': '이 기능은 더 이상 사용되지 않습니다. 포털 로그인을 사용해주세요.'}, 410)


@chat_bp.route('/auth/portal-login', methods=['POST', 'OPTIONS'])
def portal_login():
    """포털 세션 토큰을 이용한 채팅 로그인"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    data = request.get_json()
    portal_session_token = str(data.get('portalSessionToken', '')).strip()

    if not portal_session_token:
        return json_response({'error': '포털 세션 토큰이 필요합니다'}, 400)

    # 포털 세션 검증 (portal_sessions 테이블에서 확인)
    portal_session = db.query_one(
        """
        SELECT ps.user_id, ps.expires_at, pu.login_id, pu.name, pu.role
        FROM portal_sessions ps
        JOIN portal_users pu ON ps.user_id = pu.id
        WHERE ps.session_token = %s AND pu.is_active = 1
        """,
        (portal_session_token,)
    )

    if not portal_session:
        return json_response({'error': '유효하지 않은 포털 세션입니다'}, 401)

    # 세션 만료 확인
    from datetime import datetime
    expires_at = portal_session['expires_at']
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
    if expires_at < datetime.now(expires_at.tzinfo if expires_at.tzinfo else None):
        return json_response({'error': '포털 세션이 만료되었습니다'}, 401)

    portal_user_id = str(portal_session['user_id'])
    display_name = portal_session['name'] or portal_session['login_id']
    portal_role = portal_session.get('role', '')

    # UPSERT: 없으면 생성, 있으면 이름 업데이트
    user = db.query_one(
        """
        INSERT INTO chat_users (portal_user_id, display_name)
        VALUES (%s, %s)
        ON CONFLICT (portal_user_id) DO UPDATE SET
            display_name = COALESCE(NULLIF(%s, ''), chat_users.display_name),
            updated_at = NOW()
        RETURNING id, portal_user_id, display_name, avatar_url, avatar_color
        """,
        (portal_user_id, display_name, display_name)
    )

    # 채팅 세션 생성
    token = create_session(str(user['id']))

    return json_response({
        'data': {
            'user': {
                'id': str(user['id']),
                'portalUserId': user['portal_user_id'],
                'display_name': user['display_name'],
                'avatar_url': user.get('avatar_url'),
                'avatar_color': user.get('avatar_color'),
                'is_admin': portal_role == 'super_admin'
            },
            'accessToken': token
        }
    })


@chat_bp.route('/auth/logout', methods=['POST', 'OPTIONS'])
@require_auth
def logout():
    """로그아웃"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    token = get_token_from_request()
    delete_session(token)
    return json_response({'message': '로그아웃되었습니다'})


@chat_bp.route('/auth/me', methods=['GET', 'OPTIONS'])
@require_auth
def get_me():
    """현재 사용자 정보"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    return json_response({
        'data': {
            'id': user['id'],
            'display_name': user['display_name'],
            'avatar_url': user.get('avatar_url'),
            'avatar_color': user.get('avatar_color')
        }
    })


# ============ Channel Routes ============

@chat_bp.route('/channels', methods=['GET', 'OPTIONS'])
@require_auth
def get_channels():
    """채널 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()

        channels = db.query(
            """
            SELECT c.*, cm.role, cm.last_read_message_id,
                   (SELECT COUNT(*) FROM chat_messages m
                    WHERE m.channel_id = c.id
                    AND m.created_at > COALESCE(cm.last_read_at, '1970-01-01')
                    AND m.sender_id != %s
                    AND m.deleted_at IS NULL) as unread_count
            FROM chat_channels c
            JOIN chat_channel_members cm ON c.id = cm.channel_id
            WHERE cm.user_id = %s AND c.deleted_at IS NULL
            ORDER BY c.created_at DESC
            """,
            (user['id'], user['id'])
        )
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)

    return json_response({
        'data': [{
            'id': str(ch['id']),
            'name': ch['name'],
            'type': ch['type'],
            'avatar_url': ch.get('avatar_url'),
            'last_message_at': ch['updated_at'].isoformat() if ch.get('updated_at') else None,
            'is_pinned': False,  # TODO: implement pinning
            'unread_count': ch['unread_count'] or 0,
            'last_read_message_id': str(ch['last_read_message_id']) if ch.get('last_read_message_id') else None
        } for ch in channels]
    })


@chat_bp.route('/channels', methods=['POST', 'OPTIONS'])
@require_auth
def create_channel():
    """채널 생성"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()

        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        channel_type = data.get('type', 'group')
        is_private = data.get('isPrivate', False)
        member_ids = data.get('memberIds', [])

        if not name:
            return json_response({'error': '채널 이름을 입력해주세요'}, 400)

        # 채널 생성
        channel = db.query_one(
            """
            INSERT INTO chat_channels (name, description, type, is_private, created_by)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING *
            """,
            (name, description, channel_type, is_private, user['id'])
        )

        # 생성자를 admin으로 추가
        db.execute(
            "INSERT INTO chat_channel_members (channel_id, user_id, role) VALUES (%s, %s, 'admin')",
            (channel['id'], user['id'])
        )

        # 멤버 추가
        for member_id in member_ids:
            if member_id != user['id']:
                db.execute(
                    "INSERT INTO chat_channel_members (channel_id, user_id, role) VALUES (%s, %s, 'member') ON CONFLICT DO NOTHING",
                    (channel['id'], member_id)
                )

        return json_response({
            'data': {
                'id': str(channel['id']),
                'name': channel['name'],
                'type': channel['type'],
                'avatar_url': None,
                'last_message_at': None,
                'is_pinned': False,
                'unread_count': 0,
                'last_read_message_id': None
            }
        }, 201)
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/channels/<channel_id>', methods=['GET', 'OPTIONS'])
@require_auth
def get_channel(channel_id):
    """채널 상세 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()

        channel = db.query_one(
            """
            SELECT c.*, cm.role, cm.last_read_message_id,
                   (SELECT COUNT(*) FROM chat_messages m
                    WHERE m.channel_id = c.id
                    AND m.created_at > COALESCE(cm.last_read_at, '1970-01-01')
                    AND m.sender_id != %s
                    AND m.deleted_at IS NULL) as unread_count
            FROM chat_channels c
            JOIN chat_channel_members cm ON c.id = cm.channel_id
            WHERE c.id = %s AND cm.user_id = %s AND c.deleted_at IS NULL
            """,
            (user['id'], channel_id, user['id'])
        )

        if not channel:
            return json_response({'error': '채널을 찾을 수 없습니다'}, 404)

        # 멤버 목록
        members = db.query(
            """
            SELECT u.id, u.display_name, u.avatar_url, u.avatar_color, cm.role
            FROM chat_channel_members cm
            JOIN chat_users u ON cm.user_id = u.id
            WHERE cm.channel_id = %s
            """,
            (channel_id,)
        )

        return json_response({
            'data': {
                'id': str(channel['id']),
                'name': channel['name'],
                'type': channel['type'],
                'avatar_url': channel.get('avatar_url'),
                'last_message_at': channel['updated_at'].isoformat() if channel.get('updated_at') else None,
                'is_pinned': False,
                'unread_count': channel['unread_count'] or 0,
                'last_read_message_id': str(channel['last_read_message_id']) if channel.get('last_read_message_id') else None,
                'members': [{
                    'id': str(m['id']),
                    'display_name': m['display_name'],
                    'avatar_url': m.get('avatar_url'),
                    'avatar_color': m.get('avatar_color'),
                    'role': m['role']
                } for m in members]
            }
        })
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/channels/<channel_id>', methods=['PATCH', 'PUT', 'OPTIONS'])
@require_auth
def update_channel(channel_id):
    """채널 수정 (이름 변경 등)"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()

        # 채널 멤버인지 확인
        member = db.query_one(
            "SELECT role FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
            (channel_id, user['id'])
        )
        if not member:
            return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

        # 업데이트할 필드 수집
        updates = []
        params = []

        if 'name' in data:
            updates.append("name = %s")
            params.append(data['name'])
        if 'description' in data:
            updates.append("description = %s")
            params.append(data['description'])

        if not updates:
            return json_response({'error': '수정할 내용이 없습니다'}, 400)

        params.append(channel_id)

        channel = db.query_one(
            f"""
            UPDATE chat_channels
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE id = %s AND deleted_at IS NULL
            RETURNING *
            """,
            tuple(params)
        )

        if not channel:
            return json_response({'error': '채널을 찾을 수 없습니다'}, 404)

        return json_response({
            'data': {
                'id': str(channel['id']),
                'name': channel['name'],
                'type': channel['type'],
                'avatar_url': channel.get('avatar_url'),
                'last_message_at': channel['updated_at'].isoformat() if channel.get('updated_at') else None,
                'is_pinned': False,
                'unread_count': 0,
                'last_read_message_id': None
            }
        })
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/channels/<channel_id>', methods=['DELETE'])
@require_auth
def delete_channel(channel_id):
    """채널 삭제"""
    try:
        user = get_current_user()

        # admin 권한 확인
        member = db.query_one(
            "SELECT role FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
            (channel_id, user['id'])
        )
        if not member or member['role'] != 'admin':
            return json_response({'error': '채널 삭제 권한이 없습니다'}, 403)

        # 소프트 삭제
        result = db.query_one(
            "UPDATE chat_channels SET deleted_at = NOW() WHERE id = %s RETURNING id",
            (channel_id,)
        )

        if not result:
            return json_response({'error': '채널을 찾을 수 없습니다'}, 404)

        return json_response({'message': '채널이 삭제되었습니다'})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


# ============ Message Routes ============

@chat_bp.route('/channels/<channel_id>/messages', methods=['GET', 'OPTIONS'])
@require_auth
def get_messages(channel_id):
    """메시지 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    before = request.args.get('before')
    around_message_id = request.args.get('around_message_id')
    limit = min(int(request.args.get('limit', 50)), 100)

    # 채널 멤버 확인
    is_member = db.query_one(
        "SELECT 1 FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
        (channel_id, user['id'])
    )
    if not is_member:
        return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

    # 메시지 조회 (reactions 포함)
    current_user_id = user['id']

    if around_message_id:
        # 특정 메시지 주변의 메시지 조회
        half_limit = limit // 2
        messages = db.query(
            """
            WITH target AS (
                SELECT created_at FROM chat_messages WHERE id = %s
            ),
            before_msgs AS (
                SELECT m.*,
                       json_build_object('id', u.id, 'displayName', u.display_name,
                                         'avatarUrl', u.avatar_url, 'avatarColor', u.avatar_color) as sender,
                       COALESCE(
                           (SELECT json_agg(json_build_object(
                               'emoji', r.emoji,
                               'count', r.cnt,
                               'has_reacted', r.has_reacted
                           ))
                           FROM (
                               SELECT emoji,
                                      COUNT(*) as cnt,
                                      bool_or(user_id = %s) as has_reacted
                               FROM chat_message_reactions
                               WHERE message_id = m.id
                               GROUP BY emoji
                           ) r),
                           '[]'::json
                       ) as reactions
                FROM chat_messages m
                LEFT JOIN chat_users u ON m.sender_id = u.id
                WHERE m.channel_id = %s AND m.deleted_at IS NULL AND m.parent_id IS NULL
                AND m.created_at < (SELECT created_at FROM target)
                ORDER BY m.created_at DESC
                LIMIT %s
            ),
            after_msgs AS (
                SELECT m.*,
                       json_build_object('id', u.id, 'displayName', u.display_name,
                                         'avatarUrl', u.avatar_url, 'avatarColor', u.avatar_color) as sender,
                       COALESCE(
                           (SELECT json_agg(json_build_object(
                               'emoji', r.emoji,
                               'count', r.cnt,
                               'has_reacted', r.has_reacted
                           ))
                           FROM (
                               SELECT emoji,
                                      COUNT(*) as cnt,
                                      bool_or(user_id = %s) as has_reacted
                               FROM chat_message_reactions
                               WHERE message_id = m.id
                               GROUP BY emoji
                           ) r),
                           '[]'::json
                       ) as reactions
                FROM chat_messages m
                LEFT JOIN chat_users u ON m.sender_id = u.id
                WHERE m.channel_id = %s AND m.deleted_at IS NULL AND m.parent_id IS NULL
                AND m.created_at >= (SELECT created_at FROM target)
                ORDER BY m.created_at ASC
                LIMIT %s
            )
            SELECT * FROM before_msgs
            UNION ALL
            SELECT * FROM after_msgs
            ORDER BY created_at ASC
            """,
            (around_message_id, current_user_id, channel_id, half_limit, current_user_id, channel_id, half_limit + 1)
        )
    elif before:
        messages = db.query(
            """
            SELECT m.*,
                   json_build_object('id', u.id, 'displayName', u.display_name,
                                     'avatarUrl', u.avatar_url, 'avatarColor', u.avatar_color) as sender,
                   COALESCE(
                       (SELECT json_agg(json_build_object(
                           'emoji', r.emoji,
                           'count', r.cnt,
                           'has_reacted', r.has_reacted
                       ))
                       FROM (
                           SELECT emoji,
                                  COUNT(*) as cnt,
                                  bool_or(user_id = %s) as has_reacted
                           FROM chat_message_reactions
                           WHERE message_id = m.id
                           GROUP BY emoji
                       ) r),
                       '[]'::json
                   ) as reactions
            FROM chat_messages m
            LEFT JOIN chat_users u ON m.sender_id = u.id
            WHERE m.channel_id = %s AND m.deleted_at IS NULL AND m.parent_id IS NULL
            AND m.created_at < (SELECT created_at FROM chat_messages WHERE id = %s)
            ORDER BY m.created_at DESC
            LIMIT %s
            """,
            (current_user_id, channel_id, before, limit)
        )
    else:
        messages = db.query(
            """
            SELECT m.*,
                   json_build_object('id', u.id, 'displayName', u.display_name,
                                     'avatarUrl', u.avatar_url, 'avatarColor', u.avatar_color) as sender,
                   COALESCE(
                       (SELECT json_agg(json_build_object(
                           'emoji', r.emoji,
                           'count', r.cnt,
                           'has_reacted', r.has_reacted
                       ))
                       FROM (
                           SELECT emoji,
                                  COUNT(*) as cnt,
                                  bool_or(user_id = %s) as has_reacted
                           FROM chat_message_reactions
                           WHERE message_id = m.id
                           GROUP BY emoji
                       ) r),
                       '[]'::json
                   ) as reactions
            FROM chat_messages m
            LEFT JOIN chat_users u ON m.sender_id = u.id
            WHERE m.channel_id = %s AND m.deleted_at IS NULL AND m.parent_id IS NULL
            ORDER BY m.created_at DESC
            LIMIT %s
            """,
            (current_user_id, channel_id, limit)
        )

    # 역순으로 정렬 (오래된 것부터) - around_message_id 사용 시 이미 정렬됨
    if not around_message_id:
        messages = list(reversed(messages))

    # snake_case로 응답 (클라이언트 호환)
    def convert_sender(sender):
        if not sender:
            return None
        return {
            'id': sender.get('id'),
            'display_name': sender.get('displayName') or sender.get('display_name'),
            'avatar_url': sender.get('avatarUrl') or sender.get('avatar_url'),
            'avatar_color': sender.get('avatarColor') or sender.get('avatar_color')
        }

    return json_response({
        'data': [{
            'id': str(m['id']),
            'channel_id': str(m['channel_id']),
            'content': m['content'],
            'type': m['type'],
            'sender': convert_sender(m['sender']),
            'thread_count': m.get('thread_count', 0),
            'is_edited': m.get('is_edited', False),
            'is_pinned': m.get('is_pinned', False),
            'reactions': m.get('reactions', []),
            'created_at': m['created_at'].isoformat() if m.get('created_at') else None,
            'updated_at': m['updated_at'].isoformat() if m.get('updated_at') else None
        } for m in messages]
    })


@chat_bp.route('/messages/<message_id>/thread', methods=['GET', 'OPTIONS'])
@require_auth
def get_thread_messages(message_id):
    """스레드 메시지 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    current_user_id = user['id']

    # 부모 메시지 존재 확인 및 채널 ID 가져오기
    parent = db.query_one(
        "SELECT channel_id FROM chat_messages WHERE id = %s AND deleted_at IS NULL",
        (message_id,)
    )
    if not parent:
        return json_response({'error': '메시지를 찾을 수 없습니다'}, 404)

    channel_id = parent['channel_id']

    # 채널 멤버 확인
    is_member = db.query_one(
        "SELECT 1 FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
        (channel_id, user['id'])
    )
    if not is_member:
        return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

    # 스레드 메시지 조회 (reactions 포함)
    messages = db.query(
        """
        SELECT m.*,
               json_build_object('id', u.id, 'displayName', u.display_name,
                                 'avatarUrl', u.avatar_url, 'avatarColor', u.avatar_color) as sender,
               COALESCE(
                   (SELECT json_agg(json_build_object(
                       'emoji', r.emoji,
                       'count', r.cnt,
                       'has_reacted', r.has_reacted
                   ))
                   FROM (
                       SELECT emoji,
                              COUNT(*) as cnt,
                              bool_or(user_id = %s) as has_reacted
                       FROM chat_message_reactions
                       WHERE message_id = m.id
                       GROUP BY emoji
                   ) r),
                   '[]'::json
               ) as reactions
        FROM chat_messages m
        LEFT JOIN chat_users u ON m.sender_id = u.id
        WHERE m.parent_id = %s AND m.deleted_at IS NULL
        ORDER BY m.created_at ASC
        """,
        (current_user_id, message_id)
    )

    def convert_sender(sender):
        if not sender:
            return None
        return {
            'id': sender.get('id'),
            'display_name': sender.get('displayName') or sender.get('display_name'),
            'avatar_url': sender.get('avatarUrl') or sender.get('avatar_url'),
            'avatar_color': sender.get('avatarColor') or sender.get('avatar_color')
        }

    return json_response({
        'data': [{
            'id': str(m['id']),
            'channel_id': str(m['channel_id']),
            'parent_id': str(m['parent_id']) if m.get('parent_id') else None,
            'content': m['content'],
            'type': m['type'],
            'sender': convert_sender(m['sender']),
            'thread_count': m.get('thread_count', 0),
            'is_edited': m.get('is_edited', False),
            'reactions': m.get('reactions', []),
            'created_at': m['created_at'].isoformat() if m.get('created_at') else None,
            'updated_at': m['updated_at'].isoformat() if m.get('updated_at') else None
        } for m in messages]
    })


@chat_bp.route('/messages', methods=['POST', 'OPTIONS'])
@require_auth
def create_message():
    """메시지 전송"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    data = request.get_json()

    channel_id = data.get('channel_id') or data.get('channelId')
    content = data.get('content', '').strip()
    msg_type = data.get('type', 'text')
    parent_id = data.get('parent_id') or data.get('parentId')

    if not channel_id or not content:
        return json_response({'error': '채널 ID와 내용을 입력해주세요'}, 400)

    # 채널 멤버 확인
    is_member = db.query_one(
        "SELECT 1 FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
        (channel_id, user['id'])
    )
    if not is_member:
        return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

    # HTML Sanitize
    sanitized_content = sanitize_html(content)

    # 메시지 생성
    message = db.query_one(
        """
        INSERT INTO chat_messages (channel_id, sender_id, content, type, parent_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING *
        """,
        (channel_id, user['id'], sanitized_content, msg_type, parent_id)
    )

    # 스레드 카운트 증가
    if parent_id:
        db.execute(
            "UPDATE chat_messages SET thread_count = thread_count + 1 WHERE id = %s",
            (parent_id,)
        )

    return json_response({
        'data': {
            'id': str(message['id']),
            'channel_id': str(message['channel_id']),
            'content': message['content'],
            'type': message['type'],
            'sender': {
                'id': user['id'],
                'display_name': user['display_name'],
                'avatar_url': user.get('avatar_url'),
                'avatar_color': user.get('avatar_color')
            },
            'created_at': message['created_at'].isoformat() if message.get('created_at') else None
        }
    }, 201)


@chat_bp.route('/messages/<message_id>', methods=['PUT', 'OPTIONS'])
@require_auth
def update_message(message_id):
    """메시지 수정"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    data = request.get_json()
    content = data.get('content', '').strip()

    if not content:
        return json_response({'error': '내용을 입력해주세요'}, 400)

    sanitized_content = sanitize_html(content)

    message = db.query_one(
        """
        UPDATE chat_messages
        SET content = %s, is_edited = true, updated_at = NOW()
        WHERE id = %s AND sender_id = %s AND deleted_at IS NULL
        RETURNING *
        """,
        (sanitized_content, message_id, user['id'])
    )

    if not message:
        return json_response({'error': '메시지를 찾을 수 없거나 수정 권한이 없습니다'}, 404)

    return json_response({
        'data': {
            'id': str(message['id']),
            'content': message['content'],
            'is_edited': True,
            'updated_at': message['updated_at'].isoformat() if message.get('updated_at') else None
        }
    })


@chat_bp.route('/messages/<message_id>', methods=['DELETE', 'OPTIONS'])
@require_auth
def delete_message(message_id):
    """메시지 삭제"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()

    result = db.query_one(
        """
        UPDATE chat_messages
        SET deleted_at = NOW()
        WHERE id = %s AND sender_id = %s AND deleted_at IS NULL
        RETURNING id, channel_id
        """,
        (message_id, user['id'])
    )

    if not result:
        return json_response({'error': '메시지를 찾을 수 없거나 삭제 권한이 없습니다'}, 404)

    return json_response({'message': '메시지가 삭제되었습니다'})


# ============ User Routes ============

@chat_bp.route('/users', methods=['GET', 'OPTIONS'])
@require_auth
def get_users():
    """사용자 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    users = db.query(
        """
        SELECT id, display_name, avatar_url, avatar_color, status
        FROM chat_users
        WHERE is_active = true
        ORDER BY display_name
        """
    )

    return json_response({
        'data': [{
            'id': str(u['id']),
            'display_name': u['display_name'],
            'avatar_url': u.get('avatar_url'),
            'avatar_color': u.get('avatar_color'),
            'status': u.get('status', 'offline')
        } for u in users]
    })


@chat_bp.route('/users/me', methods=['PUT', 'PATCH', 'OPTIONS'])
@require_auth
def update_me():
    """내 정보 수정"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    data = request.get_json()

    display_name = data.get('display_name') or data.get('displayName')
    avatar_color = data.get('avatar_color') or data.get('avatarColor')
    avatar_url = data.get('avatar_url') or data.get('avatarUrl')

    updates = []
    params = []

    if display_name:
        updates.append("display_name = %s")
        params.append(display_name.strip())
    if avatar_color:
        updates.append("avatar_color = %s")
        params.append(avatar_color)
    if 'avatar_url' in data or 'avatarUrl' in data:
        updates.append("avatar_url = %s")
        params.append(avatar_url)  # None도 허용 (이미지 제거)

    if not updates:
        return json_response({'error': '수정할 내용이 없습니다'}, 400)

    params.append(user['id'])

    result = db.query_one(
        f"""
        UPDATE chat_users
        SET {', '.join(updates)}, updated_at = NOW()
        WHERE id = %s
        RETURNING id, display_name, avatar_url, avatar_color
        """,
        tuple(params)
    )

    return json_response({
        'data': {
            'id': str(result['id']),
            'display_name': result['display_name'],
            'avatar_url': result.get('avatar_url'),
            'avatar_color': result.get('avatar_color')
        }
    })


# ============ Channel Layout Routes ============

@chat_bp.route('/users/me/channel-layout', methods=['GET', 'OPTIONS'])
@require_auth
def get_channel_layout():
    """채널 레이아웃 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()

    result = db.query_one(
        "SELECT layout_data FROM chat_user_preferences WHERE user_id = %s",
        (user['id'],)
    )

    if result and result.get('layout_data'):
        return json_response({'data': result['layout_data']})

    # 기본값
    return json_response({
        'data': {
            'items': [],
            'hiddenChannels': [],
            'pinnedChannels': [],
            'gridSize': 4,
            'layoutMode': 'grid',
            'zoom': 100,
            'fontSize': 14
        }
    })


@chat_bp.route('/users/me/channel-layout', methods=['PUT', 'OPTIONS'])
@require_auth
def update_channel_layout():
    """채널 레이아웃 저장"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    data = request.get_json()

    import json
    layout_json = json.dumps(data)

    db.execute(
        """
        INSERT INTO chat_user_preferences (user_id, layout_data)
        VALUES (%s, %s)
        ON CONFLICT (user_id) DO UPDATE SET layout_data = EXCLUDED.layout_data, updated_at = NOW()
        """,
        (user['id'], layout_json)
    )

    return json_response({'data': data})


# ============ User Status & Settings Routes ============

@chat_bp.route('/users/me/status', methods=['POST', 'OPTIONS'])
@require_auth
def update_status():
    """사용자 상태 업데이트"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()

        status = data.get('status', 'online')
        status_message = data.get('status_message', '')

        db.execute(
            """
            UPDATE chat_users
            SET status = %s, status_message = %s, last_seen_at = NOW(), updated_at = NOW()
            WHERE id = %s
            """,
            (status, status_message, user['id'])
        )

        return json_response({'success': True})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/users/me/settings', methods=['GET', 'OPTIONS'])
@require_auth
def get_settings():
    """사용자 설정 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()

    result = db.query_one(
        "SELECT * FROM chat_user_settings WHERE user_id = %s",
        (user['id'],)
    )

    if result:
        return json_response({
            'data': {
                'notification_enabled': result.get('notification_enabled', True),
                'notification_sound': result.get('notification_sound', True),
                'theme': result.get('theme', 'system'),
                'language': result.get('language', 'ko')
            }
        })

    # 기본값
    return json_response({
        'data': {
            'notification_enabled': True,
            'notification_sound': True,
            'theme': 'system',
            'language': 'ko'
        }
    })


@chat_bp.route('/users/me/settings', methods=['PATCH', 'OPTIONS'])
@require_auth
def update_settings():
    """사용자 설정 업데이트"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()

        db.execute(
            """
            INSERT INTO chat_user_settings (user_id, notification_enabled, notification_sound, theme, language)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                notification_enabled = COALESCE(EXCLUDED.notification_enabled, chat_user_settings.notification_enabled),
                notification_sound = COALESCE(EXCLUDED.notification_sound, chat_user_settings.notification_sound),
                theme = COALESCE(EXCLUDED.theme, chat_user_settings.theme),
                language = COALESCE(EXCLUDED.language, chat_user_settings.language),
                updated_at = NOW()
            """,
            (
                user['id'],
                data.get('notification_enabled', True),
                data.get('notification_sound', True),
                data.get('theme', 'system'),
                data.get('language', 'ko')
            )
        )

        return json_response({'success': True, 'data': data})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


# ============ Direct Channel Routes ============

@chat_bp.route('/channels/direct', methods=['POST', 'OPTIONS'])
@require_auth
def create_direct_channel():
    """1:1 다이렉트 채널 생성 또는 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()
        target_user_id = data.get('user_id')

        if not target_user_id:
            return json_response({'error': '대상 사용자 ID가 필요합니다'}, 400)

        if target_user_id == user['id']:
            return json_response({'error': '자기 자신과 채팅을 만들 수 없습니다'}, 400)

        # 기존 다이렉트 채널 찾기
        existing = db.query_one(
            """
            SELECT c.* FROM chat_channels c
            WHERE c.type = 'direct' AND c.deleted_at IS NULL
            AND EXISTS (SELECT 1 FROM chat_channel_members WHERE channel_id = c.id AND user_id = %s)
            AND EXISTS (SELECT 1 FROM chat_channel_members WHERE channel_id = c.id AND user_id = %s)
            """,
            (user['id'], target_user_id)
        )

        if existing:
            return json_response({
                'data': {
                    'id': str(existing['id']),
                    'name': existing['name'],
                    'type': existing['type'],
                    'avatar_url': existing.get('avatar_url'),
                    'last_message_at': existing['updated_at'].isoformat() if existing.get('updated_at') else None,
                    'is_pinned': False,
                    'unread_count': 0,
                    'last_read_message_id': None
                }
            })

        # 새 다이렉트 채널 생성
        channel = db.query_one(
            """
            INSERT INTO chat_channels (type, created_by)
            VALUES ('direct', %s)
            RETURNING *
            """,
            (user['id'],)
        )

        # 두 멤버 추가
        db.execute(
            "INSERT INTO chat_channel_members (channel_id, user_id, role) VALUES (%s, %s, 'member')",
            (channel['id'], user['id'])
        )
        db.execute(
            "INSERT INTO chat_channel_members (channel_id, user_id, role) VALUES (%s, %s, 'member')",
            (channel['id'], target_user_id)
        )

        return json_response({
            'data': {
                'id': str(channel['id']),
                'name': None,
                'type': 'direct',
                'avatar_url': None,
                'last_message_at': None,
                'is_pinned': False,
                'unread_count': 0,
                'last_read_message_id': None
            }
        }, 201)
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


# ============ Channel Members Routes ============

@chat_bp.route('/channels/<channel_id>/members', methods=['GET', 'OPTIONS'])
@require_auth
def get_channel_members(channel_id):
    """채널 멤버 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()

        # 채널 멤버인지 확인
        is_member = db.query_one(
            "SELECT 1 FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
            (channel_id, user['id'])
        )
        if not is_member:
            return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

        members = db.query(
            """
            SELECT u.id, u.display_name, u.avatar_url, u.avatar_color, u.status,
                   cm.role, cm.joined_at
            FROM chat_channel_members cm
            JOIN chat_users u ON cm.user_id = u.id
            WHERE cm.channel_id = %s
            ORDER BY cm.joined_at
            """,
            (channel_id,)
        )

        return json_response({
            'data': [{
                'id': str(m['id']),
                'display_name': m['display_name'],
                'avatar_url': m.get('avatar_url'),
                'avatar_color': m.get('avatar_color'),
                'status': m.get('status', 'offline'),
                'role': m['role'],
                'joined_at': m['joined_at'].isoformat() if m.get('joined_at') else None
            } for m in members]
        })
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/channels/<channel_id>/members', methods=['POST', 'OPTIONS'])
@require_auth
def add_channel_members(channel_id):
    """채널에 멤버 추가"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()

        # 채널 멤버인지 확인
        member = db.query_one(
            "SELECT role FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
            (channel_id, user['id'])
        )
        if not member:
            return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

        # 단일 또는 복수 멤버 추가
        user_ids = data.get('user_ids', [])
        if data.get('user_id'):
            user_ids.append(data.get('user_id'))

        role = data.get('role', 'member')

        for uid in user_ids:
            db.execute(
                """
                INSERT INTO chat_channel_members (channel_id, user_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (channel_id, user_id) DO NOTHING
                """,
                (channel_id, uid, role)
            )

        return json_response({'success': True})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/channels/<channel_id>/members/<user_id>', methods=['DELETE', 'OPTIONS'])
@require_auth
def remove_channel_member(channel_id, user_id):
    """채널에서 멤버 제거"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        current_user = get_current_user()

        # 본인이거나 admin인 경우만 제거 가능
        member = db.query_one(
            "SELECT role FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
            (channel_id, current_user['id'])
        )
        if not member:
            return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

        if user_id != current_user['id'] and member['role'] != 'admin':
            return json_response({'error': '멤버 제거 권한이 없습니다'}, 403)

        db.execute(
            "DELETE FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
            (channel_id, user_id)
        )

        return json_response({'success': True})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


# ============ Message Reactions Routes ============

@chat_bp.route('/messages/<message_id>/reactions', methods=['POST', 'OPTIONS'])
@require_auth
def add_reaction(message_id):
    """리액션 추가"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()
        emoji = data.get('emoji')

        if not emoji:
            return json_response({'error': '이모지가 필요합니다'}, 400)

        # 메시지 존재 확인
        message = db.query_one(
            "SELECT channel_id FROM chat_messages WHERE id = %s AND deleted_at IS NULL",
            (message_id,)
        )
        if not message:
            return json_response({'error': '메시지를 찾을 수 없습니다'}, 404)

        # 리액션 추가
        db.execute(
            """
            INSERT INTO chat_message_reactions (message_id, user_id, emoji)
            VALUES (%s, %s, %s)
            ON CONFLICT (message_id, user_id, emoji) DO NOTHING
            """,
            (message_id, user['id'], emoji)
        )

        # 리액션 목록 반환
        reactions = get_message_reactions(message_id, user['id'])
        return json_response({'data': reactions})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/messages/<message_id>/reactions/<emoji>', methods=['DELETE', 'OPTIONS'])
@require_auth
def remove_reaction(message_id, emoji):
    """리액션 제거"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()

        db.execute(
            "DELETE FROM chat_message_reactions WHERE message_id = %s AND user_id = %s AND emoji = %s",
            (message_id, user['id'], emoji)
        )

        reactions = get_message_reactions(message_id, user['id'])
        return json_response({'data': reactions})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


def get_message_reactions(message_id: str, current_user_id: str) -> list:
    """메시지의 리액션 목록 조회"""
    reactions = db.query(
        """
        SELECT emoji, COUNT(*) as count,
               ARRAY_AGG(user_id::text) as users,
               BOOL_OR(user_id = %s) as has_reacted
        FROM chat_message_reactions
        WHERE message_id = %s
        GROUP BY emoji
        """,
        (current_user_id, message_id)
    )
    return [{
        'emoji': r['emoji'],
        'count': r['count'],
        'users': r['users'],
        'hasReacted': r['has_reacted']
    } for r in reactions]


# ============ Message Read Routes ============

@chat_bp.route('/channels/<channel_id>/messages/read', methods=['POST', 'OPTIONS'])
@require_auth
def mark_messages_read(channel_id):
    """메시지 읽음 처리"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        data = request.get_json()
        message_id = data.get('message_id')

        if not message_id:
            return json_response({'error': '메시지 ID가 필요합니다'}, 400)

        # 채널 멤버 last_read 업데이트
        db.execute(
            """
            UPDATE chat_channel_members
            SET last_read_message_id = %s, last_read_at = NOW()
            WHERE channel_id = %s AND user_id = %s
            """,
            (message_id, channel_id, user['id'])
        )

        return json_response({'success': True})
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


# ============ Search Routes ============

@chat_bp.route('/messages/search', methods=['GET', 'OPTIONS'])
@require_auth
def search_messages():
    """메시지 검색"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        user = get_current_user()
        q = request.args.get('q', '')
        channel_id = request.args.get('channel_id')
        limit = min(int(request.args.get('limit', 20)), 100)
        offset = int(request.args.get('offset', 0))

        if not q:
            return json_response({'error': '검색어가 필요합니다'}, 400)

        # 사용자가 속한 채널에서만 검색
        if channel_id:
            messages = db.query(
                """
                SELECT m.id, m.channel_id, m.sender_id, m.content, m.created_at,
                       u.display_name as sender_name, u.avatar_url as sender_avatar,
                       c.name as channel_name, c.type as channel_type
                FROM chat_messages m
                LEFT JOIN chat_users u ON m.sender_id = u.id
                LEFT JOIN chat_channels c ON m.channel_id = c.id
                WHERE m.channel_id = %s
                AND m.content ILIKE %s
                AND m.deleted_at IS NULL
                AND EXISTS (SELECT 1 FROM chat_channel_members WHERE channel_id = m.channel_id AND user_id = %s)
                ORDER BY m.created_at DESC
                LIMIT %s OFFSET %s
                """,
                (channel_id, f'%{q}%', user['id'], limit, offset)
            )
        else:
            messages = db.query(
                """
                SELECT m.id, m.channel_id, m.sender_id, m.content, m.created_at,
                       u.display_name as sender_name, u.avatar_url as sender_avatar,
                       c.name as channel_name, c.type as channel_type
                FROM chat_messages m
                LEFT JOIN chat_users u ON m.sender_id = u.id
                LEFT JOIN chat_channels c ON m.channel_id = c.id
                WHERE m.content ILIKE %s
                AND m.deleted_at IS NULL
                AND EXISTS (SELECT 1 FROM chat_channel_members WHERE channel_id = m.channel_id AND user_id = %s)
                ORDER BY m.created_at DESC
                LIMIT %s OFFSET %s
                """,
                (f'%{q}%', user['id'], limit, offset)
            )

        # 디버그: 첫 번째 결과 출력
        if messages:
            first = messages[0]
            print(f"[Search Debug] First result keys: {list(first.keys())}")
            print(f"[Search Debug] channel_id: {first.get('channel_id')}, channel_name: {first.get('channel_name')}, channel_type: {first.get('channel_type')}")

        return json_response({
            'data': [{
                'id': str(m['id']),
                'channel_id': str(m['channel_id']),
                'channel_name': m.get('channel_name'),
                'channel_type': m.get('channel_type'),
                'content': m['content'],
                'sender': {
                    'id': str(m['sender_id']) if m.get('sender_id') else None,
                    'display_name': m.get('sender_name'),
                    'avatar_url': m.get('sender_avatar'),
                    'avatar_color': None
                },
                'created_at': m['created_at'].isoformat() if m.get('created_at') else None
            } for m in messages]
        })
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


@chat_bp.route('/search/users', methods=['GET', 'OPTIONS'])
@require_auth
def search_users():
    """사용자 검색"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        q = request.args.get('q', '')
        limit = min(int(request.args.get('limit', 20)), 100)
        offset = int(request.args.get('offset', 0))

        if not q:
            return json_response({'error': '검색어가 필요합니다'}, 400)

        users = db.query(
            """
            SELECT id, display_name, avatar_url, avatar_color, status
            FROM chat_users
            WHERE is_active = true
            AND display_name ILIKE %s
            ORDER BY display_name
            LIMIT %s OFFSET %s
            """,
            (f'%{q}%', limit, offset)
        )

        return json_response({
            'data': [{
                'id': str(u['id']),
                'display_name': u['display_name'],
                'avatar_url': u.get('avatar_url'),
                'avatar_color': u.get('avatar_color'),
                'status': u.get('status', 'offline')
            } for u in users]
        })
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)


# ============ File Upload Routes ============

@chat_bp.route('/files/upload', methods=['POST', 'OPTIONS'])
@require_auth
def upload_file():
    """파일 업로드 (아바타 이미지 등)"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    try:
        import os
        import uuid
        from werkzeug.utils import secure_filename

        if 'file' not in request.files:
            return json_response({'error': '파일이 없습니다'}, 400)

        file = request.files['file']
        if file.filename == '':
            return json_response({'error': '파일이 선택되지 않았습니다'}, 400)

        # 허용된 확장자 확인
        allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
        if ext not in allowed_extensions:
            return json_response({'error': f'허용되지 않는 파일 형식입니다. 허용: {", ".join(allowed_extensions)}'}, 400)

        # 파일 저장 경로 설정
        upload_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uploads', 'avatars')
        os.makedirs(upload_folder, exist_ok=True)

        # 고유 파일명 생성
        filename = f"{uuid.uuid4().hex}.{ext}"
        filepath = os.path.join(upload_folder, filename)

        # 파일 저장
        file.save(filepath)

        # URL 반환 (상대 경로)
        file_url = f"/uploads/avatars/{filename}"

        return json_response({
            'data': {
                'url': file_url,
                'filename': filename
            }
        })
    except Exception as e:
        import traceback
        return json_response({'error': str(e), 'trace': traceback.format_exc()}, 500)
