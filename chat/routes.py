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
    return add_cors_headers(response)


# HTML Sanitization
ALLOWED_TAGS = ['p', 'br', 'strong', 'em', 'u', 'a', 'code', 'pre', 'ul', 'ol', 'li']
ALLOWED_ATTRS = {'a': ['href', 'target', 'rel']}


def sanitize_html(content: str) -> str:
    return bleach.clean(content, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS)


# ============ Auth Routes ============

@chat_bp.route('/auth/register', methods=['POST', 'OPTIONS'])
def register():
    """회원가입"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    data = request.get_json()
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    display_name = data.get('displayName', '').strip()

    if not email or not password:
        return json_response({'error': '이메일과 비밀번호를 입력해주세요'}, 400)

    # 이메일 중복 확인
    existing = db.query_one("SELECT id FROM chat_users WHERE email = %s", (email,))
    if existing:
        return json_response({'error': '이미 사용 중인 이메일입니다'}, 400)

    # 사용자 생성
    password_hash = hash_password(password)
    result = db.query_one(
        """
        INSERT INTO chat_users (email, password_hash, display_name)
        VALUES (%s, %s, %s)
        RETURNING id, email, display_name, avatar_url, avatar_color, created_at
        """,
        (email, password_hash, display_name or email.split('@')[0])
    )

    # 세션 생성
    token = create_session(str(result['id']))

    return json_response({
        'data': {
            'user': {
                'id': str(result['id']),
                'email': result['email'],
                'displayName': result['display_name'],
                'avatarUrl': result.get('avatar_url'),
                'avatarColor': result.get('avatar_color')
            },
            'accessToken': token
        }
    }, 201)


@chat_bp.route('/auth/login', methods=['POST', 'OPTIONS'])
def login():
    """로그인"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    data = request.get_json()
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')

    if not email or not password:
        return json_response({'error': '이메일과 비밀번호를 입력해주세요'}, 400)

    # 사용자 조회
    user = db.query_one(
        """
        SELECT id, email, password_hash, display_name, avatar_url, avatar_color
        FROM chat_users WHERE email = %s
        """,
        (email,)
    )

    if not user or not verify_password(password, user['password_hash']):
        return json_response({'error': '이메일 또는 비밀번호가 올바르지 않습니다'}, 401)

    # 세션 생성
    token = create_session(str(user['id']))

    return json_response({
        'data': {
            'user': {
                'id': str(user['id']),
                'email': user['email'],
                'displayName': user['display_name'],
                'avatarUrl': user.get('avatar_url'),
                'avatarColor': user.get('avatar_color')
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
            'email': user['email'],
            'displayName': user['display_name'],
            'avatarUrl': user.get('avatar_url'),
            'avatarColor': user.get('avatar_color')
        }
    })


# ============ Channel Routes ============

@chat_bp.route('/channels', methods=['GET', 'OPTIONS'])
@require_auth
def get_channels():
    """채널 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

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

    return json_response({
        'data': [{
            'id': str(ch['id']),
            'name': ch['name'],
            'description': ch.get('description'),
            'type': ch['type'],
            'isPrivate': ch.get('is_private', False),
            'role': ch['role'],
            'unreadCount': ch['unread_count'] or 0
        } for ch in channels]
    })


@chat_bp.route('/channels', methods=['POST', 'OPTIONS'])
@require_auth
def create_channel():
    """채널 생성"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    data = request.get_json()

    name = data.get('name', '').strip()
    description = data.get('description', '').strip()
    channel_type = data.get('type', 'public')
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
            'description': channel.get('description'),
            'type': channel['type'],
            'isPrivate': channel.get('is_private', False)
        }
    }, 201)


@chat_bp.route('/channels/<channel_id>', methods=['GET', 'OPTIONS'])
@require_auth
def get_channel(channel_id):
    """채널 상세 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()

    channel = db.query_one(
        """
        SELECT c.*, cm.role
        FROM chat_channels c
        JOIN chat_channel_members cm ON c.id = cm.channel_id
        WHERE c.id = %s AND cm.user_id = %s AND c.deleted_at IS NULL
        """,
        (channel_id, user['id'])
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
            'description': channel.get('description'),
            'type': channel['type'],
            'isPrivate': channel.get('is_private', False),
            'role': channel['role'],
            'members': [{
                'id': str(m['id']),
                'displayName': m['display_name'],
                'avatarUrl': m.get('avatar_url'),
                'avatarColor': m.get('avatar_color'),
                'role': m['role']
            } for m in members]
        }
    })


# ============ Message Routes ============

@chat_bp.route('/channels/<channel_id>/messages', methods=['GET', 'OPTIONS'])
@require_auth
def get_messages(channel_id):
    """메시지 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    before = request.args.get('before')
    limit = min(int(request.args.get('limit', 50)), 100)

    # 채널 멤버 확인
    is_member = db.query_one(
        "SELECT 1 FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
        (channel_id, user['id'])
    )
    if not is_member:
        return json_response({'error': '채널에 접근할 수 없습니다'}, 403)

    # 메시지 조회
    if before:
        messages = db.query(
            """
            SELECT m.*,
                   json_build_object('id', u.id, 'displayName', u.display_name,
                                     'avatarUrl', u.avatar_url, 'avatarColor', u.avatar_color) as sender
            FROM chat_messages m
            LEFT JOIN chat_users u ON m.sender_id = u.id
            WHERE m.channel_id = %s AND m.deleted_at IS NULL AND m.parent_id IS NULL
            AND m.created_at < (SELECT created_at FROM chat_messages WHERE id = %s)
            ORDER BY m.created_at DESC
            LIMIT %s
            """,
            (channel_id, before, limit)
        )
    else:
        messages = db.query(
            """
            SELECT m.*,
                   json_build_object('id', u.id, 'displayName', u.display_name,
                                     'avatarUrl', u.avatar_url, 'avatarColor', u.avatar_color) as sender
            FROM chat_messages m
            LEFT JOIN chat_users u ON m.sender_id = u.id
            WHERE m.channel_id = %s AND m.deleted_at IS NULL AND m.parent_id IS NULL
            ORDER BY m.created_at DESC
            LIMIT %s
            """,
            (channel_id, limit)
        )

    # 역순으로 정렬 (오래된 것부터)
    messages = list(reversed(messages))

    return json_response({
        'data': [{
            'id': str(m['id']),
            'channelId': str(m['channel_id']),
            'content': m['content'],
            'type': m['type'],
            'sender': m['sender'],
            'threadCount': m.get('thread_count', 0),
            'isEdited': m.get('is_edited', False),
            'isPinned': m.get('is_pinned', False),
            'createdAt': m['created_at'].isoformat() if m.get('created_at') else None,
            'updatedAt': m['updated_at'].isoformat() if m.get('updated_at') else None
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

    channel_id = data.get('channelId')
    content = data.get('content', '').strip()
    msg_type = data.get('type', 'text')
    parent_id = data.get('parentId')

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
            'channelId': str(message['channel_id']),
            'content': message['content'],
            'type': message['type'],
            'sender': {
                'id': user['id'],
                'displayName': user['display_name'],
                'avatarUrl': user.get('avatar_url'),
                'avatarColor': user.get('avatar_color')
            },
            'createdAt': message['created_at'].isoformat() if message.get('created_at') else None
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
            'isEdited': True,
            'updatedAt': message['updated_at'].isoformat() if message.get('updated_at') else None
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
        SELECT id, email, display_name, avatar_url, avatar_color, status
        FROM chat_users
        WHERE deleted_at IS NULL
        ORDER BY display_name
        """
    )

    return json_response({
        'data': [{
            'id': str(u['id']),
            'email': u['email'],
            'displayName': u['display_name'],
            'avatarUrl': u.get('avatar_url'),
            'avatarColor': u.get('avatar_color'),
            'status': u.get('status', 'offline')
        } for u in users]
    })


@chat_bp.route('/users/me', methods=['PUT', 'OPTIONS'])
@require_auth
def update_me():
    """내 정보 수정"""
    if request.method == 'OPTIONS':
        return cors_preflight()

    user = get_current_user()
    data = request.get_json()

    display_name = data.get('displayName')
    avatar_color = data.get('avatarColor')

    updates = []
    params = []

    if display_name:
        updates.append("display_name = %s")
        params.append(display_name.strip())
    if avatar_color:
        updates.append("avatar_color = %s")
        params.append(avatar_color)

    if not updates:
        return json_response({'error': '수정할 내용이 없습니다'}, 400)

    params.append(user['id'])

    result = db.query_one(
        f"""
        UPDATE chat_users
        SET {', '.join(updates)}, updated_at = NOW()
        WHERE id = %s
        RETURNING id, email, display_name, avatar_url, avatar_color
        """,
        tuple(params)
    )

    return json_response({
        'data': {
            'id': str(result['id']),
            'email': result['email'],
            'displayName': result['display_name'],
            'avatarUrl': result.get('avatar_url'),
            'avatarColor': result.get('avatar_color')
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
            'gridSize': 1,
            'layoutMode': 'list'
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
