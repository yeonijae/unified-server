"""
Socket.io 이벤트 핸들러
"""

from flask_socketio import emit, join_room, leave_room, disconnect
from .auth import validate_session
from . import db
import bleach

# 연결된 사용자 관리
connected_users = {}  # {user_id: [sid1, sid2, ...]}
user_rooms = {}  # {sid: [room1, room2, ...]}

# HTML Sanitization
ALLOWED_TAGS = ['p', 'br', 'strong', 'em', 'u', 'a', 'code', 'pre', 'ul', 'ol', 'li']
ALLOWED_ATTRS = {'a': ['href', 'target', 'rel']}


def sanitize_html(content: str) -> str:
    return bleach.clean(content, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS)


def register_handlers(socketio, log_callback=None):
    """Socket.io 이벤트 핸들러 등록"""

    def log(message):
        if log_callback:
            log_callback(message)
        print(f"[Socket] {message}")

    @socketio.on('connect')
    def handle_connect(auth=None):
        """연결 시 인증 처리"""
        from flask import request
        sid = request.sid

        # 토큰 검증
        token = None
        if auth and isinstance(auth, dict):
            token = auth.get('token')

        if not token:
            log(f"Connection rejected: no token (sid={sid})")
            disconnect()
            return False

        user = validate_session(token)
        if not user:
            log(f"Connection rejected: invalid token (sid={sid})")
            disconnect()
            return False

        user_id = user['id']

        # 사용자 연결 등록
        if user_id not in connected_users:
            connected_users[user_id] = []
        connected_users[user_id].append(sid)
        user_rooms[sid] = []

        # 개인 룸 입장
        join_room(f"user:{user_id}")
        user_rooms[sid].append(f"user:{user_id}")

        log(f"User connected: {user['display_name']} ({user_id})")

        # 사용자 상태 업데이트
        db.execute(
            "UPDATE chat_users SET status = 'online', last_seen_at = NOW() WHERE id = %s",
            (user_id,)
        )

        # 온라인 상태 브로드캐스트
        emit('presence:update', {
            'userId': user_id,
            'status': 'online'
        }, broadcast=True)

        return True

    @socketio.on('disconnect')
    def handle_disconnect():
        """연결 해제"""
        from flask import request
        sid = request.sid

        # 연결된 사용자 찾기
        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                sids.remove(sid)
                if not sids:
                    del connected_users[uid]
                break

        # 룸 정리
        if sid in user_rooms:
            del user_rooms[sid]

        if user_id:
            # 다른 세션이 없으면 오프라인 처리
            if user_id not in connected_users:
                db.execute(
                    "UPDATE chat_users SET status = 'offline', last_seen_at = NOW() WHERE id = %s",
                    (user_id,)
                )
                emit('presence:update', {
                    'userId': user_id,
                    'status': 'offline'
                }, broadcast=True)

            log(f"User disconnected: {user_id}")

    @socketio.on('channel:join')
    def handle_channel_join(data):
        """채널 입장"""
        from flask import request
        sid = request.sid
        channel_id = data.get('channelId')

        if not channel_id:
            return

        room = f"channel:{channel_id}"
        join_room(room)

        if sid in user_rooms:
            user_rooms[sid].append(room)

        log(f"Joined channel: {channel_id}")

    @socketio.on('channel:leave')
    def handle_channel_leave(data):
        """채널 퇴장"""
        from flask import request
        sid = request.sid
        channel_id = data.get('channelId')

        if not channel_id:
            return

        room = f"channel:{channel_id}"
        leave_room(room)

        if sid in user_rooms and room in user_rooms[sid]:
            user_rooms[sid].remove(room)

        log(f"Left channel: {channel_id}")

    @socketio.on('message:send')
    def handle_message_send(data):
        """메시지 전송"""
        from flask import request
        sid = request.sid

        # 사용자 찾기
        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        channel_id = data.get('channelId')
        content = data.get('content', '').strip()
        msg_type = data.get('type', 'text')
        parent_id = data.get('parentId')

        if not channel_id or not content:
            return

        # 채널 멤버 확인
        is_member = db.query_one(
            "SELECT 1 FROM chat_channel_members WHERE channel_id = %s AND user_id = %s",
            (channel_id, user_id)
        )
        if not is_member:
            return

        # HTML Sanitize
        sanitized_content = sanitize_html(content)

        # 메시지 생성
        message = db.query_one(
            """
            INSERT INTO chat_messages (channel_id, sender_id, content, type, parent_id)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING *
            """,
            (channel_id, user_id, sanitized_content, msg_type, parent_id)
        )

        # 스레드 카운트 증가
        if parent_id:
            db.execute(
                "UPDATE chat_messages SET thread_count = thread_count + 1 WHERE id = %s",
                (parent_id,)
            )

        # 사용자 정보 조회
        user = db.query_one(
            "SELECT id, display_name, avatar_url, avatar_color FROM chat_users WHERE id = %s",
            (user_id,)
        )

        # 메시지 브로드캐스트
        emit('message:new', {
            'id': str(message['id']),
            'channelId': str(message['channel_id']),
            'content': message['content'],
            'type': message['type'],
            'parentId': str(message['parent_id']) if message.get('parent_id') else None,
            'sender': {
                'id': str(user['id']),
                'displayName': user['display_name'],
                'avatarUrl': user.get('avatar_url'),
                'avatarColor': user.get('avatar_color')
            },
            'createdAt': message['created_at'].isoformat() if message.get('created_at') else None
        }, room=f"channel:{channel_id}")

        log(f"Message sent: {user['display_name']} -> channel:{channel_id}")

    @socketio.on('message:update')
    def handle_message_update(data):
        """메시지 수정"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        message_id = data.get('messageId')
        content = data.get('content', '').strip()

        if not message_id or not content:
            return

        sanitized_content = sanitize_html(content)

        message = db.query_one(
            """
            UPDATE chat_messages
            SET content = %s, is_edited = true, updated_at = NOW()
            WHERE id = %s AND sender_id = %s AND deleted_at IS NULL
            RETURNING id, channel_id, content, updated_at
            """,
            (sanitized_content, message_id, user_id)
        )

        if message:
            emit('message:updated', {
                'id': str(message['id']),
                'channelId': str(message['channel_id']),
                'content': message['content'],
                'isEdited': True,
                'updatedAt': message['updated_at'].isoformat() if message.get('updated_at') else None
            }, room=f"channel:{message['channel_id']}")

    @socketio.on('message:delete')
    def handle_message_delete(data):
        """메시지 삭제"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        message_id = data.get('messageId')

        if not message_id:
            return

        result = db.query_one(
            """
            UPDATE chat_messages
            SET deleted_at = NOW()
            WHERE id = %s AND sender_id = %s AND deleted_at IS NULL
            RETURNING id, channel_id
            """,
            (message_id, user_id)
        )

        if result:
            emit('message:deleted', {
                'id': str(result['id']),
                'channelId': str(result['channel_id'])
            }, room=f"channel:{result['channel_id']}")

    @socketio.on('typing:start')
    def handle_typing_start(data):
        """타이핑 시작"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        channel_id = data.get('channelId')
        if not channel_id:
            return

        user = db.query_one(
            "SELECT display_name FROM chat_users WHERE id = %s",
            (user_id,)
        )

        emit('typing:update', {
            'channelId': channel_id,
            'userId': user_id,
            'userName': user['display_name'] if user else 'Unknown',
            'isTyping': True
        }, room=f"channel:{channel_id}", include_self=False)

    @socketio.on('typing:stop')
    def handle_typing_stop(data):
        """타이핑 종료"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        channel_id = data.get('channelId')
        if not channel_id:
            return

        emit('typing:update', {
            'channelId': channel_id,
            'userId': user_id,
            'isTyping': False
        }, room=f"channel:{channel_id}", include_self=False)

    @socketio.on('reaction:add')
    def handle_reaction_add(data):
        """리액션 추가"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        message_id = data.get('messageId')
        emoji = data.get('emoji')

        if not message_id or not emoji:
            return

        # 리액션 추가
        db.execute(
            """
            INSERT INTO chat_message_reactions (message_id, user_id, emoji)
            VALUES (%s, %s, %s)
            ON CONFLICT (message_id, user_id, emoji) DO NOTHING
            """,
            (message_id, user_id, emoji)
        )

        # 채널 ID 조회
        result = db.query_one(
            "SELECT channel_id FROM chat_messages WHERE id = %s",
            (message_id,)
        )

        if result:
            emit('reaction:updated', {
                'messageId': message_id,
                'userId': user_id,
                'emoji': emoji,
                'action': 'add'
            }, room=f"channel:{result['channel_id']}")

    @socketio.on('reaction:remove')
    def handle_reaction_remove(data):
        """리액션 제거"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        message_id = data.get('messageId')
        emoji = data.get('emoji')

        if not message_id or not emoji:
            return

        # 리액션 삭제
        db.execute(
            "DELETE FROM chat_message_reactions WHERE message_id = %s AND user_id = %s AND emoji = %s",
            (message_id, user_id, emoji)
        )

        # 채널 ID 조회
        result = db.query_one(
            "SELECT channel_id FROM chat_messages WHERE id = %s",
            (message_id,)
        )

        if result:
            emit('reaction:updated', {
                'messageId': message_id,
                'userId': user_id,
                'emoji': emoji,
                'action': 'remove'
            }, room=f"channel:{result['channel_id']}")

    log("Socket handlers registered")
