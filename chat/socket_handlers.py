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
        print(f"[Socket] {message}", flush=True)

    @socketio.on('connect')
    def handle_connect(auth=None):
        """연결 시 인증 처리"""
        from flask import request
        sid = request.sid

        log(f"Connection attempt: sid={sid}, auth={auth}")

        # 토큰 검증
        token = None
        if auth and isinstance(auth, dict):
            token = auth.get('token')

        if not token:
            log(f"Connection rejected: no token (sid={sid})")
            disconnect()
            return False

        log(f"Token received: {token[:20]}...")
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

        log(f"channel:join received: data={data}, sid={sid}")

        channel_id = data.get('channel_id') or data.get('channelId')

        if not channel_id:
            log(f"channel:join rejected: no channel_id")
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
        channel_id = data.get('channel_id') or data.get('channelId')

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

        log(f"message:send received: data={data}, sid={sid}")

        # 사용자 찾기
        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            log(f"message:send rejected: user not found for sid={sid}")
            return

        # 클라이언트는 snake_case로 전송 (channel_id, parent_id)
        channel_id = data.get('channel_id') or data.get('channelId')
        content = data.get('content', '').strip()
        msg_type = data.get('type', 'text')
        parent_id = data.get('parent_id') or data.get('parentId')

        log(f"message:send parsed: channel_id={channel_id}, content={content[:50] if content else 'empty'}...")

        if not channel_id or not content:
            log(f"message:send rejected: missing channel_id or content")
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

        # 메시지 브로드캐스트 (snake_case for client compatibility)
        emit('message:new', {
            'id': str(message['id']),
            'channel_id': str(message['channel_id']),
            'content': message['content'],
            'type': message['type'],
            'parent_id': str(message['parent_id']) if message.get('parent_id') else None,
            'thread_count': 0,
            'is_edited': False,
            'sender': {
                'id': str(user['id']),
                'display_name': user['display_name'],
                'avatar_url': user.get('avatar_url'),
                'avatar_color': user.get('avatar_color')
            },
            'created_at': message['created_at'].isoformat() if message.get('created_at') else None
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

        message_id = data.get('message_id') or data.get('messageId')
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
                'channel_id': str(message['channel_id']),
                'content': message['content'],
                'is_edited': True,
                'updated_at': message['updated_at'].isoformat() if message.get('updated_at') else None
            }, room=f"channel:{message['channel_id']}")

    # message:edit는 message:update의 별칭
    @socketio.on('message:edit')
    def handle_message_edit(data):
        """메시지 수정 (별칭)"""
        handle_message_update(data)

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

        message_id = data.get('message_id') or data.get('messageId')

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
                'channel_id': str(result['channel_id'])
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

        channel_id = data.get('channel_id') or data.get('channelId')
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

        channel_id = data.get('channel_id') or data.get('channelId')
        if not channel_id:
            return

        emit('typing:update', {
            'channelId': channel_id,
            'userId': user_id,
            'isTyping': False
        }, room=f"channel:{channel_id}", include_self=False)

    def emit_reaction_update(message_id, user_id):
        """리액션 업데이트 이벤트 전송 (헬퍼 함수)"""
        result = db.query_one(
            "SELECT channel_id FROM chat_messages WHERE id = %s",
            (message_id,)
        )
        if not result:
            return

        channel_id = result['channel_id']

        # 해당 메시지의 전체 리액션 조회 (집계)
        reactions = db.query(
            """
            SELECT emoji,
                   COUNT(*) as count,
                   bool_or(user_id = %s) as has_reacted
            FROM chat_message_reactions
            WHERE message_id = %s
            GROUP BY emoji
            """,
            (user_id, message_id)
        )

        reactions_list = [
            {
                'emoji': r['emoji'],
                'count': r['count'],
                'has_reacted': r['has_reacted']
            }
            for r in reactions
        ]

        emit('reaction:update', {
            'message_id': str(message_id),
            'channel_id': str(channel_id),
            'reactions': reactions_list
        }, room=f"channel:{channel_id}")

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

        message_id = data.get('message_id') or data.get('messageId')
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

        emit_reaction_update(message_id, user_id)

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

        message_id = data.get('message_id') or data.get('messageId')
        emoji = data.get('emoji')

        if not message_id or not emoji:
            return

        # 리액션 삭제
        db.execute(
            "DELETE FROM chat_message_reactions WHERE message_id = %s AND user_id = %s AND emoji = %s",
            (message_id, user_id, emoji)
        )

        emit_reaction_update(message_id, user_id)

    @socketio.on('reaction:toggle')
    def handle_reaction_toggle(data):
        """리액션 토글 (있으면 제거, 없으면 추가)"""
        print(f"[Socket] reaction:toggle received: message_id={data.get('message_id')}", flush=True)
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            print(f"[Socket] reaction:toggle - user not found for sid: {sid}", flush=True)
            return

        message_id = data.get('message_id') or data.get('messageId')
        emoji = data.get('emoji')
        print(f"[Socket] reaction:toggle - user_id: {user_id}, message_id: {message_id}", flush=True)

        if not message_id or not emoji:
            print(f"[Socket] reaction:toggle - missing message_id or emoji", flush=True)
            return

        # 기존 리액션 확인
        existing = db.query_one(
            "SELECT id FROM chat_message_reactions WHERE message_id = %s AND user_id = %s AND emoji = %s",
            (message_id, user_id, emoji)
        )

        if existing:
            # 리액션 제거
            db.execute(
                "DELETE FROM chat_message_reactions WHERE message_id = %s AND user_id = %s AND emoji = %s",
                (message_id, user_id, emoji)
            )
        else:
            # 리액션 추가
            db.execute(
                """
                INSERT INTO chat_message_reactions (message_id, user_id, emoji)
                VALUES (%s, %s, %s)
                ON CONFLICT (message_id, user_id, emoji) DO NOTHING
                """,
                (message_id, user_id, emoji)
            )

        emit_reaction_update(message_id, user_id)

    @socketio.on('message:read')
    def handle_message_read(data):
        """메시지 읽음 처리"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        channel_id = data.get('channel_id') or data.get('channelId')
        message_id = data.get('message_id') or data.get('messageId')

        if not channel_id or not message_id:
            return

        # 읽음 처리
        db.execute(
            """
            UPDATE chat_channel_members
            SET last_read_message_id = %s, last_read_at = NOW()
            WHERE channel_id = %s AND user_id = %s
            """,
            (message_id, channel_id, user_id)
        )

        # 읽음 브로드캐스트
        emit('message:read_update', {
            'channelId': channel_id,
            'messageId': message_id,
            'userId': user_id
        }, room=f"channel:{channel_id}")

    @socketio.on('presence:status')
    def handle_presence_status(data):
        """사용자 상태 변경"""
        from flask import request
        sid = request.sid

        user_id = None
        for uid, sids in connected_users.items():
            if sid in sids:
                user_id = uid
                break

        if not user_id:
            return

        status = data.get('status', 'online')
        status_message = data.get('status_message', '')

        db.execute(
            "UPDATE chat_users SET status = %s, status_message = %s, last_seen_at = NOW() WHERE id = %s",
            (status, status_message, user_id)
        )

        emit('presence:update', {
            'userId': user_id,
            'status': status,
            'statusMessage': status_message
        }, broadcast=True)

    @socketio.on('presence:get')
    def handle_presence_get(data):
        """사용자들의 상태 조회"""
        user_ids = data.get('user_ids', [])

        if not user_ids:
            return

        presence = {}
        for uid in user_ids:
            if uid in connected_users:
                presence[uid] = 'online'
            else:
                presence[uid] = 'offline'

        emit('presence:list', presence)

    @socketio.on('channel:get_online')
    def handle_channel_get_online(data):
        """채널의 온라인 멤버 조회"""
        channel_id = data.get('channel_id') or data.get('channelId')

        if not channel_id:
            return

        # 채널 멤버 조회
        members = db.query(
            "SELECT user_id FROM chat_channel_members WHERE channel_id = %s",
            (channel_id,)
        )

        online_users = []
        for m in members:
            uid = str(m['user_id'])
            if uid in connected_users:
                online_users.append(uid)

        emit('channel:online_members', {
            'channelId': channel_id,
            'userIds': online_users
        })

    log("Socket handlers registered")
