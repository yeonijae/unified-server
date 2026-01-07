"""
Chat Server - Flask + Socket.io 통합
"""

from flask import Flask
from flask_cors import CORS
from flask_socketio import SocketIO

from . import db
from . import auth
from .routes import chat_bp
from .socket_handlers import register_handlers

# 전역 Socket.io 인스턴스
_socketio = None
_app = None
_log_callback = None


def create_chat_app(config: dict, log_callback=None) -> tuple:
    """채팅 앱 생성

    Args:
        config: DB 설정 딕셔너리 (host, port, user, password, database)
        log_callback: 로그 출력 콜백 함수

    Returns:
        (Flask app, SocketIO instance)
    """
    global _socketio, _app, _log_callback
    _log_callback = log_callback

    def log(message):
        if _log_callback:
            _log_callback(message)
        print(f"[Chat] {message}")

    # Flask 앱 생성
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'haniwon-chat-secret-key'

    # CORS 설정
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # Socket.io 설정
    socketio = SocketIO(
        app,
        cors_allowed_origins="*",
        async_mode='threading',  # threading 모드 사용 (eventlet 없이)
        ping_timeout=60,
        ping_interval=25,
        logger=False,
        engineio_logger=False
    )

    # DB 초기화
    db.init_db(config, log)
    auth.init_auth(db)

    # 테이블 확인
    try:
        db.ensure_tables()
        log("Database initialized")
    except Exception as e:
        log(f"Database init error: {e}")

    # 라우트 등록
    app.register_blueprint(chat_bp)

    # Socket 핸들러 등록
    register_handlers(socketio, log)

    # 헬스 체크 엔드포인트
    @app.route('/health')
    def health():
        return {'status': 'ok', 'service': 'chat'}

    @app.route('/')
    def index():
        return {'message': 'HaniChat Server', 'version': '1.0.0'}

    _app = app
    _socketio = socketio

    log("Chat server created")

    return app, socketio


def get_socketio():
    """SocketIO 인스턴스 반환"""
    return _socketio


def get_app():
    """Flask 앱 인스턴스 반환"""
    return _app


def run_server(host='0.0.0.0', port=3300, config=None, log_callback=None):
    """채팅 서버 실행 (독립 실행 시 사용)"""
    if config is None:
        config = {
            'host': '192.168.0.173',
            'port': 5432,
            'user': 'haniwon_user',
            'password': '7582',
            'database': 'haniwon'
        }

    app, socketio = create_chat_app(config, log_callback)

    def log(message):
        if log_callback:
            log_callback(message)
        print(f"[Chat] {message}")

    log(f"Starting chat server on {host}:{port}")

    # Socket.io 서버 실행
    socketio.run(app, host=host, port=port, debug=False, use_reloader=False)


# 독립 실행
if __name__ == '__main__':
    run_server()
