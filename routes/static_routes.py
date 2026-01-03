"""
Static File Server 라우트
- React/Vite 빌드 파일 서빙
- GitHub Webhook
- Git Pull / Bun Build API
"""

import threading
from pathlib import Path
from flask import Blueprint, request, jsonify, Response, send_from_directory, send_file
from services import git_build
from config import VERSION, load_config, save_config

static_bp = Blueprint('static', __name__)

# MIME 타입 매핑
MIME_TYPES = {
    '.js': 'application/javascript',
    '.mjs': 'application/javascript',
    '.css': 'text/css',
    '.html': 'text/html',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.ttf': 'font/ttf',
    '.eot': 'application/vnd.ms-fontobject',
}

# www 폴더 경로 (app.py에서 설정)
www_folder = None


def set_www_folder(path):
    """www 폴더 설정"""
    global www_folder
    www_folder = path


def get_www_folder():
    """www 폴더 반환"""
    return www_folder


# ============ 웹 콘솔 HTML ============

def get_console_html():
    config = load_config()
    port = config.get('static_port', 11111)
    folder = config.get('www_folder', 'Not set')
    return f'''<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Static File Server Console</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; min-height: 100vh; padding: 20px; }}
    h1 {{ color: #f59e0b; margin-bottom: 20px; }}
    h2 {{ color: #f59e0b; margin: 20px 0 10px; font-size: 1.1rem; }}
    .container {{ max-width: 800px; margin: 0 auto; }}
    .panel {{ background: #16213e; border-radius: 8px; padding: 20px; margin-bottom: 20px; }}
    button {{ background: #f59e0b; color: #000; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: bold; margin: 10px 5px 10px 0; }}
    button:hover {{ background: #d97706; }}
    button.secondary {{ background: #444; color: #fff; }}
    .status {{ padding: 10px; border-radius: 4px; margin-top: 10px; }}
    .status.success {{ background: #2d5a27; }}
    .status.error {{ background: #5a2727; }}
    .info-grid {{ display: grid; grid-template-columns: 120px 1fr; gap: 10px; }}
    .info-label {{ color: #888; }}
    .info-value {{ color: #fff; word-break: break-all; }}
    .version {{ color: #666; font-size: 12px; margin-top: 20px; text-align: center; }}
    #log {{ background: #0f0f23; border-radius: 4px; padding: 15px; height: 200px; overflow-y: auto; font-family: 'Consolas', monospace; font-size: 12px; color: #0f0; }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Static File Server Console (Port {port})</h1>
    <div class="panel">
      <h2>Server Info</h2>
      <div class="info-grid">
        <div class="info-label">Status:</div>
        <div class="info-value" id="status">Loading...</div>
        <div class="info-label">WWW Folder:</div>
        <div class="info-value">{folder}</div>
        <div class="info-label">Building:</div>
        <div class="info-value" id="building">-</div>
        <div class="info-label">Last Webhook:</div>
        <div class="info-value" id="lastWebhook">-</div>
      </div>
      <button onclick="refreshInfo()">Refresh</button>
    </div>
    <div class="panel">
      <h2>Actions</h2>
      <button onclick="gitPull()">Git Pull</button>
      <button onclick="bunInstall()">Bun Install</button>
      <button onclick="bunBuild()">Bun Build</button>
      <div id="actionStatus"></div>
    </div>
    <div class="panel">
      <h2>Log</h2>
      <div id="log">Server started...</div>
    </div>
    <p class="version">Static File Server v{VERSION}</p>
  </div>
  <script>
    async function api(endpoint, options = {{}}) {{
      const res = await fetch(endpoint, {{ ...options, headers: {{ 'Content-Type': 'application/json' }} }});
      return res.json();
    }}
    function showStatus(id, msg, isErr) {{
      const el = document.getElementById(id);
      el.className = 'status ' + (isErr ? 'error' : 'success');
      el.textContent = msg;
      setTimeout(() => el.textContent = '', 5000);
    }}
    function addLog(msg) {{
      const log = document.getElementById('log');
      const time = new Date().toLocaleTimeString();
      log.innerHTML += `[${{time}}] ${{msg}}\\n`;
      log.scrollTop = log.scrollHeight;
    }}
    async function refreshInfo() {{
      try {{
        const data = await api('/api/server-info');
        document.getElementById('status').textContent = data.status || 'running';
        document.getElementById('building').textContent = data.is_building ? 'Yes' : 'No';
        document.getElementById('lastWebhook').textContent = data.last_webhook || 'Never';
      }} catch(e) {{
        document.getElementById('status').textContent = 'Error';
      }}
    }}
    async function gitPull() {{
      addLog('Starting git pull...');
      try {{
        const data = await api('/api/git-pull', {{ method: 'POST' }});
        if (data.error) {{ showStatus('actionStatus', 'Error: '+data.error, true); addLog('Git pull failed: '+data.error); }}
        else {{ showStatus('actionStatus', data.message || 'Success'); addLog('Git pull: '+data.message); }}
      }} catch(e) {{ showStatus('actionStatus', 'Error: '+e.message, true); }}
    }}
    async function bunInstall() {{
      addLog('Starting bun install...');
      try {{
        const data = await api('/api/bun-install', {{ method: 'POST' }});
        if (data.error) {{ showStatus('actionStatus', 'Error: '+data.error, true); addLog('Install failed: '+data.error); }}
        else {{ showStatus('actionStatus', data.message || 'Success'); addLog('Install: '+data.message); }}
      }} catch(e) {{ showStatus('actionStatus', 'Error: '+e.message, true); }}
    }}
    async function bunBuild() {{
      addLog('Starting bun build...');
      try {{
        const data = await api('/api/bun-build', {{ method: 'POST' }});
        if (data.error) {{ showStatus('actionStatus', 'Error: '+data.error, true); addLog('Build failed: '+data.error); }}
        else {{ showStatus('actionStatus', data.message || 'Success'); addLog('Build: '+data.message); }}
      }} catch(e) {{ showStatus('actionStatus', 'Error: '+e.message, true); }}
    }}
    refreshInfo();
    setInterval(refreshInfo, 10000);
  </script>
</body>
</html>'''


# ============ API 라우트 ============

@static_bp.route('/api/server-info')
def server_info():
    config = load_config()
    return jsonify({
        "version": VERSION,
        "www_folder": str(www_folder) if www_folder else None,
        "dist_folder": str(Path(www_folder) / "dist") if www_folder else None,
        "status": "running",
        "is_building": git_build.get_is_building(),
        "last_webhook": git_build.get_last_webhook_time(),
        "bun_available": git_build.bun_exists(),
        "git_available": git_build.git_exists()
    })


@static_bp.route('/api/git-pull', methods=['POST'])
def api_git_pull():
    """Git Pull 실행"""
    if not www_folder:
        return jsonify({"error": "WWW folder not configured"}), 400

    def callback(success, message):
        git_build.log(message)

    success, has_changes = git_build.run_git_pull(www_folder, callback)

    if success:
        return jsonify({
            "success": True,
            "has_changes": has_changes,
            "message": "Pull successful" + (" (changes detected)" if has_changes else " (no changes)")
        })
    else:
        return jsonify({"error": "Git pull failed"}), 500


@static_bp.route('/api/bun-install', methods=['POST'])
def api_bun_install():
    """Bun Install 실행"""
    if not www_folder:
        return jsonify({"error": "WWW folder not configured"}), 400

    def callback(success, message):
        git_build.log(message)

    # 비동기로 실행
    def run_install():
        git_build.run_bun_install(www_folder, callback)

    threading.Thread(target=run_install, daemon=True).start()

    return jsonify({
        "success": True,
        "message": "Install started in background"
    })


@static_bp.route('/api/bun-build', methods=['POST'])
def api_bun_build():
    """Bun Build 실행"""
    if not www_folder:
        return jsonify({"error": "WWW folder not configured"}), 400

    def callback(success, message):
        git_build.log(message)

    # 비동기로 실행
    def run_build():
        git_build.run_bun_build(www_folder, callback)

    threading.Thread(target=run_build, daemon=True).start()

    return jsonify({
        "success": True,
        "message": "Build started in background"
    })


@static_bp.route('/webhook', methods=['POST'])
def webhook():
    """GitHub Webhook 처리"""
    config = load_config()
    current_secret = config.get("webhook_secret", "")

    if not config.get("webhook_enabled", False):
        return jsonify({"error": "Webhook disabled"}), 403

    if not current_secret:
        return jsonify({"error": "Webhook secret not configured"}), 403

    # GitHub 서명 검증
    signature = request.headers.get('X-Hub-Signature-256')
    if not git_build.verify_github_signature(request.data, signature, current_secret):
        git_build.log("Webhook: Invalid signature")
        return jsonify({"error": "Invalid signature"}), 401

    # 이벤트 타입 확인
    event_type = request.headers.get('X-GitHub-Event', 'unknown')

    if event_type == 'ping':
        git_build.log("Webhook: GitHub ping received")
        return jsonify({"message": "pong"})

    if event_type == 'push':
        if not www_folder:
            return jsonify({"error": "WWW folder not configured"}), 400

        # 비동기로 처리
        def callback(success, message):
            git_build.log(message)

        threading.Thread(
            target=git_build.handle_webhook,
            args=(www_folder, callback),
            daemon=True
        ).start()
        return jsonify({"message": "Build triggered"})

    return jsonify({"message": f"Event '{event_type}' ignored"})


# ============ SSL 인증서 다운로드 ============

@static_bp.route('/cert')
def download_cert():
    """SSL 인증서 다운로드 (iPad 설치용)"""
    import sys
    from pathlib import Path

    # exe 실행 시 exe 위치 기준
    if getattr(sys, 'frozen', False):
        app_dir = Path(sys.executable).parent
    else:
        app_dir = Path(__file__).parent.parent

    cert_file = app_dir / "certs" / "server.crt"

    if not cert_file.exists():
        return "인증서 파일이 없습니다. HTTPS 서버를 먼저 시작하세요.", 404

    return send_file(
        cert_file,
        mimetype='application/x-x509-ca-cert',
        as_attachment=True,
        download_name='haniwon-server.crt'
    )


# ============ 정적 파일 서빙 ============

@static_bp.route('/console')
def console():
    """관리 콘솔"""
    return Response(get_console_html(), mimetype='text/html')


@static_bp.route('/', defaults={'path': ''})
@static_bp.route('/<path:path>')
def serve(path):
    """정적 파일 서빙 (React SPA 지원)"""
    # API, webhook, console 경로는 제외
    if path.startswith('api/') or path == 'webhook' or path == 'console':
        return jsonify({"error": "Not found"}), 404

    if not www_folder:
        return "WWW folder not configured. Use GUI to set it.", 500

    # dist 폴더 우선
    dist_path = Path(www_folder) / "dist"
    if not dist_path.exists():
        dist_path = Path(www_folder)

    # 파일이 존재하면 서빙
    if path and (dist_path / path).exists():
        file_path = dist_path / path
        if file_path.is_file():
            ext = file_path.suffix.lower()
            mimetype = MIME_TYPES.get(ext)
            return send_from_directory(str(dist_path), path, mimetype=mimetype)

    # 그 외에는 index.html 반환 (SPA 라우팅)
    index_path = dist_path / 'index.html'
    if index_path.exists():
        return send_file(str(index_path), mimetype='text/html')

    return "index.html not found. Run 'Build' first.", 404
