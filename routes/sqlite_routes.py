"""
SQLite API 라우트
- 테이블 관리
- SQL 실행
- 백업
- Whisper 음성→텍스트 변환

CORS Preflight (OPTIONS) 요청을 명시적으로 처리
"""

import os
import tempfile
from flask import Blueprint, request, jsonify, Response, make_response
from services import sqlite_db
from config import VERSION, load_config

sqlite_bp = Blueprint('sqlite', __name__)


# ============ CORS 헬퍼 함수 ============

def add_cors_headers(response):
    """응답에 CORS 헤더 추가"""
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
    response.headers['Access-Control-Max-Age'] = '3600'
    return response


def cors_preflight_response():
    """OPTIONS preflight 요청에 대한 응답"""
    response = make_response()
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
    response.headers['Access-Control-Max-Age'] = '3600'
    return response


def json_response(data, status=200):
    """JSON 응답 + CORS 헤더"""
    response = make_response(jsonify(data), status)
    return add_cors_headers(response)


# ============ Blueprint 레벨 CORS 처리 ============

@sqlite_bp.after_request
def after_request(response):
    """모든 응답에 CORS 헤더 추가"""
    return add_cors_headers(response)


# ============ 웹 콘솔 HTML ============

def get_console_html():
    config = load_config()
    port = config.get('sqlite_api_port', 3200)
    return f'''<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SQLite API Console</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; min-height: 100vh; padding: 20px; }}
    h1 {{ color: #10b981; margin-bottom: 20px; }}
    h2 {{ color: #10b981; margin: 20px 0 10px; font-size: 1.1rem; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    .panel {{ background: #16213e; border-radius: 8px; padding: 20px; margin-bottom: 20px; }}
    .row {{ display: flex; gap: 20px; flex-wrap: wrap; }}
    .col {{ flex: 1; min-width: 300px; }}
    textarea {{ width: 100%; height: 120px; background: #0f0f23; border: 1px solid #333; border-radius: 4px; color: #0f0; font-family: 'Consolas', monospace; font-size: 14px; padding: 10px; resize: vertical; }}
    button {{ background: #10b981; color: #000; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: bold; margin: 10px 5px 10px 0; }}
    button:hover {{ background: #0d9668; }}
    button.danger {{ background: #ff4757; color: #fff; }}
    .result {{ background: #0f0f23; border-radius: 4px; padding: 15px; overflow-x: auto; max-height: 400px; overflow-y: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #333; }}
    th {{ background: #1a1a2e; color: #10b981; position: sticky; top: 0; }}
    tr:hover {{ background: rgba(16,185,129,0.1); }}
    .tables-list {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 15px; }}
    .table-btn {{ background: #2d2d44; padding: 8px 15px; border-radius: 4px; cursor: pointer; }}
    .table-btn:hover {{ background: #3d3d54; }}
    .table-btn.active {{ background: #10b981; color: #000; }}
    .status {{ padding: 10px; border-radius: 4px; margin-top: 10px; }}
    .status.success {{ background: #2d5a27; }}
    .status.error {{ background: #5a2727; }}
    input[type="text"] {{ background: #0f0f23; border: 1px solid #333; border-radius: 4px; color: #fff; padding: 8px 12px; }}
    .form-row {{ display: flex; align-items: center; margin-bottom: 10px; gap: 10px; }}
    .form-row label {{ min-width: 100px; }}
    .version {{ color: #666; font-size: 12px; margin-top: 20px; text-align: center; }}
  </style>
</head>
<body>
  <div class="container">
    <h1>SQLite API Console (Port {port})</h1>
    <div class="panel">
      <h2>Tables</h2>
      <div id="tablesList" class="tables-list">Loading...</div>
      <button onclick="refreshTables()">Refresh</button>
    </div>
    <div class="row">
      <div class="col">
        <div class="panel">
          <h2>SQL Query</h2>
          <textarea id="sqlQuery">SELECT * FROM sqlite_master WHERE type='table';</textarea>
          <button onclick="executeSQL()">Execute SQL</button>
          <div id="queryStatus"></div>
        </div>
      </div>
      <div class="col">
        <div class="panel">
          <h2>Quick Create Table</h2>
          <div class="form-row"><label>Table Name:</label><input type="text" id="newTableName" placeholder="users"></div>
          <textarea id="tableColumns">[{{"name": "id", "type": "INTEGER PRIMARY KEY AUTOINCREMENT"}}, {{"name": "name", "type": "TEXT NOT NULL"}}, {{"name": "email", "type": "TEXT"}}]</textarea>
          <button onclick="createTable()">Create Table</button>
          <div id="createStatus"></div>
        </div>
      </div>
    </div>
    <div class="panel">
      <h2>Results</h2>
      <div id="results" class="result">Run a query to see results</div>
    </div>
    <p class="version">SQLite API Server v{VERSION}</p>
  </div>
  <script>
    let currentTable = null;
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
    function renderTable(data) {{
      if (!data.columns || !data.columns.length) return '<p>No data</p>';
      let html = '<table><thead><tr>' + data.columns.map(c => '<th>'+c+'</th>').join('') + '</tr></thead><tbody>';
      if (data.rows) data.rows.forEach(row => {{ html += '<tr>' + data.columns.map((c,i) => '<td>'+(row[i]===null?'<i>NULL</i>':row[i])+'</td>').join('') + '</tr>'; }});
      return html + '</tbody></table>';
    }}
    async function refreshTables() {{
      const data = await api('/api/tables');
      const container = document.getElementById('tablesList');
      if (data.tables && data.tables.length) container.innerHTML = data.tables.map(t => '<div class="table-btn '+(t===currentTable?'active':'')+'" onclick="selectTable(\\''+t+'\\')">'+t+'</div>').join('') + '<button class="danger" onclick="dropCurrentTable()" style="margin-left:auto;">Drop</button>';
      else container.innerHTML = '<p>No tables</p>';
    }}
    async function selectTable(name) {{
      currentTable = name;
      document.getElementById('sqlQuery').value = 'SELECT * FROM "'+name+'" LIMIT 100;';
      refreshTables();
      await executeSQL();
    }}
    async function executeSQL() {{
      const sql = document.getElementById('sqlQuery').value;
      try {{
        const data = await api('/api/execute', {{ method: 'POST', body: JSON.stringify({{ sql }}) }});
        if (data.error) {{ showStatus('queryStatus', 'Error: '+data.error, true); document.getElementById('results').innerHTML = '<pre style="color:#ff4757">'+data.error+'</pre>'; }}
        else {{ showStatus('queryStatus', data.message || 'Success'); document.getElementById('results').innerHTML = renderTable(data); refreshTables(); }}
      }} catch(e) {{ showStatus('queryStatus', 'Error: '+e.message, true); }}
    }}
    async function createTable() {{
      const name = document.getElementById('newTableName').value;
      if (!name) {{ showStatus('createStatus', 'Enter table name', true); return; }}
      try {{
        const columns = JSON.parse(document.getElementById('tableColumns').value);
        const sql = 'CREATE TABLE "'+name+'" ('+columns.map(c => c.name + ' ' + c.type).join(', ')+')';
        const data = await api('/api/execute', {{ method: 'POST', body: JSON.stringify({{ sql }}) }});
        if (data.error) showStatus('createStatus', 'Error: '+data.error, true);
        else {{ showStatus('createStatus', 'Table created'); refreshTables(); }}
      }} catch(e) {{ showStatus('createStatus', 'Error: '+e.message, true); }}
    }}
    async function dropCurrentTable() {{
      if (!currentTable || !confirm('Drop "'+currentTable+'"?')) return;
      const data = await api('/api/execute', {{ method: 'POST', body: JSON.stringify({{ sql: 'DROP TABLE "'+currentTable+'"' }}) }});
      if (data.error) alert('Error: '+data.error);
      else {{ currentTable = null; document.getElementById('results').innerHTML = 'Dropped'; refreshTables(); }}
    }}
    refreshTables();
  </script>
</body>
</html>'''


# ============ 라우트 ============

@sqlite_bp.route('/')
def index():
    return Response(get_console_html(), mimetype='text/html')


@sqlite_bp.route('/api/info', methods=['GET', 'OPTIONS'])
def api_info():
    if request.method == 'OPTIONS':
        return cors_preflight_response()
    return json_response({
        "version": VERSION,
        "type": "sqlite",
        "status": "running",
        "db_path": sqlite_db.get_db_path(),
        "sql_logging": sqlite_db.is_sql_logging_enabled()
    })


@sqlite_bp.route('/api/logging', methods=['GET', 'POST', 'OPTIONS'])
def toggle_logging():
    """SQL 로깅 on/off 토글"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    if request.method == 'GET':
        return json_response({
            "sql_logging": sqlite_db.is_sql_logging_enabled()
        })

    # POST: 로깅 상태 변경
    data = request.get_json() or {}
    enabled = data.get('enabled')

    if enabled is None:
        # 토글
        enabled = not sqlite_db.is_sql_logging_enabled()

    sqlite_db.set_sql_logging(enabled)
    return json_response({
        "sql_logging": sqlite_db.is_sql_logging_enabled(),
        "message": f"SQL 로깅 {'활성화' if enabled else '비활성화'}됨"
    })


@sqlite_bp.route('/api/health', methods=['GET', 'OPTIONS'])
def health_check():
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    sqlite_ok = False
    try:
        result = sqlite_db.test_connection()
        sqlite_ok = result.get('success', False)
    except:
        pass
    return json_response({"status": "ok", "sqlite_connected": sqlite_ok})


@sqlite_bp.route('/api/tables', methods=['GET', 'OPTIONS'])
def get_tables():
    """테이블 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        db_path = sqlite_db.get_db_path()
        if not db_path:
            return json_response({"tables": [], "error": "SQLite DB not configured"})

        conn = sqlite_db.get_connection()
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return json_response({"tables": tables})
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@sqlite_bp.route('/api/tables/<name>/schema', methods=['GET', 'OPTIONS'])
def get_table_schema(name):
    """테이블 스키마 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        conn = sqlite_db.get_connection()
        cursor = conn.execute(f'PRAGMA table_info("{name}")')
        columns = []
        for row in cursor.fetchall():
            columns.append({
                "cid": row[0],
                "name": row[1],
                "type": row[2],
                "notnull": row[3],
                "default_value": row[4],
                "pk": row[5]
            })
        conn.close()
        return json_response({"columns": columns})
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@sqlite_bp.route('/api/tables/<name>', methods=['GET', 'OPTIONS'])
def get_table_data(name):
    """테이블 데이터 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))

        conn = sqlite_db.get_connection()
        cursor = conn.execute(f'SELECT * FROM "{name}" LIMIT ? OFFSET ?', (limit, offset))
        columns = [description[0] for description in cursor.description] if cursor.description else []
        rows = [list(row) for row in cursor.fetchall()]

        # 전체 개수
        count_cursor = conn.execute(f'SELECT COUNT(*) FROM "{name}"')
        total = count_cursor.fetchone()[0]

        conn.close()
        return json_response({
            "columns": columns,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@sqlite_bp.route('/api/execute', methods=['POST', 'OPTIONS'])
@sqlite_bp.route('/api/sqlite/execute', methods=['POST', 'OPTIONS'])
def execute():
    """SQLite SQL 명령어 실행 (OPTIONS preflight 명시적 처리)"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        db_path = sqlite_db.get_db_path()
        if not db_path:
            return json_response({"error": "SQLite DB not configured"}, 400)

        data = request.get_json()
        sql_query = data.get('sql', '').strip()

        if not sql_query:
            return json_response({"error": "SQL query is required"}, 400)

        conn = sqlite_db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)

        if sql_query.upper().startswith('SELECT') or sql_query.upper().startswith('PRAGMA'):
            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            rows_list = [list(row) for row in rows]
            conn.close()
            return json_response({
                "columns": columns,
                "rows": rows_list,
                "message": f"Found {len(rows_list)} rows"
            })
        else:
            conn.commit()
            affected = cursor.rowcount
            last_id = cursor.lastrowid
            conn.close()
            return json_response({
                "success": True,
                "affected_rows": affected,
                "lastrowid": last_id,
                "message": f"Query executed. {affected} rows affected."
            })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@sqlite_bp.route('/api/backup', methods=['POST', 'OPTIONS'])
def manual_backup():
    """SQLite 수동 백업"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        result = sqlite_db.do_backup()
        if result:
            return json_response({"success": True, "message": "Backup created"})
        else:
            return json_response({"error": "Backup failed or not configured"}, 400)
    except Exception as e:
        return json_response({"error": str(e)}, 400)


# ============ CRUD 헬퍼 엔드포인트 ============

@sqlite_bp.route('/api/tables/<name>/insert', methods=['POST', 'OPTIONS'])
def insert_row(name):
    """테이블에 행 삽입"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json()
        if not data:
            return json_response({"error": "Data is required"}, 400)

        columns = ', '.join(f'"{k}"' for k in data.keys())
        placeholders = ', '.join('?' for _ in data.keys())
        values = list(data.values())

        sql = f'INSERT INTO "{name}" ({columns}) VALUES ({placeholders})'

        conn = sqlite_db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
        last_id = cursor.lastrowid
        conn.close()

        return json_response({
            "success": True,
            "id": last_id,
            "message": f"Row inserted with id {last_id}"
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@sqlite_bp.route('/api/tables/<name>/update', methods=['POST', 'OPTIONS'])
def update_row(name):
    """테이블 행 업데이트"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json()
        if not data or 'where' not in data or 'set' not in data:
            return json_response({"error": "Both 'where' and 'set' are required"}, 400)

        set_clause = ', '.join(f'"{k}" = ?' for k in data['set'].keys())
        where_clause = ' AND '.join(f'"{k}" = ?' for k in data['where'].keys())
        values = list(data['set'].values()) + list(data['where'].values())

        sql = f'UPDATE "{name}" SET {set_clause} WHERE {where_clause}'

        conn = sqlite_db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
        affected = cursor.rowcount
        conn.close()

        return json_response({
            "success": True,
            "affected_rows": affected,
            "message": f"{affected} rows updated"
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@sqlite_bp.route('/api/tables/<name>/delete', methods=['POST', 'OPTIONS'])
def delete_row(name):
    """테이블 행 삭제"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json()
        if not data or 'where' not in data:
            return json_response({"error": "'where' condition is required"}, 400)

        where_clause = ' AND '.join(f'"{k}" = ?' for k in data['where'].keys())
        values = list(data['where'].values())

        sql = f'DELETE FROM "{name}" WHERE {where_clause}'

        conn = sqlite_db.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
        affected = cursor.rowcount
        conn.close()

        return json_response({
            "success": True,
            "affected_rows": affected,
            "message": f"{affected} rows deleted"
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


# ============ Whisper 음성→텍스트 변환 ============

@sqlite_bp.route('/api/whisper/transcribe', methods=['POST', 'OPTIONS'])
def whisper_transcribe():
    """OpenAI Whisper API로 음성→텍스트 변환

    Request:
        - audio: 오디오 파일 (multipart/form-data)
        - language: 언어 코드 (기본: ko)
        - prompt: 문맥 힌트 (선택)

    Response:
        - transcript: 변환된 텍스트
        - duration: 오디오 길이 (초)
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        # OpenAI API 키 확인
        config = load_config()
        api_key = config.get('openai_api_key') or os.environ.get('OPENAI_API_KEY')

        if not api_key:
            return json_response({
                "error": "OpenAI API key not configured. Set 'openai_api_key' in config or OPENAI_API_KEY env var."
            }, 400)

        # 파일 확인
        if 'audio' not in request.files:
            return json_response({"error": "No audio file provided"}, 400)

        audio_file = request.files['audio']
        if audio_file.filename == '':
            return json_response({"error": "No audio file selected"}, 400)

        # 파라미터
        language = request.form.get('language', 'ko')
        prompt = request.form.get('prompt', '')

        # 임시 파일로 저장
        suffix = os.path.splitext(audio_file.filename)[1] or '.webm'
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            audio_file.save(tmp.name)
            tmp_path = tmp.name

        try:
            # OpenAI API 호출
            import requests as req

            with open(tmp_path, 'rb') as f:
                response = req.post(
                    'https://api.openai.com/v1/audio/transcriptions',
                    headers={
                        'Authorization': f'Bearer {api_key}'
                    },
                    files={
                        'file': (audio_file.filename or 'audio.webm', f, 'audio/webm')
                    },
                    data={
                        'model': 'whisper-1',
                        'language': language,
                        'prompt': prompt,
                        'response_format': 'verbose_json'
                    }
                )

            if response.status_code != 200:
                error_msg = response.json().get('error', {}).get('message', 'Unknown error')
                return json_response({
                    "error": f"Whisper API error: {error_msg}"
                }, response.status_code)

            result = response.json()

            return json_response({
                "success": True,
                "transcript": result.get('text', ''),
                "duration": result.get('duration', 0),
                "language": result.get('language', language)
            })

        finally:
            # 임시 파일 삭제
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except ImportError:
        return json_response({
            "error": "requests library not installed. Run: pip install requests"
        }, 500)
    except Exception as e:
        sqlite_db.log(f"Whisper 오류: {str(e)}")
        return json_response({"error": str(e)}, 500)


# ============ GPT SOAP 변환 ============

@sqlite_bp.route('/api/gpt/soap', methods=['POST', 'OPTIONS'])
def gpt_soap():
    """GPT로 진료 녹취록을 SOAP 형식으로 변환

    Request:
        - transcript: 녹취록 텍스트
        - acting_type: 진료 종류 (침치료, 추나, 약상담 등)
        - patient_info: 환자 정보 (선택, 참고용)

    Response:
        - subjective: 주관적 증상 (환자 호소)
        - objective: 객관적 소견 (검사/관찰)
        - assessment: 평가/진단
        - plan: 치료 계획
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        config = load_config()
        api_key = config.get('openai_api_key') or os.environ.get('OPENAI_API_KEY')

        if not api_key:
            return json_response({
                "error": "OpenAI API key not configured."
            }, 400)

        data = request.get_json() or {}
        transcript = data.get('transcript', '')
        acting_type = data.get('acting_type', '진료')
        patient_info = data.get('patient_info', '')

        if not transcript or len(transcript.strip()) < 10:
            return json_response({
                "error": "Transcript too short for SOAP conversion"
            }, 400)

        # GPT 프롬프트
        system_prompt = """당신은 한의원 진료 기록을 SOAP 형식으로 정리하는 전문가입니다.
주어진 진료 녹취록을 분석하여 SOAP 형식으로 변환해주세요.

출력 형식 (JSON):
{
  "subjective": "환자가 호소하는 증상, 불편함, 병력 등 (환자 말 인용)",
  "objective": "의사의 관찰, 검사 소견, 촉진/시진 결과 등",
  "assessment": "진단명, 상태 평가, 변증 등",
  "plan": "치료 계획, 처방, 다음 예약, 생활 지도 등"
}

주의사항:
- 녹취록에 없는 내용은 추측하지 말고 해당 항목을 비워두세요
- 한의학 용어와 일반 용어를 적절히 혼용하세요
- 간결하고 명확하게 작성하세요
- 반드시 유효한 JSON 형식으로 응답하세요"""

        user_prompt = f"""진료 종류: {acting_type}
{f'환자 정보: {patient_info}' if patient_info else ''}

녹취록:
{transcript}"""

        import requests as req

        response = req.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'gpt-4o-mini',
                'messages': [
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': user_prompt}
                ],
                'temperature': 0.3,
                'response_format': {'type': 'json_object'}
            }
        )

        if response.status_code != 200:
            error_msg = response.json().get('error', {}).get('message', 'Unknown error')
            return json_response({
                "error": f"GPT API error: {error_msg}"
            }, response.status_code)

        result = response.json()
        content = result.get('choices', [{}])[0].get('message', {}).get('content', '{}')

        import json
        soap_data = json.loads(content)

        return json_response({
            "success": True,
            "subjective": soap_data.get('subjective', ''),
            "objective": soap_data.get('objective', ''),
            "assessment": soap_data.get('assessment', ''),
            "plan": soap_data.get('plan', ''),
            "usage": result.get('usage', {})
        })

    except json.JSONDecodeError as e:
        return json_response({
            "error": f"Failed to parse GPT response as JSON: {str(e)}"
        }, 500)
    except Exception as e:
        sqlite_db.log(f"GPT SOAP 오류: {str(e)}")
        return json_response({"error": str(e)}, 500)


# ============ GPT Chat (범용) ============

@sqlite_bp.route('/api/gpt/chat', methods=['POST', 'OPTIONS'])
def gpt_chat():
    """GPT Chat API (범용 채팅/분석용)

    Request:
        - messages: 메시지 배열 [{"role": "system/user/assistant", "content": "..."}]
        - temperature: 온도 (기본: 0.7)
        - model: 모델명 (기본: gpt-4o-mini)

    Response:
        - content: GPT 응답 텍스트
        - usage: 토큰 사용량
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        config = load_config()
        api_key = config.get('openai_api_key') or os.environ.get('OPENAI_API_KEY')

        if not api_key:
            return json_response({
                "error": "OpenAI API key not configured."
            }, 400)

        data = request.get_json() or {}
        messages = data.get('messages', [])
        temperature = data.get('temperature', 0.7)
        model = data.get('model', 'gpt-4o-mini')

        if not messages:
            return json_response({
                "error": "messages array is required"
            }, 400)

        import requests as req

        response = req.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': model,
                'messages': messages,
                'temperature': temperature
            }
        )

        if response.status_code != 200:
            error_msg = response.json().get('error', {}).get('message', 'Unknown error')
            return json_response({
                "error": f"GPT API error: {error_msg}"
            }, response.status_code)

        result = response.json()
        content = result.get('choices', [{}])[0].get('message', {}).get('content', '')

        return json_response({
            "success": True,
            "content": content,
            "message": content,  # 호환성
            "usage": result.get('usage', {})
        })

    except Exception as e:
        sqlite_db.log(f"GPT Chat 오류: {str(e)}")
        return json_response({"error": str(e)}, 500)


# ============ GPT 화자 분리 (Diarization) ============

@sqlite_bp.route('/api/gpt/diarize', methods=['POST', 'OPTIONS'])
def gpt_diarize():
    """GPT로 녹취록 화자 분리

    Request:
        - transcript: 녹취록 텍스트
        - acting_type: 진료 종류 (선택)

    Response:
        - formatted: 화자 분리된 텍스트 ([의사] ... [환자] ...)
        - utterances: [{speaker: 'doctor'|'patient', text: '...'}]
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        config = load_config()
        api_key = config.get('openai_api_key') or os.environ.get('OPENAI_API_KEY')

        if not api_key:
            return json_response({
                "error": "OpenAI API key not configured."
            }, 400)

        data = request.get_json() or {}
        transcript = data.get('transcript', '')
        acting_type = data.get('acting_type', '진료')

        if not transcript or len(transcript.strip()) < 10:
            return json_response({
                "error": "Transcript too short for diarization"
            }, 400)

        system_prompt = """당신은 한의원 진료 녹취록을 분석하는 전문가입니다.
주어진 녹취록을 의사와 환자의 대화로 분리해주세요.

규칙:
1. 질문하거나 설명하는 쪽이 의사입니다
2. 증상을 호소하거나 답변하는 쪽이 환자입니다
3. 각 발화를 [의사] 또는 [환자] 태그로 시작해주세요
4. 원문을 최대한 유지하되, 자연스럽게 대화 단위로 나눠주세요

출력 형식:
[의사] 어디가 불편하세요?
[환자] 허리가 아파요.
[의사] 언제부터 아프셨어요?
..."""

        user_prompt = f"진료 유형: {acting_type}\n\n녹취록:\n{transcript}"

        import requests as req

        response = req.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'gpt-4o-mini',
                'messages': [
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': user_prompt}
                ],
                'temperature': 0.3
            }
        )

        if response.status_code != 200:
            error_msg = response.json().get('error', {}).get('message', 'Unknown error')
            return json_response({
                "error": f"GPT API error: {error_msg}"
            }, response.status_code)

        result = response.json()
        content = result.get('choices', [{}])[0].get('message', {}).get('content', '')

        # 파싱: [의사], [환자] 태그 추출
        utterances = []
        for line in content.split('\n'):
            trimmed = line.strip()
            if not trimmed:
                continue
            if trimmed.startswith('[의사]'):
                utterances.append({
                    'speaker': 'doctor',
                    'text': trimmed.replace('[의사]', '').strip()
                })
            elif trimmed.startswith('[환자]'):
                utterances.append({
                    'speaker': 'patient',
                    'text': trimmed.replace('[환자]', '').strip()
                })

        return json_response({
            "success": True,
            "formatted": content,
            "utterances": utterances,
            "usage": result.get('usage', {})
        })

    except Exception as e:
        sqlite_db.log(f"GPT Diarize 오류: {str(e)}")
        return json_response({"error": str(e)}, 500)


# ============ MSSQL Waiting → SQLite 동기화 ============

def _ensure_waiting_queue_columns():
    """waiting_queue 테이블에 MSSQL 동기화용 컬럼 추가"""
    conn = None
    try:
        conn = sqlite_db.get_connection()
        cursor = conn.cursor()

        # 기존 컬럼 확인
        cursor.execute("PRAGMA table_info(waiting_queue)")
        columns = {row[1] for row in cursor.fetchall()}

        # 필요한 컬럼 추가
        if 'mssql_waiting_pk' not in columns:
            cursor.execute("ALTER TABLE waiting_queue ADD COLUMN mssql_waiting_pk INTEGER")
        if 'mssql_intotime' not in columns:
            cursor.execute("ALTER TABLE waiting_queue ADD COLUMN mssql_intotime TEXT")
        if 'synced_at' not in columns:
            cursor.execute("ALTER TABLE waiting_queue ADD COLUMN synced_at TEXT")
        if 'chart_number' not in columns:
            cursor.execute("ALTER TABLE waiting_queue ADD COLUMN chart_number TEXT")
        if 'patient_name' not in columns:
            cursor.execute("ALTER TABLE waiting_queue ADD COLUMN patient_name TEXT")
        if 'age' not in columns:
            cursor.execute("ALTER TABLE waiting_queue ADD COLUMN age INTEGER")
        if 'sex' not in columns:
            cursor.execute("ALTER TABLE waiting_queue ADD COLUMN sex TEXT")

        conn.commit()
        return True
    except Exception as e:
        sqlite_db.log(f"waiting_queue 컬럼 추가 오류: {e}")
        return False
    finally:
        if conn:
            conn.close()


@sqlite_bp.route('/api/waiting-queue/sync', methods=['POST', 'OPTIONS'])
def sync_waiting_queue():
    """MSSQL Waiting+Treating 데이터를 SQLite waiting_queue에 동기화

    Request Body:
        - waiting: MSSQL Waiting+Treating 테이블 데이터 배열
          [{id, patient_id, chart_no, patient_name, age, sex, waiting_since, doctor, ...}, ...]

    Response:
        - added: 추가된 환자 수
        - skipped_duplicate: 이미 있어서 스킵된 수 (같은 patient_id + intotime)
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    conn = None
    try:
        # 컬럼 확인/추가
        _ensure_waiting_queue_columns()

        data = request.get_json() or {}
        mssql_waiting = data.get('waiting', [])

        if not mssql_waiting:
            return json_response({
                "added": 0,
                "skipped_duplicate": 0,
                "message": "No waiting data provided"
            })

        conn = sqlite_db.get_connection()
        cursor = conn.cursor()

        # 현재 waiting_queue에 있는 patient_id 조회 (중복 방지)
        cursor.execute("""
            SELECT patient_id FROM waiting_queue
            WHERE queue_type = 'treatment'
        """)
        existing_patient_ids = {row[0] for row in cursor.fetchall()}

        # 현재 최대 position 조회
        cursor.execute("SELECT MAX(position) FROM waiting_queue WHERE queue_type = 'treatment'")
        max_pos_row = cursor.fetchone()
        next_position = (max_pos_row[0] or -1) + 1

        added = 0
        skipped_duplicate = 0

        from datetime import datetime
        now = datetime.now().isoformat()

        for patient in mssql_waiting:
            patient_id = patient.get('patient_id')
            intotime = patient.get('waiting_since') or patient.get('intotime')

            if not patient_id:
                continue

            # 이미 있으면 스킵 (같은 patient_id)
            if patient_id in existing_patient_ids:
                skipped_duplicate += 1
                continue

            # INSERT OR IGNORE: UNIQUE 제약 있어도 에러 없이 스킵
            cursor.execute("""
                INSERT OR IGNORE INTO waiting_queue
                (patient_id, queue_type, details, position, doctor,
                 mssql_waiting_pk, mssql_intotime, synced_at,
                 chart_number, patient_name, age, sex)
                VALUES (?, 'treatment', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                patient_id,
                patient.get('status') or '',
                next_position,
                patient.get('doctor') or '',
                patient.get('id'),  # mssql_waiting_pk
                intotime,
                now,
                patient.get('chart_no') or '',
                patient.get('patient_name') or '',
                patient.get('age'),
                patient.get('sex') or ''
            ))

            # INSERT OR IGNORE는 rowcount로 실제 삽입 여부 확인
            if cursor.rowcount > 0:
                next_position += 1
                added += 1
                existing_patient_ids.add(patient_id)

        conn.commit()

        return json_response({
            "success": True,
            "added": added,
            "skipped_duplicate": skipped_duplicate,
            "message": f"동기화 완료: {added}명 추가"
        })

    except Exception as e:
        sqlite_db.log(f"waiting_queue 동기화 오류: {e}")
        return json_response({"error": str(e)}, 500)
    finally:
        if conn:
            conn.close()
