"""
PostgreSQL API 라우트
- 테이블 관리
- SQL 실행
- CRUD 헬퍼
"""

import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from flask import Blueprint, request, jsonify, Response, make_response, stream_with_context
from services import postgres_db
from config import VERSION, load_config

postgres_bp = Blueprint('postgres', __name__)


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

@postgres_bp.after_request
def after_request(response):
    """모든 응답에 CORS 헤더 추가"""
    return add_cors_headers(response)


# ============ 웹 콘솔 HTML ============

def get_console_html():
    config = load_config()
    port = config.get('postgres_api_port', 3200)
    return f'''<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PostgreSQL API Console</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; min-height: 100vh; padding: 20px; }}
    h1 {{ color: #3b82f6; margin-bottom: 20px; }}
    h2 {{ color: #3b82f6; margin: 20px 0 10px; font-size: 1.1rem; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    .panel {{ background: #16213e; border-radius: 8px; padding: 20px; margin-bottom: 20px; }}
    .row {{ display: flex; gap: 20px; flex-wrap: wrap; }}
    .col {{ flex: 1; min-width: 300px; }}
    textarea {{ width: 100%; height: 120px; background: #0f0f23; border: 1px solid #333; border-radius: 4px; color: #0f0; font-family: 'Consolas', monospace; font-size: 14px; padding: 10px; resize: vertical; }}
    button {{ background: #3b82f6; color: #fff; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: bold; margin: 10px 5px 10px 0; }}
    button:hover {{ background: #2563eb; }}
    button.danger {{ background: #ff4757; color: #fff; }}
    .result {{ background: #0f0f23; border-radius: 4px; padding: 15px; overflow-x: auto; max-height: 400px; overflow-y: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #333; }}
    th {{ background: #1a1a2e; color: #3b82f6; position: sticky; top: 0; }}
    tr:hover {{ background: rgba(59,130,246,0.1); }}
    .tables-list {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 15px; }}
    .table-btn {{ background: #2d2d44; padding: 8px 15px; border-radius: 4px; cursor: pointer; }}
    .table-btn:hover {{ background: #3d3d54; }}
    .table-btn.active {{ background: #3b82f6; color: #fff; }}
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
    <h1>PostgreSQL API Console (Port {port})</h1>
    <div class="panel">
      <h2>Tables</h2>
      <div id="tablesList" class="tables-list">Loading...</div>
      <button onclick="refreshTables()">Refresh</button>
    </div>
    <div class="row">
      <div class="col">
        <div class="panel">
          <h2>SQL Query</h2>
          <textarea id="sqlQuery">SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';</textarea>
          <button onclick="executeSQL()">Execute SQL</button>
          <div id="queryStatus"></div>
        </div>
      </div>
      <div class="col">
        <div class="panel">
          <h2>Quick Create Table</h2>
          <div class="form-row"><label>Table Name:</label><input type="text" id="newTableName" placeholder="users"></div>
          <textarea id="tableColumns">[{{"name": "id", "type": "SERIAL PRIMARY KEY"}}, {{"name": "name", "type": "VARCHAR(100) NOT NULL"}}, {{"name": "email", "type": "VARCHAR(255)"}}]</textarea>
          <button onclick="createTable()">Create Table</button>
          <div id="createStatus"></div>
        </div>
      </div>
    </div>
    <div class="panel">
      <h2>Results</h2>
      <div id="results" class="result">Run a query to see results</div>
    </div>
    <p class="version">PostgreSQL API Server v{VERSION}</p>
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
      if (data.rows) data.rows.forEach(row => {{ html += '<tr>' + data.columns.map(c => '<td>'+(row[c]===null?'<i>NULL</i>':row[c])+'</td>').join('') + '</tr>'; }});
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

@postgres_bp.route('/')
def index():
    return Response(get_console_html(), mimetype='text/html')


@postgres_bp.route('/api/info', methods=['GET', 'OPTIONS'])
def api_info():
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    config = postgres_db.get_db_config()
    return json_response({
        "version": VERSION,
        "type": "postgresql",
        "status": "running",
        "host": config.get('host', ''),
        "database": config.get('database', ''),
        "sql_logging": postgres_db.is_sql_logging_enabled()
    })


@postgres_bp.route('/api/logging', methods=['GET', 'POST', 'OPTIONS'])
def toggle_logging():
    """SQL 로깅 on/off 토글"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    if request.method == 'GET':
        return json_response({
            "sql_logging": postgres_db.is_sql_logging_enabled()
        })

    # POST: 로깅 상태 변경
    data = request.get_json() or {}
    enabled = data.get('enabled')

    if enabled is None:
        # 토글
        enabled = not postgres_db.is_sql_logging_enabled()

    postgres_db.set_sql_logging(enabled)
    return json_response({
        "sql_logging": postgres_db.is_sql_logging_enabled(),
        "message": f"SQL 로깅 {'활성화' if enabled else '비활성화'}됨"
    })


@postgres_bp.route('/api/health', methods=['GET', 'OPTIONS'])
def health_check():
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    pg_ok = False
    try:
        result = postgres_db.test_connection()
        pg_ok = result.get('success', False)
    except:
        pass
    return json_response({"status": "ok", "postgres_connected": pg_ok})


@postgres_bp.route('/api/tables', methods=['GET', 'OPTIONS'])
def get_tables():
    """테이블 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        results = postgres_db.get_tables()
        tables = [row['table_name'] for row in results]
        return json_response({"tables": tables})
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@postgres_bp.route('/api/tables/<name>/schema', methods=['GET', 'OPTIONS'])
def get_table_schema(name):
    """테이블 스키마 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        columns = postgres_db.get_table_columns(name)
        return json_response({"columns": columns})
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@postgres_bp.route('/api/tables/<name>', methods=['GET', 'OPTIONS'])
def get_table_data(name):
    """테이블 데이터 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))

        # 데이터 조회
        query = f'SELECT * FROM "{name}" LIMIT %s OFFSET %s'
        rows = postgres_db.execute_query(query, (limit, offset))

        # 컬럼명 추출
        columns = list(rows[0].keys()) if rows else []

        # 전체 개수
        count_result = postgres_db.execute_query(f'SELECT COUNT(*) as cnt FROM "{name}"')
        total = count_result[0]['cnt'] if count_result else 0

        return json_response({
            "columns": columns,
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@postgres_bp.route('/api/execute', methods=['POST', 'OPTIONS'])
def execute():
    """PostgreSQL SQL 명령어 실행"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json()
        sql_query = data.get('sql', '').strip()

        if not sql_query:
            return json_response({"error": "SQL query is required"}, 400)

        # SELECT/WITH 쿼리인지 확인
        upper_sql = sql_query.upper()
        is_select = upper_sql.startswith('SELECT') or upper_sql.startswith('WITH')

        if is_select:
            rows = postgres_db.execute_query(sql_query)
            columns = list(rows[0].keys()) if rows else []
            return json_response({
                "columns": columns,
                "rows": rows,
                "message": f"Found {len(rows)} rows"
            })
        else:
            affected = postgres_db.execute_query(sql_query, fetch=False)
            return json_response({
                "success": True,
                "affected_rows": affected,
                "message": f"Query executed. {affected} rows affected."
            })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


# ============ CRUD 헬퍼 엔드포인트 ============

@postgres_bp.route('/api/tables/<name>/insert', methods=['POST', 'OPTIONS'])
def insert_row(name):
    """테이블에 행 삽입"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json()
        if not data:
            return json_response({"error": "Data is required"}, 400)

        columns = ', '.join(f'"{k}"' for k in data.keys())
        placeholders = ', '.join('%s' for _ in data.keys())
        values = list(data.values())

        sql = f'INSERT INTO "{name}" ({columns}) VALUES ({placeholders}) RETURNING id'

        try:
            result = postgres_db.execute_query(sql, tuple(values))
            last_id = result[0]['id'] if result else None
        except:
            # RETURNING이 실패하면 (id 컬럼 없음) 일반 INSERT
            sql = f'INSERT INTO "{name}" ({columns}) VALUES ({placeholders})'
            postgres_db.execute_query(sql, tuple(values), fetch=False)
            last_id = None

        return json_response({
            "success": True,
            "id": last_id,
            "message": f"Row inserted" + (f" with id {last_id}" if last_id else "")
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@postgres_bp.route('/api/tables/<name>/update', methods=['POST', 'OPTIONS'])
def update_row(name):
    """테이블 행 업데이트"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json()
        if not data or 'where' not in data or 'set' not in data:
            return json_response({"error": "Both 'where' and 'set' are required"}, 400)

        set_clause = ', '.join(f'"{k}" = %s' for k in data['set'].keys())
        where_clause = ' AND '.join(f'"{k}" = %s' for k in data['where'].keys())
        values = list(data['set'].values()) + list(data['where'].values())

        sql = f'UPDATE "{name}" SET {set_clause} WHERE {where_clause}'
        affected = postgres_db.execute_query(sql, tuple(values), fetch=False)

        return json_response({
            "success": True,
            "affected_rows": affected,
            "message": f"{affected} rows updated"
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


@postgres_bp.route('/api/tables/<name>/delete', methods=['POST', 'OPTIONS'])
def delete_row(name):
    """테이블 행 삭제"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json()
        if not data or 'where' not in data:
            return json_response({"error": "'where' condition is required"}, 400)

        where_clause = ' AND '.join(f'"{k}" = %s' for k in data['where'].keys())
        values = list(data['where'].values())

        sql = f'DELETE FROM "{name}" WHERE {where_clause}'
        affected = postgres_db.execute_query(sql, tuple(values), fetch=False)

        return json_response({
            "success": True,
            "affected_rows": affected,
            "message": f"{affected} rows deleted"
        })
    except Exception as e:
        return json_response({"error": str(e)}, 400)


# ============ SSE (Server-Sent Events) 실시간 스트림 ============

@postgres_bp.route('/api/sse-test', methods=['GET'])
def sse_test():
    """SSE 기본 테스트 (DB 연결 없이)"""
    def event_stream():
        import time
        yield f"data: {{\"type\": \"connected\", \"message\": \"SSE test\"}}\n\n"
        for i in range(3):
            time.sleep(1)
            yield f"data: {{\"count\": {i}}}\n\n"
        yield f"data: {{\"type\": \"done\"}}\n\n"

    response = Response(event_stream(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    return add_cors_headers(response)


@postgres_bp.route('/api/subscribe', methods=['GET', 'OPTIONS'])
def subscribe_all():
    """모든 테이블 변경사항 구독 (SSE)"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    # DB 연결을 먼저 테스트
    config = postgres_db.get_db_config()
    postgres_db.log(f"[SSE] 연결 시도: {config.get('host')}:{config.get('port')}", force=True)

    try:
        test_conn = psycopg2.connect(
            host=config.get('host', 'localhost'),
            port=config.get('port', 5432),
            user=config.get('user', ''),
            password=config.get('password', ''),
            database=config.get('database', ''),
            connect_timeout=5
        )
        test_conn.close()
        postgres_db.log("[SSE] DB 연결 테스트 성공", force=True)
    except Exception as e:
        postgres_db.log(f"[SSE] DB 연결 테스트 실패: {str(e)}", force=True)
        return json_response({"error": f"DB connection failed: {str(e)}"}, 500)

    def event_stream():
        # 즉시 첫 메시지 전송 (Waitress 호환)
        yield f": SSE stream started\n\n"

        conn = None
        cur = None
        try:
            conn = psycopg2.connect(
                host=config.get('host', 'localhost'),
                port=config.get('port', 5432),
                user=config.get('user', ''),
                password=config.get('password', ''),
                database=config.get('database', ''),
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("LISTEN table_changes")
            postgres_db.log("[SSE] LISTEN 성공", force=True)

            # 연결 성공 알림
            yield f"data: {{\"type\": \"connected\", \"message\": \"SSE connected\"}}\n\n"

            last_keepalive = time.time()
            while True:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    yield f"data: {notify.payload}\n\n"

                if time.time() - last_keepalive >= 5:
                    yield f": keepalive\n\n"
                    last_keepalive = time.time()

                time.sleep(0.1)
        except GeneratorExit:
            postgres_db.log("[SSE] 클라이언트 연결 종료", force=True)
        except Exception as e:
            postgres_db.log(f"[SSE] 에러: {str(e)}", force=True)
            yield f"data: {{\"type\": \"error\", \"message\": \"{str(e)}\"}}\n\n"
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    postgres_db.log("[SSE] Response 생성 중", force=True)
    response = Response(event_stream(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    postgres_db.log("[SSE] Response 반환", force=True)
    return add_cors_headers(response)


@postgres_bp.route('/api/subscribe/<table>', methods=['GET', 'OPTIONS'])
def subscribe_table(table):
    """특정 테이블 변경사항만 구독 (SSE)"""
    import json as json_lib

    if request.method == 'OPTIONS':
        return cors_preflight_response()

    # DB 연결을 먼저 테스트
    config = postgres_db.get_db_config()
    try:
        test_conn = psycopg2.connect(
            host=config.get('host', 'localhost'),
            port=config.get('port', 5432),
            user=config.get('user', ''),
            password=config.get('password', ''),
            database=config.get('database', ''),
            connect_timeout=5
        )
        test_conn.close()
    except Exception as e:
        postgres_db.log(f"[SSE] DB 연결 테스트 실패: {str(e)}", force=True)
        return json_response({"error": f"DB connection failed: {str(e)}"}, 500)

    def event_stream():
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(
                host=config.get('host', 'localhost'),
                port=config.get('port', 5432),
                user=config.get('user', ''),
                password=config.get('password', ''),
                database=config.get('database', ''),
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("LISTEN table_changes")

            yield f"data: {{\"type\": \"connected\", \"table\": \"{table}\"}}\n\n"

            last_keepalive = time.time()
            while True:
                # Windows 호환: select 대신 poll + sleep 사용
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    try:
                        payload = json_lib.loads(notify.payload)
                        # 해당 테이블만 필터링
                        if payload.get('table') == table:
                            yield f"data: {notify.payload}\n\n"
                    except:
                        pass

                # 5초마다 keepalive 전송
                if time.time() - last_keepalive >= 5:
                    yield f": keepalive\n\n"
                    last_keepalive = time.time()

                time.sleep(0.1)  # 100ms 간격으로 확인
        except GeneratorExit:
            postgres_db.log("[SSE] 클라이언트 연결 종료", force=True)
        except Exception as e:
            postgres_db.log(f"[SSE] 에러: {str(e)}", force=True)
            yield f"data: {{\"type\": \"error\", \"message\": \"{str(e)}\"}}\n\n"
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    response = Response(stream_with_context(event_stream()), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['X-Accel-Buffering'] = 'no'
    return add_cors_headers(response)


# ============ MSSQL -> PostgreSQL 동기화 ============

def _ensure_waiting_queue_table():
    """waiting_queue 테이블 존재 확인 및 생성"""
    try:
        sql = """
        CREATE TABLE IF NOT EXISTS waiting_queue (
            id SERIAL PRIMARY KEY,
            patient_id INTEGER NOT NULL,
            queue_type VARCHAR(50) DEFAULT 'treatment',
            details TEXT,
            position INTEGER DEFAULT 0,
            doctor VARCHAR(100),
            mssql_waiting_pk INTEGER,
            mssql_intotime TEXT,
            synced_at TIMESTAMP,
            chart_number VARCHAR(50),
            patient_name VARCHAR(100),
            age INTEGER,
            sex VARCHAR(10),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(patient_id, queue_type)
        )
        """
        postgres_db.execute_query(sql, fetch=False)
    except Exception as e:
        postgres_db.log(f"waiting_queue 테이블 생성 오류: {e}")


@postgres_bp.route('/api/waiting-queue/sync', methods=['POST', 'OPTIONS'])
def sync_waiting_queue():
    """MSSQL Treating 데이터를 PostgreSQL waiting_queue에 동기화

    Request Body:
        - waiting: MSSQL Treating 테이블 데이터 배열
          [{id, patient_id, chart_no, patient_name, age, sex, waiting_since, doctor, ...}, ...]

    Response:
        - added: 추가된 환자 수
        - skipped_duplicate: 이미 있어서 스킵된 수
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        # 테이블 확인/생성
        _ensure_waiting_queue_table()

        data = request.get_json() or {}
        mssql_waiting = data.get('waiting', [])

        if not mssql_waiting:
            return json_response({
                "added": 0,
                "skipped_duplicate": 0,
                "message": "No waiting data provided"
            })

        # 현재 waiting_queue에 있는 patient_id 조회 (중복 방지)
        existing = postgres_db.execute_query("""
            SELECT patient_id FROM waiting_queue
            WHERE queue_type = 'treatment'
        """)
        existing_patient_ids = {row['patient_id'] for row in existing}

        # 현재 최대 position 조회
        max_pos_result = postgres_db.execute_query(
            "SELECT COALESCE(MAX(position), -1) as max_pos FROM waiting_queue WHERE queue_type = 'treatment'"
        )
        next_position = (max_pos_result[0]['max_pos'] if max_pos_result else -1) + 1

        added = 0
        skipped_duplicate = 0

        from datetime import datetime
        now = datetime.now().isoformat()

        for patient in mssql_waiting:
            patient_id = patient.get('patient_id')
            intotime = patient.get('waiting_since') or patient.get('intotime')

            if not patient_id:
                continue

            # 이미 있으면 스킵
            if patient_id in existing_patient_ids:
                skipped_duplicate += 1
                continue

            # INSERT (ON CONFLICT DO NOTHING)
            try:
                postgres_db.execute_query("""
                    INSERT INTO waiting_queue
                    (patient_id, queue_type, details, position, doctor,
                     mssql_waiting_pk, mssql_intotime, synced_at,
                     chart_number, patient_name, age, sex)
                    VALUES (%s, 'treatment', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (patient_id, queue_type) DO NOTHING
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
                ), fetch=False)

                next_position += 1
                added += 1
                existing_patient_ids.add(patient_id)
            except Exception as insert_error:
                postgres_db.log(f"Insert error for patient_id {patient_id}: {insert_error}")
                skipped_duplicate += 1

        return json_response({
            "success": True,
            "added": added,
            "skipped_duplicate": skipped_duplicate,
            "message": f"동기화 완료: {added}명 추가"
        })

    except Exception as e:
        postgres_db.log(f"waiting_queue 동기화 오류: {e}")
        return json_response({"error": str(e)}, 500)
