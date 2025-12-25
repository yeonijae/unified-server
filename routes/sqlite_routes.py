"""
SQLite API 라우트
- 테이블 관리
- SQL 실행
- 백업

CORS Preflight (OPTIONS) 요청을 명시적으로 처리
"""

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
