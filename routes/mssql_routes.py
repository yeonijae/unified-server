"""
MSSQL API 라우트
- 환자 검색/조회
- 오늘 진료 현황
- 예약 조회
- SQL 실행
"""

# 모듈 버전 (외부 파일로 배포 시 사용)
MODULE_VERSION = "2.6.5"

from datetime import datetime
import threading
from flask import Blueprint, request, jsonify, Response
from services import mssql_db
from services import git_build
from config import VERSION, load_config

mssql_bp = Blueprint('mssql', __name__)


# ============ 웹 콘솔 HTML ============

def get_console_html():
    config = load_config()
    port = config.get('mssql_api_port', 3100)
    return f'''<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MSSQL API Console</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; min-height: 100vh; padding: 20px; }}
    h1 {{ color: #00d9ff; margin-bottom: 20px; }}
    h2 {{ color: #00d9ff; margin: 20px 0 10px; font-size: 1.1rem; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    .panel {{ background: #16213e; border-radius: 8px; padding: 20px; margin-bottom: 20px; }}
    textarea {{ width: 100%; height: 120px; background: #0f0f23; border: 1px solid #333; border-radius: 4px; color: #0f0; font-family: 'Consolas', monospace; font-size: 14px; padding: 10px; resize: vertical; }}
    button {{ background: #00d9ff; color: #000; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: bold; margin: 10px 5px 10px 0; }}
    button:hover {{ background: #00b8d9; }}
    button.secondary {{ background: #444; color: #fff; }}
    .result {{ background: #0f0f23; border-radius: 4px; padding: 15px; overflow-x: auto; max-height: 400px; overflow-y: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #333; }}
    th {{ background: #1a1a2e; color: #00d9ff; position: sticky; top: 0; }}
    tr:hover {{ background: rgba(0,217,255,0.1); }}
    .status {{ padding: 10px; border-radius: 4px; margin-top: 10px; }}
    .status.success {{ background: #2d5a27; }}
    .status.error {{ background: #5a2727; }}
    input[type="text"] {{ background: #0f0f23; border: 1px solid #333; border-radius: 4px; color: #fff; padding: 8px 12px; }}
    .form-row {{ display: flex; align-items: center; margin-bottom: 10px; gap: 10px; }}
    .version {{ color: #666; font-size: 12px; margin-top: 20px; text-align: center; }}
    .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; }}
    .stat-card {{ background: #1a1a2e; padding: 15px; border-radius: 8px; text-align: center; }}
    .stat-value {{ font-size: 24px; color: #00d9ff; font-weight: bold; }}
    .stat-label {{ color: #888; font-size: 12px; margin-top: 5px; }}
  </style>
</head>
<body>
  <div class="container">
    <h1>MSSQL API Console (Port {port})</h1>
    <div class="panel">
      <h2>환자 검색</h2>
      <div class="form-row">
        <input type="text" id="patientSearch" placeholder="이름, 차트번호, 전화번호 검색..." style="width: 300px;">
        <button onclick="searchPatients()">검색</button>
      </div>
      <div id="patientResults" class="result">검색 결과가 여기에 표시됩니다</div>
    </div>
    <div class="panel">
      <h2>오늘 현황</h2>
      <div class="stat-grid" id="todayStats">
        <div class="stat-card"><div class="stat-value">-</div><div class="stat-label">접수 환자</div></div>
        <div class="stat-card"><div class="stat-value">-</div><div class="stat-label">수납 건수</div></div>
        <div class="stat-card"><div class="stat-value">-</div><div class="stat-label">총 수납액</div></div>
      </div>
      <button onclick="loadTodayStats()" class="secondary">새로고침</button>
    </div>
    <div class="panel">
      <h2>SQL 쿼리 실행</h2>
      <textarea id="sqlQuery">SELECT TOP 10 * FROM Customer ORDER BY recent DESC</textarea>
      <button onclick="executeSQL()">실행</button>
      <div id="queryStatus"></div>
      <div id="queryResults" class="result">쿼리 결과가 여기에 표시됩니다</div>
    </div>
    <p class="version">MSSQL API Server v{VERSION}</p>
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
    function renderTable(data) {{
      if (!data.columns || !data.columns.length) return '<p>No data</p>';
      let html = '<table><thead><tr>' + data.columns.map(c => '<th>'+c+'</th>').join('') + '</tr></thead><tbody>';
      if (data.rows) data.rows.forEach(row => {{ html += '<tr>' + data.columns.map((c,i) => '<td>'+(row[i]===null?'<i>NULL</i>':row[i])+'</td>').join('') + '</tr>'; }});
      return html + '</tbody></table>';
    }}
    async function searchPatients() {{
      const q = document.getElementById('patientSearch').value;
      if (!q) return;
      const data = await api('/api/patients/search?q=' + encodeURIComponent(q));
      if (data.error) document.getElementById('patientResults').innerHTML = '<p style="color:#ff4757">'+data.error+'</p>';
      else document.getElementById('patientResults').innerHTML = renderTable({{ columns: ['차트번호', '이름', '생년월일', '성별', '전화번호', '최근내원'], rows: data.map(p => [p.chart_no, p.name, p.birth, p.sex, p.phone, p.last_visit]) }});
    }}
    async function loadTodayStats() {{
      try {{
        const data = await api('/api/today/stats');
        if (!data.error) document.getElementById('todayStats').innerHTML = '<div class="stat-card"><div class="stat-value">'+data.registrations+'</div><div class="stat-label">접수 환자</div></div><div class="stat-card"><div class="stat-value">'+data.receipts+'</div><div class="stat-label">수납 건수</div></div><div class="stat-card"><div class="stat-value">'+(data.totalAmount||0).toLocaleString()+'원</div><div class="stat-label">총 수납액</div></div>';
      }} catch(e) {{}}
    }}
    async function executeSQL() {{
      const sql = document.getElementById('sqlQuery').value;
      try {{
        const data = await api('/api/execute', {{ method: 'POST', body: JSON.stringify({{ sql }}) }});
        if (data.error) {{ showStatus('queryStatus', 'Error: '+data.error, true); document.getElementById('queryResults').innerHTML = '<pre style="color:#ff4757">'+data.error+'</pre>'; }}
        else {{ showStatus('queryStatus', data.message || 'Success'); document.getElementById('queryResults').innerHTML = renderTable(data); }}
      }} catch(e) {{ showStatus('queryStatus', 'Error: '+e.message, true); }}
    }}
    loadTodayStats();
  </script>
</body>
</html>'''


# ============ 라우트 ============

@mssql_bp.route('/')
def index():
    return Response(get_console_html(), mimetype='text/html')


@mssql_bp.route('/api/info')
def api_info():
    return jsonify({
        "version": VERSION,
        "type": "mssql",
        "status": "running"
    })


@mssql_bp.route('/api/health')
def health_check():
    mssql_ok = False
    try:
        conn = mssql_db.get_connection()
        if conn:
            conn.close()
            mssql_ok = True
    except:
        pass
    return jsonify({"status": "ok", "mssql_connected": mssql_ok})


@mssql_bp.route('/api/patients/search')
def search_patients():
    """환자 검색"""
    try:
        q = request.args.get('q', '')
        limit = int(request.args.get('limit', 50))

        if not q or len(q) < 1:
            return jsonify([])

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT TOP %s
              Customer_PK as id,
              sn as chart_no,
              name,
              birth,
              sex,
              cell as phone,
              address,
              reg_date,
              recent as last_visit,
              MAINDOCTOR as main_doctor,
              MAINDISEASE as main_disease,
              NOTEFORDOC as doctor_memo,
              TreatCurrent as treat_type,
              NOTEFORNURSE as nurse_memo,
              ETCMemo as etc_memo,
              SUGGEST as referral_type,
              CustURL as referral_detail,
              suggcustnamesn as referrer_info
            FROM Customer
            WHERE name LIKE %s
               OR sn LIKE %s
               OR cell LIKE %s
            ORDER BY recent DESC, reg_date DESC
        """, (limit, f'%{q}%', f'%{q}%', f'%{q}%'))

        rows = cursor.fetchall()
        conn.close()

        patients = []
        for p in rows:
            patients.append({
                **p,
                'sex': 'M' if p['sex'] else 'F',
                'birth': p['birth'].strftime('%Y-%m-%d') if p['birth'] else None,
                'reg_date': p['reg_date'].strftime('%Y-%m-%d') if p['reg_date'] else None,
                'last_visit': p['last_visit'].strftime('%Y-%m-%d') if p['last_visit'] else None,
                'referral_source': mssql_db.format_referral_source(p['referral_type'], p['referral_detail'], p['referrer_info'])
            })

        return jsonify(patients)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/patients/<int:patient_id>')
def get_patient(patient_id):
    """환자 상세 조회"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT
              c.Customer_PK as id,
              c.sn as chart_no,
              c.name,
              c.birth,
              c.sex,
              c.cell as phone,
              c.tel,
              c.address,
              c.reg_date,
              c.recent as last_visit,
              c.MAINDOCTOR as main_doctor,
              c.MAINDISEASE as main_disease,
              c.NOTEFORDOC as doctor_memo,
              c.NOTEFORNURSE as nurse_memo,
              c.TreatCurrent as treat_type,
              c.ETCMemo as etc_memo,
              c.SUGGEST as referral_type,
              c.CustURL as referral_detail,
              c.suggcustnamesn as referrer_info,
              dc.Comment1 as comment1,
              dc.Comment2 as comment2
            FROM Customer c
            OUTER APPLY (
              SELECT TOP 1 Comment1, Comment2
              FROM MasterDB.dbo.DetailComment
              WHERE Customer_PK = c.Customer_PK
              ORDER BY TxDate DESC
            ) dc
            WHERE c.Customer_PK = %s
        """, (patient_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "환자를 찾을 수 없습니다."}), 404

        return jsonify({
            **row,
            'sex': 'M' if row['sex'] else 'F',
            'birth': row['birth'].strftime('%Y-%m-%d') if row['birth'] else None,
            'reg_date': row['reg_date'].strftime('%Y-%m-%d') if row['reg_date'] else None,
            'last_visit': row['last_visit'].strftime('%Y-%m-%d') if row['last_visit'] else None,
            'referral_source': mssql_db.format_referral_source(row['referral_type'], row['referral_detail'], row['referrer_info'])
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/patients/chart/<chart_no>')
def get_patient_by_chart(chart_no):
    """환자 상세 조회 (차트번호)"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        padded_chart_no = chart_no.zfill(8)

        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT
              Customer_PK as id,
              sn as chart_no,
              name,
              birth,
              sex,
              cell as phone,
              tel,
              address,
              reg_date,
              recent as last_visit,
              MAINDOCTOR as main_doctor,
              MAINDISEASE as main_disease,
              NOTEFORDOC as doctor_memo,
              NOTEFORNURSE as nurse_memo,
              TreatCurrent as treat_type,
              ETCMemo as etc_memo,
              SUGGEST as referral_type,
              CustURL as referral_detail,
              suggcustnamesn as referrer_info
            FROM Customer
            WHERE sn = %s OR sn = %s
        """, (chart_no, padded_chart_no))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "환자를 찾을 수 없습니다."}), 404

        return jsonify({
            **row,
            'sex': 'M' if row['sex'] else 'F',
            'birth': row['birth'].strftime('%Y-%m-%d') if row['birth'] else None,
            'reg_date': row['reg_date'].strftime('%Y-%m-%d') if row['reg_date'] else None,
            'last_visit': row['last_visit'].strftime('%Y-%m-%d') if row['last_visit'] else None,
            'referral_source': mssql_db.format_referral_source(row['referral_type'], row['referral_detail'], row['referrer_info'])
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/today/registrations')
def today_registrations():
    """오늘 접수 환자 목록"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        today = datetime.now().strftime('%Y-%m-%d')

        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT
              d.Detail_PK as id,
              d.Customer_PK as patient_id,
              d.SN as chart_no,
              c.name as patient_name,
              d.TxDate as treat_date,
              d.TxTime as reg_time,
              d.TxDoctor as doctor,
              d.TxItem as treat_item,
              d.DxName as diagnosis,
              d.PxName as treatment,
              d.TxMoney as amount,
              d.WriteTime as created_at,
              c.TreatCurrent as treat_type
            FROM Detail d
            JOIN Customer c ON d.Customer_PK = c.Customer_PK
            WHERE CAST(d.TxDate AS DATE) = %s
            ORDER BY d.WriteTime DESC
        """, (today,))

        rows = cursor.fetchall()
        conn.close()

        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/today/stats')
def today_stats():
    """오늘 진료 통계"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        today = datetime.now().strftime('%Y-%m-%d')
        cursor = conn.cursor(as_dict=True)

        # 접수 환자 수
        cursor.execute("""
            SELECT COUNT(DISTINCT Customer_PK) as count
            FROM Detail
            WHERE CAST(TxDate AS DATE) = %s
        """, (today,))
        reg_count = cursor.fetchone()['count']

        # 수납 건수
        cursor.execute("""
            SELECT COUNT(*) as count, SUM(General_Money + Bonin_Money) as total
            FROM Receipt
            WHERE CAST(TxDate AS DATE) = %s
        """, (today,))
        receipt = cursor.fetchone()

        # 의사별 진료 현황
        cursor.execute("""
            SELECT TxDoctor as doctor, COUNT(DISTINCT Customer_PK) as count
            FROM Detail
            WHERE CAST(TxDate AS DATE) = %s AND TxDoctor IS NOT NULL AND TxDoctor != ''
            GROUP BY TxDoctor
            ORDER BY count DESC
        """, (today,))
        by_doctor = cursor.fetchall()

        conn.close()

        return jsonify({
            "registrations": reg_count or 0,
            "receipts": receipt['count'] or 0,
            "totalAmount": receipt['total'] or 0,
            "byDoctor": by_doctor
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/today/receipts')
def today_receipts():
    """오늘 수납 내역"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        today = datetime.now().strftime('%Y-%m-%d')

        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT
              r.Receipt_PK as id,
              r.Customer_PK as patient_id,
              r.Customer_name as patient_name,
              r.sn as chart_no,
              r.Bonin_Money as insurance_self,
              r.CheongGu_Money as insurance_claim,
              r.General_Money as general_amount,
              r.MisuMoney as unpaid,
              r.WriteTime as created_at
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) = %s
            ORDER BY r.WriteTime DESC
        """, (today,))

        rows = cursor.fetchall()
        conn.close()

        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/receipts/by-date')
def receipts_by_date():
    """날짜별 수납/진료 내역 조회 (수납현황용)"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        # 날짜 파라미터 (없으면 오늘)
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        cursor = conn.cursor(as_dict=True)

        # 1. 수납 내역 조회 (Receipt + Transaction_TB JOIN으로 결제 방법 포함)
        cursor.execute("""
            SELECT
              r.Receipt_PK as id,
              r.Customer_PK as patient_id,
              r.Customer_name as patient_name,
              r.sn as chart_no,
              r.Bonin_Money as insurance_self,
              r.CheongGu_Money as insurance_claim,
              r.General_Money as general_amount,
              r.MisuMoney as unpaid,
              r.WriteTime as receipt_time,
              c.insu_type,
              c.insu_bohum_type,
              c.insu_boho_type,
              c.pregmark,
              c.SeriousTag,
              c.birth,
              ISNULL(SUM(CASE WHEN t.trans_method = '01' THEN t.trans_Money ELSE 0 END), 0) as card_amount,
              ISNULL(SUM(CASE WHEN t.trans_method = '00' THEN t.trans_Money ELSE 0 END), 0) as cash_amount,
              ISNULL(SUM(CASE WHEN t.trans_method IN ('10', '11', '12', '02') THEN t.trans_Money ELSE 0 END), 0) as transfer_amount
            FROM Receipt r
            LEFT JOIN Customer c ON r.Customer_PK = c.Customer_PK
            LEFT JOIN Transaction_TB t ON r.Receipt_PK = t.trans_Receipt_PK
            WHERE CAST(r.TxDate AS DATE) = %s
            GROUP BY r.Receipt_PK, r.Customer_PK, r.Customer_name, r.sn,
                     r.Bonin_Money, r.CheongGu_Money, r.General_Money, r.MisuMoney,
                     r.WriteTime, c.insu_type, c.insu_bohum_type, c.insu_boho_type,
                     c.pregmark, c.SeriousTag, c.birth
            ORDER BY r.WriteTime ASC
        """, (target_date,))
        receipts = cursor.fetchall()

        # 2. 각 환자별 진료 내역 조회 (Detail)
        result = []
        for r in receipts:
            patient_id = r['patient_id']

            # 해당 환자의 당일 진료 내역
            cursor.execute("""
                SELECT
                  Detail_PK as id,
                  TxItem as tx_item,
                  PxName as px_name,
                  DxName as dx_name,
                  TxDoctor as doctor,
                  TxMoney as amount,
                  InsuYes as is_covered,
                  추나여부상태 as choona_status,
                  IsYakChim as is_yakchim,
                  WriteTime as detail_time
                FROM Detail
                WHERE Customer_PK = %s AND CAST(TxDate AS DATE) = %s
                ORDER BY WriteTime ASC
            """, (patient_id, target_date))
            details = cursor.fetchall()

            # 치료 항목 분류
            treatments = []
            has_acupuncture = False
            has_choona = False
            has_yakchim = False
            uncovered_items = []
            is_jabo = False

            for d in details:
                tx_item = (d['tx_item'] or '').strip()
                px_name = (d['px_name'] or '').strip()
                is_covered = d['is_covered']

                if '자동차보험' in tx_item:
                    is_jabo = True

                if d['choona_status'] == '1':
                    has_choona = True

                if d['is_yakchim']:
                    has_yakchim = True

                if is_covered and d['choona_status'] != '1' and not d['is_yakchim']:
                    if '침' in px_name or '자락' in px_name or '부항' in px_name or '뜸' in px_name:
                        has_acupuncture = True

                if not is_covered and d['amount'] and d['amount'] > 0:
                    uncovered_items.append({
                        'name': px_name or tx_item,
                        'amount': int(d['amount'])
                    })

                treatments.append({
                    'id': d['id'],
                    'item': tx_item,
                    'name': px_name,
                    'diagnosis': d['dx_name'],
                    'doctor': d['doctor'],
                    'amount': int(d['amount'] or 0),
                    'is_covered': is_covered,
                    'time': d['detail_time'].strftime('%H:%M') if d['detail_time'] else None
                })

            # 종별 분류
            insurance_type = _classify_insurance_type(
                insu_type=r['insu_type'],
                insu_bohum_type=r['insu_bohum_type'],
                insu_boho_type=r['insu_boho_type'],
                pregmark=r['pregmark'],
                serious_tag=r['SeriousTag'],
                is_jabo=is_jabo
            )

            # receipt_time 안전하게 포맷
            receipt_time_str = None
            if r['receipt_time']:
                try:
                    if hasattr(r['receipt_time'], 'strftime'):
                        receipt_time_str = r['receipt_time'].strftime('%Y-%m-%d %H:%M')
                    else:
                        receipt_time_str = str(r['receipt_time'])
                except:
                    receipt_time_str = str(r['receipt_time'])

            # 나이 계산
            age = None
            if r['birth']:
                try:
                    birth_date = r['birth']
                    today = datetime.now()
                    age = today.year - birth_date.year
                    # 생일이 아직 안 지났으면 1살 빼기
                    if (today.month, today.day) < (birth_date.month, birth_date.day):
                        age -= 1
                except:
                    pass

            result.append({
                'id': r['id'],
                'patient_id': patient_id,
                'patient_name': r['patient_name'],
                'chart_no': r['chart_no'],
                'age': age,
                'receipt_time': receipt_time_str,
                # 수납 금액 (money 타입 -> 정수 변환)
                'insurance_self': int(r['insurance_self'] or 0),
                'insurance_claim': int(r['insurance_claim'] or 0),
                'general_amount': int(r['general_amount'] or 0),
                'total_amount': int((r['insurance_self'] or 0) + (r['general_amount'] or 0)),
                'unpaid': int(r['unpaid'] or 0),
                # 수납 방법 (Transaction_TB JOIN으로 가져옴)
                'cash': int(r['cash_amount'] or 0),
                'card': int(r['card_amount'] or 0),
                'transfer': int(r['transfer_amount'] or 0),
                # 종별
                'insurance_type': insurance_type,
                # 치료 요약
                'treatment_summary': {
                    'acupuncture': has_acupuncture,
                    'choona': has_choona,
                    'yakchim': has_yakchim,
                    'uncovered': uncovered_items
                },
                # 진료 내역
                'treatments': treatments
            })

        # 3. 통계 계산
        total_insurance_self = sum(r['insurance_self'] for r in result)
        total_general = sum(r['general_amount'] for r in result)
        total_cash = sum(r['cash'] for r in result)
        total_card = sum(r['card'] for r in result)
        total_transfer = sum(r['transfer'] for r in result)
        total_unpaid = sum(r['unpaid'] or 0 for r in result)

        conn.close()

        return jsonify({
            'date': target_date,
            'receipts': result,
            'summary': {
                'count': len(result),
                'total_amount': total_insurance_self + total_general,
                'insurance_self': total_insurance_self,
                'general_amount': total_general,
                'cash': total_cash,
                'card': total_card,
                'transfer': total_transfer,
                'unpaid': total_unpaid
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/receipts/by-patient')
def receipts_by_patient():
    """환자별 수납내역 조회 (patientId 또는 chartNo 기준, 페이지네이션 지원)"""
    try:
        # 파라미터 파싱
        patient_id = request.args.get('patientId', type=int)
        chart_no = request.args.get('chartNo', '')
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 30, type=int)
        start_date = request.args.get('startDate', '')
        end_date = request.args.get('endDate', '')

        if not patient_id and not chart_no:
            return jsonify({"error": "patientId 또는 chartNo가 필요합니다."}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # WHERE 조건 구성
        where_conditions = []
        params = []

        if patient_id:
            where_conditions.append("r.Customer_PK = %s")
            params.append(patient_id)
        elif chart_no:
            padded_chart_no = chart_no.zfill(6)
            where_conditions.append("r.sn = %s")
            params.append(padded_chart_no)

        if start_date:
            where_conditions.append("CAST(r.TxDate AS DATE) >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("CAST(r.TxDate AS DATE) <= %s")
            params.append(end_date)

        where_clause = " AND ".join(where_conditions)

        # 1. 총 건수 조회
        cursor.execute(f"""
            SELECT COUNT(*) as total_count,
                   ISNULL(SUM(r.Bonin_Money), 0) as total_insurance_self,
                   ISNULL(SUM(r.General_Money), 0) as total_general
            FROM Receipt r
            WHERE {where_clause}
        """, tuple(params))
        count_row = cursor.fetchone()
        total_count = count_row['total_count'] or 0
        total_insurance_self = int(count_row['total_insurance_self'] or 0)
        total_general = int(count_row['total_general'] or 0)
        total_pages = (total_count + limit - 1) // limit if limit > 0 else 1

        # 2. 수납 내역 조회 (페이지네이션)
        offset = (page - 1) * limit
        cursor.execute(f"""
            SELECT
              r.Receipt_PK as id,
              r.Customer_PK as patient_id,
              r.Customer_name as patient_name,
              r.sn as chart_no,
              r.TxDate as receipt_date,
              r.Bonin_Money as insurance_self,
              r.CheongGu_Money as insurance_claim,
              r.General_Money as general_amount,
              r.MisuMoney as unpaid,
              r.WriteTime as receipt_time,
              c.insu_type,
              c.insu_bohum_type,
              c.insu_boho_type,
              c.pregmark,
              c.SeriousTag,
              c.birth,
              ISNULL(SUM(CASE WHEN t.trans_method = '01' THEN t.trans_Money ELSE 0 END), 0) as card_amount,
              ISNULL(SUM(CASE WHEN t.trans_method = '00' THEN t.trans_Money ELSE 0 END), 0) as cash_amount,
              ISNULL(SUM(CASE WHEN t.trans_method IN ('10', '11', '12', '02') THEN t.trans_Money ELSE 0 END), 0) as transfer_amount
            FROM Receipt r
            LEFT JOIN Customer c ON r.Customer_PK = c.Customer_PK
            LEFT JOIN Transaction_TB t ON r.Receipt_PK = t.trans_Receipt_PK
            WHERE {where_clause}
            GROUP BY r.Receipt_PK, r.Customer_PK, r.Customer_name, r.sn,
                     r.TxDate, r.Bonin_Money, r.CheongGu_Money, r.General_Money, r.MisuMoney,
                     r.WriteTime, c.insu_type, c.insu_bohum_type, c.insu_boho_type,
                     c.pregmark, c.SeriousTag, c.birth
            ORDER BY r.TxDate DESC, r.WriteTime DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, tuple(params) + (offset, limit))
        receipts_raw = cursor.fetchall()

        # 3. 각 수납별 진료 내역 조회
        result = []
        for r in receipts_raw:
            receipt_id = r['id']
            receipt_date = r['receipt_date']
            receipt_date_str = receipt_date.strftime('%Y-%m-%d') if receipt_date else None

            # 해당 환자의 해당 날짜 진료 내역
            if receipt_date_str:
                cursor.execute("""
                    SELECT
                      Detail_PK as id,
                      TxItem as tx_item,
                      PxName as px_name,
                      DxName as dx_name,
                      TxDoctor as doctor,
                      TxMoney as amount,
                      InsuYes as is_covered,
                      추나여부상태 as choona_status,
                      IsYakChim as is_yakchim,
                      WriteTime as detail_time
                    FROM Detail
                    WHERE Customer_PK = %s AND CAST(TxDate AS DATE) = %s
                    ORDER BY WriteTime ASC
                """, (r['patient_id'], receipt_date_str))
                details = cursor.fetchall()
            else:
                details = []

            # 치료 항목 분류
            treatments = []
            has_acupuncture = False
            has_choona = False
            has_yakchim = False
            uncovered_items = []
            is_jabo = False

            for d in details:
                tx_item = (d['tx_item'] or '').strip()
                px_name = (d['px_name'] or '').strip()
                is_covered = d['is_covered']

                if '자동차보험' in tx_item:
                    is_jabo = True

                if d['choona_status'] == '1':
                    has_choona = True

                if d['is_yakchim']:
                    has_yakchim = True

                if is_covered and d['choona_status'] != '1' and not d['is_yakchim']:
                    if '침' in px_name or '자락' in px_name or '부항' in px_name or '뜸' in px_name:
                        has_acupuncture = True

                if not is_covered and d['amount'] and d['amount'] > 0:
                    uncovered_items.append({
                        'name': px_name or tx_item,
                        'amount': int(d['amount'])
                    })

                treatments.append({
                    'id': d['id'],
                    'item': tx_item,
                    'name': px_name,
                    'diagnosis': d['dx_name'],
                    'doctor': d['doctor'],
                    'amount': int(d['amount'] or 0),
                    'is_covered': is_covered,
                    'time': d['detail_time'].strftime('%H:%M') if d['detail_time'] else None
                })

            # 종별 분류
            insurance_type = _classify_insurance_type(
                insu_type=r['insu_type'],
                insu_bohum_type=r['insu_bohum_type'],
                insu_boho_type=r['insu_boho_type'],
                pregmark=r['pregmark'],
                serious_tag=r['SeriousTag'],
                is_jabo=is_jabo
            )

            # receipt_time 포맷
            receipt_time_str = None
            if r['receipt_time']:
                try:
                    if hasattr(r['receipt_time'], 'strftime'):
                        receipt_time_str = r['receipt_time'].strftime('%Y-%m-%d %H:%M')
                    else:
                        receipt_time_str = str(r['receipt_time'])
                except:
                    receipt_time_str = str(r['receipt_time'])

            # 나이 계산
            age = None
            if r['birth']:
                try:
                    birth_date = r['birth']
                    today = datetime.now()
                    age = today.year - birth_date.year
                    if (today.month, today.day) < (birth_date.month, birth_date.day):
                        age -= 1
                except:
                    pass

            result.append({
                'id': receipt_id,
                'patient_id': r['patient_id'],
                'patient_name': r['patient_name'],
                'chart_no': r['chart_no'],
                'age': age,
                'receipt_date': receipt_date_str,
                'receipt_time': receipt_time_str,
                'insurance_self': int(r['insurance_self'] or 0),
                'insurance_claim': int(r['insurance_claim'] or 0),
                'general_amount': int(r['general_amount'] or 0),
                'total_amount': int((r['insurance_self'] or 0) + (r['general_amount'] or 0)),
                'unpaid': int(r['unpaid'] or 0),
                'cash': int(r['cash_amount'] or 0),
                'card': int(r['card_amount'] or 0),
                'transfer': int(r['transfer_amount'] or 0),
                'insurance_type': insurance_type,
                'treatment_summary': {
                    'acupuncture': has_acupuncture,
                    'choona': has_choona,
                    'yakchim': has_yakchim,
                    'uncovered': uncovered_items
                },
                'treatments': treatments
            })

        conn.close()

        return jsonify({
            'receipts': result,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': total_pages,
                'has_more': page < total_pages
            },
            'summary': {
                'total_count': total_count,
                'total_amount': total_insurance_self + total_general,
                'insurance_self': total_insurance_self,
                'general_amount': total_general
            }
        })

    except Exception as e:
        mssql_db.log(f"환자별 수납내역 조회 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/reservations', methods=['GET', 'POST'])
def reservations():
    """예약 조회/생성"""
    try:
        if request.method == 'POST':
            return create_reservation()
        return get_reservations()
    except Exception as e:
        mssql_db.log(f"예약 API 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


def create_reservation():
    """예약 생성 (MSSQL Reservation_New 테이블에 INSERT)"""
    try:
        try:
            data = request.get_json(force=True, silent=True) or {}
        except Exception as parse_err:
            mssql_db.log(f"JSON 파싱 오류: {str(parse_err)}")
            return jsonify({"error": f"JSON 파싱 오류: {str(parse_err)}"}), 400

        if not data:
            return jsonify({"error": "요청 데이터가 없습니다."}), 400

        # 필수 필드 검증
        required_fields = ['patientId', 'date', 'time', 'doctor', 'item']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"필수 필드 누락: {field}"}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 1. 환자 정보 조회 (차트번호, 이름, 전화번호)
        cursor.execute("""
            SELECT sn as chart_no, name, cell as phone, tel
            FROM Customer
            WHERE Customer_PK = %s
        """, (data['patientId'],))
        patient = cursor.fetchone()

        if not patient:
            conn.close()
            return jsonify({"error": "환자를 찾을 수 없습니다."}), 404

        # 2. 예약 INSERT (Res_Key는 IDENTITY이므로 자동 생성)
        insert_sql = """
            INSERT INTO Reservation_New (
                Res_Customer_PK,
                Res_ChartNo,
                Res_Name,
                Res_MobilePhone,
                Res_Tel,
                Res_Date,
                Res_Time_0,
                Res_DoctorName,
                Res_Item,
                Res_Gubun,
                Res_Memo,
                Res_Canceled,
                Res_MsgSent,
                Res_Visited,
                Res_updatetime,
                Res_pcname
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, GETDATE(), %s
            )
        """

        cursor.execute(insert_sql, (
            data['patientId'],
            patient['chart_no'],
            patient['name'],
            patient['phone'] or '',
            patient['tel'] or '',
            data['date'],
            data['time'],
            data['doctor'],
            data['item'],
            data.get('type', '재진'),
            data.get('memo', ''),
            'yeonijae\\haniwon'
        ))
        conn.commit()

        # 방금 생성된 Res_Key 조회
        cursor.execute("SELECT SCOPE_IDENTITY() as new_key")
        new_key = cursor.fetchone()['new_key']

        # 4. 생성된 예약 조회하여 반환
        cursor.execute("""
            SELECT
                Res_Key as id,
                Res_Customer_PK as patient_id,
                Res_ChartNo as chart_no,
                Res_Name as patient_name,
                Res_MobilePhone as phone,
                Res_Date as date,
                Res_Time_0 as time,
                Res_DoctorName as doctor,
                Res_Item as item,
                Res_Gubun as type,
                Res_Memo as memo,
                Res_Visited as visited,
                Res_Canceled as canceled
            FROM Reservation_New
            WHERE Res_Key = %s
        """, (new_key,))
        created = cursor.fetchone()
        conn.close()

        return jsonify(created), 201

    except Exception as e:
        mssql_db.log(f"예약 생성 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


def get_reservations():
    """예약 조회"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        date = request.args.get('date')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        cursor = conn.cursor(as_dict=True)

        query = """
            SELECT
              r.Res_Key as id,
              r.Res_Customer_PK as patient_id,
              r.Res_ChartNo as chart_no,
              r.Res_Name as patient_name,
              r.Res_MobilePhone as phone,
              r.Res_Date as date,
              r.Res_Time_0 as time,
              r.Res_DoctorName as doctor,
              r.Res_Item as item,
              r.Res_Gubun as type,
              r.Res_Memo as memo,
              r.Res_Visited as visited,
              r.Res_Canceled as canceled,
              r.Res_updatetime as createdAt
            FROM Reservation_New r
            WHERE 1=1
        """
        params = []

        if date:
            query += " AND r.Res_Date = %s"
            params.append(date)
        elif start_date and end_date:
            query += " AND r.Res_Date BETWEEN %s AND %s"
            params.extend([start_date, end_date])

        query += " ORDER BY r.Res_Date, r.Res_Time_0"

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        conn.close()

        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/reservations/<int:reservation_id>', methods=['PATCH'])
def update_reservation(reservation_id):
    """예약 수정"""
    try:
        data = request.get_json(force=True, silent=True) or {}
        if not data:
            return jsonify({"error": "수정할 데이터가 없습니다."}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 기존 예약 확인
        cursor.execute("SELECT Res_Key FROM Reservation_New WHERE Res_Key = %s", (reservation_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": "예약을 찾을 수 없습니다."}), 404

        # 수정 가능한 필드 매핑
        field_map = {
            'date': 'Res_Date',
            'time': 'Res_Time_0',
            'doctor': 'Res_DoctorName',
            'item': 'Res_Item',
            'type': 'Res_Gubun',
            'memo': 'Res_Memo',
        }

        # UPDATE 쿼리 생성
        updates = []
        params = []
        for key, db_field in field_map.items():
            if key in data:
                updates.append(f"{db_field} = %s")
                params.append(data[key])

        if not updates:
            conn.close()
            return jsonify({"error": "수정할 필드가 없습니다."}), 400

        # 수정 시간 업데이트
        updates.append("Res_updatetime = GETDATE()")

        update_sql = f"UPDATE Reservation_New SET {', '.join(updates)} WHERE Res_Key = %s"
        params.append(reservation_id)

        cursor.execute(update_sql, tuple(params))
        conn.commit()

        # 수정된 예약 조회하여 반환
        cursor.execute("""
            SELECT
                Res_Key as id,
                Res_Customer_PK as patient_id,
                Res_ChartNo as chart_no,
                Res_Name as patient_name,
                Res_MobilePhone as phone,
                Res_Date as date,
                Res_Time_0 as time,
                Res_DoctorName as doctor,
                Res_Item as item,
                Res_Gubun as type,
                Res_Memo as memo,
                Res_Visited as visited,
                Res_Canceled as canceled
            FROM Reservation_New
            WHERE Res_Key = %s
        """, (reservation_id,))
        updated = cursor.fetchone()
        conn.close()

        return jsonify(updated)

    except Exception as e:
        mssql_db.log(f"예약 수정 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/reservations/<int:reservation_id>/cancel', methods=['POST'])
def cancel_reservation(reservation_id):
    """예약 취소"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 기존 예약 확인
        cursor.execute("SELECT Res_Key FROM Reservation_New WHERE Res_Key = %s", (reservation_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({"error": "예약을 찾을 수 없습니다."}), 404

        # 취소 처리
        cursor.execute("""
            UPDATE Reservation_New
            SET Res_Canceled = 1, Res_updatetime = GETDATE()
            WHERE Res_Key = %s
        """, (reservation_id,))
        conn.commit()
        conn.close()

        return jsonify({"success": True})

    except Exception as e:
        mssql_db.log(f"예약 취소 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/reservations/on-site-count')
def on_site_reservation_count():
    """현장예약 카운트: 특정 날짜에 침치료 받은 환자 중, 그 날짜에 생성된 미래 예약이 있는 환자 수
    - 침치료 환자 = 자보환자 + 청구금이 0원이 아닌 건보환자 (Receipt 테이블 기준)
    - Receipt + Detail JOIN으로 담당의사 매핑
    """
    try:
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 1. Receipt 테이블에서 침치료 환자 조회
        # 침치료 환자 = 자보환자(TxItem에 '자동차보험' 포함) OR 청구금(CheongGu_Money)이 0원이 아닌 환자
        cursor.execute("""
            SELECT DISTINCT
                r.Customer_PK as patient_id,
                (SELECT TOP 1 d.TxDoctor FROM Detail d
                 WHERE d.Customer_PK = r.Customer_PK
                   AND CAST(d.TxDate AS DATE) = %s
                   AND d.TxDoctor IS NOT NULL AND d.TxDoctor != ''
                 ORDER BY d.WriteTime ASC) as doctor
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) = %s
              AND (
                  -- 자보환자: Detail에 '자동차보험' TxItem이 있는 경우
                  EXISTS (
                      SELECT 1 FROM Detail d2
                      WHERE d2.Customer_PK = r.Customer_PK
                        AND CAST(d2.TxDate AS DATE) = %s
                        AND d2.TxItem LIKE '%%자동차보험%%'
                  )
                  -- OR 청구금이 0원이 아닌 경우 (건보 침치료)
                  OR ISNULL(r.CheongGu_Money, 0) > 0
              )
        """, (target_date, target_date, target_date))
        all_visited_rows = cursor.fetchall()

        # 환자별로 첫 번째 담당의사만 사용 (중복 제거)
        patient_doctor_map = {}
        for row in all_visited_rows:
            patient_id = row['patient_id']
            doctor = row['doctor']
            if patient_id not in patient_doctor_map and doctor:
                patient_doctor_map[patient_id] = doctor

        visited_patients = list(patient_doctor_map.keys())

        if not visited_patients:
            conn.close()
            return jsonify({
                "date": target_date,
                "visited_count": 0,
                "on_site_count": 0,
                "patient_ids": [],
                "by_doctor": {}
            })

        # 2. 해당 날짜에 생성된(updatetime) 미래 예약 중, 내원 환자의 예약 찾기
        # 취소 여부와 관계없이 예약을 잡았으면 카운트 (나중에 취소되어도 현장예약으로 인정)
        placeholders = ','.join(['%s'] * len(visited_patients))
        cursor.execute(f"""
            SELECT DISTINCT Res_Customer_PK as patient_id
            FROM Reservation_New
            WHERE CAST(Res_updatetime AS DATE) = %s
              AND Res_Date > %s
              AND Res_Customer_PK IN ({placeholders})
        """, (target_date, target_date, *visited_patients))

        on_site_patients = set([row['patient_id'] for row in cursor.fetchall()])

        # 3. 의사별 현장예약 카운트 계산
        by_doctor = {}
        for patient_id, doctor in patient_doctor_map.items():
            if doctor not in by_doctor:
                by_doctor[doctor] = {'visited': set(), 'on_site': set()}
            by_doctor[doctor]['visited'].add(patient_id)
            if patient_id in on_site_patients:
                by_doctor[doctor]['on_site'].add(patient_id)

        # set을 count로 변환
        by_doctor_result = {}
        for doctor, data in by_doctor.items():
            by_doctor_result[doctor] = {
                'visited_count': len(data['visited']),
                'on_site_count': len(data['on_site'])
            }

        conn.close()

        return jsonify({
            "date": target_date,
            "visited_count": len(visited_patients),
            "on_site_count": len(on_site_patients),
            "patient_ids": list(on_site_patients),
            "by_doctor": by_doctor_result
        })

    except Exception as e:
        mssql_db.log(f"현장예약 카운트 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/patients/daily-visits')
def daily_visits():
    """특정 날짜에 내원한 침환자(약환자 제외) 상세 목록"""
    try:
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 1. 오늘 내원한 침환자 (약환자 제외) 목록
        cursor.execute("""
            SELECT DISTINCT
                c.Customer_PK as patient_id,
                c.sn as chart_no,
                c.name as patient_name,
                d.TxDoctor as doctor
            FROM Detail d
            JOIN Customer c ON d.Customer_PK = c.Customer_PK
            WHERE CAST(d.TxDate AS DATE) = %s
              AND d.TxDoctor IS NOT NULL
              AND d.TxDoctor != ''
              AND NOT EXISTS (
                  SELECT 1 FROM Detail d2
                  WHERE d2.Customer_PK = d.Customer_PK
                    AND CAST(d2.TxDate AS DATE) = %s
                    AND (d2.TxItem LIKE '%%한약%%' OR d2.TxItem LIKE '%%탕%%' OR d2.TxItem LIKE '%%환%%')
              )
            ORDER BY c.name
        """, (target_date, target_date))
        patients = cursor.fetchall()

        result = []
        for patient in patients:
            patient_id = patient['patient_id']

            # 진료내역
            cursor.execute("""
                SELECT TxItem, TxQty, TxDoctor, TxPrice
                FROM Detail
                WHERE Customer_PK = %s AND CAST(TxDate AS DATE) = %s
                ORDER BY Detail_PK
            """, (patient_id, target_date))
            details = cursor.fetchall()

            # 수납내역
            cursor.execute("""
                SELECT PayType, PayMoney, PayMemo
                FROM Receipt
                WHERE Customer_PK = %s AND CAST(PayDate AS DATE) = %s
            """, (patient_id, target_date))
            receipts = cursor.fetchall()

            # 다음 예약이 있는지 확인 (현장예약)
            cursor.execute("""
                SELECT Res_Date, Res_Time_0, Res_DoctorName
                FROM Reservation_New
                WHERE Res_Customer_PK = %s
                  AND CAST(Res_updatetime AS DATE) = %s
                  AND Res_Date > %s
                  AND Res_Canceled = 0
            """, (patient_id, target_date, target_date))
            next_reservations = cursor.fetchall()

            result.append({
                'patient_id': patient_id,
                'chart_no': patient['chart_no'],
                'patient_name': patient['patient_name'],
                'doctor': patient['doctor'],
                'details': [{'item': d['TxItem'], 'qty': d['TxQty'], 'doctor': d['TxDoctor'], 'price': d['TxPrice'] or 0} for d in details],
                'receipts': [{'type': r['PayType'], 'money': r['PayMoney'] or 0, 'memo': r['PayMemo'] or ''} for r in receipts],
                'next_reservations': [{'date': str(r['Res_Date'])[:10], 'time': r['Res_Time_0'], 'doctor': r['Res_DoctorName']} for r in next_reservations],
                'has_next_reservation': len(next_reservations) > 0
            })

        conn.close()

        return jsonify({
            "date": target_date,
            "total_count": len(result),
            "patients": result
        })

    except Exception as e:
        mssql_db.log(f"일일 내원환자 조회 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/doctors')
def doctors():
    """의사 목록 조회"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT
              UserID as name,
              퇴사여부 as resigned,
              기타여부 as isOther,
              근무기간시작 as workStartDate,
              근무기간종료 as workEndDate
            FROM UserInfo.dbo.UserTable
            ORDER BY UserID
        """)

        rows = cursor.fetchall()
        conn.close()

        colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']
        doctors = []
        for i, doc in enumerate(rows):
            doctors.append({
                'id': f'doctor_{i+1}',
                'name': doc['name'].strip() if doc['name'] else '',
                'color': colors[i % len(colors)],
                'resigned': doc['resigned'] == 1,
                'isOther': doc['isOther'] == 1,
                'workStartDate': doc['workStartDate'],
                'workEndDate': doc['workEndDate']
            })

        return jsonify(doctors)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/patients/<int:patient_id>/treatments')
def patient_treatments(patient_id):
    """환자별 진료 내역"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        limit = int(request.args.get('limit', 50))

        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT TOP %s
              d.Detail_PK as id,
              d.TxDate as date,
              d.TxTime as time,
              d.TxItem as item,
              d.DxName as diagnosis,
              d.PxName as treatment,
              d.TxDoctor as doctor,
              d.TxMoney as amount,
              d.ETCNote as note
            FROM Detail d
            WHERE d.Customer_PK = %s
            ORDER BY d.TxDate DESC, d.TxTime DESC
        """, (limit, patient_id))

        rows = cursor.fetchall()
        conn.close()

        treatments = []
        for t in rows:
            treatments.append({
                **t,
                'date': t['date'].strftime('%Y-%m-%d') if t['date'] else None
            })

        return jsonify(treatments)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/patients/<int:patient_id>/detail-comments')
def patient_detail_comments(patient_id):
    """환자별 날짜별 진료메모 조회 (DetailComment + Detail 조인으로 담당의 포함)"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        limit = int(request.args.get('limit', 20))

        cursor = conn.cursor(as_dict=True)
        # DetailComment에서 Comment1, Comment2 가져오고, Detail에서 담당의(TxDoctor) 조인
        cursor.execute("""
            SELECT TOP %s
              dc.Customer_PK as patient_id,
              dc.TxDate as date,
              dc.Comment1 as comment1,
              dc.Comment2 as comment2,
              (SELECT TOP 1 d.TxDoctor
               FROM MasterDB.dbo.Detail d
               WHERE d.Customer_PK = dc.Customer_PK
                 AND CAST(d.TxDate as DATE) = CAST(dc.TxDate as DATE)
               ORDER BY d.Detail_PK) as doctor
            FROM MasterDB.dbo.DetailComment dc
            WHERE dc.Customer_PK = %s
            ORDER BY dc.TxDate DESC
        """, (limit, patient_id))

        rows = cursor.fetchall()
        conn.close()

        comments = []
        for r in rows:
            comments.append({
                'patient_id': r['patient_id'],
                'date': r['date'].strftime('%Y-%m-%d') if r['date'] else None,
                'comment1': r['comment1'] or '',
                'comment2': r['comment2'] or '',
                'doctor': r['doctor'] or ''
            })

        return jsonify(comments)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/queue/status')
def queue_status():
    """대기/치료 현황"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 대기실
        cursor.execute("""
            SELECT
              Waiting_PK as id,
              Customer_PK as patient_id,
              SN as chart_no,
              NAME as patient_name,
              AGE as age,
              SEX as sex,
              intotime as waiting_since,
              TxDoctor as doctor,
              ChartDone as chart_done,
              CAST(Status AS NVARCHAR(MAX)) as status,
              Progress as progress,
              RegType as reg_type
            FROM TreatCurrent.dbo.Waiting
            ORDER BY intotime ASC
        """)
        waiting = cursor.fetchall()

        # 치료실
        cursor.execute("""
            SELECT
              Treating_PK as id,
              Customer_PK as patient_id,
              BED as bed,
              SN as chart_no,
              NAME as patient_name,
              AGE as age,
              SEX as sex,
              IntoTime as treating_since,
              TxDoctor as doctor,
              ChartDone as chart_done,
              CAST(Status AS NVARCHAR(MAX)) as status
            FROM TreatCurrent.dbo.Treating
            ORDER BY IntoTime ASC
        """)
        treating = cursor.fetchall()

        # 베드 현황
        cursor.execute("""
            SELECT
              PK as id,
              BedName as bed_name,
              BedSeq as bed_seq,
              CustomerPK as patient_id,
              CustNameSN as patient_info,
              TreatStatus as treat_status,
              AlarmTime as alarm_time,
              StopTime as stop_time
            FROM TreatCurrent.dbo.TCSBed
            ORDER BY BedSeq ASC
        """)
        beds = cursor.fetchall()

        conn.close()

        # 데이터 변환
        for w in waiting:
            w['sex'] = 'M' if w['sex'] else 'F'
            if w['waiting_since']:
                w['waiting_since'] = w['waiting_since'].isoformat()

        for t in treating:
            t['sex'] = 'M' if t['sex'] else 'F'
            if t['treating_since']:
                t['treating_since'] = t['treating_since'].isoformat()

        for b in beds:
            if b['alarm_time']:
                b['alarm_time'] = b['alarm_time'].isoformat()

        return jsonify({
            "waiting": waiting,
            "treating": treating,
            "beds": beds,
            "summary": {
                "waiting_count": len(waiting),
                "treating_count": len(treating),
                "occupied_beds": len([b for b in beds if b['patient_id']]),
                "total_beds": len(beds)
            },
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/today/pending-payments')
def today_pending_payments():
    """오늘 수납대기 환자 목록 (치료실에 있고 수납이 생성된 환자)
    - 수납 정보 (본인부담금, 비급여, 미수금)
    - 치료 항목 (침, 추나, 약침, 비급여)
    - 종별 (건보/차상위/1종,2종/자보/일반/임산부/산정특례)
    """
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 1. 수납대기 환자 조회 (Receipt + Treating JOIN)
        cursor.execute("""
            SELECT
              r.Receipt_PK as id,
              r.Customer_PK as patient_id,
              r.Customer_name as patient_name,
              r.sn as chart_no,
              r.Bonin_Money as insurance_self,
              r.CheongGu_Money as insurance_claim,
              r.General_Money as general_amount,
              r.MisuMoney as unpaid,
              t.BED as bed,
              t.IntoTime as treating_since,
              c.insu_type,
              c.insu_bohum_type,
              c.insu_boho_type,
              c.pregmark,
              c.SeriousTag
            FROM Receipt r
            INNER JOIN TreatCurrent.dbo.Treating t ON r.Customer_PK = t.Customer_PK
            LEFT JOIN Customer c ON r.Customer_PK = c.Customer_PK
            WHERE CAST(r.TxDate AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY t.IntoTime ASC
        """)
        pending = cursor.fetchall()

        result = []
        for p in pending:
            patient_id = p['patient_id']

            # 2. 각 환자의 치료 항목 조회
            cursor.execute("""
                SELECT
                  TxItem,
                  PxName,
                  InsuYes,
                  추나여부상태 as choona_status,
                  IsYakChim as is_yakchim,
                  TxMoney
                FROM Detail
                WHERE Customer_PK = %s AND CAST(TxDate AS DATE) = CAST(GETDATE() AS DATE)
            """, (patient_id,))
            details = cursor.fetchall()

            # 치료 항목 분류
            has_acupuncture = False  # 침
            has_choona = False  # 추나
            has_yakchim = False  # 약침
            uncovered_items = []  # 비급여 항목
            is_jabo = False  # 자보 여부

            for d in details:
                tx_item = (d['TxItem'] or '').strip()
                px_name = (d['PxName'] or '').strip()
                is_covered = d['InsuYes']  # True = 급여

                # 자보 체크
                if '자동차보험' in tx_item:
                    is_jabo = True

                # 추나 체크 (추나여부상태 = '1')
                if d['choona_status'] == '1':
                    has_choona = True

                # 약침 체크
                if d['is_yakchim']:
                    has_yakchim = True

                # 침 체크 (급여이고, 추나/약침이 아닌 경우)
                if is_covered and not d['choona_status'] == '1' and not d['is_yakchim']:
                    if '침' in px_name or '자락' in px_name or '부항' in px_name or '뜸' in px_name:
                        has_acupuncture = True

                # 비급여 항목
                if not is_covered and d['TxMoney'] and d['TxMoney'] > 0:
                    uncovered_items.append({
                        'name': px_name or tx_item,
                        'amount': d['TxMoney']
                    })

            # 3. 종별 분류
            insurance_type = _classify_insurance_type(
                insu_type=p['insu_type'],
                insu_bohum_type=p['insu_bohum_type'],
                insu_boho_type=p['insu_boho_type'],
                pregmark=p['pregmark'],
                serious_tag=p['SeriousTag'],
                is_jabo=is_jabo
            )

            result.append({
                'id': p['id'],
                'patient_id': p['patient_id'],
                'patient_name': p['patient_name'],
                'chart_no': p['chart_no'],
                'bed': p['bed'],
                'treating_since': p['treating_since'].isoformat() if p['treating_since'] else None,
                # 수납 정보
                'insurance_self': p['insurance_self'] or 0,
                'insurance_claim': p['insurance_claim'] or 0,
                'general_amount': p['general_amount'] or 0,
                'unpaid': p['unpaid'],  # None=미수납, 0=완납, >0=부분수납
                # 치료 항목
                'treatments': {
                    'acupuncture': has_acupuncture,  # 침
                    'choona': has_choona,  # 추나
                    'yakchim': has_yakchim,  # 약침
                    'uncovered': uncovered_items  # 비급여 항목들
                },
                # 종별
                'insurance_type': insurance_type
            })

        conn.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _classify_insurance_type(insu_type, insu_bohum_type, insu_boho_type, pregmark, serious_tag, is_jabo):
    """종별 분류
    - 자보: TxItem에 '자동차보험' 포함
    - 임산부: pregmark = '1'
    - 산정특례: SeriousTag = '1'
    - 차상위: insu_bohum_type = 'C1' or 'C2'
    - 1종/2종/3종: insu_type = '보호' AND insu_boho_type = '1'/'2'/'3'
    - 건보(직장): insu_type = '직장'
    - 건보(지역): insu_type = '지역'
    - 일반: insu_type = '일반'
    """
    # 우선순위: 자보 > 임산부 > 산정특례 > 차상위 > 의료급여(1종/2종/3종) > 건보(직장/지역) > 일반
    if is_jabo:
        return '자보'

    if pregmark == '1':
        return '임산부'

    if serious_tag == '1':
        return '산정특례'

    if insu_bohum_type in ('C1', 'C2'):
        return '차상위'

    if insu_type == '보호':
        if insu_boho_type == '1':
            return '1종'
        elif insu_boho_type == '2':
            return '2종'
        elif insu_boho_type == '3':
            return '3종'
        return '의료급여'

    if insu_type == '직장':
        return '건보(직장)'

    if insu_type == '지역':
        return '건보(지역)'

    if insu_type == '일반':
        return '일반'

    return '기타'


@mssql_bp.route('/api/doctors')
def get_doctors():
    """의료진 목록 조회 (UserInfo.dbo.UserTable에서)"""
    try:
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # UserInfo DB의 UserTable에서 의료진 조회
        # usergubun: 0=의사, 1=간호사 등
        # invisible4reserve: 예약화면 표시 여부
        cursor.execute("""
            SELECT
              userpk as id,
              UserID as name,
              doctorname,
              UserSSN as ssn,
              usergubun,
              invisible4reserve
            FROM UserInfo.dbo.UserTable
            WHERE UserID IS NOT NULL AND UserID != '' AND UserID != 'DOCTOR'
            ORDER BY userpk
        """)
        doctors = cursor.fetchall()

        # 퇴사여부, 근무기간 정보는 별도 쿼리로 조회 (한글 컬럼명)
        cursor.execute("""
            SELECT
              userpk,
              퇴사여부 as resigned,
              근무기간시작 as work_start,
              근무기간종료 as work_end
            FROM UserInfo.dbo.UserTable
        """)
        status_info = {row['userpk']: row for row in cursor.fetchall()}

        conn.close()

        result = []
        for doc in doctors:
            name = doc['name'].strip() if doc['name'] else ''
            ssn = doc['ssn'] if doc['ssn'] else ''
            userpk = doc['id']
            status = status_info.get(userpk, {})

            # SSN에서 생년월일 추출 (YYMMDD 형식)
            dob = None
            if ssn and len(ssn) >= 6:
                yy = ssn[:2]
                mm = ssn[2:4]
                dd = ssn[4:6]
                try:
                    # 00-25는 2000년대, 26-99는 1900년대
                    century = '20' if int(yy) <= 25 else '19'
                    dob = f"{century}{yy}-{mm}-{dd}"
                except:
                    pass

            # 성별 추출 (주민번호 7번째 자리)
            gender = 'male'  # 기본값
            if ssn and len(ssn) >= 7:
                gender_digit = ssn[6]
                gender = 'female' if gender_digit in ('2', '4') else 'male'

            # 퇴사 여부
            resigned = status.get('resigned', False) or False
            work_start = status.get('work_start')
            work_end = status.get('work_end')

            result.append({
                'id': userpk,
                'name': name,
                'dob': dob,
                'gender': gender,
                'status': 'retired' if resigned else 'working',
                'hireDate': work_start.strftime('%Y-%m-%d') if work_start else None,
                'fireDate': work_end.strftime('%Y-%m-%d') if work_end else None,
                'invisible4reserve': doc['invisible4reserve'] or 0
            })

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/doctors/schedule')
def get_doctors_schedule():
    """의료진 근무일정 조회 (년/월 단위)"""
    try:
        year = int(request.args.get('year', datetime.now().year))
        month = int(request.args.get('month', datetime.now().month))

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        cursor.execute("""
            SELECT
              DoctorName as name,
              DoctorSSN as ssn,
              DateCNT as work_days,
              DateMark as date_mark
            FROM t_WorkDateInfoDetail
            WHERE TxYear = %s AND TxMonth = %s
            ORDER BY DoctorName
        """, (year, month))

        schedules = cursor.fetchall()
        conn.close()

        result = []
        for sch in schedules:
            name = sch['name'].strip() if sch['name'] else ''
            date_mark = sch['date_mark'] if sch['date_mark'] else ''

            # DateMark를 일자별 근무 여부로 변환
            work_dates = []
            for day, mark in enumerate(date_mark, start=1):
                if mark == '1':
                    work_dates.append(day)

            result.append({
                'name': name,
                'work_days': sch['work_days'],
                'work_dates': work_dates,
                'date_mark': date_mark
            })

        return jsonify({
            'year': year,
            'month': month,
            'schedules': result
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/doctors/today')
def get_doctors_today():
    """오늘 근무 의료진 목록"""
    try:
        today = datetime.now()
        year = today.year
        month = today.month
        day = today.day

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        cursor.execute("""
            SELECT
              DoctorName as name,
              DoctorSSN as ssn,
              DateMark as date_mark
            FROM t_WorkDateInfoDetail
            WHERE TxYear = %s AND TxMonth = %s
            ORDER BY DoctorName
        """, (year, month))

        schedules = cursor.fetchall()
        conn.close()

        working_today = []
        for sch in schedules:
            name = sch['name'].strip() if sch['name'] else ''
            date_mark = sch['date_mark'] if sch['date_mark'] else ''

            # 오늘 날짜 인덱스 확인 (1일 = index 0)
            if len(date_mark) >= day and date_mark[day - 1] == '1':
                ssn = sch['ssn'] if sch['ssn'] else ''

                # 성별 추출
                gender = 'male'
                if ssn and len(ssn) >= 7:
                    gender_digit = ssn[6]
                    gender = 'female' if gender_digit in ('2', '4') else 'male'

                working_today.append({
                    'name': name,
                    'gender': gender
                })

        return jsonify({
            'date': today.strftime('%Y-%m-%d'),
            'doctors': working_today,
            'count': len(working_today)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/statistics')
def get_statistics():
    """통계 대시보드 API

    Query params:
    - period: 'daily' | 'weekly' | 'monthly' (default: daily)
    - date: YYYY-MM-DD (default: today)

    Returns:
    - patients: 환자 현황 (침/자보/약 초진/재초진)
    - chuna: 추나 현황 (건보단순/복잡, 자보, 비급여)
    - reservations: 예약 현황 (예약율, 현장예약율)
    - revenue: 매출 현황 (급여/자보/비급여)
    """
    try:
        period = request.args.get('period', 'daily')  # daily, weekly, monthly
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        # 날짜 범위 계산
        from datetime import timedelta
        base_date = datetime.strptime(target_date, '%Y-%m-%d')

        if period == 'daily':
            start_date = target_date
            end_date = target_date
        elif period == 'weekly':
            # 해당 주의 월요일부터 일요일
            days_since_monday = base_date.weekday()
            start_date = (base_date - timedelta(days=days_since_monday)).strftime('%Y-%m-%d')
            end_date = (base_date + timedelta(days=6 - days_since_monday)).strftime('%Y-%m-%d')
        elif period == 'monthly':
            # 해당 월 1일부터 말일
            start_date = base_date.replace(day=1).strftime('%Y-%m-%d')
            # 다음달 1일 - 1일 = 이번달 말일
            if base_date.month == 12:
                end_date = base_date.replace(year=base_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_date = base_date.replace(month=base_date.month + 1, day=1) - timedelta(days=1)
            end_date = end_date.strftime('%Y-%m-%d')
        else:
            start_date = target_date
            end_date = target_date

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 날짜 형식 검증 (SQL Injection 방지)
        import re
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', start_date) or not re.match(r'^\d{4}-\d{2}-\d{2}$', end_date):
            return jsonify({"error": "Invalid date format"}), 400

        # === 1. 환자 현황 ===
        # 1-1. 침초진 (신규등록 + 건보청구 > 0 + NOT 자보)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND ISNULL(r.CheongGu_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d
                WHERE d.Customer_PK = c.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
        """)
        chim_chojin = cursor.fetchone()['cnt']

        # 1-2. 자보초진 (신규등록 + 자보환자)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            JOIN Detail d ON c.Customer_PK = d.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND d.TxItem LIKE '%자동차보험%'
        """)
        jabo_chojin = cursor.fetchone()['cnt']

        # 1-3. 약초진 (신규등록 + 청구금0 + 비급여 > 0)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND ISNULL(r.CheongGu_Money, 0) = 0
              AND ISNULL(r.General_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d
                WHERE d.Customer_PK = c.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
        """)
        yak_chojin = cursor.fetchone()['cnt']

        # 1-4. 침환자 재초진 (기존환자 + 진찰료(초진) + NOT 자보)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT d.Customer_PK) as cnt
            FROM Detail d
            JOIN Customer c ON d.Customer_PK = c.Customer_PK
            JOIN Receipt r ON d.Customer_PK = r.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(c.reg_date AS DATE) < CAST(d.TxDate AS DATE)
              AND d.PxName = N'진찰료(초진)'
              AND ISNULL(r.CheongGu_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = d.Customer_PK
                AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE)
                AND d2.TxItem LIKE '%자동차보험%'
              )
        """)
        chim_rechojin = cursor.fetchone()['cnt']

        # 1-5. 자보 재초진 (기존환자 + 새 사고번호)
        cursor.execute(f"""
            WITH PeriodJabo AS (
                SELECT DISTINCT d.Customer_PK, d.사고번호
                FROM Detail d
                JOIN Customer c ON d.Customer_PK = c.Customer_PK
                WHERE d.TxItem LIKE '%자동차보험%'
                  AND CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                  AND CAST(c.reg_date AS DATE) < '{start_date}'
                  AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
            ),
            PreviousAccidents AS (
                SELECT DISTINCT d.Customer_PK, d.사고번호
                FROM Detail d
                WHERE d.TxItem LIKE '%자동차보험%'
                  AND CAST(d.TxDate AS DATE) < '{start_date}'
                  AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
            )
            SELECT COUNT(DISTINCT pj.Customer_PK) as cnt
            FROM PeriodJabo pj
            WHERE NOT EXISTS (
                SELECT 1 FROM PreviousAccidents pa
                WHERE pa.Customer_PK = pj.Customer_PK AND pa.사고번호 = pj.사고번호
            )
        """)
        jabo_rechojin = cursor.fetchone()['cnt']

        # 1-6. 약환자 재초진 (기존환자 + 3개월 공백 후 비급여)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            JOIN Customer c ON r.Customer_PK = c.Customer_PK
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(c.reg_date AS DATE) < '{start_date}'
              AND ISNULL(r.CheongGu_Money, 0) = 0
              AND ISNULL(r.General_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d
                WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
              AND NOT EXISTS (
                SELECT 1 FROM Receipt r2
                WHERE r2.Customer_PK = r.Customer_PK
                AND CAST(r2.TxDate AS DATE) < CAST(r.TxDate AS DATE)
                AND CAST(r2.TxDate AS DATE) >= DATEADD(MONTH, -3, CAST(r.TxDate AS DATE))
              )
        """)
        yak_rechojin = cursor.fetchone()['cnt']

        # 1-7. 평균 침환자수 (기간 내 일평균)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CONCAT(r.Customer_PK, '_', CAST(r.TxDate AS DATE))) as total_visits
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (
                ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (
                  SELECT 1 FROM Detail d
                  WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                  AND d.TxItem LIKE '%자동차보험%'
                )
              )
        """)
        total_chim_visits = cursor.fetchone()['total_visits']

        # 기간 내 영업일 수 계산
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CAST(TxDate AS DATE)) as work_days
            FROM Receipt
            WHERE CAST(TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """)
        work_days = cursor.fetchone()['work_days'] or 1
        avg_chim_patients = round(total_chim_visits / work_days, 1)

        # === 2. 추나 현황 ===
        cursor.execute(f"""
            SELECT
                SUM(CASE WHEN d.InsuYes = 1 AND d.PxName LIKE '%단순추나%' THEN 1 ELSE 0 END) as simple_chuna,
                SUM(CASE WHEN d.InsuYes = 1 AND d.PxName LIKE '%복잡추나%' THEN 1 ELSE 0 END) as complex_chuna,
                SUM(CASE WHEN d.TxItem LIKE '%자동차보험%' AND d.PxName LIKE '%추나%' THEN 1 ELSE 0 END) as jabo_chuna,
                SUM(CASE WHEN d.InsuYes = 0 AND d.PxName LIKE '%추나%' AND d.TxItem NOT LIKE '%자동차보험%' THEN 1 ELSE 0 END) as uncovered_chuna
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.PxName LIKE '%추나%'
        """)
        chuna = cursor.fetchone()

        # === 3. 예약 현황 ===
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as total_chim
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (
                ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (
                  SELECT 1 FROM Detail d
                  WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                  AND d.TxItem LIKE '%자동차보험%'
                )
              )
        """)
        total_chim = cursor.fetchone()['total_chim']

        # 예약하고 온 환자 (치료일에 해당 날짜 예약이 있었던 경우)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as reserved_count
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (
                ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (
                  SELECT 1 FROM Detail d
                  WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                  AND d.TxItem LIKE '%자동차보험%'
                )
              )
              AND EXISTS (
                SELECT 1 FROM Reservation_New res
                WHERE res.Customer_PK = r.Customer_PK
                AND CAST(res.Res_Date AS DATE) = CAST(r.TxDate AS DATE)
              )
        """)
        reserved_count = cursor.fetchone()['reserved_count']

        # 현장예약 (치료받은 날 미래 예약 생성)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as onsite_count
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (
                ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (
                  SELECT 1 FROM Detail d
                  WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                  AND d.TxItem LIKE '%자동차보험%'
                )
              )
              AND EXISTS (
                SELECT 1 FROM Reservation_New res
                WHERE res.Customer_PK = r.Customer_PK
                AND CAST(res.Res_Date AS DATE) > CAST(r.TxDate AS DATE)
                AND CAST(res.Res_updatetime AS DATE) = CAST(r.TxDate AS DATE)
              )
        """)
        onsite_count = cursor.fetchone()['onsite_count']

        reservation_rate = round(reserved_count / total_chim * 100, 1) if total_chim > 0 else 0
        onsite_rate = round(onsite_count / total_chim * 100, 1) if total_chim > 0 else 0

        # === 4. 매출 현황 ===
        cursor.execute(f"""
            WITH JaboReceipts AS (
                SELECT DISTINCT r.Receipt_PK
                FROM Receipt r
                WHERE EXISTS (
                    SELECT 1 FROM Detail d
                    WHERE d.Customer_PK = r.Customer_PK
                    AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                    AND d.TxItem LIKE '%자동차보험%'
                )
            )
            SELECT
                SUM(CASE WHEN jr.Receipt_PK IS NULL THEN ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) ELSE 0 END) as insurance_revenue,
                SUM(CASE WHEN jr.Receipt_PK IS NOT NULL THEN ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) + ISNULL(r.General_Money, 0) ELSE 0 END) as jabo_revenue,
                SUM(CASE WHEN jr.Receipt_PK IS NULL THEN ISNULL(r.General_Money, 0) ELSE 0 END) as uncovered_revenue
            FROM Receipt r
            LEFT JOIN JaboReceipts jr ON r.Receipt_PK = jr.Receipt_PK
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """)
        revenue = cursor.fetchone()

        conn.close()

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "work_days": work_days,
            "patients": {
                "chim_chojin": chim_chojin,
                "chim_rechojin": chim_rechojin,
                "jabo_chojin": jabo_chojin,
                "jabo_rechojin": jabo_rechojin,
                "yak_chojin": yak_chojin,
                "yak_rechojin": yak_rechojin,
                "avg_chim_daily": avg_chim_patients
            },
            "chuna": {
                "insurance_simple": int(chuna['simple_chuna'] or 0),
                "insurance_complex": int(chuna['complex_chuna'] or 0),
                "jabo": int(chuna['jabo_chuna'] or 0),
                "uncovered": int(chuna['uncovered_chuna'] or 0)
            },
            "reservations": {
                "total_chim_patients": total_chim,
                "reserved_count": reserved_count,
                "reservation_rate": reservation_rate,
                "onsite_count": onsite_count,
                "onsite_rate": onsite_rate
            },
            "revenue": {
                "insurance": int(revenue['insurance_revenue'] or 0),
                "jabo": int(revenue['jabo_revenue'] or 0),
                "uncovered": int(revenue['uncovered_revenue'] or 0),
                "total": int((revenue['insurance_revenue'] or 0) + (revenue['jabo_revenue'] or 0) + (revenue['uncovered_revenue'] or 0))
            }
        })

    except Exception as e:
        mssql_db.log(f"통계 조회 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/statistics2')
def get_statistics2():
    """통계 대시보드 API v2 - 단순화된 버전

    Query params:
    - period: 'daily' | 'weekly' | 'monthly' (default: daily)
    - date: YYYY-MM-DD (default: today)
    """
    import re
    from datetime import timedelta

    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        # 날짜 형식 검증
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', target_date):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        base_date = datetime.strptime(target_date, '%Y-%m-%d')

        # 날짜 범위 계산
        if period == 'daily':
            start_date = target_date
            end_date = target_date
        elif period == 'weekly':
            days_since_monday = base_date.weekday()
            start_date = (base_date - timedelta(days=days_since_monday)).strftime('%Y-%m-%d')
            end_date = (base_date + timedelta(days=6 - days_since_monday)).strftime('%Y-%m-%d')
        elif period == 'monthly':
            start_date = base_date.replace(day=1).strftime('%Y-%m-%d')
            if base_date.month == 12:
                end_date = base_date.replace(year=base_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_date = base_date.replace(month=base_date.month + 1, day=1) - timedelta(days=1)
            end_date = end_date.strftime('%Y-%m-%d')
        else:
            start_date = target_date
            end_date = target_date

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 개별 쿼리로 각 통계 수집 (pymssql DECLARE 호환성 문제로 인해)
        stats = {}

        # 1. 침초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND ISNULL(r.CheongGu_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d WHERE d.Customer_PK = c.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
        """)
        stats['chim_chojin'] = cursor.fetchone()['cnt']

        # 2. 자보초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN Detail d ON c.Customer_PK = d.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND d.TxItem LIKE '%자동차보험%'
        """)
        stats['jabo_chojin'] = cursor.fetchone()['cnt']

        # 3. 약초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND ISNULL(r.CheongGu_Money, 0) = 0
              AND ISNULL(r.General_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d WHERE d.Customer_PK = c.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
        """)
        stats['yak_chojin'] = cursor.fetchone()['cnt']

        # 4. 침환자 재초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT d.Customer_PK) as cnt
            FROM Detail d
            INNER JOIN Customer c ON d.Customer_PK = c.Customer_PK
            INNER JOIN Receipt r ON d.Customer_PK = r.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(c.reg_date AS DATE) < CAST(d.TxDate AS DATE)
              AND d.PxName = N'진찰료(초진)'
              AND ISNULL(r.CheongGu_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = d.Customer_PK
                AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE)
                AND d2.TxItem LIKE '%자동차보험%'
              )
        """)
        stats['chim_rechojin'] = cursor.fetchone()['cnt']

        # 5. 자보 재초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT pj.Customer_PK) as cnt
            FROM (
                SELECT DISTINCT d.Customer_PK, d.사고번호
                FROM Detail d
                INNER JOIN Customer c ON d.Customer_PK = c.Customer_PK
                WHERE d.TxItem LIKE '%자동차보험%'
                  AND CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                  AND CAST(c.reg_date AS DATE) < '{start_date}'
                  AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
            ) pj
            WHERE NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = pj.Customer_PK
                  AND d2.사고번호 = pj.사고번호
                  AND d2.TxItem LIKE '%자동차보험%'
                  AND CAST(d2.TxDate AS DATE) < '{start_date}'
            )
        """)
        stats['jabo_rechojin'] = cursor.fetchone()['cnt']

        # 6. 약환자 재초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            INNER JOIN Customer c ON r.Customer_PK = c.Customer_PK
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(c.reg_date AS DATE) < '{start_date}'
              AND ISNULL(r.CheongGu_Money, 0) = 0
              AND ISNULL(r.General_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
              AND NOT EXISTS (
                SELECT 1 FROM Receipt r2 WHERE r2.Customer_PK = r.Customer_PK
                AND CAST(r2.TxDate AS DATE) < CAST(r.TxDate AS DATE)
                AND CAST(r2.TxDate AS DATE) >= DATEADD(MONTH, -3, CAST(r.TxDate AS DATE))
              )
        """)
        stats['yak_rechojin'] = cursor.fetchone()['cnt']

        # 7. 총 침환자 방문수
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CONCAT(r.Customer_PK, '_', CAST(r.TxDate AS DATE))) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%'))
        """)
        stats['total_visits'] = cursor.fetchone()['cnt']

        # 8. 영업일 수
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CAST(TxDate AS DATE)) as cnt
            FROM Receipt WHERE CAST(TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """)
        stats['work_days'] = cursor.fetchone()['cnt']

        # 9. 총 침환자수
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%'))
        """)
        stats['total_chim'] = cursor.fetchone()['cnt']

        # 10. 예약하고 온 환자
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%'))
              AND EXISTS (SELECT 1 FROM Reservation_New res WHERE res.Customer_PK = r.Customer_PK
                AND CAST(res.Res_Date AS DATE) = CAST(r.TxDate AS DATE))
        """)
        stats['reserved_count'] = cursor.fetchone()['cnt']

        # 11. 현장예약
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%'))
              AND EXISTS (SELECT 1 FROM Reservation_New res WHERE res.Customer_PK = r.Customer_PK
                AND CAST(res.Res_Date AS DATE) > CAST(r.TxDate AS DATE)
                AND CAST(res.Res_updatetime AS DATE) = CAST(r.TxDate AS DATE))
        """)
        stats['onsite_count'] = cursor.fetchone()['cnt']

        # 추나 현황 쿼리
        cursor.execute(f"""
            SELECT
                SUM(CASE WHEN d.InsuYes = 1 AND d.PxName LIKE '%단순추나%' THEN 1 ELSE 0 END) as simple_chuna,
                SUM(CASE WHEN d.InsuYes = 1 AND d.PxName LIKE '%복잡추나%' THEN 1 ELSE 0 END) as complex_chuna,
                SUM(CASE WHEN d.TxItem LIKE '%자동차보험%' AND d.PxName LIKE '%추나%' THEN 1 ELSE 0 END) as jabo_chuna,
                SUM(CASE WHEN d.InsuYes = 0 AND d.PxName LIKE '%추나%' AND d.TxItem NOT LIKE '%자동차보험%' THEN 1 ELSE 0 END) as uncovered_chuna
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.PxName LIKE '%추나%'
        """)
        chuna = cursor.fetchone()

        # 매출 현황 쿼리
        cursor.execute(f"""
            SELECT
                SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                    AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
                    THEN ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) ELSE 0 END) as insurance_revenue,
                SUM(CASE WHEN EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                    AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
                    THEN ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) + ISNULL(r.General_Money, 0) ELSE 0 END) as jabo_revenue,
                SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                    AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
                    THEN ISNULL(r.General_Money, 0) ELSE 0 END) as uncovered_revenue
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """)
        revenue = cursor.fetchone()

        conn.close()

        # 계산
        work_days = stats.get('work_days', 1) or 1
        total_visits = stats.get('total_visits', 0)
        total_chim = stats.get('total_chim', 0)
        reserved_count = stats.get('reserved_count', 0)
        onsite_count = stats.get('onsite_count', 0)

        avg_chim_daily = round(total_visits / work_days, 1)
        reservation_rate = round(reserved_count / total_chim * 100, 1) if total_chim > 0 else 0
        onsite_rate = round(onsite_count / total_chim * 100, 1) if total_chim > 0 else 0

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "work_days": work_days,
            "patients": {
                "chim_chojin": stats.get('chim_chojin', 0),
                "chim_rechojin": stats.get('chim_rechojin', 0),
                "jabo_chojin": stats.get('jabo_chojin', 0),
                "jabo_rechojin": stats.get('jabo_rechojin', 0),
                "yak_chojin": stats.get('yak_chojin', 0),
                "yak_rechojin": stats.get('yak_rechojin', 0),
                "avg_chim_daily": avg_chim_daily
            },
            "chuna": {
                "insurance_simple": int(chuna['simple_chuna'] or 0),
                "insurance_complex": int(chuna['complex_chuna'] or 0),
                "jabo": int(chuna['jabo_chuna'] or 0),
                "uncovered": int(chuna['uncovered_chuna'] or 0)
            },
            "reservations": {
                "total_chim_patients": total_chim,
                "reserved_count": reserved_count,
                "reservation_rate": reservation_rate,
                "onsite_count": onsite_count,
                "onsite_rate": onsite_rate
            },
            "revenue": {
                "insurance": int(revenue['insurance_revenue'] or 0),
                "jabo": int(revenue['jabo_revenue'] or 0),
                "uncovered": int(revenue['uncovered_revenue'] or 0),
                "total": int((revenue['insurance_revenue'] or 0) + (revenue['jabo_revenue'] or 0) + (revenue['uncovered_revenue'] or 0))
            }
        })

    except Exception as e:
        mssql_db.log(f"통계v2 조회 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ============ 개별 통계 API (v3) ============

@mssql_bp.route('/api/stats/doctors')
def stats_doctors():
    """해당 날짜에 진료한 원장 목록 API"""
    try:
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        import re
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', target_date):
            return jsonify({"error": "Invalid date format"}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # Detail 테이블에서 해당 날짜에 진료한 의사 목록 (TxDoctor)
        cursor.execute(f"""
            SELECT DISTINCT d.TxDoctor as name
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) = '{target_date}'
              AND d.TxDoctor IS NOT NULL
              AND d.TxDoctor != ''
            ORDER BY d.TxDoctor
        """)
        doctors = [row['name'] for row in cursor.fetchall()]
        conn.close()

        return jsonify({"date": target_date, "doctors": doctors})

    except Exception as e:
        mssql_db.log(f"원장목록 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


def _get_date_range(period: str, target_date: str):
    """날짜 범위 계산 헬퍼"""
    import re
    from datetime import timedelta

    if not re.match(r'^\d{4}-\d{2}-\d{2}$', target_date):
        return None, None, "Invalid date format"

    base_date = datetime.strptime(target_date, '%Y-%m-%d')

    if period == 'daily':
        start_date = target_date
        end_date = target_date
    elif period == 'weekly':
        days_since_monday = base_date.weekday()
        start_date = (base_date - timedelta(days=days_since_monday)).strftime('%Y-%m-%d')
        end_date = (base_date + timedelta(days=6 - days_since_monday)).strftime('%Y-%m-%d')
    elif period == 'monthly':
        start_date = base_date.replace(day=1).strftime('%Y-%m-%d')
        if base_date.month == 12:
            end_date = base_date.replace(year=base_date.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = base_date.replace(month=base_date.month + 1, day=1) - timedelta(days=1)
        end_date = end_date.strftime('%Y-%m-%d')
    else:
        start_date = target_date
        end_date = target_date

    return start_date, end_date, None


@mssql_bp.route('/api/stats/patients')
def stats_patients():
    """환자 현황 통계 API"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        doctor = request.args.get('doctor', '')  # 원장 필터

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        # doctor 파라미터 SQL injection 방지
        import re
        if doctor and not re.match(r'^[\w가-힣\s]+$', doctor):
            return jsonify({"error": "Invalid doctor name"}), 400

        doctor_filter = f"AND d.TxDoctor = N'{doctor}'" if doctor else ""
        doctor_filter_detail = f"AND d2.TxDoctor = N'{doctor}'" if doctor else ""

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 침초진 (원장별: 해당 환자가 그날 해당 원장에게 진료받았는지)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND ISNULL(r.CheongGu_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d WHERE d.Customer_PK = c.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
              {f"AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = c.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE) AND d.TxDoctor = N'{doctor}')" if doctor else ""}
        """)
        chim_chojin = cursor.fetchone()['cnt']

        # 자보초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN Detail d ON c.Customer_PK = d.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND d.TxItem LIKE '%자동차보험%'
              {f"AND d.TxDoctor = N'{doctor}'" if doctor else ""}
        """)
        jabo_chojin = cursor.fetchone()['cnt']

        # 약초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE CAST(c.reg_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND ISNULL(r.CheongGu_Money, 0) = 0
              AND ISNULL(r.General_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d WHERE d.Customer_PK = c.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
              {f"AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = c.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(c.reg_date AS DATE) AND d.TxDoctor = N'{doctor}')" if doctor else ""}
        """)
        yak_chojin = cursor.fetchone()['cnt']

        # 침환자 재초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT d.Customer_PK) as cnt
            FROM Detail d
            INNER JOIN Customer c ON d.Customer_PK = c.Customer_PK
            INNER JOIN Receipt r ON d.Customer_PK = r.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(c.reg_date AS DATE) < CAST(d.TxDate AS DATE)
              AND d.PxName = N'진찰료(초진)'
              AND ISNULL(r.CheongGu_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = d.Customer_PK
                AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE)
                AND d2.TxItem LIKE '%자동차보험%'
              )
              {f"AND d.TxDoctor = N'{doctor}'" if doctor else ""}
        """)
        chim_rechojin = cursor.fetchone()['cnt']

        # 자보 재초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT pj.Customer_PK) as cnt
            FROM (
                SELECT DISTINCT d.Customer_PK, d.사고번호
                FROM Detail d
                INNER JOIN Customer c ON d.Customer_PK = c.Customer_PK
                WHERE d.TxItem LIKE '%자동차보험%'
                  AND CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                  AND CAST(c.reg_date AS DATE) < '{start_date}'
                  AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
                  {f"AND d.TxDoctor = N'{doctor}'" if doctor else ""}
            ) pj
            WHERE NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = pj.Customer_PK
                  AND d2.사고번호 = pj.사고번호
                  AND d2.TxItem LIKE '%자동차보험%'
                  AND CAST(d2.TxDate AS DATE) < '{start_date}'
            )
        """)
        jabo_rechojin = cursor.fetchone()['cnt']

        # 약환자 재초진
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            INNER JOIN Customer c ON r.Customer_PK = c.Customer_PK
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND CAST(c.reg_date AS DATE) < '{start_date}'
              AND ISNULL(r.CheongGu_Money, 0) = 0
              AND ISNULL(r.General_Money, 0) > 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                AND d.TxItem LIKE '%자동차보험%'
              )
              AND NOT EXISTS (
                SELECT 1 FROM Receipt r2 WHERE r2.Customer_PK = r.Customer_PK
                AND CAST(r2.TxDate AS DATE) < CAST(r.TxDate AS DATE)
                AND CAST(r2.TxDate AS DATE) >= DATEADD(MONTH, -3, CAST(r.TxDate AS DATE))
              )
              {f"AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxDoctor = N'{doctor}')" if doctor else ""}
        """)
        yak_rechojin = cursor.fetchone()['cnt']

        # 건보 침환자 총 방문수 (자보 제외)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CONCAT(r.Customer_PK, '_', CAST(r.TxDate AS DATE))) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND ISNULL(r.CheongGu_Money, 0) > 0
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
              {f"AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxDoctor = N'{doctor}')" if doctor else ""}
        """)
        chim_total_visits = cursor.fetchone()['cnt']

        # 자보 침환자 총 방문수
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CONCAT(d.Customer_PK, '_', CAST(d.TxDate AS DATE))) as cnt
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.TxItem LIKE '%자동차보험%'
              {f"AND d.TxDoctor = N'{doctor}'" if doctor else ""}
        """)
        jabo_total_visits = cursor.fetchone()['cnt']

        # 건보 재진 = 총방문 - 초진 - 재초진
        chim_rejin = chim_total_visits - chim_chojin - chim_rechojin
        if chim_rejin < 0:
            chim_rejin = 0

        # 자보 재진 = 총방문 - 초진 - 재초진
        jabo_rejin = jabo_total_visits - jabo_chojin - jabo_rechojin
        if jabo_rejin < 0:
            jabo_rejin = 0

        # 영업일 수
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CAST(TxDate AS DATE)) as cnt
            FROM Receipt WHERE CAST(TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """)
        work_days = cursor.fetchone()['cnt'] or 1

        conn.close()

        total_visits = chim_total_visits + jabo_total_visits
        avg_chim_daily = round(total_visits / work_days, 1) if work_days > 0 else 0

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "work_days": work_days,
            "doctor": doctor if doctor else "전체",
            "chim_chojin": chim_chojin,
            "chim_rechojin": chim_rechojin,
            "chim_rejin": chim_rejin,
            "jabo_chojin": jabo_chojin,
            "jabo_rechojin": jabo_rechojin,
            "jabo_rejin": jabo_rejin,
            "yak_chojin": yak_chojin,
            "yak_rechojin": yak_rechojin,
            "avg_chim_daily": avg_chim_daily
        })

    except Exception as e:
        mssql_db.log(f"환자통계 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/chuna')
def stats_chuna():
    """추나 현황 통계 API"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        doctor = request.args.get('doctor', '')

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        import re
        if doctor and not re.match(r'^[\w가-힣\s]+$', doctor):
            return jsonify({"error": "Invalid doctor name"}), 400

        doctor_filter = f"AND d.TxDoctor = N'{doctor}'" if doctor else ""

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 자보추나 카운트 (같은 환자의 같은 날에 자동차보험 항목이 있는 경우)
        cursor.execute(f"""
            SELECT COUNT(*) as cnt
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.PxName LIKE '%추나%'
              AND EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = d.Customer_PK
                  AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE)
                  AND d2.TxItem LIKE '%자동차보험%'
              )
              {doctor_filter}
        """)
        jabo = cursor.fetchone()['cnt'] or 0

        # 건보 단순추나 (자보환자 제외)
        cursor.execute(f"""
            SELECT COUNT(*) as cnt
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.PxName LIKE '%단순추나%'
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = d.Customer_PK
                  AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE)
                  AND d2.TxItem LIKE '%자동차보험%'
              )
              {doctor_filter}
        """)
        insurance_simple = cursor.fetchone()['cnt'] or 0

        # 건보 복잡추나 (자보환자 제외)
        cursor.execute(f"""
            SELECT COUNT(*) as cnt
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.PxName LIKE '%복잡추나%'
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = d.Customer_PK
                  AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE)
                  AND d2.TxItem LIKE '%자동차보험%'
              )
              {doctor_filter}
        """)
        insurance_complex = cursor.fetchone()['cnt'] or 0

        # 비급여 추나 (자보환자 제외, 급여 아닌 것)
        cursor.execute(f"""
            SELECT COUNT(*) as cnt
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.PxName LIKE '%추나%'
              AND d.InsuYes = 0
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = d.Customer_PK
                  AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE)
                  AND d2.TxItem LIKE '%자동차보험%'
              )
              {doctor_filter}
        """)
        uncovered = cursor.fetchone()['cnt'] or 0

        conn.close()

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "doctor": doctor if doctor else "전체",
            "insurance_simple": insurance_simple,
            "insurance_complex": insurance_complex,
            "jabo": jabo,
            "uncovered": uncovered
        })

    except Exception as e:
        mssql_db.log(f"추나통계 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/reservations')
def stats_reservations():
    """예약 현황 통계 API"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        doctor = request.args.get('doctor', '')

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        import re
        if doctor and not re.match(r'^[\w가-힣\s]+$', doctor):
            return jsonify({"error": "Invalid doctor name"}), 400

        doctor_exists_filter = f"AND EXISTS (SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = r.Customer_PK AND CAST(d2.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d2.TxDoctor = N'{doctor}')" if doctor else ""

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 총 침환자수
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%'))
              {doctor_exists_filter}
        """)
        total_chim = cursor.fetchone()['cnt']

        # 예약하고 온 환자
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%'))
              AND EXISTS (SELECT 1 FROM Reservation_New res WHERE res.Res_Customer_PK = r.Customer_PK
                AND CAST(res.Res_Date AS DATE) = CAST(r.TxDate AS DATE))
              {doctor_exists_filter}
        """)
        reserved_count = cursor.fetchone()['cnt']

        # 현장예약
        cursor.execute(f"""
            SELECT COUNT(DISTINCT r.Customer_PK) as cnt
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND (ISNULL(r.CheongGu_Money, 0) > 0
                OR EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                  AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%'))
              AND EXISTS (SELECT 1 FROM Reservation_New res WHERE res.Res_Customer_PK = r.Customer_PK
                AND CAST(res.Res_Date AS DATE) > CAST(r.TxDate AS DATE)
                AND CAST(res.Res_updatetime AS DATE) = CAST(r.TxDate AS DATE))
              {doctor_exists_filter}
        """)
        onsite_count = cursor.fetchone()['cnt']

        conn.close()

        reservation_rate = round(reserved_count / total_chim * 100, 1) if total_chim > 0 else 0
        onsite_rate = round(onsite_count / total_chim * 100, 1) if total_chim > 0 else 0

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "doctor": doctor if doctor else "전체",
            "total_chim_patients": total_chim,
            "reserved_count": reserved_count,
            "reservation_rate": reservation_rate,
            "onsite_count": onsite_count,
            "onsite_rate": onsite_rate
        })

    except Exception as e:
        mssql_db.log(f"예약통계 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/revenue')
def stats_revenue():
    """매출 현황 통계 API"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        doctor = request.args.get('doctor', '')

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        import re
        if doctor and not re.match(r'^[\w가-힣\s]+$', doctor):
            return jsonify({"error": "Invalid doctor name"}), 400

        doctor_exists_filter = f"AND EXISTS (SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = r.Customer_PK AND CAST(d2.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d2.TxDoctor = N'{doctor}')" if doctor else ""

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0)), 0) as insurance
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
              {doctor_exists_filter}
        """)
        insurance = int(cursor.fetchone()['insurance'] or 0)

        # 건보추나 매출 (자보 제외, 추나 행의 TxMoney 합계)
        doctor_filter_detail = f"AND d.TxDoctor = N'{doctor}'" if doctor else ""
        cursor.execute(f"""
            SELECT ISNULL(SUM(ISNULL(d.TxMoney, 0)), 0) as chuna_revenue
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND d.PxName LIKE '%추나%'
              AND NOT EXISTS (SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = d.Customer_PK
                AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE) AND d2.TxItem LIKE '%자동차보험%')
              {doctor_filter_detail}
        """)
        chuna_revenue = int(cursor.fetchone()['chuna_revenue'] or 0)

        # 자보매출
        cursor.execute(f"""
            SELECT ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) + ISNULL(r.General_Money, 0)), 0) as jabo
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
              {doctor_exists_filter}
        """)
        jabo = int(cursor.fetchone()['jabo'] or 0)

        # 비급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT ISNULL(SUM(ISNULL(r.General_Money, 0)), 0) as uncovered
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{start_date}' AND '{end_date}'
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
              {doctor_exists_filter}
        """)
        uncovered = int(cursor.fetchone()['uncovered'] or 0)

        conn.close()

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "doctor": doctor if doctor else "전체",
            "insurance": insurance,
            "chuna_revenue": chuna_revenue,
            "jabo": jabo,
            "uncovered": uncovered,
            "total": insurance + jabo + uncovered
        })

    except Exception as e:
        mssql_db.log(f"매출통계 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/revenue-trend')
def stats_revenue_trend():
    """최근 18개월/18주 매출 추이 API (최적화: 4개 쿼리로 한번에 조회)

    Query params:
    - end_date: 기준일 (YYYY-MM-DD, 기본값: 현재)
    - period: 'monthly' (기본값) 또는 'weekly' (18주 추이)
    """
    try:
        from datetime import datetime as dt, timedelta
        import calendar

        def add_months(date, months):
            """월 덧셈/뺄셈 함수 (dateutil 대체)"""
            month = date.month - 1 + months
            year = date.year + month // 12
            month = month % 12 + 1
            day = min(date.day, calendar.monthrange(year, month)[1])
            return date.replace(year=year, month=month, day=day)

        end_date_str = request.args.get('end_date', dt.now().strftime('%Y-%m-%d'))
        period = request.args.get('period', 'monthly')

        if period == 'weekly':
            # 주간: end_date가 속한 주의 일요일까지
            end_date = dt.strptime(end_date_str[:10], '%Y-%m-%d')
            # 해당 주의 일요일로 이동 (일요일=6)
            days_until_sunday = 6 - end_date.weekday()
            end_sunday = end_date + timedelta(days=days_until_sunday)
            # 18주 전 월요일
            start_monday = end_sunday - timedelta(weeks=17, days=6)
            range_start = start_monday.strftime('%Y-%m-%d')
            range_end = end_sunday.strftime('%Y-%m-%d')
        else:
            # 월간: 기존 로직
            end_date = dt.strptime(end_date_str[:7] + '-01', '%Y-%m-%d')
            start_month = add_months(end_date, -17)
            range_start = start_month.strftime('%Y-%m-01')
            next_month = add_months(end_date, 1)
            range_end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 주간/월간에 따라 GROUP BY 형식 결정
        if period == 'weekly':
            # ISO 주차: DATEPART(iso_week, date) + 연도
            group_format = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, r.TxDate), r.TxDate)) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, r.TxDate) AS VARCHAR), 2)"
            group_format_d = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, d.TxDate), d.TxDate)) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, d.TxDate) AS VARCHAR), 2)"
        else:
            group_format = "FORMAT(r.TxDate, 'yyyy-MM')"
            group_format_d = "FORMAT(d.TxDate, 'yyyy-MM')"

        # 1. 급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT {group_format} as period_key,
                   ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0)), 0) as insurance
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format}
        """)
        insurance_by_period = {row['period_key']: int(row['insurance'] or 0) for row in cursor.fetchall()}

        # 2. 추나매출 (자보 제외)
        cursor.execute(f"""
            SELECT {group_format_d} as period_key,
                   ISNULL(SUM(ISNULL(d.TxMoney, 0)), 0) as chuna
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND d.PxName LIKE '%추나%'
              AND NOT EXISTS (SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = d.Customer_PK
                AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE) AND d2.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format_d}
        """)
        chuna_by_period = {row['period_key']: int(row['chuna'] or 0) for row in cursor.fetchall()}

        # 3. 자보매출
        cursor.execute(f"""
            SELECT {group_format} as period_key,
                   ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) + ISNULL(r.General_Money, 0)), 0) as jabo
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format}
        """)
        jabo_by_period = {row['period_key']: int(row['jabo'] or 0) for row in cursor.fetchall()}

        # 4. 비급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT {group_format} as period_key,
                   ISNULL(SUM(ISNULL(r.General_Money, 0)), 0) as uncovered
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format}
        """)
        uncovered_by_period = {row['period_key']: int(row['uncovered'] or 0) for row in cursor.fetchall()}

        conn.close()

        # 결과 조합 (과거 -> 현재 순서)
        result = []
        if period == 'weekly':
            # 18주 결과
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                # ISO 주차 계산
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                week_label = week_monday.strftime('%m/%d')  # 해당 주의 월요일 날짜로 표시
                insurance = insurance_by_period.get(period_key, 0)
                chuna = chuna_by_period.get(period_key, 0)
                jabo = jabo_by_period.get(period_key, 0)
                uncovered = uncovered_by_period.get(period_key, 0)
                result.append({
                    "month": week_label,
                    "insurance": insurance,
                    "chuna": chuna,
                    "jabo": jabo,
                    "uncovered": uncovered,
                    "total": insurance + jabo + uncovered
                })
        else:
            # 18개월 결과
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                insurance = insurance_by_period.get(month_label, 0)
                chuna = chuna_by_period.get(month_label, 0)
                jabo = jabo_by_period.get(month_label, 0)
                uncovered = uncovered_by_period.get(month_label, 0)
                result.append({
                    "month": month_label,
                    "insurance": insurance,
                    "chuna": chuna,
                    "jabo": jabo,
                    "uncovered": uncovered,
                    "total": insurance + jabo + uncovered
                })

        return jsonify({
            "end_date": end_date.strftime('%Y-%m-%d') if period == 'weekly' else end_date.strftime('%Y-%m'),
            "period": period,
            "data": result
        })

    except Exception as e:
        mssql_db.log(f"매출추이 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/all')
def stats_all():
    """통합 통계 API - 임시 테이블 활용 최적화 버전"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # ============ 1. 임시 테이블 생성 및 데이터 로드 ============
        # 기존: 각 쿼리마다 NOT EXISTS로 자보 여부 확인 (매우 느림)
        # 개선: 한 번에 자보 여부를 계산하여 임시 테이블에 저장

        # 1-1. 기간 내 Detail 데이터 + 자보 여부 임시 테이블
        cursor.execute(f"""
            -- 기존 임시 테이블 삭제
            IF OBJECT_ID('tempdb..#TempDetail') IS NOT NULL DROP TABLE #TempDetail;
            IF OBJECT_ID('tempdb..#TempReceipt') IS NOT NULL DROP TABLE #TempReceipt;
            IF OBJECT_ID('tempdb..#JaboVisits') IS NOT NULL DROP TABLE #JaboVisits;

            -- 1) 기간 내 자보 방문 (Customer_PK + 날짜) 미리 계산
            SELECT DISTINCT d.Customer_PK, CAST(d.TxDate AS DATE) as TxDateOnly
            INTO #JaboVisits
            FROM Detail d
            WHERE d.TxDate >= '{start_date}' AND d.TxDate < DATEADD(DAY, 1, '{end_date}')
              AND d.TxItem LIKE '%자동차보험%';

            CREATE INDEX IX_JaboVisits ON #JaboVisits(Customer_PK, TxDateOnly);

            -- 2) 기간 내 Detail 데이터 + 자보 여부 플래그
            SELECT
                d.Detail_PK,
                d.Customer_PK,
                CAST(d.TxDate AS DATE) as TxDateOnly,
                d.TxDoctor,
                d.PxName,
                d.TxItem,
                d.TxMoney,
                d.InsuYes,
                d.사고번호,
                CASE WHEN j.Customer_PK IS NOT NULL THEN 1 ELSE 0 END as IsJabo
            INTO #TempDetail
            FROM Detail d
            LEFT JOIN #JaboVisits j ON d.Customer_PK = j.Customer_PK AND CAST(d.TxDate AS DATE) = j.TxDateOnly
            WHERE d.TxDate >= '{start_date}' AND d.TxDate < DATEADD(DAY, 1, '{end_date}');

            CREATE INDEX IX_TempDetail_Customer ON #TempDetail(Customer_PK, TxDateOnly);
            CREATE INDEX IX_TempDetail_Doctor ON #TempDetail(TxDoctor);
            CREATE INDEX IX_TempDetail_IsJabo ON #TempDetail(IsJabo);

            -- 3) 기간 내 Receipt 데이터 + 자보 여부 + 대표 원장
            SELECT
                r.Receipt_PK,
                r.Customer_PK,
                CAST(r.TxDate AS DATE) as TxDateOnly,
                ISNULL(r.Bonin_Money, 0) as Bonin_Money,
                ISNULL(r.CheongGu_Money, 0) as CheongGu_Money,
                ISNULL(r.General_Money, 0) as General_Money,
                CASE WHEN j.Customer_PK IS NOT NULL THEN 1 ELSE 0 END as IsJabo,
                (SELECT TOP 1 td.TxDoctor FROM #TempDetail td
                 WHERE td.Customer_PK = r.Customer_PK AND td.TxDateOnly = CAST(r.TxDate AS DATE)
                 AND td.TxDoctor IS NOT NULL AND td.TxDoctor != ''
                 ORDER BY td.Detail_PK) as RepDoctor
            INTO #TempReceipt
            FROM Receipt r
            LEFT JOIN #JaboVisits j ON r.Customer_PK = j.Customer_PK AND CAST(r.TxDate AS DATE) = j.TxDateOnly
            WHERE r.TxDate >= '{start_date}' AND r.TxDate < DATEADD(DAY, 1, '{end_date}');

            CREATE INDEX IX_TempReceipt_Customer ON #TempReceipt(Customer_PK, TxDateOnly);
            CREATE INDEX IX_TempReceipt_IsJabo ON #TempReceipt(IsJabo);
            CREATE INDEX IX_TempReceipt_RepDoctor ON #TempReceipt(RepDoctor);
        """)

        # 영업일 수
        cursor.execute(f"""
            SELECT COUNT(DISTINCT TxDateOnly) as cnt FROM #TempReceipt
        """)
        work_days = cursor.fetchone()['cnt'] or 1

        # 원장별 근무일 수 (해당 원장이 진료한 날짜 수)
        cursor.execute(f"""
            SELECT TxDoctor as doctor, COUNT(DISTINCT TxDateOnly) as work_days
            FROM #TempDetail
            WHERE TxDoctor IS NOT NULL AND TxDoctor != ''
            GROUP BY TxDoctor
        """)
        work_days_by_doctor = {row['doctor']: row['work_days'] for row in cursor.fetchall()}

        # 원장 목록 조회
        cursor.execute(f"""
            SELECT DISTINCT TxDoctor as doctor
            FROM #TempDetail
            WHERE TxDoctor IS NOT NULL AND TxDoctor != ''
              AND TxDoctor NOT LIKE '%간호%' AND TxDoctor NOT LIKE '%접수%'
            ORDER BY TxDoctor
        """)
        doctors = [row['doctor'] for row in cursor.fetchall()]

        # ============ 2. 환자 통계 (임시 테이블 사용) ============

        # 침초진 (원장별) - 건보 환자 중 등록일 = 진료일
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN #TempReceipt tr ON c.Customer_PK = tr.Customer_PK AND tr.TxDateOnly = CAST(c.reg_date AS DATE)
            INNER JOIN #TempDetail td ON c.Customer_PK = td.Customer_PK AND td.TxDateOnly = CAST(c.reg_date AS DATE)
            WHERE c.reg_date >= '{start_date}' AND c.reg_date < DATEADD(DAY, 1, '{end_date}')
              AND tr.CheongGu_Money > 0
              AND tr.IsJabo = 0
            GROUP BY td.TxDoctor
        """)
        chim_chojin_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 자보초진 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN #TempDetail td ON c.Customer_PK = td.Customer_PK AND td.TxDateOnly = CAST(c.reg_date AS DATE)
            WHERE c.reg_date >= '{start_date}' AND c.reg_date < DATEADD(DAY, 1, '{end_date}')
              AND td.TxItem LIKE '%자동차보험%'
            GROUP BY td.TxDoctor
        """)
        jabo_chojin_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 약초진 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            INNER JOIN #TempReceipt tr ON c.Customer_PK = tr.Customer_PK AND tr.TxDateOnly = CAST(c.reg_date AS DATE)
            INNER JOIN #TempDetail td ON c.Customer_PK = td.Customer_PK AND td.TxDateOnly = CAST(c.reg_date AS DATE)
            WHERE c.reg_date >= '{start_date}' AND c.reg_date < DATEADD(DAY, 1, '{end_date}')
              AND tr.CheongGu_Money = 0
              AND tr.General_Money > 0
              AND tr.IsJabo = 0
            GROUP BY td.TxDoctor
        """)
        yak_chojin_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 침환자 재초진 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT td.Customer_PK) as cnt
            FROM #TempDetail td
            INNER JOIN Customer c ON td.Customer_PK = c.Customer_PK
            INNER JOIN #TempReceipt tr ON td.Customer_PK = tr.Customer_PK AND td.TxDateOnly = tr.TxDateOnly
            WHERE CAST(c.reg_date AS DATE) < td.TxDateOnly
              AND td.PxName = N'진찰료(초진)'
              AND tr.CheongGu_Money > 0
              AND td.IsJabo = 0
            GROUP BY td.TxDoctor
        """)
        chim_rechojin_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 자보 재초진 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT td.Customer_PK) as cnt
            FROM #TempDetail td
            INNER JOIN Customer c ON td.Customer_PK = c.Customer_PK
            WHERE td.TxItem LIKE '%자동차보험%'
              AND CAST(c.reg_date AS DATE) < '{start_date}'
              AND td.사고번호 IS NOT NULL AND td.사고번호 != ''
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = td.Customer_PK
                  AND d2.사고번호 = td.사고번호
                  AND d2.TxItem LIKE '%자동차보험%'
                  AND d2.TxDate < '{start_date}'
              )
            GROUP BY td.TxDoctor
        """)
        jabo_rechojin_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 약환자 재초진 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT tr.Customer_PK) as cnt
            FROM #TempReceipt tr
            INNER JOIN Customer c ON tr.Customer_PK = c.Customer_PK
            INNER JOIN #TempDetail td ON tr.Customer_PK = td.Customer_PK AND tr.TxDateOnly = td.TxDateOnly
            WHERE CAST(c.reg_date AS DATE) < '{start_date}'
              AND tr.CheongGu_Money = 0
              AND tr.General_Money > 0
              AND tr.IsJabo = 0
              AND NOT EXISTS (
                SELECT 1 FROM Receipt r2 WHERE r2.Customer_PK = tr.Customer_PK
                AND CAST(r2.TxDate AS DATE) < tr.TxDateOnly
                AND CAST(r2.TxDate AS DATE) >= DATEADD(MONTH, -3, tr.TxDateOnly)
              )
            GROUP BY td.TxDoctor
        """)
        yak_rechojin_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 건보 침환자 총 방문수 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT CONCAT(tr.Customer_PK, '_', tr.TxDateOnly)) as cnt
            FROM #TempReceipt tr
            INNER JOIN #TempDetail td ON tr.Customer_PK = td.Customer_PK AND tr.TxDateOnly = td.TxDateOnly
            WHERE tr.CheongGu_Money > 0
              AND tr.IsJabo = 0
            GROUP BY td.TxDoctor
        """)
        chim_visits_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 자보 침환자 총 방문수 (원장별)
        cursor.execute(f"""
            SELECT TxDoctor as doctor, COUNT(DISTINCT CONCAT(Customer_PK, '_', TxDateOnly)) as cnt
            FROM #TempDetail
            WHERE TxItem LIKE '%자동차보험%'
            GROUP BY TxDoctor
        """)
        jabo_visits_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # ============ 3. 추나 통계 (임시 테이블 사용) ============

        # 자보추나 (원장별)
        cursor.execute(f"""
            SELECT TxDoctor as doctor, COUNT(*) as cnt
            FROM #TempDetail
            WHERE PxName LIKE '%추나%' AND IsJabo = 1
            GROUP BY TxDoctor
        """)
        chuna_jabo_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 건보 단순추나 (원장별)
        cursor.execute(f"""
            SELECT TxDoctor as doctor, COUNT(*) as cnt
            FROM #TempDetail
            WHERE PxName LIKE '%단순추나%' AND IsJabo = 0
            GROUP BY TxDoctor
        """)
        chuna_simple_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 건보 복잡추나 (원장별)
        cursor.execute(f"""
            SELECT TxDoctor as doctor, COUNT(*) as cnt
            FROM #TempDetail
            WHERE PxName LIKE '%복잡추나%' AND IsJabo = 0
            GROUP BY TxDoctor
        """)
        chuna_complex_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 비급여 추나 (원장별)
        cursor.execute(f"""
            SELECT TxDoctor as doctor, COUNT(*) as cnt
            FROM #TempDetail
            WHERE PxName LIKE '%추나%' AND InsuYes = 0 AND IsJabo = 0
            GROUP BY TxDoctor
        """)
        chuna_uncovered_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # ============ 4. 예약 통계 (임시 테이블 사용) ============

        # 총 침환자수 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT tr.Customer_PK) as cnt
            FROM #TempReceipt tr
            INNER JOIN #TempDetail td ON tr.Customer_PK = td.Customer_PK AND tr.TxDateOnly = td.TxDateOnly
            WHERE tr.CheongGu_Money > 0 OR tr.IsJabo = 1
            GROUP BY td.TxDoctor
        """)
        res_total_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 예약하고 온 환자 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT tr.Customer_PK) as cnt
            FROM #TempReceipt tr
            INNER JOIN #TempDetail td ON tr.Customer_PK = td.Customer_PK AND tr.TxDateOnly = td.TxDateOnly
            WHERE (tr.CheongGu_Money > 0 OR tr.IsJabo = 1)
              AND EXISTS (SELECT 1 FROM Reservation_New res WHERE res.Res_Customer_PK = tr.Customer_PK
                AND CAST(res.Res_Date AS DATE) = tr.TxDateOnly)
            GROUP BY td.TxDoctor
        """)
        res_reserved_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # 현장예약 (원장별)
        cursor.execute(f"""
            SELECT td.TxDoctor as doctor, COUNT(DISTINCT tr.Customer_PK) as cnt
            FROM #TempReceipt tr
            INNER JOIN #TempDetail td ON tr.Customer_PK = td.Customer_PK AND tr.TxDateOnly = td.TxDateOnly
            WHERE (tr.CheongGu_Money > 0 OR tr.IsJabo = 1)
              AND EXISTS (SELECT 1 FROM Reservation_New res WHERE res.Res_Customer_PK = tr.Customer_PK
                AND CAST(res.Res_Date AS DATE) > tr.TxDateOnly
                AND CAST(res.Res_updatetime AS DATE) = tr.TxDateOnly)
            GROUP BY td.TxDoctor
        """)
        res_onsite_by_doctor = {row['doctor']: row['cnt'] for row in cursor.fetchall()}

        # ============ 5. 매출 통계 (임시 테이블 사용) ============

        # 급여매출 (원장별) - 대표 원장 기준
        cursor.execute(f"""
            SELECT RepDoctor as doctor, SUM(Bonin_Money + CheongGu_Money) as amount
            FROM #TempReceipt
            WHERE IsJabo = 0 AND RepDoctor IS NOT NULL
            GROUP BY RepDoctor
        """)
        revenue_insurance_by_doctor = {row['doctor']: int(row['amount'] or 0) for row in cursor.fetchall()}

        # 건보추나 매출 (원장별)
        cursor.execute(f"""
            SELECT TxDoctor as doctor, ISNULL(SUM(ISNULL(TxMoney, 0)), 0) as amount
            FROM #TempDetail
            WHERE PxName LIKE '%추나%' AND IsJabo = 0
            GROUP BY TxDoctor
        """)
        revenue_chuna_by_doctor = {row['doctor']: int(row['amount'] or 0) for row in cursor.fetchall()}

        # 자보매출 (원장별) - 대표 원장 기준
        cursor.execute(f"""
            SELECT RepDoctor as doctor, SUM(Bonin_Money + CheongGu_Money + General_Money) as amount
            FROM #TempReceipt
            WHERE IsJabo = 1 AND RepDoctor IS NOT NULL
            GROUP BY RepDoctor
        """)
        revenue_jabo_by_doctor = {row['doctor']: int(row['amount'] or 0) for row in cursor.fetchall()}

        # 비급여매출 (원장별) - 대표 원장 기준
        cursor.execute(f"""
            SELECT RepDoctor as doctor, SUM(General_Money) as amount
            FROM #TempReceipt
            WHERE IsJabo = 0 AND RepDoctor IS NOT NULL
            GROUP BY RepDoctor
        """)
        revenue_uncovered_by_doctor = {row['doctor']: int(row['amount'] or 0) for row in cursor.fetchall()}

        # 임시 테이블 정리
        cursor.execute("""
            DROP TABLE IF EXISTS #TempDetail;
            DROP TABLE IF EXISTS #TempReceipt;
            DROP TABLE IF EXISTS #JaboVisits;
        """)

        conn.close()

        # ============ 결과 조합 ============

        def build_doctor_stats(doc):
            chim_chojin = chim_chojin_by_doctor.get(doc, 0)
            chim_rechojin = chim_rechojin_by_doctor.get(doc, 0)
            chim_visits = chim_visits_by_doctor.get(doc, 0)
            chim_rejin = max(0, chim_visits - chim_chojin - chim_rechojin)

            jabo_chojin = jabo_chojin_by_doctor.get(doc, 0)
            jabo_rechojin = jabo_rechojin_by_doctor.get(doc, 0)
            jabo_visits = jabo_visits_by_doctor.get(doc, 0)
            jabo_rejin = max(0, jabo_visits - jabo_chojin - jabo_rechojin)

            total_chim = chim_chojin + chim_rechojin + chim_rejin + jabo_chojin + jabo_rechojin + jabo_rejin

            res_total = res_total_by_doctor.get(doc, 0)
            res_reserved = res_reserved_by_doctor.get(doc, 0)
            res_onsite = res_onsite_by_doctor.get(doc, 0)

            insurance = revenue_insurance_by_doctor.get(doc, 0)
            jabo_rev = revenue_jabo_by_doctor.get(doc, 0)
            uncovered = revenue_uncovered_by_doctor.get(doc, 0)

            doc_work_days = work_days_by_doctor.get(doc, 1)

            return {
                "doctor": doc,
                "work_days": doc_work_days,
                "patients": {
                    "chim_chojin": chim_chojin,
                    "chim_rechojin": chim_rechojin,
                    "chim_rejin": chim_rejin,
                    "jabo_chojin": jabo_chojin,
                    "jabo_rechojin": jabo_rechojin,
                    "jabo_rejin": jabo_rejin,
                    "yak_chojin": yak_chojin_by_doctor.get(doc, 0),
                    "yak_rechojin": yak_rechojin_by_doctor.get(doc, 0),
                    "total_chim": total_chim
                },
                "chuna": {
                    "insurance_simple": chuna_simple_by_doctor.get(doc, 0),
                    "insurance_complex": chuna_complex_by_doctor.get(doc, 0),
                    "jabo": chuna_jabo_by_doctor.get(doc, 0),
                    "uncovered": chuna_uncovered_by_doctor.get(doc, 0),
                    "total": chuna_simple_by_doctor.get(doc, 0) + chuna_complex_by_doctor.get(doc, 0) + chuna_jabo_by_doctor.get(doc, 0) + chuna_uncovered_by_doctor.get(doc, 0)
                },
                "reservations": {
                    "total_chim_patients": res_total,
                    "reserved_count": res_reserved,
                    "reservation_rate": round(res_reserved / res_total * 100, 1) if res_total > 0 else 0,
                    "onsite_count": res_onsite,
                    "onsite_rate": round(res_onsite / res_total * 100, 1) if res_total > 0 else 0
                },
                "revenue": {
                    "insurance": insurance,
                    "chuna_revenue": revenue_chuna_by_doctor.get(doc, 0),
                    "jabo": jabo_rev,
                    "uncovered": uncovered,
                    "total": insurance + jabo_rev + uncovered
                }
            }

        # 원장별 통계
        doctor_stats = [build_doctor_stats(doc) for doc in doctors]

        # 전체 통계 (합산)
        total_stats = {
            "doctor": "전체",
            "patients": {
                "chim_chojin": sum(s["patients"]["chim_chojin"] for s in doctor_stats),
                "chim_rechojin": sum(s["patients"]["chim_rechojin"] for s in doctor_stats),
                "chim_rejin": sum(s["patients"]["chim_rejin"] for s in doctor_stats),
                "jabo_chojin": sum(s["patients"]["jabo_chojin"] for s in doctor_stats),
                "jabo_rechojin": sum(s["patients"]["jabo_rechojin"] for s in doctor_stats),
                "jabo_rejin": sum(s["patients"]["jabo_rejin"] for s in doctor_stats),
                "yak_chojin": sum(s["patients"]["yak_chojin"] for s in doctor_stats),
                "yak_rechojin": sum(s["patients"]["yak_rechojin"] for s in doctor_stats),
                "total_chim": sum(s["patients"]["total_chim"] for s in doctor_stats)
            },
            "chuna": {
                "insurance_simple": sum(s["chuna"]["insurance_simple"] for s in doctor_stats),
                "insurance_complex": sum(s["chuna"]["insurance_complex"] for s in doctor_stats),
                "jabo": sum(s["chuna"]["jabo"] for s in doctor_stats),
                "uncovered": sum(s["chuna"]["uncovered"] for s in doctor_stats),
                "total": sum(s["chuna"]["total"] for s in doctor_stats)
            },
            "reservations": {
                "total_chim_patients": sum(s["reservations"]["total_chim_patients"] for s in doctor_stats),
                "reserved_count": sum(s["reservations"]["reserved_count"] for s in doctor_stats),
                "reservation_rate": 0,
                "onsite_count": sum(s["reservations"]["onsite_count"] for s in doctor_stats),
                "onsite_rate": 0
            },
            "revenue": {
                "insurance": sum(s["revenue"]["insurance"] for s in doctor_stats),
                "chuna_revenue": sum(s["revenue"]["chuna_revenue"] for s in doctor_stats),
                "jabo": sum(s["revenue"]["jabo"] for s in doctor_stats),
                "uncovered": sum(s["revenue"]["uncovered"] for s in doctor_stats),
                "total": sum(s["revenue"]["total"] for s in doctor_stats)
            }
        }

        # 전체 예약율 계산
        total_res = total_stats["reservations"]["total_chim_patients"]
        if total_res > 0:
            total_stats["reservations"]["reservation_rate"] = round(total_stats["reservations"]["reserved_count"] / total_res * 100, 1)
            total_stats["reservations"]["onsite_rate"] = round(total_stats["reservations"]["onsite_count"] / total_res * 100, 1)

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "work_days": work_days,
            "doctors": doctors,
            "doctor_stats": doctor_stats,
            "total_stats": total_stats
        })

    except Exception as e:
        mssql_db.log(f"통합통계 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/uncovered-detail')
def stats_uncovered_detail():
    """비급여 상세 통계 API - 카테고리별 금액/건수"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 자보 제외 비급여 항목 조회 (임시 테이블 활용)
        cursor.execute(f"""
            -- 자보 방문 계산
            IF OBJECT_ID('tempdb..#JaboVisits2') IS NOT NULL DROP TABLE #JaboVisits2;
            SELECT DISTINCT Customer_PK, CAST(TxDate AS DATE) as TxDateOnly
            INTO #JaboVisits2
            FROM Detail
            WHERE TxDate >= '{start_date}' AND TxDate < DATEADD(DAY, 1, '{end_date}')
              AND TxItem LIKE '%자동차보험%';

            -- 비급여 항목 전체 조회
            SELECT
                d.PxName,
                COUNT(*) as cnt,
                SUM(ISNULL(d.TxMoney, 0)) as amount
            FROM Detail d
            LEFT JOIN #JaboVisits2 j ON d.Customer_PK = j.Customer_PK AND CAST(d.TxDate AS DATE) = j.TxDateOnly
            WHERE d.TxDate >= '{start_date}' AND d.TxDate < DATEADD(DAY, 1, '{end_date}')
              AND d.InsuYes = 0
              AND ISNULL(d.TxMoney, 0) > 0
              AND j.Customer_PK IS NULL  -- 자보 제외
            GROUP BY d.PxName
            ORDER BY amount DESC;
        """)

        raw_items = cursor.fetchall()

        # 원장별-항목별 비급여 금액 조회 (자보 제외)
        cursor.execute(f"""
            SELECT
                ISNULL(d.TxDoctor, '미지정') as doctor,
                d.PxName,
                COUNT(*) as cnt,
                SUM(ISNULL(d.TxMoney, 0)) as amount
            FROM Detail d
            LEFT JOIN #JaboVisits2 j ON d.Customer_PK = j.Customer_PK AND CAST(d.TxDate AS DATE) = j.TxDateOnly
            WHERE d.TxDate >= '{start_date}' AND d.TxDate < DATEADD(DAY, 1, '{end_date}')
              AND d.InsuYes = 0
              AND ISNULL(d.TxMoney, 0) > 0
              AND j.Customer_PK IS NULL
              AND d.TxDoctor IS NOT NULL AND d.TxDoctor != ''
            GROUP BY d.TxDoctor, d.PxName
            ORDER BY d.TxDoctor, amount DESC;

            DROP TABLE IF EXISTS #JaboVisits2;
        """)

        by_doctor_items = cursor.fetchall()
        conn.close()

        # 카테고리별 분류
        categories = {
            "녹용": {"total_cnt": 0, "total_amount": 0, "items": []},
            "맞춤한약": {"total_cnt": 0, "total_amount": 0, "items": []},
            "상비한약": {"total_cnt": 0, "total_amount": 0, "items": []},
            "공진단": {"total_cnt": 0, "total_amount": 0, "items": []},
            "경옥고": {"total_cnt": 0, "total_amount": 0, "items": []},
            "약침": {"total_cnt": 0, "total_amount": 0, "items": []},
            "다이어트": {"total_cnt": 0, "total_amount": 0, "items": []},
            "기타": {"total_cnt": 0, "total_amount": 0, "items": []}
        }

        for item in raw_items:
            px = item['PxName'] or ''
            cnt = item['cnt'] or 0
            amount = int(item['amount'] or 0)

            # 카테고리 분류
            category = None
            if '녹용추가' in px:
                category = "녹용"
            elif px.startswith('한약-'):
                category = "맞춤한약"
            elif px in ['1 감기약', '2 상비약', '자운고', '6 상용환'] or '감기약' in px or '상비약' in px or '자운고' in px or '상용환' in px:
                category = "상비한약"
            elif '공진단' in px:
                category = "공진단"
            elif '경옥고' in px:
                category = "경옥고"
            elif '약침' in px or '경근' in px or px in ['멤버십-경근', '멤버십-녹용']:
                category = "약침"
            elif any(d in px for d in ['린다프리미엄', '린다스탠다드', '린다스페셜', '린다환', '린디톡스', '슬림환', '체감탕']):
                category = "다이어트"
            else:
                category = "기타"

            categories[category]["total_cnt"] += cnt
            categories[category]["total_amount"] += amount
            categories[category]["items"].append({
                "name": px,
                "cnt": cnt,
                "amount": amount
            })

        # 전체 합계
        total_cnt = sum(c["total_cnt"] for c in categories.values())
        total_amount = sum(c["total_amount"] for c in categories.values())

        # 카테고리 분류 함수
        def get_category(px):
            if '녹용추가' in px:
                return "녹용"
            elif px.startswith('한약-'):
                return "맞춤한약"
            elif px in ['1 감기약', '2 상비약', '자운고', '6 상용환'] or '감기약' in px or '상비약' in px or '자운고' in px or '상용환' in px:
                return "상비한약"
            elif '공진단' in px:
                return "공진단"
            elif '경옥고' in px:
                return "경옥고"
            elif '약침' in px or '경근' in px or px in ['멤버십-경근', '멤버십-녹용']:
                return "약침"
            elif any(d in px for d in ['린다프리미엄', '린다스탠다드', '린다스페셜', '린다환', '린디톡스', '슬림환', '체감탕']):
                return "다이어트"
            else:
                return "기타"

        # 원장별-카테고리별 집계
        doctors_set = set()
        by_doctor_category = {}  # {doctor: {category: amount}}

        for item in by_doctor_items:
            doc = item['doctor']
            px = item['PxName'] or ''
            amount = int(item['amount'] or 0)
            cat = get_category(px)

            doctors_set.add(doc)
            if doc not in by_doctor_category:
                by_doctor_category[doc] = {}
            if cat not in by_doctor_category[doc]:
                by_doctor_category[doc][cat] = 0
            by_doctor_category[doc][cat] += amount

        # 원장 목록 (금액순 정렬)
        doctor_totals = {doc: sum(cats.values()) for doc, cats in by_doctor_category.items()}
        doctors_list = sorted(doctors_set, key=lambda d: doctor_totals.get(d, 0), reverse=True)

        # 원장별 카테고리별 데이터 구조화
        by_doctor_data = []
        for doc in doctors_list:
            doc_data = {
                "doctor": doc,
                "total": doctor_totals.get(doc, 0),
                "categories": by_doctor_category.get(doc, {})
            }
            by_doctor_data.append(doc_data)

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "total_cnt": total_cnt,
            "total_amount": total_amount,
            "categories": categories,
            "doctors": doctors_list,
            "by_doctor": by_doctor_data
        })

    except Exception as e:
        mssql_db.log(f"비급여상세 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/visit-route')
def stats_visit_route():
    """내원경로 통계 API - 신규 침환자의 내원경로 분석 (약환자+자보환자 제외)"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 기간 내 신규 침환자의 내원경로 조회 (침환자현황 초진 기준과 동일)
        # 조건: 등록일에 CheongGu_Money > 0 (보험청구 있음), 자보환자 제외
        cursor.execute(f"""
            SELECT c.SUGGEST, COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE c.reg_date >= '{start_date}' AND c.reg_date < DATEADD(DAY, 1, '{end_date}')
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND r.CheongGu_Money > 0
              AND c.SUGGEST IS NOT NULL AND c.SUGGEST != ''
              AND NOT EXISTS (
                  SELECT 1 FROM custcarinsuinfo ci
                  WHERE ci.custpk = c.Customer_PK
              )
            GROUP BY c.SUGGEST
            ORDER BY cnt DESC
        """)

        raw_items = cursor.fetchall()
        conn.close()

        # 카테고리별 분류
        categories = {
            "소개": {"total": 0, "items": []},
            "검색": {"total": 0, "items": []},
            "간판": {"total": 0, "items": []},
            "기타": {"total": 0, "items": []}
        }

        # 분류 규칙
        intro_keywords = ['소개', '소문', '내원환자', '직원']
        search_keywords = ['네이버', '지도', '인터넷', '홈페이지', '검색', '블로그']
        signboard_keywords = ['간판', '현수막', '근처']

        for item in raw_items:
            suggest = item['SUGGEST'] or ''
            cnt = item['cnt'] or 0

            # 카테고리 분류
            category = "기타"
            if any(kw in suggest for kw in intro_keywords):
                category = "소개"
            elif any(kw in suggest for kw in search_keywords):
                category = "검색"
            elif any(kw in suggest for kw in signboard_keywords):
                category = "간판"

            categories[category]["total"] += cnt
            categories[category]["items"].append({
                "name": suggest,
                "cnt": cnt
            })

        # 전체 합계
        total = sum(c["total"] for c in categories.values())

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "total": total,
            "categories": categories
        })

    except Exception as e:
        mssql_db.log(f"내원경로 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/search-keywords')
def stats_search_keywords():
    """검색어 상세 통계 API - 검색 유입 침환자의 CustURL(검색어) 분석 (약환자+자보환자 제외)"""
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 검색 유입 키워드
        search_keywords = ['네이버', '지도', '인터넷', '홈페이지', '검색', '블로그']
        search_conditions = ' OR '.join([f"c.SUGGEST LIKE '%{kw}%'" for kw in search_keywords])

        # 기간 내 검색 유입 침환자의 CustURL(검색어) 집계 (침환자현황 초진 기준과 동일)
        # 조건: 등록일에 CheongGu_Money > 0 (보험청구 있음), 자보환자 제외
        cursor.execute(f"""
            SELECT c.CustURL, COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE c.reg_date >= '{start_date}' AND c.reg_date < DATEADD(DAY, 1, '{end_date}')
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND r.CheongGu_Money > 0
              AND ({search_conditions})
              AND NOT EXISTS (
                  SELECT 1 FROM custcarinsuinfo ci
                  WHERE ci.custpk = c.Customer_PK
              )
            GROUP BY c.CustURL
            ORDER BY cnt DESC
        """)

        raw_items = cursor.fetchall()

        # 유사 키워드 그룹 (같은 의미로 합칠 키워드들)
        # 각 그룹의 첫 번째 항목이 대표 키워드로 표시됨
        similar_keyword_groups = [
            ['일요일한의원', '일요일진료한의원', '휴일한의원', '일요일진료', '휴일진료'],
            ['불당동한의원', '불당한의원', '천안불당한의원', '불당동', '천안불당'],
        ]

        # 유사 키워드 -> 대표 키워드 매핑 생성
        similar_keyword_map = {}
        for group in similar_keyword_groups:
            representative = group[0]  # 첫 번째가 대표
            for kw in group:
                similar_keyword_map[kw] = representative

        # 검색어 정리 및 집계 (띄어쓰기 제거하여 동일 검색어 합치기)
        keyword_counts = {}  # 정규화된 키워드 -> 카운트
        keyword_display = {}  # 정규화된 키워드 -> 원본 키워드 (가장 많이 사용된 형태)
        keyword_original_counts = {}  # 정규화된 키워드 -> {원본: 카운트} (가장 많이 쓰인 형태 찾기용)
        total = 0
        for item in raw_items:
            cust_url = (item['CustURL'] or '').strip()
            cnt = item['cnt'] or 0
            total += cnt

            # 빈 값 처리
            if not cust_url:
                cust_url = '(미입력)'
                normalized = '(미입력)'
            else:
                # 띄어쓰기 제거하여 정규화
                normalized = cust_url.replace(' ', '').lower()
                # 유사 키워드 그룹 확인 후 대표 키워드로 치환
                if normalized in similar_keyword_map:
                    normalized = similar_keyword_map[normalized]

            if normalized in keyword_counts:
                keyword_counts[normalized] += cnt
                # 원본 형태 중 가장 많이 사용된 것 추적
                if cust_url in keyword_original_counts[normalized]:
                    keyword_original_counts[normalized][cust_url] += cnt
                else:
                    keyword_original_counts[normalized][cust_url] = cnt
            else:
                keyword_counts[normalized] = cnt
                keyword_original_counts[normalized] = {cust_url: cnt}

        # 각 정규화 키워드에 대해 가장 많이 사용된 원본 형태 선택
        for normalized, originals in keyword_original_counts.items():
            keyword_display[normalized] = max(originals.items(), key=lambda x: x[1])[0]

        # 순위대로 정렬
        sorted_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)
        keywords = [{"keyword": keyword_display[k], "cnt": v, "ratio": round(v / total * 100, 1) if total > 0 else 0} for k, v in sorted_keywords]

        conn.close()

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "total": total,
            "keywords": keywords
        })

    except Exception as e:
        mssql_db.log(f"검색어상세 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/visit-route-trend')
def stats_visit_route_trend():
    """침초진 18개월/18주 추이 API - 소개/검색/간판/기타 추이 (약환자+자보환자 제외)"""
    try:
        from datetime import datetime as dt, timedelta
        import calendar

        def add_months(date, months):
            month = date.month - 1 + months
            year = date.year + month // 12
            month = month % 12 + 1
            day = min(date.day, calendar.monthrange(year, month)[1])
            return date.replace(year=year, month=month, day=day)

        end_date_str = request.args.get('end_date', dt.now().strftime('%Y-%m-%d'))
        period = request.args.get('period', 'monthly')

        if period == 'weekly':
            end_date = dt.strptime(end_date_str[:10], '%Y-%m-%d')
            days_until_sunday = 6 - end_date.weekday()
            end_sunday = end_date + timedelta(days=days_until_sunday)
            start_monday = end_sunday - timedelta(weeks=17, days=6)
            range_start = start_monday.strftime('%Y-%m-%d')
            range_end = end_sunday.strftime('%Y-%m-%d')
            group_format = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, c.reg_date), c.reg_date)) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, c.reg_date) AS VARCHAR), 2)"
        else:
            end_date = dt.strptime(end_date_str[:7] + '-01', '%Y-%m-%d')
            start_month = add_months(end_date, -17)
            range_start = start_month.strftime('%Y-%m-01')
            next_month = add_months(end_date, 1)
            range_end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
            group_format = "FORMAT(c.reg_date, 'yyyy-MM')"
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 분류 키워드
        intro_keywords = ['소개', '소문', '내원환자', '직원']
        search_keywords = ['네이버', '지도', '인터넷', '홈페이지', '검색', '블로그']
        signboard_keywords = ['간판', '현수막', '근처']

        cursor.execute(f"""
            SELECT {group_format} as period_key, c.SUGGEST, COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE c.reg_date >= '{range_start}' AND c.reg_date < DATEADD(DAY, 1, '{range_end}')
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND r.CheongGu_Money > 0
              AND c.SUGGEST IS NOT NULL AND c.SUGGEST != ''
              AND NOT EXISTS (
                  SELECT 1 FROM custcarinsuinfo ci
                  WHERE ci.custpk = c.Customer_PK
              )
            GROUP BY {group_format}, c.SUGGEST
            ORDER BY period_key
        """)

        raw_data = cursor.fetchall()
        conn.close()

        # 기간별 카테고리 집계
        period_data = {}
        if period == 'weekly':
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                period_data[period_key] = {"intro": 0, "search": 0, "signboard": 0, "other": 0}
        else:
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                period_data[month_label] = {"intro": 0, "search": 0, "signboard": 0, "other": 0}

        for item in raw_data:
            pk = item['period_key']
            suggest = item['SUGGEST'] or ''
            cnt = item['cnt'] or 0

            if pk not in period_data:
                continue

            if any(kw in suggest for kw in intro_keywords):
                period_data[pk]["intro"] += cnt
            elif any(kw in suggest for kw in search_keywords):
                period_data[pk]["search"] += cnt
            elif any(kw in suggest for kw in signboard_keywords):
                period_data[pk]["signboard"] += cnt
            else:
                period_data[pk]["other"] += cnt

        # 결과 배열 생성
        result = []
        if period == 'weekly':
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                week_label = week_monday.strftime('%m/%d')
                data = period_data.get(period_key, {"intro": 0, "search": 0, "signboard": 0, "other": 0})
                result.append({
                    "month": week_label,
                    "intro": data["intro"],
                    "search": data["search"],
                    "signboard": data["signboard"],
                    "other": data["other"],
                    "total": data["intro"] + data["search"] + data["signboard"] + data["other"]
                })
        else:
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                data = period_data.get(month_label, {"intro": 0, "search": 0, "signboard": 0, "other": 0})
                result.append({
                    "month": month_label,
                    "intro": data["intro"],
                    "search": data["search"],
                    "signboard": data["signboard"],
                    "other": data["other"],
                    "total": data["intro"] + data["search"] + data["signboard"] + data["other"]
                })

        return jsonify({
            "end_date": end_date.strftime('%Y-%m-%d') if period == 'weekly' else end_date.strftime('%Y-%m'),
            "period": period,
            "data": result
        })

    except Exception as e:
        mssql_db.log(f"침초진추이 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/chim-patient-trend')
def stats_chim_patient_trend():
    """침환자 18개월/18주 추이 API - 평환/침초진+재초진/자보초진+재초진 추이"""
    try:
        from datetime import datetime as dt, timedelta
        import calendar

        def add_months(date, months):
            month = date.month - 1 + months
            year = date.year + month // 12
            month = month % 12 + 1
            day = min(date.day, calendar.monthrange(year, month)[1])
            return date.replace(year=year, month=month, day=day)

        end_date_str = request.args.get('end_date', dt.now().strftime('%Y-%m-%d'))
        period = request.args.get('period', 'monthly')

        if period == 'weekly':
            end_date = dt.strptime(end_date_str[:10], '%Y-%m-%d')
            days_until_sunday = 6 - end_date.weekday()
            end_sunday = end_date + timedelta(days=days_until_sunday)
            start_monday = end_sunday - timedelta(weeks=17, days=6)
        else:
            end_date = dt.strptime(end_date_str[:7] + '-01', '%Y-%m-%d')
            start_month = add_months(end_date, -17)
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 18개월/18주 데이터 수집
        result = []
        for i in range(17, -1, -1):
            if period == 'weekly':
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                period_start = week_monday.strftime('%Y-%m-%d')
                period_end = week_sunday.strftime('%Y-%m-%d')
                period_label = week_monday.strftime('%m/%d')
                # 기존환자 기준일 (해당 주 이전)
                before_period = (week_monday - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                month_date = add_months(end_date, -i)
                period_start = month_date.strftime('%Y-%m-01')
                next_month = add_months(month_date, 1)
                period_end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
                period_label = month_date.strftime('%Y-%m')
                before_period = (month_date - timedelta(days=1)).strftime('%Y-%m-%d')

            # 1. 평환 (일평균 침환자수)
            cursor.execute(f"""
                SELECT COUNT(DISTINCT CONCAT(r.Customer_PK, '_', CAST(r.TxDate AS DATE))) as total_visits
                FROM Receipt r
                WHERE CAST(r.TxDate AS DATE) BETWEEN '{period_start}' AND '{period_end}'
                  AND (
                    ISNULL(r.CheongGu_Money, 0) > 0
                    OR EXISTS (
                      SELECT 1 FROM custcarinsuinfo ci
                      WHERE ci.custpk = r.Customer_PK
                    )
                  )
            """)
            total_visits = cursor.fetchone()['total_visits'] or 0

            # 해당 기간 영업일수
            cursor.execute(f"""
                SELECT COUNT(DISTINCT CAST(TxDate AS DATE)) as work_days
                FROM Receipt
                WHERE CAST(TxDate AS DATE) BETWEEN '{period_start}' AND '{period_end}'
            """)
            work_days = cursor.fetchone()['work_days'] or 1
            avg_daily = round(total_visits / work_days, 1)

            # 2. 침 초진 (건보)
            cursor.execute(f"""
                SELECT COUNT(DISTINCT c.Customer_PK) as cnt
                FROM Customer c
                JOIN Receipt r ON c.Customer_PK = r.Customer_PK
                WHERE CAST(c.reg_date AS DATE) BETWEEN '{period_start}' AND '{period_end}'
                  AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
                  AND r.CheongGu_Money > 0
                  AND NOT EXISTS (
                      SELECT 1 FROM custcarinsuinfo ci
                      WHERE ci.custpk = c.Customer_PK
                  )
            """)
            chim_chojin = cursor.fetchone()['cnt'] or 0

            # 3. 침 재초진 (건보)
            cursor.execute(f"""
                SELECT COUNT(DISTINCT d.Customer_PK) as cnt
                FROM Detail d
                JOIN Customer c ON d.Customer_PK = c.Customer_PK
                JOIN Receipt r ON d.Customer_PK = r.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                WHERE CAST(d.TxDate AS DATE) BETWEEN '{period_start}' AND '{period_end}'
                  AND CAST(c.reg_date AS DATE) <= '{before_period}'
                  AND d.PxName = N'진찰료(초진)'
                  AND ISNULL(r.CheongGu_Money, 0) > 0
                  AND NOT EXISTS (
                    SELECT 1 FROM custcarinsuinfo ci
                    WHERE ci.custpk = d.Customer_PK
                  )
            """)
            chim_rechojin = cursor.fetchone()['cnt'] or 0

            # 4. 자보 초진
            cursor.execute(f"""
                SELECT COUNT(DISTINCT c.Customer_PK) as cnt
                FROM Customer c
                WHERE CAST(c.reg_date AS DATE) BETWEEN '{period_start}' AND '{period_end}'
                  AND EXISTS (
                      SELECT 1 FROM custcarinsuinfo ci
                      WHERE ci.custpk = c.Customer_PK
                  )
            """)
            jabo_chojin = cursor.fetchone()['cnt'] or 0

            # 5. 자보 재초진
            cursor.execute(f"""
                WITH PeriodJabo AS (
                    SELECT DISTINCT d.Customer_PK, d.사고번호
                    FROM Detail d
                    JOIN Customer c ON d.Customer_PK = c.Customer_PK
                    WHERE d.TxItem LIKE '%자동차보험%'
                      AND CAST(d.TxDate AS DATE) BETWEEN '{period_start}' AND '{period_end}'
                      AND CAST(c.reg_date AS DATE) <= '{before_period}'
                      AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
                ),
                PreviousAccidents AS (
                    SELECT DISTINCT d.Customer_PK, d.사고번호
                    FROM Detail d
                    WHERE d.TxItem LIKE '%자동차보험%'
                      AND CAST(d.TxDate AS DATE) <= '{before_period}'
                      AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
                )
                SELECT COUNT(DISTINCT pj.Customer_PK) as cnt
                FROM PeriodJabo pj
                WHERE NOT EXISTS (
                    SELECT 1 FROM PreviousAccidents pa
                    WHERE pa.Customer_PK = pj.Customer_PK AND pa.사고번호 = pj.사고번호
                )
            """)
            jabo_rechojin = cursor.fetchone()['cnt'] or 0

            result.append({
                "month": period_label,
                "avg_daily": avg_daily,
                "chim_chojin": chim_chojin,
                "chim_rechojin": chim_rechojin,
                "chim_total": chim_chojin + chim_rechojin,
                "jabo_chojin": jabo_chojin,
                "jabo_rechojin": jabo_rechojin,
                "jabo_total": jabo_chojin + jabo_rechojin
            })

        conn.close()

        return jsonify({
            "end_date": end_date.strftime('%Y-%m-%d') if period == 'weekly' else end_date.strftime('%Y-%m'),
            "period": period,
            "data": result
        })

    except Exception as e:
        mssql_db.log(f"침환자추이 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/yak-chojin-detail')
def stats_yak_chojin_detail():
    """약초진 상세 통계 API - 원장별 약초진 분류

    분류:
    - 기존-담당: 기존환자(MAINDOCTOR가 진료원장과 동일)가 한약 처음 구입
    - 기존-다른: 기존환자(MAINDOCTOR가 진료원장과 다름)가 한약 처음 구입
    - 약생초: 신규환자 + 소개 아님 (검색, 간판 등)
    - 소개-담당: 신규환자 + 소개 + 소개자의 담당원장이 진료원장과 동일
    - 소개-다른: 신규환자 + 소개 + 소개자의 담당원장이 진료원장과 다름
    """
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 모든 약초진 환자 조회 (조회기간 내 약 구입 + 최근 6개월 내 약상담 이력 없음)
        # 약 관련 PxName: 한약%, 공진단%, 경옥고%, 녹용추가%, 린다%, 슬림환%, 치료약, 종합진료비, 재처방, 내원상담
        # 제외: 상비약, 감기약, 자운고, 상용환, 보완처방 (약상담 불필요)
        # 분류: 약 구입일 기준 해당 원장에게 이전 진료 이력이 있으면 "기존", 없으면 "신규"
        cursor.execute(f"""
            SELECT
                c.Customer_PK,
                c.sn as chart_no,
                c.NAME as patient_name,
                c.MAINDOCTOR as main_doctor,
                c.SUGGEST as suggest,
                c.suggcustPK as sugg_cust_pk,
                c.CustURL as cust_url,
                c.regFamily as reg_family,
                c.reg_date,
                d.TxDoctor as tx_doctor,
                MIN(CAST(d.TxDate AS DATE)) as first_drug_date,
                -- 해당 원장에게 첫 진료일 (약 구입 제외한 모든 진료)
                (SELECT MIN(CAST(d3.TxDate AS DATE))
                 FROM Detail d3
                 WHERE d3.Customer_PK = c.Customer_PK
                   AND d3.TxDoctor = d.TxDoctor) as first_visit_date
            FROM Customer c
            INNER JOIN Detail d ON c.Customer_PK = d.Customer_PK
            WHERE CAST(d.TxDate AS DATE) >= '{start_date}' AND CAST(d.TxDate AS DATE) < DATEADD(DAY, 1, '{end_date}')
              AND d.TxItem NOT LIKE '%자동차보험%'
              AND d.TxDoctor IS NOT NULL AND d.TxDoctor != ''
              AND d.InsuYes = 0
              AND (
                  d.PxName LIKE '한약%'
                  OR d.PxName LIKE '공진단%'
                  OR d.PxName LIKE '경옥고%'
                  OR d.PxName LIKE '녹용추가%'
                  OR d.PxName LIKE '린다%'
                  OR d.PxName LIKE '슬림환%'
                  OR d.PxName LIKE '%치료약%'
                  OR d.PxName LIKE '%종합진료비%'
                  OR d.PxName = '재처방'
                  OR d.PxName = '내원상담'
              )
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = c.Customer_PK
                  AND CAST(d2.TxDate AS DATE) < '{start_date}'
                  AND CAST(d2.TxDate AS DATE) >= DATEADD(MONTH, -6, '{start_date}')
                  AND d2.InsuYes = 0
                  AND (
                      d2.PxName LIKE '한약%'
                      OR d2.PxName LIKE '공진단%'
                      OR d2.PxName LIKE '경옥고%'
                      OR d2.PxName LIKE '녹용추가%'
                      OR d2.PxName LIKE '린다%'
                      OR d2.PxName LIKE '슬림환%'
                      OR d2.PxName LIKE '%치료약%'
                      OR d2.PxName LIKE '%종합진료비%'
                      OR d2.PxName = '재처방'
                      OR d2.PxName = '내원상담'
                  )
              )
            GROUP BY c.Customer_PK, c.sn, c.NAME, c.MAINDOCTOR, c.SUGGEST, c.suggcustPK, c.CustURL, c.regFamily, c.reg_date, d.TxDoctor
        """)
        all_yak_patients = cursor.fetchall()

        # 소개자 정보 수집을 위한 데이터 준비 (신규 환자 분류용)
        import re
        from datetime import datetime as dt
        period_start = dt.strptime(start_date, '%Y-%m-%d').date()

        referrer_pks = set()  # suggcustPK
        referrer_charts = set()  # CustURL에서 추출한 차트번호
        family_pks = set()  # regFamily (소개-가족인 경우)

        for p in all_yak_patients:
            # 기존 환자는 소개자 정보 불필요 (환자 등록일이 조회 기간 시작일보다 이전)
            reg_date = p.get('reg_date')
            # reg_date 타입 변환: datetime -> date, str -> date
            if hasattr(reg_date, 'date'):
                reg_date_cmp = reg_date.date()
            elif isinstance(reg_date, str):
                reg_date_cmp = dt.strptime(reg_date[:10], '%Y-%m-%d').date()
            else:
                reg_date_cmp = reg_date
            if reg_date_cmp and reg_date_cmp < period_start:
                continue  # 기존 환자는 스킵
            suggest = (p['suggest'] or '').strip()
            if '소개' not in suggest:
                continue

            sugg_pk = p.get('sugg_cust_pk')
            cust_url = (p.get('cust_url') or '').strip()
            reg_family = p.get('reg_family')

            # 1순위: suggcustPK
            if sugg_pk and sugg_pk > 0:
                referrer_pks.add(sugg_pk)
            # 2순위: CustURL에서 차트번호 추출
            elif cust_url:
                chart_match = re.search(r'\((\d{1,6})\)', cust_url)
                if chart_match:
                    referrer_charts.add(chart_match.group(1).zfill(6))
            # 3순위: 소개-가족인 경우 regFamily
            elif '가족' in suggest and reg_family and reg_family > 0:
                family_pks.add(reg_family)

        # 소개자의 담당원장 조회 (Customer_PK로)
        referrer_main_doctors_by_pk = {}
        if referrer_pks:
            pks_str = ','.join([str(pk) for pk in referrer_pks])
            cursor.execute(f"SELECT Customer_PK, MAINDOCTOR FROM Customer WHERE Customer_PK IN ({pks_str})")
            for row in cursor.fetchall():
                referrer_main_doctors_by_pk[row['Customer_PK']] = row['MAINDOCTOR'] or ''

        # 소개자의 담당원장 조회 (차트번호로)
        referrer_main_doctors_by_sn = {}
        if referrer_charts:
            charts_str = ','.join([f"'{c}'" for c in referrer_charts])
            cursor.execute(f"SELECT sn, MAINDOCTOR FROM Customer WHERE sn IN ({charts_str})")
            for row in cursor.fetchall():
                referrer_main_doctors_by_sn[row['sn']] = row['MAINDOCTOR'] or ''

        # 가족 중 먼저 등록된 환자의 담당원장 조회 (regFamily로)
        family_main_doctors = {}
        if family_pks:
            pks_str = ','.join([str(pk) for pk in family_pks])
            cursor.execute(f"""
                SELECT regFamily, MAINDOCTOR, reg_date
                FROM Customer
                WHERE regFamily IN ({pks_str})
                ORDER BY regFamily, reg_date ASC
            """)
            for row in cursor.fetchall():
                reg_fam = row['regFamily']
                if reg_fam not in family_main_doctors:
                    family_main_doctors[reg_fam] = row['MAINDOCTOR'] or ''

        conn.close()

        # 원장별 분류 집계
        doctors_set = set()
        by_doctor = {}  # {doctor: {category: count}}

        def init_doctor(doc):
            if doc not in by_doctor:
                by_doctor[doc] = {
                    "existing_same": 0,    # 기존-담당
                    "existing_other": 0,   # 기존-다른
                    "new_direct": 0,       # 신규 (약생초)
                    "referral_same": 0,    # 소개-담당
                    "referral_other": 0    # 소개-다른
                }
                doctors_set.add(doc)

        # 환자 분류 처리
        # 분류 기준: 환자 등록일(reg_date)이 조회 기간 시작일보다 이전이면 "기존" 환자
        for p in all_yak_patients:
            tx_doc = p['tx_doctor']
            suggest = (p['suggest'] or '').strip()
            sugg_pk = p.get('sugg_cust_pk')
            cust_url = (p.get('cust_url') or '').strip()
            reg_family = p.get('reg_family')
            reg_date = p.get('reg_date')
            first_visit = p.get('first_visit_date')
            first_drug = p.get('first_drug_date')
            init_doctor(tx_doc)

            # 기존/신규 판단: 환자 등록일이 조회 기간 시작일보다 이전이면 "기존"
            # reg_date 타입 변환: datetime -> date, str -> date
            if hasattr(reg_date, 'date'):
                reg_date_cmp = reg_date.date()
            elif isinstance(reg_date, str):
                reg_date_cmp = dt.strptime(reg_date[:10], '%Y-%m-%d').date()
            else:
                reg_date_cmp = reg_date
            is_existing = reg_date_cmp and reg_date_cmp < period_start

            if is_existing:
                # 기존 환자: 해당 원장에게 이전 진료 이력이 있으면 "담당", 없으면 "다른"
                # first_visit < first_drug 이면 해당 원장에게 약 처방 전에 진료 받은 적 있음
                has_prior_visit = first_visit and first_drug and first_visit < first_drug
                if has_prior_visit:
                    by_doctor[tx_doc]["existing_same"] += 1
                else:
                    by_doctor[tx_doc]["existing_other"] += 1
            else:
                # 신규 환자: 소개 여부로 분류
                is_referral = '소개' in suggest

                if not is_referral:
                    # 약생초 (검색, 간판 등)
                    by_doctor[tx_doc]["new_direct"] += 1
                else:
                    referrer_main_doc = None

                    # 1순위: suggcustPK
                    if sugg_pk and sugg_pk > 0 and sugg_pk in referrer_main_doctors_by_pk:
                        referrer_main_doc = referrer_main_doctors_by_pk[sugg_pk]
                    # 2순위: CustURL에서 차트번호 추출
                    elif cust_url:
                        chart_match = re.search(r'\((\d{1,6})\)', cust_url)
                        if chart_match:
                            referrer_chart = chart_match.group(1).zfill(6)
                            referrer_main_doc = referrer_main_doctors_by_sn.get(referrer_chart)
                    # 3순위: 소개-가족인 경우 regFamily로 찾기
                    elif '가족' in suggest and reg_family and reg_family > 0:
                        referrer_main_doc = family_main_doctors.get(reg_family)

                    if referrer_main_doc is not None:
                        if referrer_main_doc == tx_doc:
                            by_doctor[tx_doc]["referral_same"] += 1
                        else:
                            by_doctor[tx_doc]["referral_other"] += 1
                    else:
                        # 소개자 정보 없음 - 소개-다른으로 분류
                        by_doctor[tx_doc]["referral_other"] += 1

        # 결과 정리
        doctors_list = sorted(doctors_set)
        result_by_doctor = []
        totals = {"existing_same": 0, "existing_other": 0, "new_direct": 0, "referral_same": 0, "referral_other": 0}

        for doc in doctors_list:
            stats = by_doctor[doc]
            total = sum(stats.values())
            result_by_doctor.append({
                "doctor": doc,
                "existing_same": stats["existing_same"],
                "existing_other": stats["existing_other"],
                "new_direct": stats["new_direct"],
                "referral_same": stats["referral_same"],
                "referral_other": stats["referral_other"],
                "total": total
            })
            for k in totals:
                totals[k] += stats[k]

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "doctors": doctors_list,
            "by_doctor": result_by_doctor,
            "totals": {
                **totals,
                "total": sum(totals.values())
            }
        })

    except Exception as e:
        mssql_db.log(f"약초진상세 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/yak-chojin-raw')
def stats_yak_chojin_raw():
    """약초진 Raw Data API - 환자 목록 반환

    Query params:
    - period, date: 기간
    - doctor: 원장명 (선택, 없으면 전체)
    - category: 분류 (existing_same, existing_other, new_direct, referral_same, referral_other)
    """
    try:
        period = request.args.get('period', 'daily')
        target_date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        doctor = request.args.get('doctor', '')
        category = request.args.get('category', '')

        start_date, end_date, error = _get_date_range(period, target_date)
        if error:
            return jsonify({"error": error}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)
        import re

        patients = []

        # 모든 약초진 환자 조회 (조회기간 내 약 구입 + 최근 6개월 내 약상담 이력 없음)
        # 분류: 약 구입일 기준 해당 원장에게 이전 진료 이력이 있으면 "기존", 없으면 "신규"
        # 같은 환자는 월 내 1회로만 카운트 (첫 번째 날짜 사용)
        cursor.execute(f"""
            SELECT
                c.Customer_PK,
                c.sn as chart_no,
                c.NAME as patient_name,
                c.MAINDOCTOR as main_doctor,
                c.SUGGEST as suggest,
                c.suggcustPK as sugg_cust_pk,
                c.suggcustnamesn as sugg_cust_namesn,
                c.CustURL as cust_url,
                c.regFamily as reg_family,
                CONVERT(varchar, c.reg_date, 23) as reg_date,
                d.TxDoctor as tx_doctor,
                MIN(CONVERT(varchar, d.TxDate, 23)) as tx_date,
                MIN(CAST(d.TxDate AS DATE)) as first_drug_date,
                (SELECT MIN(CAST(d3.TxDate AS DATE))
                 FROM Detail d3
                 WHERE d3.Customer_PK = c.Customer_PK
                   AND d3.TxDoctor = d.TxDoctor) as first_visit_date,
                STRING_AGG(d.PxName + '(' + CAST(d.TxMoney AS VARCHAR) + '원)', ', ') as items
            FROM Customer c
            INNER JOIN Detail d ON c.Customer_PK = d.Customer_PK
            WHERE CAST(d.TxDate AS DATE) >= '{start_date}' AND CAST(d.TxDate AS DATE) < DATEADD(DAY, 1, '{end_date}')
              AND d.TxItem NOT LIKE '%자동차보험%'
              AND d.TxDoctor IS NOT NULL AND d.TxDoctor != ''
              AND d.InsuYes = 0
              AND (
                  d.PxName LIKE '한약%'
                  OR d.PxName LIKE '공진단%'
                  OR d.PxName LIKE '경옥고%'
                  OR d.PxName LIKE '녹용추가%'
                  OR d.PxName LIKE '린다%'
                  OR d.PxName LIKE '슬림환%'
                  OR d.PxName LIKE '%치료약%'
                  OR d.PxName LIKE '%종합진료비%'
                  OR d.PxName = '재처방'
                  OR d.PxName = '내원상담'
              )
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = c.Customer_PK
                  AND CAST(d2.TxDate AS DATE) < '{start_date}'
                  AND CAST(d2.TxDate AS DATE) >= DATEADD(MONTH, -6, '{start_date}')
                  AND d2.InsuYes = 0
                  AND (
                      d2.PxName LIKE '한약%'
                      OR d2.PxName LIKE '공진단%'
                      OR d2.PxName LIKE '경옥고%'
                      OR d2.PxName LIKE '녹용추가%'
                      OR d2.PxName LIKE '린다%'
                      OR d2.PxName LIKE '슬림환%'
                      OR d2.PxName LIKE '%치료약%'
                      OR d2.PxName LIKE '%종합진료비%'
                      OR d2.PxName = '재처방'
                      OR d2.PxName = '내원상담'
                  )
              )
            GROUP BY c.Customer_PK, c.sn, c.NAME, c.MAINDOCTOR, c.SUGGEST, c.suggcustPK, c.suggcustnamesn, c.CustURL, c.regFamily, c.reg_date, d.TxDoctor
            ORDER BY MIN(d.TxDate), c.sn
        """)
        all_yak_patients = cursor.fetchall()

        # 기존/신규 판단을 위한 기준일 설정
        from datetime import datetime as dt
        period_start = dt.strptime(start_date, '%Y-%m-%d').date()

        # 소개자 정보 수집을 위한 데이터 준비 (신규 환자만 대상)
        referrer_pks = set()  # suggcustPK
        referrer_charts = set()  # CustURL에서 추출한 차트번호
        referrer_names = set()  # CustURL에서 추출한 이름 (유사 검색용)
        family_pks = set()  # regFamily (소개-가족인 경우)

        for p in all_yak_patients:
            # 기존/신규 판단: 환자 등록일이 조회 기간 시작일보다 이전이면 "기존"
            reg_date = p.get('reg_date')
            # reg_date가 datetime인 경우 date로 변환
            # reg_date 타입 변환: datetime -> date, str -> date
            if hasattr(reg_date, 'date'):
                reg_date_cmp = reg_date.date()
            elif isinstance(reg_date, str):
                reg_date_cmp = dt.strptime(reg_date[:10], '%Y-%m-%d').date()
            else:
                reg_date_cmp = reg_date
            is_existing = reg_date_cmp and reg_date_cmp < period_start

            if is_existing:
                continue  # 기존 환자는 소개자 정보 불필요

            suggest = (p['suggest'] or '').strip()
            if '소개' not in suggest:
                continue

            sugg_pk = p.get('sugg_cust_pk')
            cust_url = (p.get('cust_url') or '').strip()
            reg_family = p.get('reg_family')

            # 1순위: suggcustPK
            if sugg_pk and sugg_pk > 0:
                referrer_pks.add(sugg_pk)
            # 2순위: 소개-가족인 경우 regFamily 우선
            elif '가족' in suggest and reg_family and reg_family > 0:
                family_pks.add(reg_family)
            # 3순위: CustURL에서 차트번호/이름 추출
            elif cust_url:
                chart_match = re.search(r'\((\d{1,6})\)', cust_url)
                if chart_match:
                    referrer_charts.add(chart_match.group(1).zfill(6))
                # 이름 추출 (유사 검색용)
                name_match = re.match(r'([^(님]+)', cust_url)
                if name_match:
                    name_part = name_match.group(1).strip()
                    if name_part and len(name_part) >= 2:
                        referrer_names.add(name_part)

        # 소개자 정보 조회 (Customer_PK로)
        referrer_info_by_pk = {}
        if referrer_pks:
            pks_str = ','.join([str(pk) for pk in referrer_pks])
            cursor.execute(f"SELECT Customer_PK, sn, NAME, MAINDOCTOR FROM Customer WHERE Customer_PK IN ({pks_str})")
            for row in cursor.fetchall():
                referrer_info_by_pk[row['Customer_PK']] = {
                    'name': row['NAME'] or '',
                    'chart_no': row['sn'] or '',
                    'main_doctor': row['MAINDOCTOR'] or ''
                }

        # 소개자 정보 조회 (차트번호로)
        referrer_info_by_sn = {}
        if referrer_charts:
            charts_str = ','.join([f"'{c}'" for c in referrer_charts])
            cursor.execute(f"SELECT sn, NAME, MAINDOCTOR FROM Customer WHERE sn IN ({charts_str})")
            for row in cursor.fetchall():
                referrer_info_by_sn[row['sn']] = {
                    'name': row['NAME'] or '',
                    'chart_no': row['sn'] or '',
                    'main_doctor': row['MAINDOCTOR'] or ''
                }

        # 소개자 정보 조회 (이름으로 유사 검색)
        referrer_info_by_name = {}
        if referrer_names:
            # 이름이 포함된 환자 검색 (LIKE 검색)
            name_conditions = ' OR '.join([f"NAME LIKE '%{n}%'" for n in referrer_names])
            cursor.execute(f"SELECT sn, NAME, MAINDOCTOR FROM Customer WHERE {name_conditions}")
            for row in cursor.fetchall():
                name = row['NAME'] or ''
                if name not in referrer_info_by_name:
                    referrer_info_by_name[name] = {
                        'name': name,
                        'chart_no': row['sn'] or '',
                        'main_doctor': row['MAINDOCTOR'] or ''
                    }

        # 가족 중 먼저 등록된 환자 조회 (regFamily로)
        family_referrer_info = {}
        if family_pks:
            pks_str = ','.join([str(pk) for pk in family_pks])
            # regFamily가 같은 환자 중 가장 먼저 등록된 환자 (본인 제외)
            cursor.execute(f"""
                SELECT regFamily, sn, NAME, MAINDOCTOR, reg_date
                FROM Customer
                WHERE regFamily IN ({pks_str})
                ORDER BY regFamily, reg_date ASC
            """)
            for row in cursor.fetchall():
                reg_fam = row['regFamily']
                if reg_fam not in family_referrer_info:
                    family_referrer_info[reg_fam] = {
                        'name': row['NAME'] or '',
                        'chart_no': row['sn'] or '',
                        'main_doctor': row['MAINDOCTOR'] or ''
                    }

        # 분류
        for p in all_yak_patients:
            tx_doc = p['tx_doctor']
            suggest = (p['suggest'] or '').strip()
            sugg_pk = p.get('sugg_cust_pk')
            cust_url = (p.get('cust_url') or '').strip()
            reg_family = p.get('reg_family')
            patient_pk = p['Customer_PK']
            reg_date = p.get('reg_date')
            first_visit = p.get('first_visit_date')
            first_drug = p.get('first_drug_date')

            # 기존/신규 판단: 환자 등록일이 조회 기간 시작일보다 이전이면 "기존"
            # reg_date 타입 변환: datetime -> date, str -> date
            if hasattr(reg_date, 'date'):
                reg_date_cmp = reg_date.date()
            elif isinstance(reg_date, str):
                reg_date_cmp = dt.strptime(reg_date[:10], '%Y-%m-%d').date()
            else:
                reg_date_cmp = reg_date
            is_existing = reg_date_cmp and reg_date_cmp < period_start

            referrer_data = None  # 소개자 정보

            if is_existing:
                # 기존 환자: 해당 원장에게 이전 진료 이력이 있으면 "담당", 없으면 "다른"
                has_prior_visit = first_visit and first_drug and first_visit < first_drug
                if has_prior_visit:
                    cat = 'existing_same'
                else:
                    cat = 'existing_other'
            else:
                # 신규 환자: 소개 여부로 분류
                is_referral = '소개' in suggest
                if not is_referral:
                    cat = 'new_direct'
                else:
                    # 1순위: suggcustPK
                    if sugg_pk and sugg_pk > 0 and sugg_pk in referrer_info_by_pk:
                        ref_info = referrer_info_by_pk[sugg_pk]
                        referrer_data = ref_info
                        cat = 'referral_same' if ref_info['main_doctor'] == tx_doc else 'referral_other'
                    # 2순위: 소개-가족인 경우 regFamily 우선
                    elif '가족' in suggest and reg_family and reg_family > 0 and reg_family in family_referrer_info:
                        ref_info = family_referrer_info[reg_family]
                        # 본인이 아닌지 확인
                        if ref_info['chart_no'] != p['chart_no']:
                            referrer_data = ref_info
                            cat = 'referral_same' if ref_info['main_doctor'] == tx_doc else 'referral_other'
                        else:
                            cat = 'referral_other'
                    # 3순위: CustURL에서 차트번호/이름 추출
                    elif cust_url:
                        chart_match = re.search(r'\((\d{1,6})\)', cust_url)
                        ref_info = None
                        if chart_match:
                            referrer_chart = chart_match.group(1).zfill(6)
                            ref_info = referrer_info_by_sn.get(referrer_chart)

                        if ref_info:
                            referrer_data = ref_info
                            cat = 'referral_same' if ref_info['main_doctor'] == tx_doc else 'referral_other'
                        else:
                            # 차트번호 못찾으면 이름으로 유사 검색
                            name_match = re.match(r'([^(님]+)', cust_url)
                            name_part = name_match.group(1).strip() if name_match else ''

                            # 이름으로 검색된 환자 중 매칭되는 것 찾기
                            found_by_name = None
                            if name_part and len(name_part) >= 2:
                                for db_name, info in referrer_info_by_name.items():
                                    if name_part in db_name or db_name in name_part:
                                        found_by_name = info
                                        break

                            if found_by_name:
                                referrer_data = found_by_name
                                cat = 'referral_same' if found_by_name['main_doctor'] == tx_doc else 'referral_other'
                            else:
                                referrer_data = {'name': name_part, 'chart_no': chart_match.group(1).zfill(6) if chart_match else '', 'main_doctor': ''}
                                cat = 'referral_other'
                    else:
                        # 소개자 정보 없음
                        cat = 'referral_other'

            # yak_saeng_cho = 신규(new_direct) + 기존-다른(existing_other) + 소개-다른(referral_other)
            yak_saeng_cho_cats = ['new_direct', 'existing_other', 'referral_other']
            category_match = (not category or cat == category or (category == 'yak_saeng_cho' and cat in yak_saeng_cho_cats))
            if (not doctor or tx_doc == doctor) and category_match:
                patient_data = {
                    'chart_no': p['chart_no'],
                    'patient_name': p['patient_name'],
                    'doctor': tx_doc,
                    'category': cat,
                    'date': p['tx_date'],
                    'items': p.get('items') or ''
                }
                # 소개 환자인 경우 소개자 정보 추가
                if cat.startswith('referral'):
                    if referrer_data:
                        patient_data['referrer'] = {**referrer_data, 'cust_url': cust_url}
                    else:
                        patient_data['referrer'] = {'name': '', 'chart_no': '', 'main_doctor': '', 'cust_url': cust_url}
                # 신규 환자(new_direct)인 경우 내원경로(SUGGEST)와 CustURL을 함께 추가
                elif cat == 'new_direct':
                    patient_data['referrer'] = {'name': '', 'chart_no': '', 'main_doctor': '', 'suggest': suggest, 'cust_url': cust_url}
                patients.append(patient_data)

        conn.close()

        return jsonify({
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "doctor": doctor,
            "category": category,
            "count": len(patients),
            "patients": patients
        })

    except Exception as e:
        mssql_db.log(f"약초진Raw 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/yak-chojin-trend')
def stats_yak_chojin_trend():
    """약초진 18개월/18주 추이 API - 신규/소개/기존 추이"""
    try:
        from datetime import datetime as dt, timedelta
        import calendar
        import re

        def add_months(date, months):
            month = date.month - 1 + months
            year = date.year + month // 12
            month = month % 12 + 1
            day = min(date.day, calendar.monthrange(year, month)[1])
            return date.replace(year=year, month=month, day=day)

        end_date_str = request.args.get('end_date', dt.now().strftime('%Y-%m-%d'))
        period = request.args.get('period', 'monthly')

        if period == 'weekly':
            end_date = dt.strptime(end_date_str[:10], '%Y-%m-%d')
            days_until_sunday = 6 - end_date.weekday()
            end_sunday = end_date + timedelta(days=days_until_sunday)
            start_monday = end_sunday - timedelta(weeks=17, days=6)
            range_start = start_monday.strftime('%Y-%m-%d')
            range_end = end_sunday.strftime('%Y-%m-%d')
            group_format = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, d.TxDate), d.TxDate)) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, d.TxDate) AS VARCHAR), 2)"
        else:
            end_date = dt.strptime(end_date_str[:7] + '-01', '%Y-%m-%d')
            start_month = add_months(end_date, -17)
            range_start = start_month.strftime('%Y-%m-01')
            next_month = add_months(end_date, 1)
            range_end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
            group_format = "FORMAT(MIN(CAST(d.TxDate AS DATE)), 'yyyy-MM')"
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 약초진 환자 조회 (약 구입 + 최근 6개월 내 약상담 이력 없음)
        # 주간인 경우 ISO 주차로 그룹핑
        if period == 'weekly':
            period_select = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, MIN(CAST(d.TxDate AS DATE))), MIN(CAST(d.TxDate AS DATE)))) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, MIN(CAST(d.TxDate AS DATE))) AS VARCHAR), 2) as period_key"
        else:
            period_select = "FORMAT(MIN(CAST(d.TxDate AS DATE)), 'yyyy-MM') as period_key"

        cursor.execute(f"""
            SELECT
                c.Customer_PK,
                c.SUGGEST as suggest,
                c.suggcustPK as sugg_cust_pk,
                c.CustURL as cust_url,
                c.regFamily as reg_family,
                c.reg_date,
                d.TxDoctor as tx_doctor,
                {period_select},
                MIN(CAST(d.TxDate AS DATE)) as first_drug_date,
                (SELECT MIN(CAST(d3.TxDate AS DATE))
                 FROM Detail d3
                 WHERE d3.Customer_PK = c.Customer_PK
                   AND d3.TxDoctor = d.TxDoctor) as first_visit_date
            FROM Customer c
            INNER JOIN Detail d ON c.Customer_PK = d.Customer_PK
            WHERE CAST(d.TxDate AS DATE) >= '{range_start}' AND CAST(d.TxDate AS DATE) <= '{range_end}'
              AND d.TxItem NOT LIKE '%자동차보험%'
              AND d.TxDoctor IS NOT NULL AND d.TxDoctor != ''
              AND d.InsuYes = 0
              AND (
                  d.PxName LIKE '한약%'
                  OR d.PxName LIKE '공진단%'
                  OR d.PxName LIKE '경옥고%'
                  OR d.PxName LIKE '녹용추가%'
                  OR d.PxName LIKE '린다%'
                  OR d.PxName LIKE '슬림환%'
                  OR d.PxName LIKE '%치료약%'
                  OR d.PxName LIKE '%종합진료비%'
                  OR d.PxName = '재처방'
                  OR d.PxName = '내원상담'
              )
              AND NOT EXISTS (
                SELECT 1 FROM Detail d2
                WHERE d2.Customer_PK = c.Customer_PK
                  AND CAST(d2.TxDate AS DATE) < CAST(d.TxDate AS DATE)
                  AND CAST(d2.TxDate AS DATE) >= DATEADD(MONTH, -6, CAST(d.TxDate AS DATE))
                  AND d2.InsuYes = 0
                  AND (
                      d2.PxName LIKE '한약%'
                      OR d2.PxName LIKE '공진단%'
                      OR d2.PxName LIKE '경옥고%'
                      OR d2.PxName LIKE '녹용추가%'
                      OR d2.PxName LIKE '린다%'
                      OR d2.PxName LIKE '슬림환%'
                      OR d2.PxName LIKE '%치료약%'
                      OR d2.PxName LIKE '%종합진료비%'
                      OR d2.PxName = '재처방'
                      OR d2.PxName = '내원상담'
                  )
              )
            GROUP BY c.Customer_PK, c.SUGGEST, c.suggcustPK, c.CustURL, c.regFamily, c.reg_date, d.TxDoctor
        """)
        all_yak_patients = cursor.fetchall()

        # 소개자 정보 조회를 위한 PK 수집
        referrer_pks = set()
        referrer_charts = set()
        family_pks = set()

        for p in all_yak_patients:
            first_drug = p['first_drug_date']
            first_visit = p['first_visit_date']
            # 신규 환자인 경우에만 소개자 정보 필요
            if first_drug and first_visit and first_drug <= first_visit:
                if p['sugg_cust_pk']:
                    referrer_pks.add(p['sugg_cust_pk'])
                if p['cust_url']:
                    match = re.search(r'\\b(\\d{5,})\\b', str(p['cust_url']))
                    if match:
                        referrer_charts.add(match.group(1))
                if p['reg_family']:
                    family_pks.add(p['reg_family'])

        # 소개자 MAINDOCTOR 조회
        referrer_doctors = {}
        if referrer_pks or referrer_charts or family_pks:
            conditions = []
            if referrer_pks:
                pks_str = ','.join(str(pk) for pk in referrer_pks)
                conditions.append(f"Customer_PK IN ({pks_str})")
            if referrer_charts:
                charts_str = ','.join(f"'{c}'" for c in referrer_charts)
                conditions.append(f"sn IN ({charts_str})")
            if family_pks:
                fpks_str = ','.join(str(pk) for pk in family_pks)
                conditions.append(f"Customer_PK IN ({fpks_str})")

            cursor.execute(f"""
                SELECT Customer_PK, sn, MAINDOCTOR
                FROM Customer
                WHERE {' OR '.join(conditions)}
            """)
            for row in cursor.fetchall():
                referrer_doctors[row['Customer_PK']] = row['MAINDOCTOR']
                if row['sn']:
                    referrer_doctors[f"sn_{row['sn']}"] = row['MAINDOCTOR']

        conn.close()

        # 소개 키워드
        intro_keywords = ['소개', '소문', '내원환자', '직원', '가족']

        # 기간별 집계
        period_data = {}
        if period == 'weekly':
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                period_data[period_key] = {"new": 0, "referral": 0, "existing": 0}
        else:
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                period_data[month_label] = {"new": 0, "referral": 0, "existing": 0}

        for p in all_yak_patients:
            pk = p['period_key']
            if pk not in period_data:
                continue

            first_drug = p['first_drug_date']
            first_visit = p['first_visit_date']
            tx_doctor = p['tx_doctor']
            suggest = p['suggest'] or ''

            # 신규 vs 기존 판단
            is_new = first_drug and first_visit and first_drug <= first_visit

            if is_new:
                # 신규: 소개 vs 약생초
                is_referral = any(kw in suggest for kw in intro_keywords)
                if not is_referral and p['sugg_cust_pk']:
                    is_referral = True
                if not is_referral and p['cust_url']:
                    match = re.search(r'\\b(\\d{5,})\\b', str(p['cust_url']))
                    if match:
                        is_referral = True
                if not is_referral and p['reg_family']:
                    is_referral = True

                if is_referral:
                    period_data[pk]["referral"] += 1
                else:
                    period_data[pk]["new"] += 1
            else:
                period_data[pk]["existing"] += 1

        # 결과 배열 생성
        result = []
        if period == 'weekly':
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                week_label = week_monday.strftime('%m/%d')
                data = period_data.get(period_key, {"new": 0, "referral": 0, "existing": 0})
                result.append({
                    "month": week_label,
                    "new": data["new"],
                    "referral": data["referral"],
                    "existing": data["existing"],
                    "total": data["new"] + data["referral"] + data["existing"]
                })
        else:
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                data = period_data.get(month_label, {"new": 0, "referral": 0, "existing": 0})
                result.append({
                    "month": month_label,
                    "new": data["new"],
                    "referral": data["referral"],
                    "existing": data["existing"],
                    "total": data["new"] + data["referral"] + data["existing"]
                })

        return jsonify({
            "end_date": end_date.strftime('%Y-%m-%d') if period == 'weekly' else end_date.strftime('%Y-%m'),
            "period": period,
            "data": result
        })

    except Exception as e:
        mssql_db.log(f"약초진추이 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/uncovered-trend')
def stats_uncovered_trend():
    """비급여 18개월/18주 추이 API - 카테고리별 매출 추이"""
    try:
        from datetime import timedelta

        end_date_str = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        period = request.args.get('period', 'monthly')
        end_date = datetime.strptime(end_date_str[:10], '%Y-%m-%d')

        # 월 계산 헬퍼 함수
        def add_months(date, months):
            month = date.month - 1 + months
            year = date.year + month // 12
            month = month % 12 + 1
            day = min(date.day, [31,29 if year%4==0 and (year%100!=0 or year%400==0) else 28,31,30,31,30,31,31,30,31,30,31][month-1])
            return date.replace(year=year, month=month, day=day)

        if period == 'weekly':
            # 해당 주의 일요일로 이동
            days_until_sunday = 6 - end_date.weekday()
            end_sunday = end_date + timedelta(days=days_until_sunday)
            # 18주 전 월요일
            start_monday = end_sunday - timedelta(weeks=17, days=6)
            range_start = start_monday.strftime('%Y-%m-%d')
            range_end = end_sunday.strftime('%Y-%m-%d')
            group_format = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, d.TxDate), d.TxDate)) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, d.TxDate) AS VARCHAR), 2)"
        else:
            start_date = add_months(end_date, -17)
            start_date = start_date.replace(day=1)
            range_start = start_date.strftime('%Y-%m-%d')
            next_month = add_months(end_date.replace(day=1), 1)
            range_end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
            group_format = "FORMAT(d.TxDate, 'yyyy-MM')"
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 비급여 매출 데이터 조회 (Detail 테이블에서 InsuYes=0)
        cursor.execute(f"""
            SELECT
                {group_format} as period_key,
                d.PxName,
                SUM(ISNULL(d.TxMoney, 0)) as amount
            FROM Detail d
            WHERE d.InsuYes = 0
              AND ISNULL(d.TxMoney, 0) > 0
              AND CAST(d.TxDate AS DATE) >= '{range_start}'
              AND CAST(d.TxDate AS DATE) <= '{range_end}'
            GROUP BY {group_format}, d.PxName
            ORDER BY period_key
        """)

        rows = cursor.fetchall()
        conn.close()

        # 기간별 카테고리별 데이터 집계
        period_data = {}
        if period == 'weekly':
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                period_data[period_key] = {
                    "맞춤한약": 0, "녹용": 0, "공진단": 0, "경옥고": 0,
                    "상비한약": 0, "약침": 0, "다이어트": 0
                }
        else:
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                period_data[month_label] = {
                    "맞춤한약": 0, "녹용": 0, "공진단": 0, "경옥고": 0,
                    "상비한약": 0, "약침": 0, "다이어트": 0
                }

        # 카테고리 분류 함수
        def categorize_uncovered(px_name):
            if not px_name:
                return None
            name = px_name.strip()

            # 경옥고 (경옥고, 경옥환, 녹용경옥고, 녹용경옥환) - 녹용보다 먼저 체크
            if '경옥고' in name or '경옥환' in name:
                return "경옥고"
            # 녹용 (경옥고 제외 후)
            if '녹용' in name:
                return "녹용"
            # 공진단
            if '공진단' in name:
                return "공진단"
            # 약침
            if '약침' in name or name in ['봉침', '자하거', '태반주사', '신바로']:
                return "약침"
            # 다이어트
            if any(d in name for d in ['린다프리미엄', '린다스탠다드', '린다스페셜', '린다환', '린디톡스', '슬림환', '체감탕']):
                return "다이어트"
            # 상비한약 (상비약, 감기약, 치료약, 자운고, 상용환)
            if any(kw in name for kw in ['상비약', '감기약', '치료약', '자운고', '상용환']):
                return "상비한약"
            # 맞춤한약 (탕약, 환, 처방 등)
            if any(kw in name for kw in ['탕', '환', '고', '산', '처방', '첩', '제', '약']):
                if '공진단' not in name and '경옥고' not in name and '경옥환' not in name:
                    return "맞춤한약"

            return None

        # 데이터 집계
        for row in rows:
            pk = row['period_key']
            px_name = row['PxName']
            amount = row['amount'] or 0

            if pk not in period_data:
                continue

            category = categorize_uncovered(px_name)
            if category and category in period_data[pk]:
                period_data[pk][category] += amount

        # 결과 배열 생성
        result = []
        if period == 'weekly':
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                week_label = week_monday.strftime('%m/%d')
                data = period_data.get(period_key, {})
                result.append({
                    "month": week_label,
                    "맞춤한약": data.get("맞춤한약", 0),
                    "녹용": data.get("녹용", 0),
                    "공진단": data.get("공진단", 0),
                    "경옥고": data.get("경옥고", 0),
                    "상비한약": data.get("상비한약", 0),
                    "약침": data.get("약침", 0),
                    "다이어트": data.get("다이어트", 0)
                })
        else:
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                data = period_data.get(month_label, {})
                result.append({
                    "month": month_label,
                    "맞춤한약": data.get("맞춤한약", 0),
                    "녹용": data.get("녹용", 0),
                    "공진단": data.get("공진단", 0),
                    "경옥고": data.get("경옥고", 0),
                    "상비한약": data.get("상비한약", 0),
                    "약침": data.get("약침", 0),
                    "다이어트": data.get("다이어트", 0)
                })

        return jsonify({
            "end_date": end_date.strftime('%Y-%m-%d') if period == 'weekly' else end_date.strftime('%Y-%m'),
            "period": period,
            "data": result
        })

    except Exception as e:
        mssql_db.log(f"비급여추이 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/all-trends')
def stats_all_trends():
    """통합 18개월 추이 API - 5개 추이 데이터를 하나의 DB 연결로 조회

    반환: revenue_trend, visit_route_trend, chim_patient_trend, yak_chojin_trend, uncovered_trend
    """
    try:
        from datetime import datetime as dt, timedelta
        import calendar

        def add_months(date, months):
            month = date.month - 1 + months
            year = date.year + month // 12
            month = month % 12 + 1
            day = min(date.day, calendar.monthrange(year, month)[1])
            return date.replace(year=year, month=month, day=day)

        end_date_str = request.args.get('end_date', dt.now().strftime('%Y-%m-%d'))
        end_date = dt.strptime(end_date_str[:7] + '-01', '%Y-%m-%d')

        # 18개월 범위
        start_month = add_months(end_date, -17)
        range_start = start_month.strftime('%Y-%m-01')
        next_month = add_months(end_date, 1)
        range_end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # ========== 1. 매출추이 (revenue_trend) ==========
        # 1-1. 급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT FORMAT(r.TxDate, 'yyyy-MM') as month,
                   ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0)), 0) as insurance
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY FORMAT(r.TxDate, 'yyyy-MM')
        """)
        insurance_by_month = {row['month']: int(row['insurance'] or 0) for row in cursor.fetchall()}

        # 1-2. 추나매출 (자보 제외)
        cursor.execute(f"""
            SELECT FORMAT(d.TxDate, 'yyyy-MM') as month,
                   ISNULL(SUM(ISNULL(d.TxMoney, 0)), 0) as chuna
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND d.PxName LIKE '%추나%'
              AND NOT EXISTS (SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = d.Customer_PK
                AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE) AND d2.TxItem LIKE '%자동차보험%')
            GROUP BY FORMAT(d.TxDate, 'yyyy-MM')
        """)
        chuna_by_month = {row['month']: int(row['chuna'] or 0) for row in cursor.fetchall()}

        # 1-3. 자보매출
        cursor.execute(f"""
            SELECT FORMAT(r.TxDate, 'yyyy-MM') as month,
                   ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) + ISNULL(r.General_Money, 0)), 0) as jabo
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY FORMAT(r.TxDate, 'yyyy-MM')
        """)
        jabo_by_month = {row['month']: int(row['jabo'] or 0) for row in cursor.fetchall()}

        # 1-4. 비급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT FORMAT(r.TxDate, 'yyyy-MM') as month,
                   ISNULL(SUM(ISNULL(r.General_Money, 0)), 0) as uncovered
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY FORMAT(r.TxDate, 'yyyy-MM')
        """)
        uncovered_by_month = {row['month']: int(row['uncovered'] or 0) for row in cursor.fetchall()}

        # ========== 2. 침환자 유입추이 (visit_route_trend) ==========
        intro_keywords = ['소개', '소문', '내원환자', '직원']
        search_keywords = ['네이버', '지도', '인터넷', '홈페이지', '검색', '블로그']
        signboard_keywords = ['간판', '현수막', '근처']

        cursor.execute(f"""
            SELECT FORMAT(c.reg_date, 'yyyy-MM') as month, c.SUGGEST, COUNT(DISTINCT c.Customer_PK) as cnt
            FROM Customer c
            JOIN Receipt r ON c.Customer_PK = r.Customer_PK
            WHERE c.reg_date >= '{range_start}' AND c.reg_date < DATEADD(DAY, 1, '{range_end}')
              AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE)
              AND r.CheongGu_Money > 0
              AND c.SUGGEST IS NOT NULL AND c.SUGGEST != ''
              AND NOT EXISTS (SELECT 1 FROM custcarinsuinfo ci WHERE ci.custpk = c.Customer_PK)
            GROUP BY FORMAT(c.reg_date, 'yyyy-MM'), c.SUGGEST
        """)
        visit_route_raw = cursor.fetchall()

        # ========== 3. 침환자현황 추이 (chim_patient_trend) ==========
        # 월별 데이터 수집
        chim_patient_data = {}
        for i in range(17, -1, -1):
            month_date = add_months(end_date, -i)
            month_start = month_date.strftime('%Y-%m-01')
            next_m = add_months(month_date, 1)
            month_end = (next_m - timedelta(days=1)).strftime('%Y-%m-%d')
            month_label = month_date.strftime('%Y-%m')

            # 평환 (일평균)
            cursor.execute(f"""
                SELECT COUNT(DISTINCT CONCAT(r.Customer_PK, '_', CAST(r.TxDate AS DATE))) as total_visits
                FROM Receipt r
                WHERE CAST(r.TxDate AS DATE) BETWEEN '{month_start}' AND '{month_end}'
                  AND (ISNULL(r.CheongGu_Money, 0) > 0
                    OR EXISTS (SELECT 1 FROM custcarinsuinfo ci WHERE ci.custpk = r.Customer_PK))
            """)
            total_visits = cursor.fetchone()['total_visits'] or 0

            cursor.execute(f"""
                SELECT COUNT(DISTINCT CAST(TxDate AS DATE)) as work_days
                FROM Receipt WHERE CAST(TxDate AS DATE) BETWEEN '{month_start}' AND '{month_end}'
            """)
            work_days = cursor.fetchone()['work_days'] or 1
            avg_daily = round(total_visits / work_days, 1)

            # 침 초진
            cursor.execute(f"""
                SELECT COUNT(DISTINCT c.Customer_PK) as cnt FROM Customer c
                JOIN Receipt r ON c.Customer_PK = r.Customer_PK
                WHERE CAST(c.reg_date AS DATE) BETWEEN '{month_start}' AND '{month_end}'
                  AND CAST(r.TxDate AS DATE) = CAST(c.reg_date AS DATE) AND r.CheongGu_Money > 0
                  AND NOT EXISTS (SELECT 1 FROM custcarinsuinfo ci WHERE ci.custpk = c.Customer_PK)
            """)
            chim_chojin = cursor.fetchone()['cnt'] or 0

            # 침 재초진
            cursor.execute(f"""
                SELECT COUNT(DISTINCT d.Customer_PK) as cnt FROM Detail d
                JOIN Customer c ON d.Customer_PK = c.Customer_PK
                JOIN Receipt r ON d.Customer_PK = r.Customer_PK AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE)
                WHERE CAST(d.TxDate AS DATE) BETWEEN '{month_start}' AND '{month_end}'
                  AND CAST(c.reg_date AS DATE) < '{month_start}' AND d.PxName = N'진찰료(초진)'
                  AND ISNULL(r.CheongGu_Money, 0) > 0
                  AND NOT EXISTS (SELECT 1 FROM custcarinsuinfo ci WHERE ci.custpk = d.Customer_PK)
            """)
            chim_rechojin = cursor.fetchone()['cnt'] or 0

            # 자보 초진
            cursor.execute(f"""
                SELECT COUNT(DISTINCT c.Customer_PK) as cnt FROM Customer c
                WHERE CAST(c.reg_date AS DATE) BETWEEN '{month_start}' AND '{month_end}'
                  AND EXISTS (SELECT 1 FROM custcarinsuinfo ci WHERE ci.custpk = c.Customer_PK)
            """)
            jabo_chojin = cursor.fetchone()['cnt'] or 0

            # 자보 재초진
            cursor.execute(f"""
                WITH PeriodJabo AS (
                    SELECT DISTINCT d.Customer_PK, d.사고번호 FROM Detail d
                    JOIN Customer c ON d.Customer_PK = c.Customer_PK
                    WHERE d.TxItem LIKE '%자동차보험%'
                      AND CAST(d.TxDate AS DATE) BETWEEN '{month_start}' AND '{month_end}'
                      AND CAST(c.reg_date AS DATE) < '{month_start}'
                      AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
                ),
                PreviousAccidents AS (
                    SELECT DISTINCT d.Customer_PK, d.사고번호 FROM Detail d
                    WHERE d.TxItem LIKE '%자동차보험%' AND CAST(d.TxDate AS DATE) < '{month_start}'
                      AND d.사고번호 IS NOT NULL AND d.사고번호 != ''
                )
                SELECT COUNT(DISTINCT pj.Customer_PK) as cnt FROM PeriodJabo pj
                WHERE NOT EXISTS (SELECT 1 FROM PreviousAccidents pa
                    WHERE pa.Customer_PK = pj.Customer_PK AND pa.사고번호 = pj.사고번호)
            """)
            jabo_rechojin = cursor.fetchone()['cnt'] or 0

            chim_patient_data[month_label] = {
                "avg_daily": avg_daily,
                "chim_total": chim_chojin + chim_rechojin,
                "jabo_total": jabo_chojin + jabo_rechojin
            }

        # ========== 4. 약초진 추이 (yak_chojin_trend) ==========
        cursor.execute(f"""
            SELECT FORMAT(d.TxDate, 'yyyy-MM') as month,
                   COUNT(DISTINCT d.Customer_PK) as cnt
            FROM Detail d
            JOIN Customer c ON d.Customer_PK = c.Customer_PK
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND d.InsuYes = 0 AND ISNULL(d.TxMoney, 0) > 0
              AND (d.PxName LIKE '한약%' OR d.PxName LIKE '공진단%' OR d.PxName LIKE '경옥고%'
                   OR d.PxName LIKE '녹용추가%' OR d.PxName LIKE '린다%' OR d.PxName LIKE '슬림환%'
                   OR d.PxName LIKE '%치료약%' OR d.PxName LIKE '%종합진료비%'
                   OR d.PxName = '재처방' OR d.PxName = '내원상담')
              AND NOT EXISTS (
                  SELECT 1 FROM Detail d2
                  WHERE d2.Customer_PK = d.Customer_PK
                    AND CAST(d2.TxDate AS DATE) >= DATEADD(MONTH, -6, CAST(d.TxDate AS DATE))
                    AND CAST(d2.TxDate AS DATE) < CAST(d.TxDate AS DATE)
                    AND d2.InsuYes = 0 AND ISNULL(d2.TxMoney, 0) > 0
                    AND (d2.PxName LIKE '한약%' OR d2.PxName LIKE '공진단%' OR d2.PxName LIKE '경옥고%'
                         OR d2.PxName LIKE '녹용추가%' OR d2.PxName LIKE '린다%' OR d2.PxName LIKE '슬림환%'
                         OR d2.PxName LIKE '%치료약%' OR d2.PxName LIKE '%종합진료비%'
                         OR d2.PxName = '재처방' OR d2.PxName = '내원상담')
              )
            GROUP BY FORMAT(d.TxDate, 'yyyy-MM')
        """)
        yak_by_month_raw = cursor.fetchall()
        yak_existing_by_month = {row['month']: row['cnt'] for row in yak_by_month_raw}

        # 약초진 신규 (등록월 = 구입월)
        cursor.execute(f"""
            SELECT FORMAT(d.TxDate, 'yyyy-MM') as month, COUNT(DISTINCT d.Customer_PK) as cnt
            FROM Detail d
            JOIN Customer c ON d.Customer_PK = c.Customer_PK
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND d.InsuYes = 0 AND ISNULL(d.TxMoney, 0) > 0
              AND (d.PxName LIKE '한약%' OR d.PxName LIKE '공진단%' OR d.PxName LIKE '경옥고%'
                   OR d.PxName LIKE '녹용추가%' OR d.PxName LIKE '린다%' OR d.PxName LIKE '슬림환%'
                   OR d.PxName LIKE '%치료약%' OR d.PxName LIKE '%종합진료비%'
                   OR d.PxName = '재처방' OR d.PxName = '내원상담')
              AND FORMAT(c.reg_date, 'yyyy-MM') = FORMAT(d.TxDate, 'yyyy-MM')
              AND NOT EXISTS (
                  SELECT 1 FROM Detail d2
                  WHERE d2.Customer_PK = d.Customer_PK
                    AND CAST(d2.TxDate AS DATE) >= DATEADD(MONTH, -6, CAST(d.TxDate AS DATE))
                    AND CAST(d2.TxDate AS DATE) < CAST(d.TxDate AS DATE)
                    AND d2.InsuYes = 0 AND ISNULL(d2.TxMoney, 0) > 0
                    AND (d2.PxName LIKE '한약%' OR d2.PxName LIKE '공진단%' OR d2.PxName LIKE '경옥고%'
                         OR d2.PxName LIKE '녹용추가%' OR d2.PxName LIKE '린다%' OR d2.PxName LIKE '슬림환%'
                         OR d2.PxName LIKE '%치료약%' OR d2.PxName LIKE '%종합진료비%'
                         OR d2.PxName = '재처방' OR d2.PxName = '내원상담')
              )
            GROUP BY FORMAT(d.TxDate, 'yyyy-MM')
        """)
        yak_new_by_month = {row['month']: row['cnt'] for row in cursor.fetchall()}

        # ========== 5. 비급여 추이 (uncovered_trend) ==========
        cursor.execute(f"""
            SELECT FORMAT(d.TxDate, 'yyyy-MM') as month, d.PxName, SUM(ISNULL(d.TxMoney, 0)) as amount
            FROM Detail d
            WHERE d.InsuYes = 0 AND ISNULL(d.TxMoney, 0) > 0
              AND d.TxDate >= '{range_start}' AND d.TxDate < DATEADD(MONTH, 1, '{end_date.strftime('%Y-%m-01')}')
            GROUP BY FORMAT(d.TxDate, 'yyyy-MM'), d.PxName
        """)
        uncovered_raw = cursor.fetchall()

        conn.close()

        # ========== 결과 조합 ==========
        # 월별 초기화
        months = []
        for i in range(17, -1, -1):
            month_date = add_months(end_date, -i)
            months.append(month_date.strftime('%Y-%m'))

        # 1. revenue_trend
        revenue_trend = []
        for m in months:
            ins = insurance_by_month.get(m, 0)
            chu = chuna_by_month.get(m, 0)
            jab = jabo_by_month.get(m, 0)
            unc = uncovered_by_month.get(m, 0)
            revenue_trend.append({
                "month": m, "insurance": ins, "chuna": chu, "jabo": jab,
                "uncovered": unc, "total": ins + jab + unc
            })

        # 2. visit_route_trend
        visit_route_monthly = {m: {"intro": 0, "search": 0, "signboard": 0, "other": 0} for m in months}
        for item in visit_route_raw:
            m = item['month']
            suggest = item['SUGGEST'] or ''
            cnt = item['cnt'] or 0
            if m not in visit_route_monthly:
                continue
            if any(kw in suggest for kw in intro_keywords):
                visit_route_monthly[m]["intro"] += cnt
            elif any(kw in suggest for kw in search_keywords):
                visit_route_monthly[m]["search"] += cnt
            elif any(kw in suggest for kw in signboard_keywords):
                visit_route_monthly[m]["signboard"] += cnt
            else:
                visit_route_monthly[m]["other"] += cnt

        visit_route_trend = []
        for m in months:
            d = visit_route_monthly[m]
            visit_route_trend.append({
                "month": m, "intro": d["intro"], "search": d["search"],
                "signboard": d["signboard"], "other": d["other"],
                "total": d["intro"] + d["search"] + d["signboard"] + d["other"]
            })

        # 3. chim_patient_trend
        chim_patient_trend = []
        for m in months:
            d = chim_patient_data.get(m, {"avg_daily": 0, "chim_total": 0, "jabo_total": 0})
            chim_patient_trend.append({
                "month": m, "avg_daily": d["avg_daily"],
                "chim_total": d["chim_total"], "jabo_total": d["jabo_total"]
            })

        # 4. yak_chojin_trend
        yak_chojin_trend = []
        for m in months:
            total = yak_existing_by_month.get(m, 0)
            new_cnt = yak_new_by_month.get(m, 0)
            yak_chojin_trend.append({
                "month": m, "new": new_cnt, "existing": total - new_cnt, "total": total
            })

        # 5. uncovered_trend (카테고리 분류)
        def categorize_uncovered(px_name):
            if not px_name:
                return None
            name = px_name.strip()
            if '경옥고' in name or '경옥환' in name:
                return "경옥고"
            if '녹용' in name:
                return "녹용"
            if '공진단' in name:
                return "공진단"
            if '약침' in name or name in ['봉침', '자하거', '태반주사', '신바로']:
                return "약침"
            if any(d in name for d in ['린다프리미엄', '린다스탠다드', '린다스페셜', '린다환', '린디톡스', '슬림환', '체감탕']):
                return "다이어트"
            if any(kw in name for kw in ['상비약', '감기약', '치료약', '자운고', '상용환']):
                return "상비한약"
            if any(kw in name for kw in ['탕', '환', '고', '산', '처방', '첩', '제', '약']):
                if '공진단' not in name and '경옥고' not in name and '경옥환' not in name:
                    return "맞춤한약"
            return None

        uncovered_monthly = {m: {"맞춤한약": 0, "녹용": 0, "공진단": 0, "경옥고": 0, "상비한약": 0, "약침": 0, "다이어트": 0} for m in months}
        for row in uncovered_raw:
            m = row['month']
            if m not in uncovered_monthly:
                continue
            cat = categorize_uncovered(row['PxName'])
            if cat and cat in uncovered_monthly[m]:
                uncovered_monthly[m][cat] += row['amount'] or 0

        uncovered_trend = []
        for m in months:
            d = uncovered_monthly[m]
            uncovered_trend.append({
                "month": m, "맞춤한약": d["맞춤한약"], "녹용": d["녹용"], "공진단": d["공진단"],
                "경옥고": d["경옥고"], "상비한약": d["상비한약"], "약침": d["약침"], "다이어트": d["다이어트"]
            })

        return jsonify({
            "end_date": end_date.strftime('%Y-%m'),
            "revenue_trend": revenue_trend,
            "visit_route_trend": visit_route_trend,
            "chim_patient_trend": chim_patient_trend,
            "yak_chojin_trend": yak_chojin_trend,
            "uncovered_trend": uncovered_trend
        })

    except Exception as e:
        mssql_db.log(f"통합추이 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/execute', methods=['POST'])
def execute():
    """MSSQL 쿼리 실행"""
    try:
        data = request.get_json()
        sql_query = data.get('sql', '').strip()
        if not sql_query:
            return jsonify({"error": "SQL query is required"}), 400

        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor()
        cursor.execute(sql_query)

        # SELECT 문인 경우
        if sql_query.upper().startswith('SELECT'):
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            rows_list = [list(row) for row in rows]
            conn.close()
            return jsonify({
                "columns": columns,
                "rows": rows_list,
                "message": f"Found {len(rows_list)} rows"
            })
        else:
            conn.commit()
            affected = cursor.rowcount
            conn.close()
            return jsonify({
                "success": True,
                "affected_rows": affected,
                "message": f"Query executed. {affected} rows affected."
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ============ Self Update Webhook ============

@mssql_bp.route('/api/self-update', methods=['POST'])
def self_update_webhook():
    """GitHub Webhook으로 unified-server 자체 업데이트

    GitHub에서 push 이벤트 수신 시:
    1. 서명 검증 (webhook_secret)
    2. git pull 실행
    3. 변경사항 있으면 서버 자동 재시작
    """
    config = load_config()
    current_secret = config.get("webhook_secret", "")

    # Webhook 활성화 확인
    if not config.get("webhook_enabled", False):
        return jsonify({"error": "Webhook disabled"}), 403

    if not current_secret:
        return jsonify({"error": "Webhook secret not configured"}), 403

    # GitHub 서명 검증
    signature = request.headers.get('X-Hub-Signature-256', '')
    if not git_build.verify_github_signature(request.data, signature, current_secret):
        git_build.log("Self-update: Invalid signature")
        return jsonify({"error": "Invalid signature"}), 401

    # 이벤트 타입 확인
    event_type = request.headers.get('X-GitHub-Event', 'unknown')

    if event_type == 'ping':
        git_build.log("Self-update: GitHub ping received")
        return jsonify({"message": "pong", "version": VERSION})

    if event_type != 'push':
        return jsonify({"message": f"Ignored event: {event_type}"}), 200

    # Push 이벤트 처리 - 백그라운드에서 실행
    threading.Thread(
        target=git_build.handle_self_update,
        daemon=True
    ).start()

    return jsonify({
        "message": "Self-update triggered",
        "version": VERSION,
        "status": "processing"
    })


@mssql_bp.route('/api/self-update/status', methods=['GET'])
def self_update_status():
    """Self-update 상태 조회"""
    return jsonify({
        "version": VERSION,
        "last_update": git_build.get_last_self_update_time()
    })


@mssql_bp.route('/api/doctor-order')
def get_doctor_order():
    """원장 입사순서 조회 API (UserInfo.UserTable 기준)

    Returns:
    - doctors: 입사일 순으로 정렬된 현재 재직 원장 목록
        - name: 원장명
        - hire_date: 입사일 (근무기간시작)
    """
    try:
        import pymssql
        # UserInfo DB에 직접 연결 (mssql_db.get_connection은 MasterDB만 지원)
        conn = pymssql.connect(
            server='192.168.0.173',
            user='members',
            password='msp1234',
            port=55555,
            database='UserInfo',
            charset='utf8'
        )
        if not conn:
            return jsonify({"error": "UserInfo DB 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 현재 재직 중인 원장 (퇴사여부=false 또는 null, 근무기간시작 기준 정렬)
        cursor.execute("""
            SELECT UserID as name, 근무기간시작 as hire_date
            FROM UserTable
            WHERE (퇴사여부 = 0 OR 퇴사여부 IS NULL)
              AND 근무기간시작 IS NOT NULL
            ORDER BY 근무기간시작 ASC
        """)

        doctors = cursor.fetchall()
        conn.close()

        result = []
        for doc in doctors:
            name = doc['name'].strip() if doc['name'] else ''
            hire_date = doc['hire_date'].strftime('%Y-%m-%d') if doc['hire_date'] else None

            # DOCTOR 같은 시스템 계정 제외
            if name and name.upper() != 'DOCTOR':
                result.append({
                    'name': name,
                    'hire_date': hire_date
                })

        return jsonify({
            "doctors": result,
            "count": len(result)
        })

    except Exception as e:
        mssql_db.log(f"원장순서 조회 오류: {str(e)}")
        return jsonify({"error": str(e)}), 500


@mssql_bp.route('/api/stats/doctor-revenue-trend')
def stats_doctor_revenue_trend():
    """원장별 매출 추이 API (최근 18개월/18주)

    Query params:
    - doctor (필수): 원장명
    - period: 'monthly' (기본값) 또는 'weekly' (18주 추이)
    - end_date: 기준일 (YYYY-MM-DD, 기본값: 현재)

    Returns:
    - doctor: 원장명
    - period: 'monthly' 또는 'weekly'
    - hire_date: 입사일 (없으면 null)
    - summary: 현재/이전 기간 매출 요약 및 변화율
    - data: 기간별 매출 데이터
    """
    try:
        from datetime import datetime as dt, timedelta
        import calendar
        import pymssql

        def add_months(date, months):
            """월 덧셈/뺄셈 함수"""
            month = date.month - 1 + months
            year = date.year + month // 12
            month = month % 12 + 1
            day = min(date.day, calendar.monthrange(year, month)[1])
            return date.replace(year=year, month=month, day=day)

        # 파라미터 파싱
        doctor = request.args.get('doctor', '').strip()
        if not doctor:
            return jsonify({"error": "doctor 파라미터 필수"}), 400

        period = request.args.get('period', 'monthly')
        end_date_str = request.args.get('end_date', dt.now().strftime('%Y-%m-%d'))

        # 입사일 조회 (UserInfo DB)
        hire_date_str = None
        try:
            user_conn = pymssql.connect(
                server='192.168.0.173',
                user='members',
                password='msp1234',
                port=55555,
                database='UserInfo',
                charset='utf8'
            )
            user_cursor = user_conn.cursor(as_dict=True)
            user_cursor.execute("""
                SELECT 근무기간시작 as hire_date
                FROM UserTable
                WHERE UserID = %s
            """, (doctor,))
            row = user_cursor.fetchone()
            if row and row['hire_date']:
                hire_date_str = row['hire_date'].strftime('%Y-%m-%d')
            user_conn.close()
        except:
            pass  # 입사일 조회 실패해도 계속 진행

        # 날짜 범위 계산
        if period == 'weekly':
            end_date = dt.strptime(end_date_str[:10], '%Y-%m-%d')
            days_until_sunday = 6 - end_date.weekday()
            end_sunday = end_date + timedelta(days=days_until_sunday)
            start_monday = end_sunday - timedelta(weeks=17, days=6)
            range_start = start_monday.strftime('%Y-%m-%d')
            range_end = end_sunday.strftime('%Y-%m-%d')
        else:
            end_date = dt.strptime(end_date_str[:7] + '-01', '%Y-%m-%d')
            start_month = add_months(end_date, -17)
            range_start = start_month.strftime('%Y-%m-01')
            next_month = add_months(end_date, 1)
            range_end = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
        conn = mssql_db.get_connection()
        if not conn:
            return jsonify({"error": "MSSQL 연결 실패"}), 500

        cursor = conn.cursor(as_dict=True)

        # 주간/월간에 따라 GROUP BY 형식 결정
        if period == 'weekly':
            group_format = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, r.TxDate), r.TxDate)) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, r.TxDate) AS VARCHAR), 2)"
            group_format_d = "CAST(YEAR(DATEADD(DAY, 26 - DATEPART(iso_week, d.TxDate), d.TxDate)) AS VARCHAR) + '-W' + RIGHT('0' + CAST(DATEPART(iso_week, d.TxDate) AS VARCHAR), 2)"
        else:
            group_format = "FORMAT(r.TxDate, 'yyyy-MM')"
            group_format_d = "FORMAT(d.TxDate, 'yyyy-MM')"

        # 원장 필터 조건: 해당 원장이 처방한 진료 내역이 있는 영수증만 포함
        doctor_filter = f"EXISTS (SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = r.Customer_PK AND CAST(d2.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d2.TxDoctor = N'{doctor}')"
        doctor_filter_d = f"d.TxDoctor = N'{doctor}'"

        # 1. 급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT {group_format} as period_key,
                   ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0)), 0) as insurance
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND {doctor_filter}
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format}
        """)
        insurance_by_period = {row['period_key']: int(row['insurance'] or 0) for row in cursor.fetchall()}

        # 2. 추나매출 (자보 제외)
        cursor.execute(f"""
            SELECT {group_format_d} as period_key,
                   ISNULL(SUM(ISNULL(d.TxMoney, 0)), 0) as chuna
            FROM Detail d
            WHERE CAST(d.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND {doctor_filter_d}
              AND d.PxName LIKE '%추나%'
              AND NOT EXISTS (SELECT 1 FROM Detail d2 WHERE d2.Customer_PK = d.Customer_PK
                AND CAST(d2.TxDate AS DATE) = CAST(d.TxDate AS DATE) AND d2.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format_d}
        """)
        chuna_by_period = {row['period_key']: int(row['chuna'] or 0) for row in cursor.fetchall()}

        # 3. 자보매출
        cursor.execute(f"""
            SELECT {group_format} as period_key,
                   ISNULL(SUM(ISNULL(r.Bonin_Money, 0) + ISNULL(r.CheongGu_Money, 0) + ISNULL(r.General_Money, 0)), 0) as jabo
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND {doctor_filter}
              AND EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format}
        """)
        jabo_by_period = {row['period_key']: int(row['jabo'] or 0) for row in cursor.fetchall()}

        # 4. 비급여매출 (자보 제외)
        cursor.execute(f"""
            SELECT {group_format} as period_key,
                   ISNULL(SUM(ISNULL(r.General_Money, 0)), 0) as uncovered
            FROM Receipt r
            WHERE CAST(r.TxDate AS DATE) BETWEEN '{range_start}' AND '{range_end}'
              AND {doctor_filter}
              AND NOT EXISTS (SELECT 1 FROM Detail d WHERE d.Customer_PK = r.Customer_PK
                AND CAST(d.TxDate AS DATE) = CAST(r.TxDate AS DATE) AND d.TxItem LIKE '%자동차보험%')
            GROUP BY {group_format}
        """)
        uncovered_by_period = {row['period_key']: int(row['uncovered'] or 0) for row in cursor.fetchall()}

        conn.close()

        # 결과 조합 (과거 -> 현재 순서)
        result = []
        if period == 'weekly':
            for i in range(17, -1, -1):
                week_sunday = end_sunday - timedelta(weeks=i)
                week_monday = week_sunday - timedelta(days=6)
                iso_year, iso_week, _ = week_monday.isocalendar()
                period_key = f"{iso_year}-W{iso_week:02d}"
                week_label = week_monday.strftime('%m/%d')
                insurance = insurance_by_period.get(period_key, 0)
                chuna = chuna_by_period.get(period_key, 0)
                jabo = jabo_by_period.get(period_key, 0)
                uncovered = uncovered_by_period.get(period_key, 0)
                result.append({
                    "month": week_label,
                    "insurance": insurance,
                    "chuna": chuna,
                    "jabo": jabo,
                    "uncovered": uncovered,
                    "total": insurance + jabo + uncovered
                })
        else:
            for i in range(17, -1, -1):
                month_date = add_months(end_date, -i)
                month_label = month_date.strftime('%Y-%m')
                insurance = insurance_by_period.get(month_label, 0)
                chuna = chuna_by_period.get(month_label, 0)
                jabo = jabo_by_period.get(month_label, 0)
                uncovered = uncovered_by_period.get(month_label, 0)
                result.append({
                    "month": month_label,
                    "insurance": insurance,
                    "chuna": chuna,
                    "jabo": jabo,
                    "uncovered": uncovered,
                    "total": insurance + jabo + uncovered
                })

        # Summary 계산 (현재 기간 vs 이전 기간)
        if len(result) >= 2:
            current = result[-1]
            previous = result[-2]
            current_total = current['total']
            previous_total = previous['total']
            change_rate = 0
            if previous_total > 0:
                change_rate = round((current_total - previous_total) / previous_total * 100, 1)

            summary = {
                "current": {
                    "total": current_total,
                    "insurance": current['insurance'],
                    "chuna": current['chuna'],
                    "jabo": current['jabo'],
                    "uncovered": current['uncovered']
                },
                "previous": {
                    "total": previous_total,
                    "insurance": previous['insurance'],
                    "chuna": previous['chuna'],
                    "jabo": previous['jabo'],
                    "uncovered": previous['uncovered']
                },
                "change_rate": change_rate
            }
        else:
            summary = None

        return jsonify({
            "doctor": doctor,
            "period": period,
            "hire_date": hire_date_str,
            "end_date": end_date.strftime('%Y-%m-%d') if period == 'weekly' else end_date.strftime('%Y-%m'),
            "summary": summary,
            "data": result
        })

    except Exception as e:
        mssql_db.log(f"원장매출추이 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
