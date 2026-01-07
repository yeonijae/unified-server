"""
치료관리 API 엔드포인트 코드
이 파일의 내용을 postgres_routes.py에 추가
"""

TREATMENT_API_CODE = '''


# ============ 치료관리 API (daily_treatment_records 기반) ============

@postgres_bp.route('/api/treatments/today', methods=['GET', 'OPTIONS'])
def get_today_treatments():
    """오늘의 치료 목록 조회

    Query params:
        - status: waiting | treating | complete (optional)
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        status_filter = request.args.get('status')

        sql = """
            SELECT * FROM daily_treatment_records
            WHERE treatment_date = CURRENT_DATE::text
        """
        params = []

        if status_filter:
            sql += " AND status = %s"
            params.append(status_filter)

        sql += " ORDER BY reception_time NULLS LAST, created_at"

        rows = postgres_db.execute_query(sql, tuple(params) if params else None)

        return json_response({
            "success": True,
            "data": rows,
            "count": len(rows)
        })
    except Exception as e:
        postgres_db.log(f"get_today_treatments error: {e}")
        return json_response({"error": str(e)}, 500)


@postgres_bp.route('/api/treatments/sync', methods=['POST', 'OPTIONS'])
def sync_treatments():
    """MSSQL Treating 데이터를 daily_treatment_records에 동기화

    - 새 환자: INSERT with status='waiting'
    - 기존 환자(waiting): synced_at 업데이트
    - 기존 환자(treating/complete): 스킵
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json() or {}
        mssql_patients = data.get('waiting', [])

        if not mssql_patients:
            return json_response({
                "added": 0,
                "updated": 0,
                "skipped": 0,
                "message": "No data to sync"
            })

        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')
        now = datetime.now().isoformat()

        added = 0
        updated = 0
        skipped = 0

        for patient in mssql_patients:
            patient_id = patient.get('patient_id')
            if not patient_id:
                continue

            # 오늘 해당 환자 기록 확인
            existing = postgres_db.execute_query("""
                SELECT id, status FROM daily_treatment_records
                WHERE patient_id = %s AND treatment_date = %s
                ORDER BY visit_number DESC LIMIT 1
            """, (patient_id, today))

            if existing:
                current_status = existing[0].get('status', 'waiting')
                record_id = existing[0].get('id')

                if current_status in ('treating', 'complete'):
                    # 이미 치료중이거나 완료된 환자는 스킵
                    skipped += 1
                else:
                    # waiting 상태면 synced_at만 업데이트
                    postgres_db.execute_query("""
                        UPDATE daily_treatment_records
                        SET synced_at = %s
                        WHERE id = %s
                    """, (now, record_id), fetch=False)
                    updated += 1
            else:
                # 새 환자 INSERT
                intotime = patient.get('waiting_since') or patient.get('treating_since')

                postgres_db.execute_query("""
                    INSERT INTO daily_treatment_records
                    (patient_id, patient_name, chart_number, treatment_date,
                     status, doctor_name, reception_time,
                     mssql_waiting_pk, mssql_intotime, synced_at,
                     patient_age, patient_sex, visit_number)
                    VALUES (%s, %s, %s, %s, 'waiting', %s, %s, %s, %s, %s, %s, %s, 1)
                """, (
                    patient_id,
                    patient.get('patient_name') or '',
                    patient.get('chart_no') or '',
                    today,
                    patient.get('doctor') or '',
                    intotime,
                    patient.get('id'),  # mssql_waiting_pk
                    intotime,
                    now,
                    patient.get('age'),
                    patient.get('sex') or ''
                ), fetch=False)
                added += 1

        return json_response({
            "success": True,
            "added": added,
            "updated": updated,
            "skipped": skipped,
            "message": f"동기화 완료: 추가 {added}, 업데이트 {updated}, 스킵 {skipped}"
        })

    except Exception as e:
        postgres_db.log(f"sync_treatments error: {e}")
        return json_response({"error": str(e)}, 500)


@postgres_bp.route('/api/treatments/<int:record_id>/assign', methods=['PATCH', 'OPTIONS'])
def assign_treatment_bed(record_id):
    """환자를 베드에 배정 (waiting -> treating)

    Body:
        - room_id: 방 ID
        - bed_name: 베드 이름
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json() or {}
        room_id = data.get('room_id')
        bed_name = data.get('bed_name')

        from datetime import datetime
        now = datetime.now().isoformat()

        postgres_db.execute_query("""
            UPDATE daily_treatment_records
            SET status = 'treating',
                room_id = %s,
                bed_name = %s,
                assigned_at = %s,
                treatment_start = %s,
                updated_at = %s
            WHERE id = %s
        """, (room_id, bed_name, now, now, now, record_id), fetch=False)

        return json_response({
            "success": True,
            "message": "베드 배정 완료"
        })

    except Exception as e:
        postgres_db.log(f"assign_treatment_bed error: {e}")
        return json_response({"error": str(e)}, 500)


@postgres_bp.route('/api/treatments/<int:record_id>/complete', methods=['PATCH', 'OPTIONS'])
def complete_treatment(record_id):
    """치료 완료 (treating -> complete)"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        from datetime import datetime
        now = datetime.now().isoformat()

        postgres_db.execute_query("""
            UPDATE daily_treatment_records
            SET status = 'complete',
                treatment_end = %s,
                updated_at = %s
            WHERE id = %s
        """, (now, now, record_id), fetch=False)

        return json_response({
            "success": True,
            "message": "치료 완료"
        })

    except Exception as e:
        postgres_db.log(f"complete_treatment error: {e}")
        return json_response({"error": str(e)}, 500)


@postgres_bp.route('/api/treatments/<int:record_id>/unassign', methods=['PATCH', 'OPTIONS'])
def unassign_treatment_bed(record_id):
    """베드 배정 해제 (treating -> waiting)"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        from datetime import datetime
        now = datetime.now().isoformat()

        postgres_db.execute_query("""
            UPDATE daily_treatment_records
            SET status = 'waiting',
                room_id = NULL,
                bed_name = NULL,
                assigned_at = NULL,
                treatment_start = NULL,
                updated_at = %s
            WHERE id = %s
        """, (now, record_id), fetch=False)

        return json_response({
            "success": True,
            "message": "베드 배정 해제"
        })

    except Exception as e:
        postgres_db.log(f"unassign_treatment_bed error: {e}")
        return json_response({"error": str(e)}, 500)


@postgres_bp.route('/api/treatments/<int:record_id>', methods=['PATCH', 'OPTIONS'])
def update_treatment_record(record_id):
    """치료 기록 업데이트 (메모, 치료항목 등)"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        data = request.get_json() or {}

        # 업데이트 가능한 필드
        allowed_fields = [
            'doctor_memo', 'treatment_notes', 'patient_complaint',
            'has_acupuncture', 'has_moxa', 'has_cupping', 'has_hotpack',
            'has_chuna', 'has_physio',
            'acupuncture_start', 'acupuncture_end',
            'moxa_start', 'moxa_end',
            'cupping_start', 'cupping_end',
            'hotpack_start', 'hotpack_end',
            'chuna_start', 'chuna_end',
            'physio_start', 'physio_end',
            'yakchim_type', 'yakchim_quantity', 'herbal_prescription'
        ]

        updates = []
        values = []

        for field in allowed_fields:
            if field in data:
                updates.append(f"{field} = %s")
                values.append(data[field])

        if not updates:
            return json_response({"error": "No valid fields to update"}, 400)

        from datetime import datetime
        updates.append("updated_at = %s")
        values.append(datetime.now().isoformat())
        values.append(record_id)

        sql = f"UPDATE daily_treatment_records SET {', '.join(updates)} WHERE id = %s"
        postgres_db.execute_query(sql, tuple(values), fetch=False)

        return json_response({
            "success": True,
            "message": "업데이트 완료"
        })

    except Exception as e:
        postgres_db.log(f"update_treatment_record error: {e}")
        return json_response({"error": str(e)}, 500)


@postgres_bp.route('/api/treatments/<int:record_id>', methods=['GET', 'OPTIONS'])
def get_treatment_record(record_id):
    """치료 기록 상세 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        rows = postgres_db.execute_query("""
            SELECT * FROM daily_treatment_records WHERE id = %s
        """, (record_id,))

        if not rows:
            return json_response({"error": "Record not found"}, 404)

        return json_response({
            "success": True,
            "data": rows[0]
        })

    except Exception as e:
        postgres_db.log(f"get_treatment_record error: {e}")
        return json_response({"error": str(e)}, 500)


@postgres_bp.route('/api/treatments/history/<int:patient_id>', methods=['GET', 'OPTIONS'])
def get_patient_treatment_history(patient_id):
    """환자의 치료 이력 조회

    Query params:
        - days: 최근 N일 (default: 30)
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    try:
        days = int(request.args.get('days', 30))

        rows = postgres_db.execute_query("""
            SELECT * FROM daily_treatment_records
            WHERE patient_id = %s
            AND treatment_date >= (CURRENT_DATE - %s)::text
            ORDER BY treatment_date DESC, visit_number DESC
        """, (patient_id, days))

        return json_response({
            "success": True,
            "data": rows,
            "count": len(rows)
        })

    except Exception as e:
        postgres_db.log(f"get_patient_treatment_history error: {e}")
        return json_response({"error": str(e)}, 500)
'''

if __name__ == "__main__":
    # postgres_routes.py에 추가
    with open('routes/postgres_routes.py', 'a', encoding='utf-8') as f:
        f.write(TREATMENT_API_CODE)
    print("Treatment API added to postgres_routes.py")
