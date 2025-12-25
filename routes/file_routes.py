"""
파일 업로드/다운로드 API
- 검사결과 이미지, PDF 업로드
- 썸네일 자동 생성
- 파일 다운로드/삭제

Endpoints:
  POST   /api/files/upload          - 파일 업로드
  GET    /api/files/<path>          - 파일 다운로드
  DELETE /api/files/<path>          - 파일 삭제
  GET    /api/files/list/<path>     - 디렉토리 파일 목록
"""

from flask import Blueprint, request, jsonify, send_file, make_response
import os
import uuid
from datetime import datetime
from config import load_config

# PIL은 선택적 - 없으면 썸네일 생성 안함
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("[File API] PIL not installed - thumbnail generation disabled")

file_bp = Blueprint('files', __name__)

# 저장 경로 설정 (config에서 로드)
_config = load_config()
BASE_UPLOAD_DIR = _config.get("upload_folder", "C:/haniwon_data/uploads")
THUMBNAIL_DIR = _config.get("thumbnail_folder", "C:/haniwon_data/thumbnails")

# 허용 확장자 (config에서 로드)
_allowed_ext_str = _config.get("allowed_extensions", "jpg,jpeg,png,gif,pdf,bmp,tiff,tif")
ALLOWED_EXTENSIONS = set(ext.strip().lower() for ext in _allowed_ext_str.split(","))
ALLOWED_IMAGE_EXTENSIONS = ALLOWED_EXTENSIONS & {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'tif'}

# 최대 파일 크기 (config에서 로드)
MAX_FILE_SIZE = _config.get("max_file_size_mb", 20) * 1024 * 1024
THUMBNAIL_SIZE = (200, 200)


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


@file_bp.after_request
def after_request(response):
    """모든 응답에 CORS 헤더 추가"""
    return add_cors_headers(response)


# ============ 유틸리티 함수 ============

def get_file_extension(filename):
    """파일 확장자 추출"""
    if '.' in filename:
        return filename.rsplit('.', 1)[1].lower()
    return ''


def generate_unique_filename(original_filename):
    """고유한 파일명 생성"""
    ext = get_file_extension(original_filename)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = uuid.uuid4().hex[:6]
    return f"{timestamp}_{unique_id}.{ext}"


def ensure_directory(path):
    """디렉토리가 없으면 생성"""
    os.makedirs(path, exist_ok=True)


def create_thumbnail(source_path, patient_id, exam_type, filename):
    """이미지 썸네일 생성"""
    if not PIL_AVAILABLE:
        return None

    try:
        # 썸네일 저장 경로
        thumb_dir = os.path.join(THUMBNAIL_DIR, "exams", str(patient_id), exam_type)
        ensure_directory(thumb_dir)

        thumb_name = f"thumb_{filename}"
        # 확장자를 jpg로 통일
        if not thumb_name.lower().endswith('.jpg'):
            thumb_name = thumb_name.rsplit('.', 1)[0] + '.jpg'

        thumb_path = os.path.join(thumb_dir, thumb_name)

        with Image.open(source_path) as img:
            # RGBA를 RGB로 변환 (JPEG 저장을 위해)
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')

            img.thumbnail(THUMBNAIL_SIZE)
            img.save(thumb_path, 'JPEG', quality=85)

        # 상대 경로 반환
        relative_path = f"exams/{patient_id}/{exam_type}/{thumb_name}"
        return relative_path

    except Exception as e:
        print(f"[File API] Thumbnail creation failed: {e}")
        return None


# ============ API 엔드포인트 ============

@file_bp.route('/api/files/info', methods=['GET', 'OPTIONS'])
def file_api_info():
    """File API 정보"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    return json_response({
        "service": "File Upload API",
        "base_upload_dir": BASE_UPLOAD_DIR,
        "thumbnail_dir": THUMBNAIL_DIR,
        "max_file_size_mb": MAX_FILE_SIZE // (1024 * 1024),
        "allowed_extensions": list(ALLOWED_EXTENSIONS),
        "thumbnail_enabled": PIL_AVAILABLE
    })


@file_bp.route('/api/files/upload', methods=['POST', 'OPTIONS'])
def upload_file():
    """
    파일 업로드

    Request (multipart/form-data):
      - file: 파일 데이터
      - patient_id: 환자 ID (필수)
      - exam_type: 검사 유형 (필수) - thermography, inbody, body_shape, etc.
      - category: 카테고리 (기본: exams)

    Response:
      {
        "success": true,
        "file_path": "exams/12345/thermography/2024-12/20241225_abc123.jpg",
        "file_url": "/api/files/exams/12345/thermography/2024-12/20241225_abc123.jpg",
        "thumbnail_path": "exams/12345/thermography/thumb_20241225_abc123.jpg",
        "thumbnail_url": "/api/files/thumbnails/exams/12345/thermography/thumb_20241225_abc123.jpg",
        "original_name": "front_view.jpg",
        "file_size": 1234567,
        "mime_type": "image/jpeg"
      }
    """
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    # 파일 확인
    if 'file' not in request.files:
        return json_response({"error": "파일이 없습니다"}, 400)

    file = request.files['file']
    if file.filename == '':
        return json_response({"error": "파일명이 없습니다"}, 400)

    # 필수 파라미터 확인
    patient_id = request.form.get('patient_id')
    exam_type = request.form.get('exam_type')
    category = request.form.get('category', 'exams')

    if not patient_id:
        return json_response({"error": "patient_id는 필수입니다"}, 400)

    if not exam_type:
        return json_response({"error": "exam_type은 필수입니다"}, 400)

    # 확장자 검증
    ext = get_file_extension(file.filename)
    if ext not in ALLOWED_EXTENSIONS:
        return json_response({
            "error": f"허용되지 않는 파일 형식입니다: {ext}",
            "allowed": list(ALLOWED_EXTENSIONS)
        }, 400)

    # 파일 크기 확인 (Content-Length 헤더 또는 실제 읽기)
    file.seek(0, 2)  # 파일 끝으로 이동
    file_size = file.tell()
    file.seek(0)  # 다시 처음으로

    if file_size > MAX_FILE_SIZE:
        return json_response({
            "error": f"파일 크기가 너무 큽니다. 최대 {MAX_FILE_SIZE // (1024*1024)}MB"
        }, 400)

    try:
        # 저장 경로 생성
        year_month = datetime.now().strftime('%Y-%m')
        save_dir = os.path.join(BASE_UPLOAD_DIR, category, str(patient_id), exam_type, year_month)
        ensure_directory(save_dir)

        # 고유 파일명 생성
        unique_filename = generate_unique_filename(file.filename)
        file_path = os.path.join(save_dir, unique_filename)

        # 파일 저장
        file.save(file_path)

        # 상대 경로 (DB 저장용)
        relative_path = f"{category}/{patient_id}/{exam_type}/{year_month}/{unique_filename}"

        # 썸네일 생성 (이미지인 경우)
        thumbnail_path = None
        thumbnail_url = None
        if ext in ALLOWED_IMAGE_EXTENSIONS:
            thumbnail_path = create_thumbnail(file_path, patient_id, exam_type, unique_filename)
            if thumbnail_path:
                thumbnail_url = f"/api/files/thumbnails/{thumbnail_path}"

        return json_response({
            "success": True,
            "file_path": relative_path,
            "file_url": f"/api/files/{relative_path}",
            "thumbnail_path": thumbnail_path,
            "thumbnail_url": thumbnail_url,
            "original_name": file.filename,
            "file_size": file_size,
            "mime_type": file.content_type or f"image/{ext}"
        })

    except Exception as e:
        return json_response({"error": f"파일 저장 실패: {str(e)}"}, 500)


@file_bp.route('/api/files/<path:file_path>', methods=['GET', 'OPTIONS'])
def get_file(file_path):
    """파일 다운로드/조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    full_path = os.path.join(BASE_UPLOAD_DIR, file_path)

    # 경로 탐색 공격 방지
    full_path = os.path.normpath(full_path)
    if not full_path.startswith(os.path.normpath(BASE_UPLOAD_DIR)):
        return json_response({"error": "잘못된 경로입니다"}, 400)

    if not os.path.exists(full_path):
        return json_response({"error": "파일을 찾을 수 없습니다"}, 404)

    try:
        response = send_file(full_path)
        return add_cors_headers(response)
    except Exception as e:
        return json_response({"error": f"파일 전송 실패: {str(e)}"}, 500)


@file_bp.route('/api/files/thumbnails/<path:file_path>', methods=['GET', 'OPTIONS'])
def get_thumbnail(file_path):
    """썸네일 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    full_path = os.path.join(THUMBNAIL_DIR, file_path)

    # 경로 탐색 공격 방지
    full_path = os.path.normpath(full_path)
    if not full_path.startswith(os.path.normpath(THUMBNAIL_DIR)):
        return json_response({"error": "잘못된 경로입니다"}, 400)

    if not os.path.exists(full_path):
        return json_response({"error": "썸네일을 찾을 수 없습니다"}, 404)

    try:
        response = send_file(full_path)
        return add_cors_headers(response)
    except Exception as e:
        return json_response({"error": f"파일 전송 실패: {str(e)}"}, 500)


@file_bp.route('/api/files/<path:file_path>', methods=['DELETE', 'OPTIONS'])
def delete_file(file_path):
    """파일 삭제"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    full_path = os.path.join(BASE_UPLOAD_DIR, file_path)

    # 경로 탐색 공격 방지
    full_path = os.path.normpath(full_path)
    if not full_path.startswith(os.path.normpath(BASE_UPLOAD_DIR)):
        return json_response({"error": "잘못된 경로입니다"}, 400)

    if not os.path.exists(full_path):
        return json_response({"error": "파일을 찾을 수 없습니다"}, 404)

    try:
        # 파일 삭제
        os.remove(full_path)

        # 썸네일도 삭제 시도
        # file_path 예: exams/12345/thermography/2024-12/20241225_abc123.jpg
        parts = file_path.split('/')
        if len(parts) >= 4 and parts[0] == 'exams':
            patient_id = parts[1]
            exam_type = parts[2]
            filename = parts[-1]
            thumb_name = f"thumb_{filename}".rsplit('.', 1)[0] + '.jpg'
            thumb_path = os.path.join(THUMBNAIL_DIR, "exams", patient_id, exam_type, thumb_name)
            if os.path.exists(thumb_path):
                os.remove(thumb_path)

        return json_response({
            "success": True,
            "message": "파일이 삭제되었습니다"
        })

    except Exception as e:
        return json_response({"error": f"파일 삭제 실패: {str(e)}"}, 500)


@file_bp.route('/api/files/list/<path:dir_path>', methods=['GET', 'OPTIONS'])
def list_files(dir_path):
    """디렉토리 내 파일 목록 조회"""
    if request.method == 'OPTIONS':
        return cors_preflight_response()

    full_path = os.path.join(BASE_UPLOAD_DIR, dir_path)

    # 경로 탐색 공격 방지
    full_path = os.path.normpath(full_path)
    if not full_path.startswith(os.path.normpath(BASE_UPLOAD_DIR)):
        return json_response({"error": "잘못된 경로입니다"}, 400)

    if not os.path.exists(full_path):
        return json_response({"files": [], "directories": []})

    if not os.path.isdir(full_path):
        return json_response({"error": "디렉토리가 아닙니다"}, 400)

    try:
        files = []
        directories = []

        for item in os.listdir(full_path):
            item_path = os.path.join(full_path, item)
            if os.path.isfile(item_path):
                stat = os.stat(item_path)
                files.append({
                    "name": item,
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
            elif os.path.isdir(item_path):
                directories.append(item)

        return json_response({
            "path": dir_path,
            "files": files,
            "directories": directories
        })

    except Exception as e:
        return json_response({"error": f"목록 조회 실패: {str(e)}"}, 500)
