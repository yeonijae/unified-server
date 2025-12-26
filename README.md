# Haniwon Unified Server

한의원 통합 서버 - Static, MSSQL, SQLite 3개 서버를 하나의 GUI로 관리

## 주요 기능

### 3개 서버 통합 관리
- **Static Server** (포트 11111): 정적 파일 서빙, Git Pull + Bun Build 지원
- **MSSQL Server** (포트 3100): MSSQL 데이터베이스 API
- **SQLite Server** (포트 3200): SQLite 데이터베이스 API + 파일 업로드

### 특징
- Windows 시스템 트레이 상주
- Windows 시작 시 자동 실행
- 서버별 Auto Start 설정
- MSSQL 라우트 핫 리로드 (재시작 없이 API 업데이트)
- GitHub Webhook 지원 (자동 빌드/업데이트)
- 암호화된 설정 파일 (config.enc)
- 암호화된 API 라우트 (mssql_routes.enc)

## 설치 및 실행

### 요구사항
- Windows 10/11
- Python 3.11+ (개발 시)

### 실행
```
Haniwon-Unified-Server-v2.8.0.exe
```

### 개발 환경
```bash
pip install -r requirements.txt
python gui.py
```

## 프로젝트 구조

```
unified-server/
├── gui.py                  # 메인 GUI 애플리케이션
├── config.py               # 설정 관리
├── config.json             # 설정 파일
├── mssql_routes.enc        # 암호화된 MSSQL API 라우트
├── encrypt_routes.py       # 라우트 암호화 스크립트
│
├── routes/
│   ├── static_routes.py    # Static 서버 라우트
│   ├── mssql_routes.py     # MSSQL API 라우트 (원본)
│   ├── sqlite_routes.py    # SQLite API 라우트
│   └── file_routes.py      # 파일 업로드/다운로드 API
│
└── services/
    ├── mssql_db.py         # MSSQL Connection Pool
    ├── mssql_loader.py     # MSSQL 라우트 로더 (암호화 지원)
    ├── sqlite_db.py        # SQLite 관리
    ├── git_build.py        # Git/Bun 빌드 관리
    ├── crypto_loader.py    # 암호화 모듈 로더
    ├── secure_config.py    # 암호화 설정 관리
    └── server_manager.py   # Health Monitor
```

## API 엔드포인트

### MSSQL API (포트 3100)
- `GET /api/patients/search?q=검색어` - 환자 검색
- `GET /api/today/stats` - 오늘 진료 통계
- `GET /api/reservations` - 예약 조회
- `POST /api/reservations` - 예약 생성
- `POST /api/execute` - SQL 쿼리 실행
- 기타 통계/추이 API 다수

### SQLite API (포트 3200)
- `POST /api/execute` - SQL 쿼리 실행
- `GET /api/tables` - 테이블 목록
- `POST /api/files/upload` - 파일 업로드
- `GET /api/files/<path>` - 파일 다운로드
- `DELETE /api/files/<path>` - 파일 삭제

### Static Server (포트 11111)
- `GET /` - 정적 파일 서빙
- `GET /console` - 웹 콘솔
- `POST /webhook` - GitHub Webhook

## 라우트 암호화

MSSQL API 라우트는 암호화하여 배포:

```bash
python encrypt_routes.py 2.8.0
```

결과: `mssql_routes.enc` 파일 생성

## GitHub 연동

### Webhook 설정
1. GitHub Repository > Settings > Webhooks
2. Payload URL: `http://서버IP:3100/api/self-update`
3. Content type: `application/json`
4. Secret: config에서 설정한 값

### 자동 업데이트
Push 시 자동으로:
1. `mssql_routes.enc` 다운로드
2. 핫 리로드 적용

## 버전 관리

- **APP_VERSION** (config.py): EXE 버전
- **MODULE_VERSION** (mssql_routes.py): API 버전

두 버전은 독립적으로 관리됨

## 빌드

```bash
python -m PyInstaller --onefile --noconsole --name "Haniwon-Unified-Server-v2.8.0" --add-data "routes;routes" --add-data "services;services" gui.py --clean
```

결과: `dist/Haniwon-Unified-Server-v2.8.0.exe`

## 라이선스

Private - Haniwon Internal Use Only
