"""
SSL 인증서 유틸리티
- 자체 서명 인증서 자동 생성
- HTTPS 서버 지원
"""

import os
import sys
import ssl
import ipaddress
from pathlib import Path
from datetime import datetime, timedelta

# 인증서 저장 경로 (exe 실행 시 exe 위치 기준)
if getattr(sys, 'frozen', False):
    # PyInstaller exe
    APP_DIR = Path(sys.executable).parent
else:
    # 개발 환경
    APP_DIR = Path(__file__).parent.parent

CERT_DIR = APP_DIR / "certs"
CERT_FILE = CERT_DIR / "server.crt"
KEY_FILE = CERT_DIR / "server.key"


def ensure_cert_exists():
    """인증서가 없으면 자동 생성"""
    CERT_DIR.mkdir(exist_ok=True)

    if CERT_FILE.exists() and KEY_FILE.exists():
        # 인증서 만료 확인 (1년 이상 된 경우 재생성)
        cert_age = datetime.now() - datetime.fromtimestamp(CERT_FILE.stat().st_mtime)
        if cert_age < timedelta(days=365):
            return str(CERT_FILE), str(KEY_FILE)

    # 자체 서명 인증서 생성
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization

        # 개인키 생성
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # 인증서 생성
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "KR"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Seoul"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Seoul"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Haniwon"),
            x509.NameAttribute(NameOID.COMMON_NAME, "haniwon-server"),
        ])

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.utcnow())
            .not_valid_after(datetime.utcnow() + timedelta(days=365 * 3))
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName("localhost"),
                    x509.DNSName("*.local"),
                    x509.DNSName("haniwon-server"),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                    x509.IPAddress(ipaddress.IPv4Address("192.168.0.173")),
                    x509.IPAddress(ipaddress.IPv4Address("192.168.0.61")),
                ]),
                critical=False,
            )
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=0),
                critical=True,
            )
            .sign(key, hashes.SHA256(), default_backend())
        )

        # 파일 저장
        with open(KEY_FILE, "wb") as f:
            f.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))

        with open(CERT_FILE, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

        print(f"[SSL] 인증서 생성 완료: {CERT_FILE}")
        return str(CERT_FILE), str(KEY_FILE)

    except ImportError:
        # cryptography 라이브러리가 없으면 OpenSSL 명령어 사용
        return _generate_with_openssl()


def _generate_with_openssl():
    """OpenSSL 명령어로 인증서 생성 (fallback)"""
    import subprocess

    try:
        # OpenSSL로 자체 서명 인증서 생성
        cmd = [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(KEY_FILE),
            "-out", str(CERT_FILE),
            "-days", "1095",  # 3년
            "-nodes",  # 암호 없음
            "-subj", "/C=KR/ST=Seoul/L=Seoul/O=Haniwon/CN=haniwon-server",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:192.168.0.173"
        ]

        subprocess.run(cmd, check=True, capture_output=True)
        print(f"[SSL] OpenSSL로 인증서 생성 완료: {CERT_FILE}")
        return str(CERT_FILE), str(KEY_FILE)

    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"[SSL] 인증서 생성 실패: {e}")
        return None, None


def get_ssl_context():
    """SSL Context 반환 (iOS Safari 호환)"""
    cert_file, key_file = ensure_cert_exists()

    if not cert_file or not key_file:
        return None

    try:
        # TLS 1.2 이상 사용 (iOS Safari 호환성)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2

        # iOS Safari 호환 설정
        context.options |= ssl.OP_NO_SSLv2
        context.options |= ssl.OP_NO_SSLv3
        context.options |= ssl.OP_NO_RENEGOTIATION  # 재협상 비활성화

        context.load_cert_chain(cert_file, key_file)
        print(f"[SSL] SSL Context 생성 완료 (TLS 1.2+, 재협상 비활성화)")
        return context
    except Exception as e:
        print(f"[SSL] SSL Context 생성 실패: {e}")
        return None
