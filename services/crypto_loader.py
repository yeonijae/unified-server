"""
암호화된 Python 모듈 로더
- AES-256 암호화된 .enc 파일을 런타임에 복호화하여 실행
"""

import os
import sys
import hashlib
import base64
from pathlib import Path
from datetime import datetime

# pycryptodome 사용
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad, unpad
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    print("[Warning] pycryptodome not installed. Encryption disabled.")


# 암호화 키 생성 (하드웨어 + 앱 고유값 조합)
def _generate_key():
    """암호화 키 생성 - 난독화된 방식으로"""
    # 여러 값을 조합하여 키 생성 (리버싱 시 추적 어렵게)
    parts = [
        "H4n1w0n",           # 앱 고유값 1
        "Un1f13d",           # 앱 고유값 2
        "S3rv3r",            # 앱 고유값 3
        "2024",              # 버전 연도
        "API",               # 모듈 타입
    ]
    combined = "".join(parts) + "".join(reversed(parts))
    return hashlib.sha256(combined.encode()).digest()  # 32 bytes = AES-256


# 전역 키 (런타임에 생성)
_ENCRYPTION_KEY = None

def _get_key():
    global _ENCRYPTION_KEY
    if _ENCRYPTION_KEY is None:
        _ENCRYPTION_KEY = _generate_key()
    return _ENCRYPTION_KEY


def encrypt_file(source_path: str, output_path: str = None, version: str = "1.0.0") -> str:
    """Python 파일을 암호화하여 .enc 파일로 저장

    Args:
        source_path: 원본 .py 파일 경로
        output_path: 출력 .enc 파일 경로 (없으면 자동 생성)
        version: 모듈 버전

    Returns:
        출력 파일 경로
    """
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("pycryptodome required: pip install pycryptodome")

    source_path = Path(source_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    if output_path is None:
        output_path = source_path.with_suffix('.enc')
    else:
        output_path = Path(output_path)

    # 원본 코드 읽기
    with open(source_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    # 헤더 정보 추가 (복호화 시 버전 확인용)
    header = f"#!HANIWON_ENC_V1|{version}|{datetime.now().isoformat()}\n"
    full_content = header + source_code

    # AES-256 CBC 암호화
    key = _get_key()
    iv = os.urandom(16)  # 랜덤 IV
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encrypted = cipher.encrypt(pad(full_content.encode('utf-8'), AES.block_size))

    # IV + 암호문 저장 (Base64)
    with open(output_path, 'wb') as f:
        f.write(iv + encrypted)

    print(f"[Encrypted] {source_path.name} -> {output_path.name} (v{version})")
    return str(output_path)


def decrypt_and_load(enc_path: str, module_name: str = None):
    """암호화된 .enc 파일을 복호화하여 모듈로 로드

    Args:
        enc_path: .enc 파일 경로
        module_name: 모듈 이름 (없으면 파일명 사용)

    Returns:
        로드된 모듈, 버전 정보
    """
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("pycryptodome required")

    enc_path = Path(enc_path)
    if not enc_path.exists():
        raise FileNotFoundError(f"Encrypted file not found: {enc_path}")

    # 암호문 읽기
    with open(enc_path, 'rb') as f:
        data = f.read()

    # IV + 암호문 분리
    iv = data[:16]
    encrypted = data[16:]

    # AES-256 CBC 복호화
    key = _get_key()
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = unpad(cipher.decrypt(encrypted), AES.block_size)
    source_code = decrypted.decode('utf-8')

    # 헤더 파싱
    lines = source_code.split('\n', 1)
    header = lines[0]
    code = lines[1] if len(lines) > 1 else ""

    version = "unknown"
    if header.startswith("#!HANIWON_ENC_V1|"):
        parts = header.split('|')
        if len(parts) >= 2:
            version = parts[1]

    # 모듈로 로드
    if module_name is None:
        module_name = enc_path.stem

    import types
    module = types.ModuleType(module_name)
    module.__file__ = str(enc_path)
    module.__version__ = version

    # 코드 실행
    exec(compile(code, str(enc_path), 'exec'), module.__dict__)

    return module, version


def get_module_version(enc_path: str) -> str:
    """암호화된 파일의 버전만 확인 (전체 복호화 없이)"""
    if not CRYPTO_AVAILABLE:
        return "unknown"

    try:
        enc_path = Path(enc_path)
        with open(enc_path, 'rb') as f:
            data = f.read()

        iv = data[:16]
        encrypted = data[16:]

        key = _get_key()
        cipher = AES.new(key, AES.MODE_CBC, iv)
        # 첫 블록만 복호화 (헤더 확인용)
        first_block = cipher.decrypt(encrypted[:64])

        # 헤더에서 버전 추출
        header_end = first_block.find(b'\n')
        if header_end > 0:
            header = first_block[:header_end].decode('utf-8', errors='ignore')
            if header.startswith("#!HANIWON_ENC_V1|"):
                parts = header.split('|')
                if len(parts) >= 2:
                    return parts[1]
    except:
        pass

    return "unknown"


# 테스트용
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Encrypt/Decrypt Python modules")
    parser.add_argument("action", choices=["encrypt", "decrypt", "version"])
    parser.add_argument("file", help="File path")
    parser.add_argument("-v", "--version", default="1.0.0", help="Version string")
    parser.add_argument("-o", "--output", help="Output path")

    args = parser.parse_args()

    if args.action == "encrypt":
        encrypt_file(args.file, args.output, args.version)
    elif args.action == "decrypt":
        module, ver = decrypt_and_load(args.file)
        print(f"Loaded module: {module.__name__} v{ver}")
    elif args.action == "version":
        ver = get_module_version(args.file)
        print(f"Version: {ver}")
