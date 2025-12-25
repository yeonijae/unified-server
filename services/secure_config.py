"""
안전한 설정 관리 모듈
- AES-256으로 config 전체를 암호화
- 메모장으로 열어도 내용 보이지 않음
"""

import os
import sys
import json
import hashlib
from pathlib import Path
from datetime import datetime

# pycryptodome
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad, unpad
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False


# 암호화 키 생성 (앱 고유값)
def _generate_key():
    """암호화 키 생성 - 난독화"""
    parts = [
        "H4n1w0n",
        "C0nf1g",
        "S3cur3",
        "K3y2024",
    ]
    combined = "".join(parts) + "".join(reversed(parts))
    return hashlib.sha256(combined.encode()).digest()  # 32 bytes


_ENCRYPTION_KEY = None

def _get_key():
    global _ENCRYPTION_KEY
    if _ENCRYPTION_KEY is None:
        _ENCRYPTION_KEY = _generate_key()
    return _ENCRYPTION_KEY


# 파일 매직 헤더 (암호화 여부 식별)
MAGIC_HEADER = b"HCFG"  # Haniwon ConFiG


def encrypt_config_file(config: dict, output_path: str):
    """설정을 암호화하여 바이너리 파일로 저장

    메모장으로 열면 깨진 문자만 보임
    """
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("pycryptodome required: pip install pycryptodome")

    # JSON → bytes
    json_data = json.dumps(config, ensure_ascii=False).encode('utf-8')

    # AES-256-CBC 암호화
    key = _get_key()
    iv = os.urandom(16)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encrypted = cipher.encrypt(pad(json_data, AES.block_size))

    # 파일 저장: MAGIC + IV + 암호문
    with open(output_path, 'wb') as f:
        f.write(MAGIC_HEADER)
        f.write(iv)
        f.write(encrypted)


def decrypt_config_file(input_path: str) -> dict:
    """암호화된 설정 파일 복호화"""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("pycryptodome required")

    with open(input_path, 'rb') as f:
        data = f.read()

    # 매직 헤더 확인
    if not data.startswith(MAGIC_HEADER):
        raise ValueError("Not an encrypted config file")

    # MAGIC(4) + IV(16) + 암호문
    iv = data[4:20]
    encrypted = data[20:]

    # 복호화
    key = _get_key()
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = unpad(cipher.decrypt(encrypted), AES.block_size)

    return json.loads(decrypted.decode('utf-8'))


def is_encrypted_file(file_path: str) -> bool:
    """파일이 암호화되어 있는지 확인"""
    try:
        with open(file_path, 'rb') as f:
            header = f.read(4)
        return header == MAGIC_HEADER
    except:
        return False


def load_secure_config(config_path: str) -> dict:
    """설정 파일 로드 (암호화/일반 자동 감지)"""
    path = Path(config_path)
    enc_path = path.with_suffix('.enc')

    # .enc 파일 우선
    if enc_path.exists() and is_encrypted_file(str(enc_path)):
        return decrypt_config_file(str(enc_path))

    # .json 파일 (암호화된 경우)
    if path.exists() and is_encrypted_file(str(path)):
        return decrypt_config_file(str(path))

    # .json 파일 (일반)
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    return {}


def save_secure_config(config: dict, config_path: str):
    """설정을 암호화하여 저장 (.enc 파일)"""
    path = Path(config_path)
    enc_path = path.with_suffix('.enc')

    # 암호화된 .enc 파일로 저장
    encrypt_config_file(config, str(enc_path))

    # 기존 .json 파일이 있으면 삭제 (선택적)
    # if path.exists():
    #     path.unlink()


def migrate_to_encrypted(json_path: str):
    """기존 config.json을 config.enc로 마이그레이션"""
    path = Path(json_path)
    if not path.exists():
        print(f"File not found: {path}")
        return False

    # JSON 로드
    with open(path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # 암호화 저장
    enc_path = path.with_suffix('.enc')
    encrypt_config_file(config, str(enc_path))

    print(f"Migrated: {path} -> {enc_path}")
    print(f"You can now delete {path}")
    return True


# CLI 도구
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Secure config tool")
    parser.add_argument("action", choices=["encrypt", "decrypt", "migrate", "check"])
    parser.add_argument("-c", "--config", default="config.json", help="Config file path")
    args = parser.parse_args()

    if not CRYPTO_AVAILABLE:
        print("Error: pycryptodome not installed")
        print("Run: pip install pycryptodome")
        sys.exit(1)

    if args.action == "encrypt" or args.action == "migrate":
        migrate_to_encrypted(args.config)

    elif args.action == "decrypt":
        path = Path(args.config)
        enc_path = path.with_suffix('.enc')

        if enc_path.exists():
            config = decrypt_config_file(str(enc_path))
        elif is_encrypted_file(args.config):
            config = decrypt_config_file(args.config)
        else:
            print("Not an encrypted file")
            sys.exit(1)

        print(json.dumps(config, indent=2, ensure_ascii=False))

    elif args.action == "check":
        path = Path(args.config)
        enc_path = path.with_suffix('.enc')

        if enc_path.exists() and is_encrypted_file(str(enc_path)):
            print(f"Encrypted: {enc_path}")
        elif is_encrypted_file(args.config):
            print(f"Encrypted: {args.config}")
        else:
            print(f"Not encrypted: {args.config}")
