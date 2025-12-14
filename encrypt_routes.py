"""
MSSQL Routes 암호화 도구
- mssql_routes.py를 암호화하여 mssql_routes.enc 생성
- 버전 번호 포함

사용법:
    python encrypt_routes.py 1.4.1
    python encrypt_routes.py 1.4.1 --output dist/mssql_routes.enc
"""

import sys
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Encrypt mssql_routes.py")
    parser.add_argument("version", help="Version number (e.g., 1.4.1)")
    parser.add_argument("-i", "--input", default="routes/mssql_routes.py", help="Input file path")
    parser.add_argument("-o", "--output", default="mssql_routes.enc", help="Output file path")
    args = parser.parse_args()

    # 경로 확인
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    # pycryptodome 확인
    try:
        from services.crypto_loader import encrypt_file
    except ImportError:
        print("Error: pycryptodome not installed")
        print("Run: pip install pycryptodome")
        sys.exit(1)

    # 암호화
    try:
        output_path = encrypt_file(
            source_path=str(input_path),
            output_path=args.output,
            version=args.version
        )
        print(f"\nSuccess!")
        print(f"  Input:   {input_path}")
        print(f"  Output:  {output_path}")
        print(f"  Version: {args.version}")
        print(f"\nDeploy {Path(output_path).name} to server alongside exe")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
