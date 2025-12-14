// Node.js로 mssql_routes.py 암호화
// Python과 동일한 AES-256-CBC 사용

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// 암호화 키 생성 (Python과 동일)
function generateKey() {
    const parts = [
        "H4n1w0n",
        "Un1f13d",
        "S3rv3r",
        "2024",
        "API",
    ];
    const combined = parts.join('') + parts.reverse().join('');
    return crypto.createHash('sha256').update(combined).digest();
}

// 암호화
function encryptFile(inputPath, outputPath, version) {
    const key = generateKey();
    const iv = crypto.randomBytes(16);

    // 파일 읽기
    const sourceCode = fs.readFileSync(inputPath, 'utf-8');

    // 헤더 추가
    const header = `#!HANIWON_ENC_V1|${version}|${new Date().toISOString()}\n`;
    const fullContent = header + sourceCode;

    // AES-256-CBC 암호화
    const cipher = crypto.createCipheriv('aes-256-cbc', key, iv);
    let encrypted = cipher.update(fullContent, 'utf-8');
    encrypted = Buffer.concat([encrypted, cipher.final()]);

    // IV + 암호문 저장
    const output = Buffer.concat([iv, encrypted]);
    fs.writeFileSync(outputPath, output);

    console.log(`[Encrypted] ${path.basename(inputPath)} -> ${path.basename(outputPath)} (v${version})`);
    console.log(`  Size: ${output.length} bytes`);
}

// 실행
const version = process.argv[2] || '1.4.1';
const inputPath = path.join(__dirname, 'routes', 'mssql_routes.py');
const outputPath = path.join(__dirname, 'mssql_routes.enc');

encryptFile(inputPath, outputPath, version);
console.log('\nSuccess!');
