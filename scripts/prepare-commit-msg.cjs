const fs = require("fs");
const { execSync } = require("child_process");

const msgFile = process.argv[2];
const source = process.argv[3] || ""; // merge/squash 등
const msg = fs.readFileSync(msgFile, "utf8");

if (!msg || source === "merge" || source === "squash") process.exit(0);

const firstLine = msg.split("\n")[0].trim();
if (!firstLine) process.exit(0);

const branch = execSync("git branch --show-current").toString().trim();
const m = branch.match(/^([a-z]+)\/([A-Z]+-\d+)$/);

if (!m) {
  console.error(`브랜치명 규칙 위반: "${branch}" (필수: <type>/<TICKET> 예: feat/HSC-01)`);
  process.exit(1);
}

const branchType = m[1];
const ticket = m[2];

const mm = firstLine.match(/^([a-z]+)\s*:\s*(.+)$/);
if (!mm) {
  console.error(`커밋 메시지 규칙 위반: "${firstLine}" (필수: <type>: <message>)`);
  process.exit(1);
}

const msgType = mm[1];
const rest = mm[2];

if (msgType !== branchType) {
  console.error(`type 불일치: 브랜치="${branchType}", 커밋="${msgType}"`);
  console.error(`- 브랜치가 ${branchType}/${ticket}이면 커밋은 "${branchType}: ..." 로 작성해야 합니다.`);
  process.exit(1);
}

const expectedPrefix = `${branchType}/${ticket}: `;
if (firstLine.startsWith(expectedPrefix)) process.exit(0);

const lines = msg.split("\n");
lines[0] = `${branchType}/${ticket}: ${rest}`;
fs.writeFileSync(msgFile, lines.join("\n"), "utf8");