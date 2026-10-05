'use strict';
/* GitHub 数据持久化：SQLite 数据文件定期备份到私有 GitHub 仓库，容器重启不丢数据 */
const fs = require('fs');
const path = require('path');

const DB_PATH = process.env.SQLITE_PATH || path.join(__dirname, 'factory.sqlite');
const REPO = process.env.GITHUB_DB_REPO || '';    // 如 nbxsxsl/factory-db
const TOKEN = process.env.GITHUB_TOKEN || '';
const FILE = 'factory-db.sqlite';
const BRANCH = process.env.GITHUB_DB_BRANCH || 'main';

function configured() { return !!(REPO && TOKEN); }

/* 启动时同步恢复：本地无数据文件则从 GitHub 拉取（阻塞直到完成或失败，防止空库覆盖旧数据） */
function restoreFromGH() {
  if (!configured()) return false;
  if (fs.existsSync(DB_PATH) && fs.statSync(DB_PATH).size > 0) return false;
  const url = `https://api.github.com/repos/${REPO}/contents/${FILE}?ref=${BRANCH}`;
  const script = `const fs=require('fs');fetch(${JSON.stringify(url)},{headers:{Authorization:'Bearer ${TOKEN}','User-Agent':'factory-app',Accept:'application/vnd.github+json'}}).then(async r=>{if(!r.ok)process.exit(3);const j=await r.json();if(j.encoding!=='base64')process.exit(5);fs.writeFileSync(${JSON.stringify(DB_PATH)},Buffer.from(j.content,'base64'));}).catch(()=>process.exit(4));`;
  try {
    require('child_process').execSync(`node -e ${JSON.stringify(script)}`, { timeout: 60000, stdio: 'ignore' });
    return fs.existsSync(DB_PATH) && fs.statSync(DB_PATH).size > 0;
  } catch (e) { return false; }
}

let _sha = null;
async function backupNow() {
  if (!configured() || !fs.existsSync(DB_PATH)) return false;
  const content = fs.readFileSync(DB_PATH).toString('base64');
  const body = { message: 'factory-db auto backup ' + new Date().toISOString(), content, branch: BRANCH };
  if (_sha) body.sha = _sha;
  else {
    try {
      const gr = await fetch(`https://api.github.com/repos/${REPO}/contents/${FILE}?ref=${BRANCH}`, { headers: { Authorization: `Bearer ${TOKEN}`, 'User-Agent': 'factory-app' } });
      if (gr.ok) body.sha = (await gr.json()).sha;
    } catch (e) { /* ignore */ }
  }
  const r = await fetch(`https://api.github.com/repos/${REPO}/contents/${FILE}`, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${TOKEN}`, 'User-Agent': 'factory-app', 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error('GitHub backup failed: ' + r.status);
  _sha = (await r.json()).content.sha;
  return true;
}

let timer = null, backing = false;
function scheduleBackup(ms) {
  if (!configured()) return;
  if (timer) clearTimeout(timer);
  timer = setTimeout(() => {
    if (backing) return;
    backing = true;
    backupNow().catch(e => console.error('gh-backup:', e.message)).finally(() => { backing = false; });
  }, ms || 10000);
}

module.exports = { configured, restoreFromGH, restoreFromHF: restoreFromGH, scheduleBackup, backupNow };