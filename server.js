/* 加工厂报工系统后端 v2 —— Node + MySQL + WebSocket
 * 替换旧 server.py：数据集中存 MySQL，登录/注册走数据库（含限流、token、邀请码），
 * WebSocket 实时广播数据更新，OCR 由常驻 Python 子进程（PaddleOCR）完成。
 */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn } = require('child_process');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8000;
const USE_SQLITE = process.env.DB_MODE === 'sqlite';
const ROOT = USE_SQLITE ? path.join(__dirname, 'public') : path.resolve(__dirname, '..');   // 静态目录

/* 数据库：本地用 MySQL；云端容器用 SQLite（免安装）+ HF 仓库备份持久化 */
let pool, qImpl;
if (USE_SQLITE) {
  const hf = require('./gh-backup');
  const restored = hf.restoreFromHF();
  const s = require('./sqlite');
  pool = s.pool; qImpl = s.q;
  global.hfScheduleBackup = () => hf.scheduleBackup();
  console.log('DB: sqlite', restored ? '(从 HF 仓库恢复)' : '(新建)');
} else {
  const mysql = require('mysql2/promise');
  const DB_CONF = { host: '127.0.0.1', user: 'root', password: '', database: 'factory', charset: 'utf8mb4' };
  pool = mysql.createPool({ ...DB_CONF, connectionLimit: 10 });
  global.hfScheduleBackup = () => {};
}

const SESSION_TTL = 7 * 86400 * 1000;                 // token 有效期 7 天
const FAIL_LIMIT = 5, FAIL_WINDOW = 10 * 60 * 1000, FAIL_LOCK = 5 * 60 * 1000;
const INVITE_TTL = 7 * 86400 * 1000;                  // 邀请码 7 天有效

const q = USE_SQLITE ? qImpl : async (sql, args) => (await pool.execute(sql, args))[0];

/* ---------------- 工具 ---------------- */
const now = () => Date.now();
const uuid = () => crypto.randomUUID();
const sha = s => crypto.createHash('sha256').update(String(s)).digest('hex').slice(0, 32);
function json(res, code, obj) {
  const body = Buffer.from(JSON.stringify(obj), 'utf8');
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-Filename, Authorization',
    'Content-Length': body.length
  });
  res.end(body);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', c => { size += c.length; if (size > 30 * 1024 * 1024) { reject(new Error('too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

/* ---------------- 登录限流（内存） ---------------- */
const FAILS = {};
function failState(phone) {
  const f = (FAILS[phone] || []).filter(t => now() - t < FAIL_WINDOW);
  FAILS[phone] = f;
  if (f.length >= FAIL_LIMIT) {
    const last = f[f.length - 1];
    const remain = Math.ceil((FAIL_LOCK - (now() - last)) / 1000);
    if (remain > 0) return { locked: true, remain };
  }
  return { locked: false, remain: 0 };
}
const recordFail = p => { (FAILS[p] = FAILS[p] || []).push(now()); };
const clearFails = p => { delete FAILS[p]; };

/* ---------------- WebSocket 推送 ---------------- */
const wss = new WebSocketServer({ noServer: true });
function broadcast(msg, exclude) {
  const data = JSON.stringify(msg);
  for (const ws of wss.clients) {
    if (ws !== exclude && ws.readyState === 1) ws.send(data);
  }
}

/* ---------------- 数据库初始化与迁移 ---------------- */
async function initDb() {
  if (USE_SQLITE) return;   // SQLite 建表已在 sqlite.js 完成
  await q(`CREATE TABLE IF NOT EXISTS users(
    phone VARCHAR(20) PRIMARY KEY, name VARCHAR(50) NOT NULL,
    pwd VARCHAR(64) NOT NULL, role VARCHAR(20) DEFAULT 'worker',
    created_at BIGINT NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  await q(`CREATE TABLE IF NOT EXISTS sessions(
    token VARCHAR(64) PRIMARY KEY, phone VARCHAR(20) NOT NULL,
    expires_at BIGINT NOT NULL, INDEX(phone)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  await q(`CREATE TABLE IF NOT EXISTS invites(
    code VARCHAR(32) PRIMARY KEY, created_by VARCHAR(20), role VARCHAR(20) DEFAULT 'worker', created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL, used_by VARCHAR(20) DEFAULT NULL, used_at BIGINT DEFAULT NULL)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  try { await q('ALTER TABLE invites ADD COLUMN role VARCHAR(20) DEFAULT \'worker\''); } catch (e) {}
  await q(`CREATE TABLE IF NOT EXISTS docs(
    kind VARCHAR(30) NOT NULL, doc_id VARCHAR(64) NOT NULL,
    json LONGTEXT NOT NULL, updated_at BIGINT NOT NULL,
    PRIMARY KEY(kind, doc_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  await q(`CREATE TABLE IF NOT EXISTS kv(
    k VARCHAR(64) PRIMARY KEY, v LONGTEXT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // 首次启动：从 server-data.json 迁移
  const [r] = await q('SELECT COUNT(*) c FROM docs');
  if (r.c === 0) {
    const f = path.join(ROOT, 'server-data.json');
    if (fs.existsSync(f)) {
      try {
        const db = JSON.parse(fs.readFileSync(f, 'utf8'));
        await saveDb(db, null, true);
        console.log('已从 server-data.json 迁移数据到 MySQL');
      } catch (e) { console.error('迁移失败:', e.message); }
    }
  }
}

const DOC_KINDS = ['projects', 'orders', 'claims', 'works', 'notices', 'records', 'editRequests'];

async function saveDb(db, excludeWs, quiet) {
  const ts = now();
  // 服务器时间纠偏：拒绝未来时间戳
  let updatedAt = Number(db.updatedAt) || ts;
  if (updatedAt > ts + 60000) updatedAt = ts;
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const kind of DOC_KINDS) {
      const arr = Array.isArray(db[kind]) ? db[kind] : [];
      await conn.query('DELETE FROM docs WHERE kind=?', [kind]);
      for (const it of arr) {
        const id = String((it && (it.id ?? it.code ?? it.phone)) ?? uuid()).slice(0, 64);
        await conn.execute('INSERT INTO docs(kind,doc_id,json,updated_at) VALUES(?,?,?,?)',
          [kind, id, JSON.stringify(it), ts]);
      }
    }
    if (Array.isArray(db.users)) {
      for (const u of db.users) {
        if (!u || !u.phone) continue;
        /* 内部工具：明文存密码，保证 /api/db 下发后各设备离线登录可用；同步不会抹掉密码 */
        await conn.execute(`INSERT INTO users(phone,name,pwd,role,created_at) VALUES(?,?,?,?,?)
          ON DUPLICATE KEY UPDATE name=VALUES(name), role=VALUES(role)`,
          [String(u.phone), String(u.name || ''), String(u.pwd || ''), String(u.role || 'worker'), ts]);
      }
      /* 删除同步：各设备已删除的员工从 users 表真正移除（否则会被 loadDb 复活）。
         安全性：旧缓存设备的提交已在此前的 409 STALE 守卫拦截，能走到这里的都是持有最新数据的提交 */
      if (db.users.length) {
        const phones = new Set(db.users.filter(u => u && u.phone).map(u => String(u.phone)));
        const [exist] = await conn.query('SELECT phone FROM users');
        for (const r of exist) {
          const ph = String(r.phone);
          if (!phones.has(ph)) {
            await conn.execute('DELETE FROM users WHERE phone=?', [ph]);
            await conn.execute('DELETE FROM sessions WHERE phone=?', [ph]);
          }
        }
      }
    }
    if (Array.isArray(db.invites)) {
      for (const i of db.invites) {
        if (!i || !i.code) continue;
        await conn.execute(`INSERT INTO invites(code,created_by,role,created_at,expires_at) VALUES(?,?,?,?,?)
          ON DUPLICATE KEY UPDATE created_by=VALUES(created_by), role=VALUES(role)`,
          [String(i.code).toUpperCase(), String(i.createdBy || ''), String(i.role || 'worker'), ts, ts + INVITE_TTL]);
      }
      /* 删除同步：已删除的邀请码从 invites 表真正移除 */
      if (db.invites.length) {
        const codes = new Set(db.invites.filter(i => i && i.code).map(i => String(i.code).toUpperCase()));
        const [exi] = await conn.query('SELECT code FROM invites');
        for (const r of exi) if (!codes.has(String(r.code))) await conn.execute('DELETE FROM invites WHERE code=?', [r.code]);
      }
    }
    await conn.execute(`INSERT INTO kv(k,v) VALUES('settings',?) ON DUPLICATE KEY UPDATE v=VALUES(v)`,
      [JSON.stringify(db.settings || {})]);
    await conn.execute(`INSERT INTO kv(k,v) VALUES('updatedAt',?) ON DUPLICATE KEY UPDATE v=VALUES(v)`,
      [String(updatedAt)]);
    await conn.commit();
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
  if (!quiet) broadcast({ type: 'db_updated', updatedAt }, excludeWs);
  if (global.hfScheduleBackup) global.hfScheduleBackup();
  return updatedAt;
}

async function loadDb() {
  const db = { users: [], invites: [], projects: [], orders: [], claims: [], works: [], notices: [], records: [], editRequests: [], settings: {} };
  const rows = await q('SELECT kind,doc_id,json FROM docs');
  const byKind = {};
  for (const r of rows) {
    (byKind[r.kind] = byKind[r.kind] || []).push(JSON.parse(r.json));
  }
  for (const k of DOC_KINDS) db[k] = byKind[k] || [];
  db.users = (await q('SELECT phone,name,pwd,role,created_at createdAt FROM users ORDER BY created_at'))
    .map(u => ({ id: 'u_'+u.phone, phone: u.phone, name: u.name, pwd: u.pwd, role: u.role, status: 'active', createdAt: u.createdAt }));
  db.invites = (await q('SELECT code,created_by createdBy,role,created_at createdAt,expires_at expiresAt,used_by usedBy,used_at usedAt FROM invites'))
    .map(i => ({ code: i.code, createdBy: i.createdBy, role: i.role || 'worker', createdAt: i.createdAt, expiresAt: i.expiresAt,
      /* 兼容前端旧字段：used/expireAt */
      used: !!i.usedBy, usedBy: i.usedBy, usedAt: i.usedAt, expireAt: i.expiresAt }));
  const [st] = await q(`SELECT v FROM kv WHERE k='settings'`);
  if (st) { try { db.settings = JSON.parse(st.v); } catch (e) {} }
  const [ts] = await q(`SELECT v FROM kv WHERE k='updatedAt'`);
  db.updatedAt = ts ? Number(ts.v) || 0 : 0;   // 空库为 0，避免新库把合法提交误判 STALE
  return db;
}

/* ---------------- 登录/注册/会话 ---------------- */
async function newSession(phone) {
  await q('DELETE FROM sessions WHERE expires_at<?', [now()]);
  const token = crypto.randomBytes(32).toString('hex');
  await q('INSERT INTO sessions(token,phone,expires_at) VALUES(?,?,?)', [token, phone, now() + SESSION_TTL]);
  return token;
}
async function checkToken(token) {
  if (!token) return null;
  const rows = await q('SELECT token,phone,expires_at FROM sessions WHERE token=?', [token]);
  const s = rows[0];
  if (!s || s.expires_at < now()) {
    if (s) await q('DELETE FROM sessions WHERE token=?', [token]);
    return null;
  }
  await q('UPDATE sessions SET expires_at=? WHERE token=?', [now() + SESSION_TTL, token]);
  const u = await q('SELECT phone,name,role FROM users WHERE phone=?', [s.phone]);
  return u[0] || null;
}

async function handleLogin(req, res, body) {
  let d; try { d = JSON.parse(body.toString('utf8')); } catch (e) { return json(res, 400, { ok: false, code: 'BAD_REQUEST', message: '请求格式错误' }); }
  const phone = String(d.phone || '').trim(), pwd = String(d.pwd || '');
  if (!/^1\d{10}$/.test(phone)) return json(res, 400, { ok: false, code: 'BAD_PHONE', message: '手机号格式不正确，应为11位数字' });
  const lock = failState(phone);
  if (lock.locked) return json(res, 429, { ok: false, code: 'RATE_LIMITED', message: `尝试次数过多，请 ${Math.ceil(lock.remain / 60)} 分钟后再试` });
  const rows = await q('SELECT phone,name,pwd,role FROM users WHERE phone=?', [phone]);
  const u = rows[0];
  if (!u) { recordFail(phone); return json(res, 401, { ok: false, code: 'USER_NOT_FOUND', message: '账号不存在，请先注册或联系管理员开通' }); }
  if (u.pwd !== pwd) {
    recordFail(phone);
    const left = Math.max(0, FAIL_LIMIT - (FAILS[phone] || []).length);
    return json(res, 401, { ok: false, code: 'WRONG_PWD', message: left > 2 ? '密码错误，请重新输入' : `密码错误，还可尝试 ${left} 次后将锁定5分钟` });
  }
  clearFails(phone);
  const token = await newSession(phone);
  const db = await loadDb();
  return json(res, 200, { ok: true, token, user: { phone: u.phone, name: u.name, role: u.role }, db });
}

async function handleCheck(req, res) {
  const token = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
  const u = await checkToken(token);
  if (!u) return json(res, 401, { ok: false, code: 'TOKEN_EXPIRED', message: '登录已过期，请重新登录' });
  const db = await loadDb();
  return json(res, 200, { ok: true, user: u, db });
}

async function handleLogout(req, res) {
  const token = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
  if (token) await q('DELETE FROM sessions WHERE token=?', [token]);
  return json(res, 200, { ok: true });
}

async function handleRegister(req, res, body) {
  let d; try { d = JSON.parse(body.toString('utf8')); } catch (e) { return json(res, 400, { ok: false, code: 'BAD_REQUEST', message: '请求格式错误' }); }
  const phone = String(d.phone || '').trim(), pwd = String(d.pwd || ''), name = String(d.name || '').trim();
  const code = String(d.invite || d.code || '').trim().toUpperCase();   /* 兼容前端 code/invite 两种字段名 */
  if (!code) return json(res, 400, { ok: false, code: 'INVITE_REQUIRED', message: '请输入邀请码' });
  if (!/^1\d{10}$/.test(phone)) return json(res, 400, { ok: false, code: 'BAD_PHONE', message: '手机号格式不正确，应为11位数字' });
  if (!name) return json(res, 400, { ok: false, code: 'NAME_REQUIRED', message: '请输入姓名' });
  if (pwd.length < 6) return json(res, 400, { ok: false, code: 'WEAK_PWD', message: '密码至少6位' });
  const lock = failState(phone);
  if (lock.locked) return json(res, 429, { ok: false, code: 'RATE_LIMITED', message: `尝试次数过多，请 ${Math.ceil(lock.remain / 60)} 分钟后再试` });
  const inv = (await q('SELECT * FROM invites WHERE code=?', [code]))[0];
  if (!inv) return json(res, 400, { ok: false, code: 'INVITE_INVALID', message: '邀请码不存在，请核对后重试' });
  if (inv.used_by) return json(res, 400, { ok: false, code: 'INVITE_USED', message: '邀请码已被使用，请向管理员重新获取' });
  if (inv.expires_at < now()) return json(res, 400, { ok: false, code: 'INVITE_EXPIRED', message: '邀请码已过期，请向管理员重新获取' });
  const dup = await q('SELECT phone FROM users WHERE phone=?', [phone]);
  if (dup.length) return json(res, 409, { ok: false, code: 'PHONE_EXISTS', message: '该手机号已注册，请直接登录' });
  const role = inv.role === 'admin' ? 'admin' : 'worker';
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const [upd] = await conn.execute('UPDATE invites SET used_by=?, used_at=? WHERE code=? AND used_by IS NULL', [phone, now(), code]);
    if (upd.affectedRows !== 1) throw Object.assign(new Error('invite race'), { code: 'INVITE_USED' });
    await conn.execute('INSERT INTO users(phone,name,pwd,role,created_at) VALUES(?,?,?,?,?)', [phone, name, pwd, role, now()]);
    await conn.commit();
  } catch (e) {
    await conn.rollback();
    return json(res, 409, { ok: false, code: e.code || 'PHONE_EXISTS', message: e.code === 'INVITE_USED' ? '邀请码已被使用' : '该手机号已注册，请直接登录' });
  } finally {
    conn.release();
  }
  clearFails(phone);
  if (global.hfScheduleBackup) global.hfScheduleBackup();
  /* 注册后立即推进 updatedAt：其它设备的旧缓存提交会被 409 拦下，防止误删新注册用户 */
  await q(`INSERT INTO kv(k,v) VALUES('updatedAt',?) ON DUPLICATE KEY UPDATE v=VALUES(v)`, [String(now())]);
  const token = await newSession(phone);
  const db = await loadDb();
  return json(res, 200, { ok: true, token, user: { phone, name, role }, db });
}

/* ---------------- /api/db ---------------- */
async function handleDbGet(res) {
  const db = await loadDb();
  return json(res, 200, db);
}
async function handleDbPost(req, res, body) {
  let data;
  try { data = JSON.parse(body.toString('utf8')); } catch (e) { return json(res, 400, { ok: false, code: 'BAD_REQUEST', message: '数据格式错误' }); }
  if (!data || typeof data !== 'object' || !('users' in data) || !('orders' in data)) {
    return json(res, 400, { ok: false, code: 'BAD_REQUEST', message: '数据不完整' });
  }
  // 防回灌守卫：拒绝旧演示数据覆盖真实数据
  const cur = await loadDb();
  const curPhones = new Set(cur.users.map(u => String(u.phone)));
  const incPhones = new Set(data.users.map(u => String(u.phone)));
  if (incPhones.has('13800000001') && ['15067402472', '13351903281'].some(p => curPhones.has(p) && !incPhones.has(p)) && curPhones.has('15067402472')) {
    return json(res, 409, { ok: false, code: 'DEMO_OVERWRITE_BLOCKED', message: '拒绝覆盖：提交数据疑似旧演示数据' });
  }
  const token = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
  const user = await checkToken(token);
  // 过期写入守卫：提交的数据比服务器当前数据旧 → 409，客户端会自动拉取最新再重试，防止“最后写入者覆盖”丢人
  const incUpd = Number(data.updatedAt) || 0;
  const curUpd = Number(cur.updatedAt) || 0;
  if (curUpd > 0 && incUpd > 0 && incUpd < curUpd - 2000) {
    return json(res, 409, { ok: false, code: 'STALE', updatedAt: curUpd, message: '本地数据过期，已自动拉取最新' });
  }
  const updatedAt = await saveDb(data, req.ws, false);
  // 备份到本地文件
  try {
    const bkDir = path.join(ROOT, 'backups');
    fs.mkdirSync(bkDir, { recursive: true });
    fs.writeFileSync(path.join(bkDir, `backup-${new Date().toISOString().slice(0, 16).replace(/[:T]/g, '')}.json`), body);
    fs.writeFileSync(path.join(ROOT, 'server-data.json'), JSON.stringify(data, null, 1));
    scheduleExcelExport();
  } catch (e) {}
  return json(res, 200, { ok: true, updatedAt, serverUser: user ? user.phone : null });
}

/* ---------------- Excel 自动导出：每次数据变化后落盘到 excel/ 目录，可直接用 Excel 打开 ---------------- */
const EXCEL_DIR = path.join(ROOT, 'excel');
let __xlTimer = null;
function scheduleExcelExport() {
  if (__xlTimer) clearTimeout(__xlTimer);
  __xlTimer = setTimeout(() => { exportExcel().catch(e => console.error('excel export:', e.message)); }, 2000);
}
function esc(v) { return String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
function xlsFile(headers, rows) {
  const th = headers.map(h => `<th style="border:1px solid #999;background:#d9e2f3;padding:4px">${esc(h)}</th>`).join('');
  const tr = rows.map(r => '<tr>' + r.map(c => `<td style="border:1px solid #bbb;padding:3px">${esc(c)}</td>`).join('') + '</tr>').join('');
  return '\ufeff<html xmlns:x="urn:schemas-microsoft-com:office:excel"><head><meta charset="utf-8"></head><body><table style="border-collapse:collapse;font-family:\u5fae\u8f6f\u96c5\u9ed1">' + `<tr>${th}</tr>` + tr + '</table></body></html>';
}
async function exportExcel() {
  const db = await loadDb();
  fs.mkdirSync(EXCEL_DIR, { recursive: true });
  const nameOf = id => { const u = (db.users || []).find(x => x.id === id) || {}; return u.name || id || ''; };
  const orderOf = id => (db.orders || []).find(o => o.id === id) || {};
  // 员工表
  let rows = (db.users || []).map(u => [u.name, u.phone, u.workNo || '', u.role === 'superadmin' ? '超级管理员' : (u.role === 'admin' ? '管理员' : '员工'), u.status || 'active']);
  fs.writeFileSync(path.join(EXCEL_DIR, '员工表.xls'), xlsFile(['姓名', '手机号', '工号', '角色', '状态'], rows));
  // 订单表
  rows = (db.orders || []).map(o => [o.workNo, o.productName || '', o.qty || 0, o.hasSwitch === true ? '有开关' : (o.hasSwitch === false ? '无开关(含插绝缘片)' : '未判定'), (o.steps || []).map(s => `${s.name}¥${s.price || 0}`).join(' '), o.allocated ? '已分配' : '未分配']);
  fs.writeFileSync(path.join(EXCEL_DIR, '订单表.xls'), xlsFile(['工单号', '产品', '数量', '开关判定', '工序单价', '分配状态'], rows));
  // 申领记录
  rows = (db.claims || []).map(c => { const o = orderOf(c.orderId); return [o.workNo || '', nameOf(c.workerId), (c.items || []).map(it => { const s = (o.steps || []).find(x => x.id === it.stepId) || {}; return `${s.name}×${it.qty}`; }).join('、'), c.status || '', c.time || '']; });
  fs.writeFileSync(path.join(EXCEL_DIR, '申领记录.xls'), xlsFile(['工单号', '员工', '申领明细', '状态', '时间'], rows));
  // 报工明细（工资依据）
  rows = (db.works || []).map(w => { const o = orderOf(w.orderId); const s = (o.steps || []).find(x => x.id === w.stepId) || {}; const d = w.ts ? new Date(w.ts) : null; return [o.workNo || '', nameOf(w.workerId), s.name || w.stepId, w.qty || 0, w.price || 0, w.amount || 0, d ? d.toLocaleString('zh-CN') : '']; });
  fs.writeFileSync(path.join(EXCEL_DIR, '报工明细.xls'), xlsFile(['工单号', '员工', '项目', '数量', '单价', '金额', '时间'], rows));
}

/* ---------------- OCR ---------------- */
const PHOTO_DIR = path.join(ROOT, 'photos');
fs.mkdirSync(PHOTO_DIR, { recursive: true });
let ocrProc = null, ocrBuf = '', ocrWaiter = null;
function getOcrProc() {
  if (ocrProc && !ocrProc.killed) return ocrProc;
  ocrProc = spawn('python', [path.join(__dirname, 'ocr_worker.py')], { cwd: __dirname, windowsHide: true });
  ocrProc.stdout.setEncoding('utf8');
  ocrProc.stdout.on('data', c => {
    ocrBuf += c;
    let i;
    while ((i = ocrBuf.indexOf('\n')) >= 0) {
      const line = ocrBuf.slice(0, i).trim();
      ocrBuf = ocrBuf.slice(i + 1);
      if (line && ocrWaiter) { ocrWaiter(line); ocrWaiter = null; }
    }
  });
  ocrProc.on('exit', () => { ocrProc = null; });
  return ocrProc;
}
function runOcr(imgPath, timeoutMs = 120000) {
  const p = getOcrProc();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { ocrWaiter = null; reject(new Error('OCR timeout')); }, timeoutMs);
    ocrWaiter = line => { clearTimeout(timer); try { resolve(JSON.parse(line)); } catch (e) { reject(e); } };
    p.stdin.write(imgPath + '\n');
  });
}
async function handleOcr(req, res, body) {
  if (!body || !body.length) return json(res, 400, { ok: false, code: 'BAD_REQUEST', message: '未收到图片' });
  const name = `ocr-${Date.now()}-${Math.floor(Math.random() * 1e4)}.jpg`;
  const p = path.join(PHOTO_DIR, name);
  fs.writeFileSync(p, body);
  try {
    const r = await runOcr(p);
    return json(res, 200, { ok: r.ok, fields: r.fields || {}, lines: r.lines || [], code: r.ok ? 'OK' : 'OCR_FAIL' });
  } catch (e) {
    return json(res, 500, { ok: false, code: 'OCR_FAIL', message: '识别失败：' + e.message });
  }
}

/* ---------------- 静态文件 ---------------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.ico': 'image/x-icon', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml', '.webmanifest': 'application/manifest+json; charset=utf-8' };
function serveStatic(req, res, pathname) {
  let p = pathname === '/' ? '/' + encodeURIComponent('预览版.html') : pathname;
  const file = path.join(ROOT, decodeURIComponent(p));
  if (!file.startsWith(ROOT)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not Found'); }
    let body = data;
    /* 注入局域网直连地址：同一 WiFi 时可一键切换更快入口 */
    if (file.endsWith('.html')) {
      let lan = '';
      try {
        const os = require('os');
        const nis = os.networkInterfaces();
        const cands = [];
        for (const k of Object.keys(nis)) {
          for (const n of nis[k] || []) {
            if (n.family !== 'IPv4' || n.internal) continue;
            const ip = n.address;
            let rank = 9;
            if (/^192\.168\./.test(ip)) rank = 1;
            else if (/^10\./.test(ip)) rank = 2;
            else if (/^172\.(1[6-9]|2\d|3[01])\./.test(ip)) rank = 3;
            else if (/^100\./.test(ip)) rank = 8; /* Tailscale 虚拟网卡，最后考虑 */
            cands.push({ ip, rank });
          }
        }
        cands.sort((a, b) => a.rank - b.rank);
        if (cands.length) lan = `http://${cands[0].ip}:${PORT}`;
      } catch (e) {}
      if (lan && !USE_SQLITE) body = Buffer.from(data.toString('utf8').replace(/__LAN_ORIGIN__/g, lan), 'utf8');
    }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream', 'Access-Control-Allow-Origin': '*' });
    res.end(body);
  });
}

/* ---------------- HTTP 路由 ---------------- */
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  const p = u.pathname;
  if (req.method === 'OPTIONS') return json(res, 204, {});
  try {
    if (req.method === 'GET') {
      if (p === '/api/db') return await handleDbGet(res);
      if (p === '/api/check') return await handleCheck(req, res);
      if (p === '/api/stats') {
        const db = await loadDb();
        return json(res, 200, { orders: db.orders.length, users: db.users.length, lastSync: db.updatedAt });
      }
      return serveStatic(req, res, p);
    }
    if (req.method === 'POST') {
      const body = await readBody(req);
      if (p === '/api/login') return await handleLogin(req, res, body);
      if (p === '/api/check') return await handleCheck(req, res);
      if (p === '/api/logout') return await handleLogout(req, res);
      if (p === '/api/register') return await handleRegister(req, res, body);
      if (p === '/api/db') return await handleDbPost(req, res, body);
      if (p === '/api/ocr') { if (USE_SQLITE) return json(res, 501, { ok: false, message: '拍照识别仅本地服务器支持，请使用「直接填写」手动录入' }); return await handleOcr(req, res, body); }
      return json(res, 404, { ok: false, code: 'NOT_FOUND', message: '接口不存在' });
    }
    json(res, 405, { ok: false, code: 'METHOD', message: '不支持的请求方法' });
  } catch (e) {
    console.error('API error:', p, e);
    if (!res.headersSent) json(res, 500, { ok: false, code: 'SERVER_ERROR', message: '服务器异常，请稍后重试' });
  }
});
server.on('upgrade', (req, socket, head) => {
  const { pathname } = new URL(req.url, 'http://x');
  if (pathname === '/ws') wss.handleUpgrade(req, socket, head, ws => {
    ws.isAlive = true;
    ws.on('pong', () => ws.isAlive = true);
    ws.on('message', m => { try { const d = JSON.parse(m); if (d.type === 'ping') ws.send('{"type":"pong"}'); } catch (e) {} });
  });
  else socket.destroy();
});
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30000);

initDb().then(() => {
  server.listen(PORT, '0.0.0.0', () => console.log(`Factory backend v2 listening on :${PORT} (MySQL + WebSocket)`));
}).catch(e => { console.error('初始化失败:', e); process.exit(1); });