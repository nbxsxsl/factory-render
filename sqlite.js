'use strict';
/* SQLite 适配层：为 Hugging Face Space 容器提供与 mysql2 相同的调用接口
 * 支持 server.js 用到的：q(sql,args)、pool.getConnection() 事务与 execute/query */
const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');

const DB_PATH = process.env.SQLITE_PATH || path.join(__dirname, 'factory.sqlite');
const db = new DatabaseSync(DB_PATH);

db.exec(`
CREATE TABLE IF NOT EXISTS docs(kind TEXT NOT NULL, doc_id TEXT NOT NULL, json TEXT NOT NULL, updated_at INTEGER, PRIMARY KEY(kind,doc_id));
CREATE TABLE IF NOT EXISTS users(phone TEXT PRIMARY KEY, name TEXT, pwd TEXT, role TEXT, created_at INTEGER);
CREATE TABLE IF NOT EXISTS invites(code TEXT PRIMARY KEY, created_by TEXT, role TEXT, created_at INTEGER, expires_at INTEGER, used_by TEXT, used_at INTEGER);
CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, phone TEXT, expires_at INTEGER);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
`);

/* MySQL 方言 → SQLite */
function translate(sql) {
  return String(sql)
    .replace(/ON DUPLICATE KEY UPDATE v=VALUES\(v\)/i, 'ON CONFLICT(k) DO UPDATE SET v=excluded.v')
    .replace(/ON DUPLICATE KEY UPDATE name=VALUES\(name\), role=VALUES\(role\)/i, 'ON CONFLICT(phone) DO UPDATE SET name=excluded.name, role=excluded.role')
    .replace(/ON DUPLICATE KEY UPDATE created_by=VALUES\(created_by\), role=VALUES\(role\)/i, 'ON CONFLICT(code) DO UPDATE SET created_by=excluded.created_by, role=excluded.role');
}
const isWrite = sql => /^\s*(INSERT|UPDATE|DELETE|REPLACE)/i.test(String(sql));

module.exports = {
  q: async (sql, args) => db.prepare(translate(sql)).all(...(args || [])),
  pool: {
    async getConnection() {
      let inTx = false;
      return {
        async query(sql, args) { return [db.prepare(translate(sql)).all(...(args || []))]; },
        async execute(sql, args) {
          const s = translate(sql);
          if (isWrite(s)) { const r = db.prepare(s).run(...(args || [])); return [{ affectedRows: r.changes, insertId: Number(r.lastInsertRowid) || 0 }]; }
          return [db.prepare(s).all(...(args || []))];
        },
        async beginTransaction() { inTx = true; db.exec('BEGIN'); },
        async commit() { if (inTx) db.exec('COMMIT'); inTx = false; },
        async rollback() { if (inTx) { try { db.exec('ROLLBACK'); } catch (e) {} } inTx = false; },
        release() {}
      };
    }
  },
  db
};