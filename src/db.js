'use strict';

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const DATA_DIR = path.join(process.cwd(), '.queuectl');
const DB_PATH = path.join(DATA_DIR, 'queuectl.db');

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const db = new Database(DB_PATH);

// WAL mode lets multiple worker processes read/write the same file
// concurrently without corrupting it. busy_timeout makes a writer that
// finds the DB locked retry for up to 5s instead of throwing immediately,
// which is what keeps concurrent "claim a job" transactions safe.
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    command      TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL DEFAULT 3,
    priority     INTEGER NOT NULL DEFAULT 0,
    timeout_ms   INTEGER,
    run_at       TEXT NOT NULL,
    next_run_at  TEXT NOT NULL,
    locked_by    TEXT,
    locked_at    TEXT,
    last_error   TEXT,
    output_log   TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
  CREATE INDEX IF NOT EXISTS idx_jobs_next_run_at ON jobs(next_run_at);

  CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS workers (
    worker_id   TEXT PRIMARY KEY,
    pid         INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    started_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    current_job TEXT
  );
`);

const defaults = {
  max_retries: '3',
  backoff_base: '2',
  poll_interval_ms: '500'
};
const insertDefault = db.prepare(
  'INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)'
);
for (const [k, v] of Object.entries(defaults)) {
  insertDefault.run(k, v);
}

module.exports = { db, DATA_DIR, DB_PATH };
