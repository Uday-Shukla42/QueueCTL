'use strict';

const { db } = require('./db');

const ALLOWED_KEYS = new Set(['max-retries', 'backoff-base', 'poll-interval-ms']);

// CLI uses kebab-case (max-retries) but we store snake_case internally.
function toInternalKey(key) {
  return key.replace(/-/g, '_');
}

function get(key) {
  const internal = toInternalKey(key);
  const row = db.prepare('SELECT value FROM config WHERE key = ?').get(internal);
  return row ? row.value : undefined;
}

function getAll() {
  const rows = db.prepare('SELECT key, value FROM config').all();
  const out = {};
  for (const row of rows) out[row.key] = row.value;
  return out;
}

function set(key, value) {
  if (!ALLOWED_KEYS.has(key)) {
    throw new Error(
      `Unknown config key "${key}". Allowed keys: ${[...ALLOWED_KEYS].join(', ')}`
    );
  }
  const internal = toInternalKey(key);
  if (!/^\d+$/.test(String(value))) {
    throw new Error(`Config value for "${key}" must be a non-negative integer`);
  }
  db.prepare(
    `INSERT INTO config (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`
  ).run(internal, String(value));
  return value;
}

module.exports = { get, getAll, set, ALLOWED_KEYS };
