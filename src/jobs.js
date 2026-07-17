'use strict';

const crypto = require('crypto');
const { db } = require('./db');
const config = require('./config');
const { computeNextRunAt } = require('./backoff');

const VALID_STATES = ['pending', 'processing', 'completed', 'failed', 'dead'];

function nowIso() {
  return new Date().toISOString();
}

function genId() {
  return `job_${crypto.randomBytes(6).toString('hex')}`;
}

/**
 * Add a new job to the queue.
 * spec: { id?, command, max_retries?, priority?, timeout_ms?, run_at? }
 */
function enqueue(spec) {
  if (!spec || typeof spec.command !== 'string' || !spec.command.trim()) {
    throw new Error('Job must include a non-empty "command" string');
  }

  const id = spec.id && String(spec.id).trim() ? String(spec.id).trim() : genId();

  const existing = db.prepare('SELECT id FROM jobs WHERE id = ?').get(id);
  if (existing) {
    throw new Error(`Job with id "${id}" already exists`);
  }

  const maxRetries = Number.isInteger(spec.max_retries)
    ? spec.max_retries
    : parseInt(config.get('max-retries'), 10);

  const priority = Number.isInteger(spec.priority) ? spec.priority : 0;
  const timeoutMs = Number.isInteger(spec.timeout_ms) ? spec.timeout_ms : null;

  const now = nowIso();
  // run_at supports scheduled/delayed jobs; defaults to "now" so the job
  // is immediately eligible for pickup.
  const runAt = spec.run_at ? new Date(spec.run_at).toISOString() : now;

  db.prepare(
    `INSERT INTO jobs
      (id, command, state, attempts, max_retries, priority, timeout_ms,
       run_at, next_run_at, created_at, updated_at)
     VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)`
  ).run(id, spec.command, maxRetries, priority, timeoutMs, runAt, runAt, now, now);

  return getJob(id);
}

function getJob(id) {
  return db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
}

function listJobs({ state } = {}) {
  if (state) {
    if (!VALID_STATES.includes(state)) {
      throw new Error(`Invalid state "${state}". Valid states: ${VALID_STATES.join(', ')}`);
    }
    return db
      .prepare('SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC')
      .all(state);
  }
  return db.prepare('SELECT * FROM jobs ORDER BY created_at ASC').all();
}

/**
 * Atomically claim the next eligible pending job for a worker.
 * Uses an IMMEDIATE transaction so that when multiple worker processes
 * poll concurrently against the same SQLite file, only one of them can
 * win the row -- this is what prevents duplicate/overlapping processing.
 */
const claimTxn = db.transaction((workerId) => {
  const now = nowIso();
  const row = db
    .prepare(
      `SELECT * FROM jobs
       WHERE state = 'pending' AND next_run_at <= ?
       ORDER BY priority DESC, next_run_at ASC
       LIMIT 1`
    )
    .get(now);

  if (!row) return null;

  const result = db
    .prepare(
      `UPDATE jobs
       SET state = 'processing', locked_by = ?, locked_at = ?, updated_at = ?
       WHERE id = ? AND state = 'pending'`
    )
    .run(workerId, now, now, row.id);

  if (result.changes === 0) return null; // another worker beat us to it
  return getJob(row.id);
});

function claimNextJob(workerId) {
  // .immediate(...) runs the wrapped function in a BEGIN IMMEDIATE
  // transaction, acquiring the write lock right away instead of at first
  // write. That closes the race window between SELECT and UPDATE when
  // multiple worker processes poll the same SQLite file concurrently, so
  // two workers can never claim the same job.
  return claimTxn.immediate(workerId);
}

function markCompleted(id, output) {
  const now = nowIso();
  db.prepare(
    `UPDATE jobs
     SET state = 'completed', output_log = ?, locked_by = NULL, locked_at = NULL,
         last_error = NULL, updated_at = ?
     WHERE id = ?`
  ).run(output ?? null, now, id);
  return getJob(id);
}

/**
 * Record a failed attempt. Moves the job to 'dead' (DLQ) once attempts
 * exhaust max_retries, otherwise reschedules it as 'failed' -> eligible
 * again after an exponential backoff delay.
 */
function markFailed(id, errorMessage) {
  const job = getJob(id);
  if (!job) throw new Error(`Job "${id}" not found`);

  const attempts = job.attempts + 1;
  const now = nowIso();
  const base = parseInt(config.get('backoff-base'), 10);

  if (attempts >= job.max_retries) {
    db.prepare(
      `UPDATE jobs
       SET state = 'dead', attempts = ?, last_error = ?, locked_by = NULL,
           locked_at = NULL, updated_at = ?
       WHERE id = ?`
    ).run(attempts, errorMessage ?? null, now, id);
  } else {
    const nextRunAt = computeNextRunAt(base, attempts).toISOString();
    db.prepare(
      `UPDATE jobs
       SET state = 'pending', attempts = ?, last_error = ?, locked_by = NULL,
           locked_at = NULL, next_run_at = ?, updated_at = ?
       WHERE id = ?`
    ).run(attempts, errorMessage ?? null, nextRunAt, now, id);
  }
  return getJob(id);
}

function dlqList() {
  return db
    .prepare("SELECT * FROM jobs WHERE state = 'dead' ORDER BY updated_at DESC")
    .all();
}

/** Requeue a dead-lettered job: resets attempts and puts it back to pending. */
function dlqRetry(id) {
  const job = getJob(id);
  if (!job) throw new Error(`Job "${id}" not found`);
  if (job.state !== 'dead') {
    throw new Error(`Job "${id}" is not in the DLQ (current state: ${job.state})`);
  }
  const now = nowIso();
  db.prepare(
    `UPDATE jobs
     SET state = 'pending', attempts = 0, last_error = NULL,
         next_run_at = ?, updated_at = ?
     WHERE id = ?`
  ).run(now, now, id);
  return getJob(id);
}

function statusSummary() {
  const counts = {};
  for (const s of VALID_STATES) counts[s] = 0;
  const rows = db
    .prepare('SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state')
    .all();
  for (const r of rows) counts[r.state] = r.cnt;

  const workers = db
    .prepare("SELECT * FROM workers WHERE status = 'running' ORDER BY started_at ASC")
    .all();

  return { counts, workers };
}

module.exports = {
  VALID_STATES,
  enqueue,
  getJob,
  listJobs,
  claimNextJob,
  markCompleted,
  markFailed,
  dlqList,
  dlqRetry,
  statusSummary
};
