'use strict';

const jobs = require('./jobs');
const config = require('./config');
const { runCommand } = require('./executor');
const { db } = require('./db');

const workerId = process.env.QUEUECTL_WORKER_ID || `worker_${process.pid}`;
let shuttingDown = false;
let currentJobId = null;

function nowIso() {
  return new Date().toISOString();
}

function registerWorker() {
  const now = nowIso();
  db.prepare(
    `INSERT INTO workers (worker_id, pid, status, started_at, updated_at, current_job)
     VALUES (?, ?, 'running', ?, ?, NULL)
     ON CONFLICT(worker_id) DO UPDATE SET
       pid = excluded.pid, status = 'running', updated_at = excluded.updated_at`
  ).run(workerId, process.pid, now, now);
}

function updateWorkerHeartbeat(currentJob) {
  db.prepare(
    `UPDATE workers SET updated_at = ?, current_job = ? WHERE worker_id = ?`
  ).run(nowIso(), currentJob, workerId);
}

function markWorkerStopped() {
  db.prepare(
    `UPDATE workers SET status = 'stopped', updated_at = ?, current_job = NULL
     WHERE worker_id = ?`
  ).run(nowIso(), workerId);
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function processOneJob(job) {
  currentJobId = job.id;
  updateWorkerHeartbeat(job.id);
  log(`picked up job ${job.id} -> ${job.command}`);

  const result = await runCommand(job.command, job.timeout_ms);

  if (result.success) {
    jobs.markCompleted(job.id, result.output);
    log(`job ${job.id} completed`);
  } else {
    const updated = jobs.markFailed(job.id, result.error);
    if (updated.state === 'dead') {
      log(`job ${job.id} exhausted retries -> moved to DLQ (${result.error})`);
    } else {
      log(
        `job ${job.id} failed (attempt ${updated.attempts}/${updated.max_retries}), ` +
          `retry scheduled at ${updated.next_run_at} -- ${result.error}`
      );
    }
  }

  currentJobId = null;
  updateWorkerHeartbeat(null);
}

function log(msg) {
  // Prefixed so `worker start` output is legible when running multiple workers.
  console.log(`[${workerId}] ${msg}`);
}

async function mainLoop() {
  registerWorker();
  log('started');

  process.on('SIGTERM', () => {
    shuttingDown = true;
    log('received SIGTERM, finishing current job then shutting down gracefully...');
  });
  process.on('SIGINT', () => {
    shuttingDown = true;
    log('received SIGINT, finishing current job then shutting down gracefully...');
  });

  while (!shuttingDown) {
    const pollInterval = parseInt(config.get('poll-interval-ms') || '500', 10);
    const job = jobs.claimNextJob(workerId);

    if (job) {
      await processOneJob(job);
      // Loop again immediately to drain the queue without idle-waiting.
      continue;
    }

    await sleep(pollInterval);
  }

  markWorkerStopped();
  log('stopped');
  process.exit(0);
}

mainLoop().catch((err) => {
  console.error(`[${workerId}] fatal error:`, err);
  try {
    markWorkerStopped();
  } catch (_) {
    /* best effort */
  }
  process.exit(1);
});
