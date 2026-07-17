'use strict';

const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { DATA_DIR } = require('./db');

const PIDS_FILE = path.join(DATA_DIR, 'worker-pids.json');
const WORKER_SCRIPT = path.join(__dirname, 'worker.js');

function readPidsFile() {
  if (!fs.existsSync(PIDS_FILE)) return [];
  try {
    return JSON.parse(fs.readFileSync(PIDS_FILE, 'utf8'));
  } catch (_) {
    return [];
  }
}

function writePidsFile(entries) {
  fs.writeFileSync(PIDS_FILE, JSON.stringify(entries, null, 2));
}

function isProcessAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (_) {
    return false;
  }
}

/** Start `count` detached worker processes. Returns the list of spawned entries. */
function startWorkers(count) {
  const existing = readPidsFile().filter((e) => isProcessAlive(e.pid));
  const spawned = [];

  const logDir = path.join(DATA_DIR, 'logs');
  if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

  for (let i = 0; i < count; i++) {
    const workerId = `worker_${Date.now()}_${i}_${Math.random().toString(36).slice(2, 7)}`;
    const logFile = path.join(logDir, `${workerId}.log`);
    const out = fs.openSync(logFile, 'a');
    const err = fs.openSync(logFile, 'a');

    const child = spawn(process.execPath, [WORKER_SCRIPT], {
      detached: true,
      stdio: ['ignore', out, err],
      cwd: process.cwd(),
      env: { ...process.env, QUEUECTL_WORKER_ID: workerId }
    });
    child.unref();
    spawned.push({ workerId, pid: child.pid, startedAt: new Date().toISOString(), logFile });
  }

  writePidsFile([...existing, ...spawned]);
  return spawned;
}

/** Send SIGTERM to all tracked, alive worker processes for graceful shutdown. */
function stopWorkers() {
  const entries = readPidsFile();
  const stopped = [];
  const stillAlive = [];

  for (const entry of entries) {
    if (isProcessAlive(entry.pid)) {
      try {
        process.kill(entry.pid, 'SIGTERM');
        stopped.push(entry);
      } catch (err) {
        stillAlive.push(entry);
      }
    }
  }

  // Clear the pid file; workers mark themselves 'stopped' in the DB once
  // they finish their current job and exit on their own.
  writePidsFile([]);
  return stopped;
}

function listTrackedWorkers() {
  return readPidsFile().map((e) => ({ ...e, alive: isProcessAlive(e.pid) }));
}

module.exports = { startWorkers, stopWorkers, listTrackedWorkers };
