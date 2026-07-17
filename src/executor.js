'use strict';

const { exec } = require('child_process');

/**
 * Execute a job's shell command.
 * Resolves with { success, output, error } -- never rejects, so the
 * caller can always update job state based on the result.
 */
function runCommand(command, timeoutMs) {
  return new Promise((resolve) => {
    const opts = { shell: true, maxBuffer: 10 * 1024 * 1024 };
    if (timeoutMs && timeoutMs > 0) opts.timeout = timeoutMs;

    const child = exec(command, opts, (error, stdout, stderr) => {
      const output = [stdout, stderr].filter(Boolean).join('\n').trim();
      if (error) {
        const reason = error.killed
          ? `Timed out after ${timeoutMs}ms`
          : error.code
          ? `Command exited with code ${error.code}: ${error.message}`
          : error.message;
        resolve({ success: false, output, error: reason });
      } else {
        resolve({ success: true, output, error: null });
      }
    });

    // Commands that don't exist (ENOENT) surface here rather than in the
    // exec callback's `error.code`, so handle it defensively too.
    child.on('error', (err) => {
      resolve({ success: false, output: '', error: err.message });
    });
  });
}

module.exports = { runCommand };
