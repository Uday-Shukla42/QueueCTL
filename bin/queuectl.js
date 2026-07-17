#!/usr/bin/env node
'use strict';

const { Command } = require('commander');
const jobs = require('../src/jobs');
const config = require('../src/config');
const workerManager = require('../src/workerManager');

const program = new Command();

program
  .name('queuectl')
  .description('CLI-based background job queue system with retries, exponential backoff, and a Dead Letter Queue')
  .version('1.0.0');

// ---------------------------------------------------------------------------
// enqueue
// ---------------------------------------------------------------------------
program
  .command('enqueue <jobJson>')
  .description('Add a new job to the queue. jobJson: \'{"id":"job1","command":"sleep 2"}\'')
  .option('--max-retries <n>', 'override the default max retries for this job', parseInt)
  .option('--priority <n>', 'higher runs first (default 0)', parseInt)
  .option('--timeout-ms <n>', 'kill the command if it runs longer than this', parseInt)
  .option('--run-at <isoDate>', 'schedule the job to become eligible at a future time')
  .action((jobJson, opts) => {
    let spec;
    try {
      spec = JSON.parse(jobJson);
    } catch (err) {
      console.error(`Error: invalid JSON for job spec: ${err.message}`);
      process.exitCode = 1;
      return;
    }
    if (Number.isInteger(opts.maxRetries)) spec.max_retries = opts.maxRetries;
    if (Number.isInteger(opts.priority)) spec.priority = opts.priority;
    if (Number.isInteger(opts.timeoutMs)) spec.timeout_ms = opts.timeoutMs;
    if (opts.runAt) spec.run_at = opts.runAt;

    try {
      const job = jobs.enqueue(spec);
      console.log(`Enqueued job "${job.id}" (state: ${job.state})`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exitCode = 1;
    }
  });

// ---------------------------------------------------------------------------
// worker start / stop
// ---------------------------------------------------------------------------
const worker = program.command('worker').description('Manage worker processes');

worker
  .command('start')
  .description('Start one or more worker processes')
  .option('--count <n>', 'number of workers to start', (v) => parseInt(v, 10), 1)
  .action((opts) => {
    const spawned = workerManager.startWorkers(opts.count);
    console.log(`Started ${spawned.length} worker(s):`);
    for (const w of spawned) {
      console.log(`  - ${w.workerId} (pid ${w.pid}) -- log: ${w.logFile}`);
    }
  });

worker
  .command('stop')
  .description('Stop running workers gracefully (finishes current job before exit)')
  .action(() => {
    const stopped = workerManager.stopWorkers();
    if (stopped.length === 0) {
      console.log('No running workers found.');
      return;
    }
    console.log(`Sent shutdown signal to ${stopped.length} worker(s):`);
    for (const w of stopped) {
      console.log(`  - ${w.workerId} (pid ${w.pid})`);
    }
  });

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------
program
  .command('status')
  .description('Show summary of all job states & active workers')
  .action(() => {
    const { counts, workers } = jobs.statusSummary();
    console.log('Job states:');
    for (const [state, count] of Object.entries(counts)) {
      console.log(`  ${state.padEnd(10)} ${count}`);
    }
    console.log('');
    console.log(`Active workers: ${workers.length}`);
    for (const w of workers) {
      console.log(
        `  - ${w.worker_id} (pid ${w.pid}) ${w.current_job ? `processing ${w.current_job}` : 'idle'}`
      );
    }
  });

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------
program
  .command('list')
  .description('List jobs, optionally filtered by state')
  .option('--state <state>', 'pending | processing | completed | failed | dead')
  .action((opts) => {
    let rows;
    try {
      rows = jobs.listJobs({ state: opts.state });
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exitCode = 1;
      return;
    }
    if (rows.length === 0) {
      console.log('No jobs found.');
      return;
    }
    printJobTable(rows);
  });

// ---------------------------------------------------------------------------
// dlq
// ---------------------------------------------------------------------------
const dlq = program.command('dlq').description('View or retry Dead Letter Queue jobs');

dlq
  .command('list')
  .description('List all jobs in the Dead Letter Queue')
  .action(() => {
    const rows = jobs.dlqList();
    if (rows.length === 0) {
      console.log('DLQ is empty.');
      return;
    }
    printJobTable(rows, true);
  });

dlq
  .command('retry <jobId>')
  .description('Requeue a dead-lettered job (resets attempts)')
  .action((jobId) => {
    try {
      const job = jobs.dlqRetry(jobId);
      console.log(`Job "${job.id}" requeued (state: ${job.state})`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exitCode = 1;
    }
  });

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------
const cfg = program.command('config').description('Manage configuration (retry, backoff, etc.)');

cfg
  .command('set <key> <value>')
  .description('Set a config value, e.g. queuectl config set max-retries 3')
  .action((key, value) => {
    try {
      config.set(key, value);
      console.log(`Config "${key}" set to ${value}`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exitCode = 1;
    }
  });

cfg
  .command('get [key]')
  .description('Get a config value, or all values if no key given')
  .action((key) => {
    if (key) {
      const value = config.get(key);
      if (value === undefined) {
        console.log(`(not set) ${key}`);
      } else {
        console.log(`${key} = ${value}`);
      }
    } else {
      const all = config.getAll();
      for (const [k, v] of Object.entries(all)) {
        console.log(`${k.replace(/_/g, '-')} = ${v}`);
      }
    }
  });

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
function printJobTable(rows, includeError = false) {
  const cols = ['id', 'state', 'attempts/max', 'command', 'updated_at'];
  const widths = { id: 14, state: 11, attempts: 13, command: 30, updated_at: 24 };

  const header = [
    'ID'.padEnd(widths.id),
    'STATE'.padEnd(widths.state),
    'ATTEMPTS'.padEnd(widths.attempts),
    'COMMAND'.padEnd(widths.command),
    'UPDATED_AT'.padEnd(widths.updated_at)
  ].join(' ');
  console.log(header);
  console.log('-'.repeat(header.length));

  for (const r of rows) {
    console.log(
      [
        String(r.id).slice(0, widths.id).padEnd(widths.id),
        String(r.state).padEnd(widths.state),
        `${r.attempts}/${r.max_retries}`.padEnd(widths.attempts),
        String(r.command).slice(0, widths.command).padEnd(widths.command),
        String(r.updated_at).padEnd(widths.updated_at)
      ].join(' ')
    );
    if (includeError && r.last_error) {
      console.log(`    last_error: ${r.last_error}`);
    }
  }
}

program.parseAsync(process.argv);
