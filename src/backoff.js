'use strict';

/**
 * Exponential backoff delay, in seconds: delay = base ^ attempts
 * `attempts` is the attempt count *after* the failure that just happened
 * (i.e. the number of times the job has now been tried), so the first
 * retry (attempts=1) waits base^1 seconds, the second (attempts=2) waits
 * base^2, and so on.
 */
function computeDelaySeconds(base, attempts) {
  return Math.pow(base, attempts);
}

function computeNextRunAt(base, attempts, from = new Date()) {
  const delaySeconds = computeDelaySeconds(base, attempts);
  return new Date(from.getTime() + delaySeconds * 1000);
}

module.exports = { computeDelaySeconds, computeNextRunAt };
