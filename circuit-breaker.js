'use strict';

const { logger } = require('./logger');

class CircuitBreaker {
  constructor(options = {}) {
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeoutMs = options.resetTimeoutMs || 30_000;
    this.halfOpenMax = options.halfOpenMax || 1;
    this.agents = new Map();
  }

  _getState(name) {
    if (!this.agents.has(name)) {
      this.agents.set(name, {
        state: 'CLOSED', failures: 0, lastFailure: 0, halfOpenAttempts: 0,
      });
    }
    return this.agents.get(name);
  }

  canRequest(name) {
    const s = this._getState(name);
    if (s.state === 'CLOSED') return true;
    if (s.state === 'OPEN') {
      if (Date.now() - s.lastFailure >= this.resetTimeoutMs) {
        s.state = 'HALF_OPEN';
        s.halfOpenAttempts = 0;
        logger.info({ agent: name }, 'Circuit breaker -> HALF_OPEN');
        return true;
      }
      return false;
    }
    return s.halfOpenAttempts < this.halfOpenMax;
  }

  recordSuccess(name) {
    const s = this._getState(name);
    if (s.state === 'HALF_OPEN') {
      s.state = 'CLOSED';
      s.failures = 0;
      logger.info({ agent: name }, 'Circuit breaker -> CLOSED (recovered)');
    }
    s.failures = 0;
  }

  recordFailure(name) {
    const s = this._getState(name);
    s.failures++;
    s.lastFailure = Date.now();

    if (s.state === 'HALF_OPEN') {
      s.state = 'OPEN';
      logger.warn({ agent: name }, 'Circuit breaker -> OPEN (half-open test failed)');
      return;
    }

    if (s.failures >= this.failureThreshold) {
      s.state = 'OPEN';
      logger.warn({ agent: name, failures: s.failures }, 'Circuit breaker -> OPEN');
    }
  }

  status() {
    const result = {};
    for (const [name, s] of this.agents) {
      result[name] = { state: s.state, failures: s.failures };
    }
    return result;
  }
}

module.exports = { CircuitBreaker };
