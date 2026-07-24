'use strict';

const config = require('./config');

let pino;
try {
  pino = require('pino');
} catch {
  pino = null;
}

const logger = pino ? pino({
  level: config.env === 'production' ? 'info' : 'debug',
  transport: config.env !== 'production'
    ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:HH:MM:ss' } }
    : undefined,
  base: { service: 'agent-relay' },
  timestamp: pino.stdTimeFunctions.isoTime,
}) : {
  _write(level, args) {
    const ts = new Date().toISOString();
    const prefix = args.length > 1 && typeof args[0] === 'object' ? JSON.stringify(args.shift()) + ' ' : '';
    console[level](`[${ts}] [${level.toUpperCase()}] ${prefix}${args.join(' ')}`);
  },
  debug: (...args) => logger._write('debug', args),
  info: (...args) => logger._write('info', args),
  warn: (...args) => logger._write('warn', args),
  error: (...args) => logger._write('error', args),
  child: () => logger,
};

function child(bindings) {
  if (pino) return logger.child(bindings);
  return logger;
}

module.exports = { logger, child };
