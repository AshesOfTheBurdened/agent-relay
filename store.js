'use strict';

const config = require('./config');
const { logger } = require('./logger');

class MessageStore {
  constructor(maxSize = config.messageHistorySize) {
    this.maxSize = maxSize;
    this.messages = [];
    this.seqCounter = 0;
    this.redis = null;
  }

  async connectRedis(redisClient) {
    this.redis = redisClient;
    try {
      const lastSeq = await this.redis.get('relay:lastSeq');
      if (lastSeq) {
        this.seqCounter = parseInt(lastSeq, 10);
        logger.info({ seq: this.seqCounter }, 'Recovered sequence counter from Redis');
      }
    } catch {}
  }

  async push(channel, msg) {
    const seq = ++this.seqCounter;
    const entry = { seq, channel, msg, timestamp: Date.now() };

    this.messages.push(entry);
    if (this.messages.length > this.maxSize) {
      this.messages.shift();
    }

    if (this.redis) {
      try {
        const key = `relay:msg:${seq}`;
        await this.redis.set(key, JSON.stringify(entry), 'EX', 3600);
        await this.redis.set('relay:lastSeq', String(seq));
      } catch {}
    }

    return seq;
  }

  getAfter(lastSeq) {
    return this.messages.filter((e) => e.seq > lastSeq);
  }

  getRecent(channel, limit = 50) {
    return this.messages.filter((e) => e.channel === channel).slice(-limit);
  }

  currentSeq() {
    return this.seqCounter;
  }

  async close() {
    if (this.redis) {
      try { await this.redis.quit(); } catch {}
      this.redis = null;
    }
  }
}

module.exports = { MessageStore };
