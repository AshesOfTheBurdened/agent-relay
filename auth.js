'use strict';

const crypto = require('crypto');
const config = require('./config');

const ROLES = {
  admin: ['chat', 'mcp.call', 'mcp.execute', 'admin', 'presence', 'stream'],
  agent: ['chat', 'mcp.call', 'presence', 'stream'],
  executor: ['chat', 'mcp.execute', 'presence', 'stream'],
  observer: ['chat', 'presence'],
};

let jwt;
try {
  jwt = require('jsonwebtoken');
} catch {
  jwt = null;
}

function generateToken(name, role = 'agent', expiresIn = config.jwtExpiry) {
  const permissions = ROLES[role] || ROLES.agent;
  if (jwt) {
    return jwt.sign(
      { sub: name, role, permissions },
      config.jwtSecret,
      { expiresIn, jwtid: crypto.randomUUID() }
    );
  }
  const header = { alg: 'HS256', typ: 'JWT' };
  const payload = { sub: name, role, permissions, exp: Math.floor(Date.now() / 1000) + 86400, jti: crypto.randomUUID() };
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const h = b64(header), p = b64(payload);
  const sig = crypto.createHmac('sha256', config.jwtSecret).update(`${h}.${p}`).digest('base64url');
  return `${h}.${p}.${sig}`;
}

function authenticate(token) {
  if (!token) return null;

  if (jwt) {
    try {
      const claims = jwt.verify(token, config.jwtSecret);
      return {
        name: claims.sub,
        role: claims.role || 'agent',
        permissions: claims.permissions || ROLES.agent,
      };
    } catch {}
  } else {
    try {
      const parts = token.split('.');
      if (parts.length === 3) {
        const h = JSON.parse(Buffer.from(parts[0], 'base64url').toString());
        const p = JSON.parse(Buffer.from(parts[1], 'base64url').toString());
        if (h.alg === 'HS256') {
          const sig = crypto.createHmac('sha256', config.jwtSecret).update(`${parts[0]}.${parts[1]}`).digest('base64url');
          if (timingSafeEqual(parts[2], sig) && p.exp > Math.floor(Date.now() / 1000)) {
            return { name: p.sub, role: p.role || 'agent', permissions: p.permissions || ROLES.agent };
          }
        }
      }
    } catch {}
  }

  if (config.legacyToken && timingSafeEqual(token, config.legacyToken)) {
    return { name: null, role: 'agent', permissions: ROLES.agent, legacy: true };
  }

  return null;
}

function authorize(agent, action) {
  if (!agent || !agent.permissions) return false;
  return agent.permissions.includes(action);
}

function timingSafeEqual(a, b) {
  const bufA = Buffer.from(String(a));
  const bufB = Buffer.from(String(b));
  if (bufA.length !== bufB.length) {
    crypto.timingSafeEqual(bufA, bufA);
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

module.exports = { generateToken, authenticate, authorize, ROLES };
