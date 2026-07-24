#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const dns = require('dns');
const net = require('net');
const crypto = require('crypto');
const { execSync, execFileSync, spawnSync } = require('child_process');
const config = require('./config');
const { logger, child } = require('./logger');

const log = child({ agent: config.agentName });

let ws = null;
let sessionId = null;
let lastSeq = 0;
let reconnectAttempt = 0;
let reconnectTimer = null;
let heartbeatTimer = null;
let shuttingDown = false;

const MAX_FILE_SIZE = 10 * 1024 * 1024;
const MAX_OUTPUT_SIZE = 100 * 1024;
const COMMAND_TIMEOUT = 30_000;

function safePath(input) {
  const resolved = path.resolve(config.mcpWorkspace, String(input || ''));
  let real;
  try {
    real = fs.realpathSync(resolved);
  } catch {
    const dir = path.dirname(resolved);
    let realDir;
    try { realDir = fs.realpathSync(dir); } catch { realDir = dir; }
    real = path.join(realDir, path.basename(resolved));
  }
  const base = path.resolve(config.mcpWorkspace);
  if (!real.startsWith(base + path.sep) && real !== base) {
    throw new Error('Path escapes workspace');
  }
  return real;
}

const tools = {};

function registerTool(name, fn) {
  tools[name] = fn;
}

registerTool('execute_command', (args = {}) => {
  const command = typeof args.command === 'string' ? args.command : '';
  const cwd = safePath(args.cwd || '.');
  const timeout = Math.min(Math.max(1_000, Number(args.timeout) || COMMAND_TIMEOUT), 60_000);
  if (!command) return { content: [{ type: 'text', text: 'command is required' }], isError: true };
  try {
    const stdout = execSync(command, { cwd, timeout, encoding: 'utf8', maxBuffer: MAX_OUTPUT_SIZE, windowsHide: true });
    return { content: [{ type: 'text', text: stdout || '(no output)' }] };
  } catch (error) {
    const text = (error.stdout || '') + (error.stderr ? '\n' + error.stderr : '') || error.message;
    return { content: [{ type: 'text', text: text.slice(0, MAX_OUTPUT_SIZE) }], isError: true };
  }
});

registerTool('read_file', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    const stats = fs.statSync(p);
    if (stats.size > MAX_FILE_SIZE) {
      return { content: [{ type: 'text', text: `File too large (${stats.size} bytes, max ${MAX_FILE_SIZE})` }], isError: true };
    }
    const lines = fs.readFileSync(p, 'utf8').split('\n');
    const offset = Math.max(0, Number(args.offset) || 0);
    const limit = Math.max(0, Number(args.limit) || 0);
    const end = limit ? Math.min(offset + limit, lines.length) : lines.length;
    return {
      content: [{ type: 'text', text: lines.slice(offset, end).join('\n') }],
      totalLines: lines.length, startLine: offset, path: p,
    };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('write_file', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    const content = typeof args.content === 'string' ? args.content : '';
    fs.mkdirSync(path.dirname(p), { recursive: true });
    fs.writeFileSync(p, content, 'utf8');
    return { content: [{ type: 'text', text: `Written ${Buffer.byteLength(content)} bytes to ${args.path}` }], size: Buffer.byteLength(content), path: args.path };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('search_files', (args = {}) => {
  try {
    const p = safePath(args.path || '.');
    const pattern = typeof args.pattern === 'string' && args.pattern ? args.pattern : '*';
    const output = execFileSync('find', [p, '-type', 'f', '-name', pattern], { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const matches = output.split('\n').filter(Boolean).slice(0, 100);
    return { content: [{ type: 'text', text: matches.join('\n') || '(no matches)' }], count: matches.length, truncated: matches.length === 100 };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('search_content', (args = {}) => {
  try {
    const p = safePath(args.path || '.');
    const pattern = typeof args.pattern === 'string' ? args.pattern : '';
    if (!pattern) return { content: [{ type: 'text', text: 'pattern is required' }], isError: true };
    const output = execFileSync('grep', ['-rIn', '--include=*.js', '--include=*.ts', '--include=*.tsx', '--include=*.jsx', '--include=*.json', '--include=*.html', '--include=*.css', '--include=*.md', '--', pattern, p], { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const matches = output.split('\n').filter(Boolean).slice(0, 200).map(line => {
      const [file, lineNumber, ...content] = line.split(':');
      return { file, line: Number(lineNumber) || 0, content: content.join(':') };
    });
    return {
      content: [{ type: 'text', text: matches.map(m => `${m.file}:${m.line}:${m.content}`).join('\n') || '(no matches)' }],
      count: matches.length, truncated: matches.length === 200,
    };
  } catch (error) {
    if (error.status === 1) return { content: [{ type: 'text', text: '(no matches)' }], count: 0 };
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('get_environment', () => {
  const toolList = ['node', 'npm', 'git', 'python3', 'curl', 'grep', 'find', 'sed', 'awk', 'gcc', 'rustc', 'cargo', 'go', 'docker'];
  const available = {};
  for (const tool of toolList) {
    try { execFileSync(process.platform === 'win32' ? 'where' : 'which', [tool], { stdio: 'ignore', timeout: 2_000, windowsHide: true }); available[tool] = true; }
    catch { available[tool] = false; }
  }
  return {
    content: [{ type: 'text', text: JSON.stringify({
      agent: config.agentName, workspace: config.mcpWorkspace,
      platform: process.platform, arch: process.arch,
      nodeVersion: process.version, pid: process.pid,
      tools: available, uptime: process.uptime(),
    }, null, 2) }],
  };
});

registerTool('system_info', () => {
  try {
    const cpus = os.cpus();
    const totalRam = os.totalmem();
    const freeRam = os.freemem();
    let uptimeStr = '';
    const uptimeSec = os.uptime();
    const days = Math.floor(uptimeSec / 86400);
    const hours = Math.floor((uptimeSec % 86400) / 3600);
    const mins = Math.floor((uptimeSec % 3600) / 60);
    uptimeStr = `${days}d ${hours}h ${mins}m`;
    let loadAvg;
    try { loadAvg = os.loadavg(); } catch { loadAvg = [0, 0, 0]; }
    let diskInfo = '(unavailable)';
    try {
      const df = execSync('df -h /', { encoding: 'utf8', timeout: 5_000, windowsHide: true });
      diskInfo = df.trim();
    } catch {}
    return {
      content: [{ type: 'text', text: JSON.stringify({
        hostname: os.hostname(),
        platform: process.platform,
        arch: process.arch,
        kernel: os.release(),
        uptime: uptimeStr,
        uptimeSeconds: uptimeSec,
        loadAverage: loadAvg,
        totalRam,
        freeRam,
        ramUsedPercent: totalRam > 0 ? Math.round((1 - freeRam / totalRam) * 100) : 0,
        cpus: cpus.length,
        cpuModel: cpus[0]?.model || 'unknown',
        disk: diskInfo,
      }, null, 2) }],
    };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('disk_usage', (args = {}) => {
  try {
    const target = args.path || '/';
    const isWin = process.platform === 'win32';
    let output;
    if (isWin) {
      output = execSync(`wmic logicaldisk get size,freespace,caption`, { encoding: 'utf8', timeout: 10_000, windowsHide: true });
    } else {
      output = execSync(`df -h${target ? ' ' + target : ''}`, { encoding: 'utf8', timeout: 10_000, windowsHide: true });
    }
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('memory_info', () => {
  try {
    const info = {
      totalRam: os.totalmem(),
      freeRam: os.freemem(),
      usedRam: os.totalmem() - os.freemem(),
      ramPercent: os.totalmem() > 0 ? Math.round(((os.totalmem() - os.freemem()) / os.totalmem()) * 100) : 0,
    };
    let swapOutput = '(unavailable)';
    try {
      if (process.platform === 'win32') {
        swapOutput = execSync('wmic os get TotalVirtualMemorySize,FreeVirtualMemory', { encoding: 'utf8', timeout: 5_000, windowsHide: true }).trim();
      } else {
        swapOutput = execSync('free -h 2>/dev/null || cat /proc/meminfo 2>/dev/null', { encoding: 'utf8', timeout: 5_000, windowsHide: true }).trim();
      }
    } catch {}
    return { content: [{ type: 'text', text: JSON.stringify({ ...info, swap: swapOutput }, null, 2) }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('cpu_info', () => {
  try {
    const cpus = os.cpus();
    let temp = '(unavailable)';
    try {
      if (process.platform === 'linux') {
        const raw = execSync('cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null', { encoding: 'utf8', timeout: 3_000, windowsHide: true });
        if (raw.trim()) temp = raw.trim().split('\n').map(t => (parseInt(t) / 1000).toFixed(1) + '°C').join(', ');
      }
    } catch {}
    const load = os.loadavg();
    return {
      content: [{ type: 'text', text: JSON.stringify({
        model: cpus[0]?.model || 'unknown',
        cores: cpus.length,
        architecture: os.arch(),
        loadAverage1m: load[0],
        loadAverage5m: load[1],
        loadAverage15m: load[2],
        temperature: temp,
        speedMHz: cpus[0]?.speed || 0,
      }, null, 2) }],
    };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('user_info', () => {
  try {
    let userInfo;
    try { userInfo = os.userInfo(); } catch { userInfo = { username: 'unknown', homedir: os.homedir() }; }
    let whoOutput = '';
    try { whoOutput = execSync('who 2>/dev/null || echo (unavailable)', { encoding: 'utf8', timeout: 5_000, windowsHide: true }).trim(); } catch {}
    let groupsOutput = '';
    try { groupsOutput = execSync('groups 2>/dev/null || echo (unavailable)', { encoding: 'utf8', timeout: 5_000, windowsHide: true }).trim(); } catch {}
    return {
      content: [{ type: 'text', text: JSON.stringify({
        username: userInfo.username,
        uid: userInfo.uid,
        gid: userInfo.gid,
        homeDir: userInfo.homedir,
        shell: userInfo.shell || '(unknown)',
        groups: groupsOutput,
        loggedIn: whoOutput,
      }, null, 2) }],
    };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('environment_vars', (args = {}) => {
  try {
    const action = args.action || 'list';
    if (action === 'list') {
      const vars = Object.entries(process.env).map(([key, value]) => ({ key, value }));
      return { content: [{ type: 'text', text: JSON.stringify(vars, null, 2) }], count: vars.length };
    }
    if (action === 'get') {
      const key = args.name || args.key;
      if (!key) return { content: [{ type: 'text', text: 'name/key is required for get action' }], isError: true };
      return { content: [{ type: 'text', text: `${key}=${process.env[key] || '(not set)'}` }] };
    }
    if (action === 'set') {
      const key = args.name || args.key;
      const value = String(args.value ?? '');
      if (!key) return { content: [{ type: 'text', text: 'name/key is required for set action' }], isError: true };
      process.env[key] = value;
      return { content: [{ type: 'text', text: `Set ${key}=${value}` }] };
    }
    return { content: [{ type: 'text', text: `Unknown action: ${action}. Use list, get, or set.` }], isError: true };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('process_list', (args = {}) => {
  try {
    const isWin = process.platform === 'win32';
    let output;
    if (isWin) {
      output = execSync('tasklist /FO CSV /NH', { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
      const lines = output.trim().split('\n').slice(0, 200);
      const processes = lines.map(line => {
        const parts = line.replace(/^"|"$/g, '').split('","');
        return { name: parts[0], pid: parseInt(parts[1]), sessionName: parts[2], sessionNum: parts[3], memUsage: parts[4] };
      });
      return { content: [{ type: 'text', text: JSON.stringify(processes, null, 2) }], count: processes.length, truncated: lines.length < output.trim().split('\n').length };
    }
    const sortBy = args.sort || '%cpu';
    output = execSync(`ps aux --sort=-${sortBy} 2>/dev/null || ps aux`, { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const lines = output.trim().split('\n');
    const header = lines[0];
    const processes = lines.slice(1, 201).map(line => {
      const parts = line.trim().split(/\s+/);
      return {
        user: parts[0], pid: parseInt(parts[1]), cpu: parts[2], mem: parts[3],
        vsz: parts[4], rss: parts[5], tty: parts[6], stat: parts[7],
        start: parts[8], time: parts[9], command: parts.slice(10).join(' '),
      };
    });
    return { content: [{ type: 'text', text: header + '\n' + lines.slice(1, 201).join('\n') }], count: processes.length, truncated: lines.length > 201 };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('process_kill', (args = {}) => {
  try {
    const pid = parseInt(args.pid);
    if (isNaN(pid)) return { content: [{ type: 'text', text: 'pid is required (number)' }], isError: true };
    const signal = args.signal || (process.platform === 'win32' ? '9' : 'SIGTERM');
    const isWin = process.platform === 'win32';
    if (isWin) {
      execSync(`taskkill /PID ${pid}${signal === '9' || signal === 'SIGKILL' ? ' /F' : ''}`, { encoding: 'utf8', timeout: 10_000, windowsHide: true });
    } else {
      process.kill(pid, signal);
    }
    return { content: [{ type: 'text', text: `Sent ${signal} to PID ${pid}` }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('process_info', (args = {}) => {
  try {
    const pid = parseInt(args.pid);
    if (isNaN(pid)) return { content: [{ type: 'text', text: 'pid is required (number)' }], isError: true };
    const isWin = process.platform === 'win32';
    let output;
    if (isWin) {
      output = execSync(`tasklist /FI "PID eq ${pid}" /FO CSV`, { encoding: 'utf8', timeout: 10_000, windowsHide: true });
    } else {
      output = execSync(`ps -p ${pid} -o pid,ppid,user,%cpu,%mem,vsz,rss,tty,stat,start,time,args --no-headers 2>/dev/null || ps -p ${pid} -o pid,ppid,user,%cpu,%mem,vsz,rss,tty,stat,start,time,command`, { encoding: 'utf8', timeout: 10_000, windowsHide: true });
    }
    let envInfo = {};
    try {
      const envData = fs.readFileSync(`/proc/${pid}/environ`, 'utf8');
      envInfo = Object.fromEntries(envData.split('\0').filter(Boolean).map(e => e.split(/=(.*)/)));
    } catch {}
    return { content: [{ type: 'text', text: output.trim() }], environ: envInfo };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('network_info', () => {
  try {
    const interfaces = os.networkInterfaces();
    const result = {};
    for (const [name, addrs] of Object.entries(interfaces)) {
      result[name] = (addrs || []).map(a => ({
        address: a.address, netmask: a.netmask, family: a.family,
        mac: a.mac, internal: a.internal, cidr: a.cidr || null,
      }));
    }
    return { content: [{ type: 'text', text: JSON.stringify(result, null, 2) }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('network_connections', (args = {}) => {
  try {
    const isWin = process.platform === 'win32';
    let output;
    if (isWin) {
      output = execSync('netstat -an', { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    } else {
      const useSs = args.tool === 'netstat' ? false : true;
      if (useSs) {
        try { output = execSync('ss -tunapl 2>/dev/null', { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true }); }
        catch { output = execSync('netstat -tunap 2>/dev/null', { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true }); }
      } else {
        output = execSync('netstat -tunap 2>/dev/null', { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
      }
    }
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('http_request', async (args = {}) => {
  try {
    const url = typeof args.url === 'string' ? args.url : '';
    if (!url) return { content: [{ type: 'text', text: 'url is required' }], isError: true };
    const method = (args.method || 'GET').toUpperCase();
    const headers = args.headers || {};
    const body = args.body || undefined;
    const timeout = Math.min(Math.max(1_000, Number(args.timeout) || 30_000), 120_000);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
      const response = await fetch(url, { method, headers, body, signal: controller.signal });
      const text = await response.text();
      return {
        content: [{ type: 'text', text: text }],
        status: response.status, statusText: response.statusText,
        headers: Object.fromEntries(response.headers.entries()),
      };
    } finally {
      clearTimeout(timer);
    }
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('ping_host', (args = {}) => {
  try {
    const host = typeof args.host === 'string' ? args.host : '';
    if (!host) return { content: [{ type: 'text', text: 'host is required' }], isError: true };
    const count = Math.min(Math.max(1, Number(args.count) || 4), 100);
    const isWin = process.platform === 'win32';
    const cmd = isWin ? `ping -n ${count} ${host}` : `ping -c ${count} ${host}`;
    const output = execSync(cmd, { encoding: 'utf8', timeout: 60_000, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    const text = (error.stdout || '') + (error.stderr ? '\n' + error.stderr : '') || error.message;
    return { content: [{ type: 'text', text: text }], isError: true };
  }
});

registerTool('dns_lookup', async (args = {}) => {
  try {
    const hostname = typeof args.hostname === 'string' ? args.hostname : '';
    if (!hostname) return { content: [{ type: 'text', text: 'hostname is required' }], isError: true };
    const type = args.type || 'A';
    return new Promise((resolve) => {
      dns.resolve(hostname, type, (err, addresses) => {
        if (err) return resolve({ content: [{ type: 'text', text: err.message }], isError: true });
        dns.lookup(hostname, { all: true }, (lookupErr, lookupResult) => {
          if (lookupErr) lookupResult = [];
          resolve({
            content: [{ type: 'text', text: JSON.stringify({ hostname, type, addresses, lookup: lookupResult }, null, 2) }],
          });
        });
      });
    });
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('port_check', (args = {}) => {
  try {
    const port = parseInt(args.port);
    if (isNaN(port) || port < 1 || port > 65535) return { content: [{ type: 'text', text: 'valid port (1-65535) is required' }], isError: true };
    const host = args.host || '127.0.0.1';
    const timeout = Math.min(Math.max(500, Number(args.timeout) || 2000), 10_000);
    return new Promise((resolve) => {
      const sock = new net.Socket();
      sock.setTimeout(timeout);
      sock.on('connect', () => { sock.destroy(); resolve({ content: [{ type: 'text', text: `Port ${port} is OPEN on ${host}` }], open: true }); });
      sock.on('error', () => { sock.destroy(); resolve({ content: [{ type: 'text', text: `Port ${port} is CLOSED on ${host}` }], open: false }); });
      sock.on('timeout', () => { sock.destroy(); resolve({ content: [{ type: 'text', text: `Port ${port} connection TIMEOUT on ${host}` }], open: false, timeout: true }); });
      sock.connect(port, host);
    });
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_copy', (args = {}) => {
  try {
    const src = safePath(args.src || args.source || '');
    const dest = safePath(args.dest || args.destination || '');
    if (!src || !dest) return { content: [{ type: 'text', text: 'src and dest are required' }], isError: true };
    const srcStat = fs.statSync(src);
    if (srcStat.isDirectory()) {
      fs.cpSync(src, dest, { recursive: true, errorOnExist: false });
    } else {
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.copyFileSync(src, dest);
    }
    return { content: [{ type: 'text', text: `Copied ${src} -> ${dest}` }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_move', (args = {}) => {
  try {
    const src = safePath(args.src || args.source || '');
    const dest = safePath(args.dest || args.destination || '');
    if (!src || !dest) return { content: [{ type: 'text', text: 'src and dest are required' }], isError: true };
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.renameSync(src, dest);
    return { content: [{ type: 'text', text: `Moved ${src} -> ${dest}` }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_delete', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    if (!p) return { content: [{ type: 'text', text: 'path is required' }], isError: true };
    const recursive = args.recursive !== false;
    const stat = fs.statSync(p);
    if (stat.isDirectory()) {
      fs.rmSync(p, { recursive, force: true });
    } else {
      fs.unlinkSync(p);
    }
    return { content: [{ type: 'text', text: `Deleted ${args.path}` }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_info', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    if (!p) return { content: [{ type: 'text', text: 'path is required' }], isError: true };
    const stat = fs.statSync(p);
    const mode = stat.mode.toString(8).slice(-3);
    let owner = '(unknown)';
    try { owner = `${stat.uid}:${stat.gid}`; } catch {}
    return {
      content: [{ type: 'text', text: JSON.stringify({
        path: args.path, exists: true, size: stat.size,
        isDirectory: stat.isDirectory(), isFile: stat.isFile(),
        isSymlink: stat.isSymbolicLink(),
        permissions: mode, owner,
        created: stat.birthtime, modified: stat.mtime, accessed: stat.atime,
        modeRaw: stat.mode,
      }, null, 2) }],
    };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_permissions', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    const mode = typeof args.mode === 'string' ? args.mode : '';
    if (!p || !mode) return { content: [{ type: 'text', text: 'path and mode are required (e.g. 755)' }], isError: true };
    const numericMode = parseInt(mode, 8);
    if (isNaN(numericMode)) return { content: [{ type: 'text', text: `Invalid mode: ${mode}` }], isError: true };
    if (args.recursive) {
      const stat = fs.statSync(p);
      if (stat.isDirectory()) {
        const files = execSync(`find ${p} -exec chmod ${mode} {} +`, { encoding: 'utf8', timeout: 30_000, windowsHide: true });
        return { content: [{ type: 'text', text: `Changed permissions recursively on ${args.path} to ${mode}` }] };
      }
    }
    fs.chmodSync(p, numericMode);
    return { content: [{ type: 'text', text: `Changed permissions on ${args.path} to ${mode}` }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_exists', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    if (!p) return { content: [{ type: 'text', text: 'path is required' }], isError: true };
    let exists = false;
    let stat = null;
    try { stat = fs.statSync(p); exists = true; } catch {}
    return {
      content: [{ type: 'text', text: exists ? `Path exists: ${args.path}` : `Path does not exist: ${args.path}` }],
      exists, isDirectory: stat?.isDirectory?.() || false, isFile: stat?.isFile?.() || false,
    };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_find', (args = {}) => {
  try {
    const p = safePath(args.path || '.');
    const pattern = args.pattern || '';
    const type = args.type || 'f';
    const maxDepth = Math.min(Math.max(1, Number(args.maxDepth) || 0), 50);
    const isWin = process.platform === 'win32';
    let cmd;
    if (isWin) {
      cmd = `dir /s /b ${p}\\*${pattern}*`;
    } else {
      const depthArg = maxDepth > 0 ? ` -maxdepth ${maxDepth}` : '';
      cmd = `find ${p} -type ${type}${depthArg}${pattern ? ` -name '${pattern}'` : ''}`;
    }
    const output = execSync(cmd, { encoding: 'utf8', timeout: 30_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const matches = output.trim().split('\n').filter(Boolean);
    return { content: [{ type: 'text', text: matches.join('\n') || '(no matches)' }], count: matches.length };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('file_type', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    if (!p) return { content: [{ type: 'text', text: 'path is required' }], isError: true };
    const isWin = process.platform === 'win32';
    let mime = '(unknown)';
    let type = '(unknown)';
    if (isWin) {
      const ext = path.extname(p).toLowerCase();
      type = ext || '(no extension)';
    } else {
      try {
        const output = execSync(`file -b --mime-type "${p}" 2>/dev/null || file -b "${p}"`, { encoding: 'utf8', timeout: 5_000, windowsHide: true });
        mime = output.trim();
        type = execSync(`file -b "${p}"`, { encoding: 'utf8', timeout: 5_000, windowsHide: true }).trim();
      } catch {
        type = path.extname(p) || '(unknown)';
      }
    }
    return { content: [{ type: 'text', text: JSON.stringify({ path: args.path, mime, type }, null, 2) }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('archive', (args = {}) => {
  try {
    const action = args.action || 'create';
    const format = args.format || 'tar.gz';
    const source = safePath(args.source || args.src || '');
    const dest = safePath(args.destination || args.dest || '');
    if (!source || !dest) return { content: [{ type: 'text', text: 'source and destination are required' }], isError: true };
    let output;
    if (action === 'create') {
      if (format === 'zip') {
        output = execSync(`zip -r "${dest}" "${source}"`, { encoding: 'utf8', timeout: 120_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
      } else {
        output = execSync(`tar -czf "${dest}" -C "${path.dirname(source)}" "${path.basename(source)}"`, { encoding: 'utf8', timeout: 120_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
      }
      return { content: [{ type: 'text', text: `Created ${format} archive: ${dest}` }], output: output.trim() };
    }
    if (action === 'extract') {
      const ext = path.extname(dest || source);
      if (format === 'zip' || ext === '.zip') {
        output = execSync(`unzip -o "${source}" -d "${dest}"`, { encoding: 'utf8', timeout: 120_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
      } else {
        fs.mkdirSync(dest, { recursive: true });
        output = execSync(`tar -xzf "${source}" -C "${dest}"`, { encoding: 'utf8', timeout: 120_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
      }
      return { content: [{ type: 'text', text: `Extracted ${source} to ${dest}` }], output: output.trim() };
    }
    return { content: [{ type: 'text', text: `Unknown action: ${action}. Use create or extract.` }], isError: true };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('download', async (args = {}) => {
  try {
    const url = typeof args.url === 'string' ? args.url : '';
    const dest = safePath(args.dest || args.destination || args.path || '');
    if (!url || !dest) return { content: [{ type: 'text', text: 'url and dest are required' }], isError: true };
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    const response = await fetch(url);
    if (!response.ok) return { content: [{ type: 'text', text: `HTTP ${response.status}: ${response.statusText}` }], isError: true };
    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(dest, buffer);
    return { content: [{ type: 'text', text: `Downloaded ${url} to ${args.dest || args.path} (${buffer.length} bytes)` }], size: buffer.length };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('upload', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    if (!p) return { content: [{ type: 'text', text: 'path is required' }], isError: true };
    const content = fs.readFileSync(p, 'utf8');
    const stat = fs.statSync(p);
    return { content: [{ type: 'text', text: content }], size: stat.size, path: args.path };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('docker_ps', (args = {}) => {
  try {
    const all = args.all ? '-a' : '';
    const output = execSync(`docker ps ${all} --no-trunc 2>/dev/null || echo 'Docker not available'`, { encoding: 'utf8', timeout: 15_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('docker_info', () => {
  try {
    const output = execSync('docker info 2>/dev/null || echo \'Docker not available\'', { encoding: 'utf8', timeout: 15_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('docker_images', (args = {}) => {
  try {
    const filter = args.filter || '';
    const cmd = `docker images${filter ? ` --filter=${filter}` : ''} 2>/dev/null || echo 'Docker not available'`;
    const output = execSync(cmd, { encoding: 'utf8', timeout: 15_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('git_status', (args = {}) => {
  try {
    const repo = safePath(args.path || args.repo || '.');
    const output = execSync('git status 2>&1', { cwd: repo, encoding: 'utf8', timeout: 15_000, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('git_log', (args = {}) => {
  try {
    const repo = safePath(args.path || args.repo || '.');
    const count = Math.min(Math.max(1, Number(args.count) || 10), 100);
    const format = args.format || '%h %s (%an, %ar)';
    const branch = args.branch || '';
    const cmd = `git log${branch ? ' ' + branch : ''} --max-count=${count} --format="${format}" 2>&1`;
    const output = execSync(cmd, { cwd: repo, encoding: 'utf8', timeout: 15_000, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('npm_list', (args = {}) => {
  try {
    const global = args.global ? '-g' : '';
    const depth = args.depth ? `--depth=${args.depth}` : '--depth=0';
    const p = args.path ? safePath(args.path) : '.';
    const cwd = args.global ? undefined : p;
    const output = execSync(`npm list ${global} ${depth} 2>&1`, { cwd, encoding: 'utf8', timeout: 30_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: (error.stdout || '') + (error.stderr || '') || error.message }], isError: true };
  }
});

registerTool('pip_list', (args = {}) => {
  try {
    const pipCmd = process.platform === 'win32' ? 'pip' : 'pip3';
    const output = execSync(`${pipCmd} list 2>&1 || pip list 2>&1`, { encoding: 'utf8', timeout: 30_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    return { content: [{ type: 'text', text: output.trim() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('compile', (args = {}) => {
  try {
    const file = safePath(args.file || args.path || '');
    const code = typeof args.code === 'string' ? args.code : '';
    const language = args.language || '';
    const outputFile = args.output || 'a.out';
    if (!file && !code) return { content: [{ type: 'text', text: 'file or code is required' }], isError: true };
    let lang = language;
    let tmpFile = file;
    if (code && !file) {
      const ext = { 'c': '.c', 'cpp': '.cpp', 'rust': '.rs', 'rs': '.rs', 'go': '.go', 'python': '.py', 'py': '.py', 'node': '.js', 'js': '.js', 'ts': '.ts' }[lang] || '.js';
      tmpFile = path.join(os.tmpdir(), `compile_${Date.now()}${ext}`);
      fs.writeFileSync(tmpFile, code, 'utf8');
    }
    if (!lang && tmpFile) {
      const ext = path.extname(tmpFile);
      if (ext === '.c') lang = 'c';
      else if (ext === '.cpp' || ext === '.cc') lang = 'cpp';
      else if (ext === '.rs') lang = 'rust';
      else if (ext === '.go') lang = 'go';
      else if (ext === '.py') lang = 'python';
      else if (ext === '.js' || ext === '.mjs') lang = 'node';
      else if (ext === '.ts') lang = 'ts';
      else lang = 'node';
    }
    let output;
    switch (lang) {
      case 'c':
        output = execSync(`gcc -o "${outputFile}" "${tmpFile}" 2>&1`, { encoding: 'utf8', timeout: 60_000, windowsHide: true });
        return { content: [{ type: 'text', text: `Compiled with gcc: ${output.trim() || 'OK'}` }], binary: outputFile };
      case 'cpp':
        output = execSync(`g++ -o "${outputFile}" "${tmpFile}" 2>&1`, { encoding: 'utf8', timeout: 60_000, windowsHide: true });
        return { content: [{ type: 'text', text: `Compiled with g++: ${output.trim() || 'OK'}` }], binary: outputFile };
      case 'rust':
        output = execSync(`rustc -o "${outputFile}" "${tmpFile}" 2>&1`, { encoding: 'utf8', timeout: 120_000, windowsHide: true });
        return { content: [{ type: 'text', text: `Compiled with rustc: ${output.trim() || 'OK'}` }], binary: outputFile };
      case 'go':
        output = execSync(`go build -o "${outputFile}" "${tmpFile}" 2>&1`, { encoding: 'utf8', timeout: 120_000, windowsHide: true });
        return { content: [{ type: 'text', text: `Compiled with go: ${output.trim() || 'OK'}` }], binary: outputFile };
      case 'python':
        output = execSync(`python3 "${tmpFile}" 2>&1`, { encoding: 'utf8', timeout: 60_000, windowsHide: true });
        return { content: [{ type: 'text', text: output.trim() || '(no output)' }] };
      case 'node':
        output = execSync(`node "${tmpFile}" 2>&1`, { encoding: 'utf8', timeout: 60_000, windowsHide: true });
        return { content: [{ type: 'text', text: output.trim() || '(no output)' }] };
      case 'ts':
        output = execSync(`npx tsc "${tmpFile}" 2>&1`, { encoding: 'utf8', timeout: 60_000, windowsHide: true });
        return { content: [{ type: 'text', text: output.trim() || 'Compiled OK' }] };
      default:
        return { content: [{ type: 'text', text: `Unsupported language: ${lang}` }], isError: true };
    }
  } catch (error) {
    const text = (error.stdout || '') + (error.stderr ? '\n' + error.stderr : '') || error.message;
    return { content: [{ type: 'text', text: text }], isError: true };
  }
});

registerTool('screenshot', (args = {}) => {
  try {
    if (process.platform !== 'linux') {
      return { content: [{ type: 'text', text: 'Screenshot only available on Linux with import (ImageMagick)' }], isError: true };
    }
    const outputPath = args.output || path.join(os.tmpdir(), `screenshot_${Date.now()}.png`);
    const display = args.display || ':0';
    execSync(`import -display ${display} -window root "${outputPath}" 2>/dev/null`, { encoding: 'utf8', timeout: 15_000, windowsHide: true });
    const stat = fs.statSync(outputPath);
    return { content: [{ type: 'text', text: `Screenshot saved: ${outputPath} (${stat.size} bytes)` }], path: outputPath, size: stat.size };
  } catch (error) {
    return { content: [{ type: 'text', text: `Screenshot failed: ${error.message}. Install ImageMagick (import command).` }], isError: true };
  }
});

registerTool('calendar', (args = {}) => {
  try {
    const now = new Date();
    const tz = args.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
    const result = {
      iso: now.toISOString(),
      unix: Math.floor(now.getTime() / 1000),
      date: now.toLocaleDateString('en-US', { timeZone: tz }),
      time: now.toLocaleTimeString('en-US', { timeZone: tz }),
      timezone: tz,
      utcOffset: now.getTimezoneOffset(),
      year: now.getFullYear(),
      month: now.getMonth() + 1,
      day: now.getDate(),
      hours: now.getHours(),
      minutes: now.getMinutes(),
      seconds: now.getSeconds(),
      weekday: now.toLocaleDateString('en-US', { weekday: 'long' }),
    };
    return { content: [{ type: 'text', text: JSON.stringify(result, null, 2) }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('hash', (args = {}) => {
  try {
    const input = typeof args.input === 'string' ? args.input : '';
    if (!input) return { content: [{ type: 'text', text: 'input string is required' }], isError: true };
    const algorithm = args.algorithm || 'sha256';
    const supported = ['md5', 'sha1', 'sha256', 'sha512'];
    if (!supported.includes(algorithm)) return { content: [{ type: 'text', text: `Unsupported algorithm: ${algorithm}. Use: ${supported.join(', ')}` }], isError: true };
    const hash = crypto.createHash(algorithm).update(input).digest('hex');
    return { content: [{ type: 'text', text: hash }], algorithm, input };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('base64', (args = {}) => {
  try {
    const input = typeof args.input === 'string' ? args.input : '';
    if (!input) return { content: [{ type: 'text', text: 'input string is required' }], isError: true };
    const action = args.action || 'encode';
    if (action === 'encode') {
      const encoded = Buffer.from(input).toString('base64');
      return { content: [{ type: 'text', text: encoded }], action: 'encode' };
    }
    if (action === 'decode') {
      const decoded = Buffer.from(input, 'base64').toString('utf8');
      return { content: [{ type: 'text', text: decoded }], action: 'decode' };
    }
    return { content: [{ type: 'text', text: `Unknown action: ${action}. Use encode or decode.` }], isError: true };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('uuid', () => {
  try {
    return { content: [{ type: 'text', text: crypto.randomUUID() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('random', (args = {}) => {
  try {
    const length = Math.min(Math.max(1, Number(args.length) || 16), 1024);
    const encoding = args.encoding || 'hex';
    const bytes = crypto.randomBytes(length);
    let result;
    if (encoding === 'hex') result = bytes.toString('hex');
    else if (encoding === 'base64') result = bytes.toString('base64');
    else if (encoding === 'buffer') result = bytes.toString('hex');
    else result = bytes.toString('hex');
    return { content: [{ type: 'text', text: result }], length, encoding };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('wait', (args = {}) => {
  try {
    const ms = Math.min(Math.max(1, Number(args.ms || args.duration || args.time) || 1000), 120_000);
    const start = Date.now();
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
    const elapsed = Date.now() - start;
    return { content: [{ type: 'text', text: `Waited ${elapsed}ms` }], duration: elapsed, requested: ms };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('echo', (args = {}) => {
  return { content: [{ type: 'text', text: JSON.stringify(args, null, 2) }] };
});

registerTool('whoami', () => {
  try {
    let username;
    try { username = os.userInfo().username; } catch { username = process.env.USER || process.env.USERNAME || 'unknown'; }
    return { content: [{ type: 'text', text: username }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('hostname', (args = {}) => {
  try {
    return { content: [{ type: 'text', text: os.hostname() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('temp_dir', () => {
  try {
    return { content: [{ type: 'text', text: os.tmpdir() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('home_dir', () => {
  try {
    return { content: [{ type: 'text', text: os.homedir() }] };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

const { WebSocketConnection, acceptKey } = require('./websocket');

function connect() {
  if (shuttingDown) return;

  log.info({ url: config.relayUrl, attempt: reconnectAttempt }, 'Connecting to relay...');

  const url = new URL(config.relayUrl);
  if (!['ws:', 'wss:'].includes(url.protocol)) { log.error('Invalid relay URL protocol'); return; }

  const secure = url.protocol === 'wss:';
  const key = crypto.randomBytes(16).toString('base64');
  const request = (secure ? require('https') : require('http')).request({
    hostname: url.hostname, port: Number(url.port) || (secure ? 443 : 80),
    method: 'GET', path: `${url.pathname || '/'}${url.search || ''}`,
    headers: { Upgrade: 'websocket', Connection: 'Upgrade', 'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13' },
    rejectUnauthorized: process.env.RELAY_TLS_REJECT_UNAUTHORIZED !== 'false',
  });

  request.setTimeout(10_000, () => request.destroy(new Error('Connection timed out')));

  request.once('upgrade', (response, socket, head) => {
    if (response.statusCode !== 101) { socket.destroy(); return scheduleReconnect(`Relay rejected (${response.statusCode})`); }
    ws = new WebSocketConnection(socket, { initialData: head, maskOutgoing: true });
    reconnectAttempt = 0;
    log.info('Connected!');

    sendControl({
      type: 'join',
      name: config.agentName,
      token: config.agentToken,
      sessionId,
      lastSeq,
      executor: true,
    });

    heartbeatTimer = setInterval(() => {
      sendControl({ type: 'ping', timestamp: Date.now() });
    }, config.heartbeatIntervalMs);
    heartbeatTimer.unref?.();

    ws.on('message', raw => {
      try {
        let msg = JSON.parse(raw);
        routeLegacyMessage(msg);
      } catch (err) {
        log.warn({ err: err.message }, 'Error parsing message');
      }
    });

    ws.on('close', () => {
      log.warn('Disconnected');
      cleanup();
      scheduleReconnect();
    });

    ws.on('socketError', (err) => {
      log.error({ err: err.message }, 'Socket error');
    });
  });

  request.once('response', response => {
    response.resume();
    scheduleReconnect(`Relay rejected WebSocket upgrade (${response.statusCode})`);
  });
  request.once('error', error => scheduleReconnect(`Connection error: ${error.message}`));
  request.end();
}

function cleanup() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
}

function scheduleReconnect() {
  if (shuttingDown) return;
  const base = Math.min(1000 * Math.pow(2, reconnectAttempt), 30_000);
  const jitter = base * 0.3 * Math.random();
  const delay = Math.round(base + jitter);
  reconnectAttempt++;
  log.info({ delay, attempt: reconnectAttempt }, `Reconnecting in ${delay}ms...`);
  reconnectTimer = setTimeout(connect, delay);
  reconnectTimer.unref?.();
}

function routeMessage(channel, msg) {
  switch (channel) {
    case CHANNELS.CONTROL: handleControl(msg); break;
    case CHANNELS.CHAT: handleChat(msg); break;
    case CHANNELS.MCP: handleMcp(msg); break;
    case CHANNELS.STREAM: handleStream(msg); break;
    case CHANNELS.PRESENCE: handlePresence(msg); break;
  }
}

function routeLegacyMessage(msg) {
  switch (msg.type) {
    case 'joined': sessionId = msg.id; lastSeq = msg.seq || 0; log.info({ sessionId }, 'Registered'); break;
    case 'status': log.info(`[status] ${msg.count} online`); break;
    case 'chat': handleChat(msg); break;
    case 'mcp_call': handleMcp(msg); break;
    case 'error': log.error(`[relay] ${msg.message}`); break;
    case 'pong': break;
    case 'server.shutdown': log.warn('Server shutting down, will reconnect...'); break;
  }
}

function handleControl(msg) {
  switch (msg.type) {
    case 'joined':
      sessionId = msg.id || msg.sessionId;
      lastSeq = msg.seq || 0;
      log.info({ sessionId }, 'Registered');
      break;
    case 'welcome':
      sessionId = msg.sessionId;
      lastSeq = msg.seq || 0;
      log.info({ sessionId, agents: msg.agents }, 'Joined relay');
      break;
    case 'status':
      log.info(`[status] ${msg.count} online: ${(msg.agents || []).map(a => `${a.name}${a.executor ? '*' : ''}`).join(', ')}`);
      break;
    case 'error':
      log.error(`[relay] ${msg.code || 'error'}: ${msg.message}`);
      break;
    case 'heartbeat.ack':
      break;
    case 'server.shutdown':
      log.warn('Server shutting down...');
      break;
    default:
      if (msg.type !== 'pong' && msg.type !== 'leave') {
        log.debug({ type: msg.type }, 'Unhandled control message');
      }
  }
}

function handleChat(msg) {
  if (msg.from && msg.text) {
    log.info(`[chat ${msg.from}] ${msg.text}`);
  }
}

async function handleMcp(msg) {
  if (msg.type === 'mcp_call') {
    const method = msg.method || msg.call?.name;
    const callId = msg.callId || msg.call?.id;
    const relayCallId = msg.relayCallId;
    const params = msg.params ?? msg.call?.params ?? {};

    const tool = tools[method];
    let result;

    log.info(`[exec] ${method} (call ${callId})`);

    try {
      if (!tool) throw new Error(`Tool not found: ${method}`);
      result = await Promise.resolve(tool(params));
    } catch (error) {
      result = { content: [{ type: 'text', text: error.message }], isError: true };
    }

    const response = { type: 'mcp_result', relayCallId, callId, result };
    ws.send(JSON.stringify(response));
  }
}

function handleStream(msg) {
  log.debug({ from: msg.from, streamId: msg.streamId }, 'Stream');
}

function handlePresence(msg) {
  log.debug({ from: msg.from, status: msg.status }, 'Presence');
}

function sendControl(msg) {
  if (!ws || !ws.isOpen) return false;
  ws.send(JSON.stringify(msg));
  return true;
}

function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info({ signal }, 'Shutting down...');

  cleanup();
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }

  if (ws && !ws.closed) {
    sendControl({ type: 'goodbye', name: config.agentName });
    ws.close(1001, 'Agent shutting down');
  }

  setTimeout(() => process.exit(0), 500).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

if (require.main === module) {
  if (!config.agentToken) log.warn('AGENT_RELAY_TOKEN not set');
  log.info(`Starting "${config.agentName}" in ${config.mcpWorkspace}`);
  connect();
}

module.exports = { tools, safePath };