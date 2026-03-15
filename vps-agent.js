import express from 'express';
import { execFile, exec } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Load .env from the host-kit directory automatically if present
const envFilePath = path.join(process.env.OPENCLAW_HOST_KIT_DIR || __dirname, '.env');
if (fs.existsSync(envFilePath)) {
    const lines = fs.readFileSync(envFilePath, 'utf8').split('\n');
    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx === -1) continue;
        const key = trimmed.slice(0, eqIdx).trim();
        const val = trimmed.slice(eqIdx + 1).trim().replace(/^["']|["']$/g, '');
        if (key && !(key in process.env)) process.env[key] = val;
    }
    console.log(`[vps-agent] loaded env from ${envFilePath}`);
}

const app = express();
app.use(express.json());

const PORT = process.env.VPS_AGENT_PORT || 4444;
const INTERNAL_SECRET = process.env.OPENCLAW_INTERNAL_SECRET || '';
const HOST_KIT_DIR = process.env.OPENCLAW_HOST_KIT_DIR || __dirname;
const MEM0_PLUGIN_PACKAGE = '@mem0/openclaw-mem0';
const MEM0_PLUGIN_KEY = 'openclaw-mem0';
const MEM0_ENV_PLACEHOLDER = '${MEM0_API_KEY}';
const MEM0_API_KEY = process.env.OPENCLAW_MEM0_API_KEY || process.env.MEM0_API_KEY || '';

const CONTAINER_RAM_LIMIT_MB = parseInt(process.env.OPENCLAW_CONTAINER_RAM_MB || '5120'); // 5 GB default

// ── Request Logger (to file for container creation requests) ─────────────────
const LOG_DIR = process.env.VPS_AGENT_LOG_DIR || '/var/log/openclaw';
const REQUEST_LOG_FILE = path.join(LOG_DIR, 'provision-requests.log');

// Ensure log directory exists
try {
    if (!fs.existsSync(LOG_DIR)) {
        fs.mkdirSync(LOG_DIR, { recursive: true, mode: 0o755 });
    }
} catch (err) {
    console.warn(`[vps-agent] could not create log dir ${LOG_DIR}:`, err.message);
}

function logProvisionRequest(req, data) {
    const timestamp = new Date().toISOString();
    const logEntry = {
        timestamp,
        method: req.method,
        path: req.path,
        ip: req.ip || req.headers['x-forwarded-for'] || req.socket.remoteAddress,
        ...data
    };
    
    const logLine = JSON.stringify(logEntry) + '\n';
    
    try {
        fs.appendFileSync(REQUEST_LOG_FILE, logLine, { encoding: 'utf8', mode: 0o644 });
    } catch (err) {
        console.error(`[vps-agent] failed to write provision log:`, err.message);
    }
    
    console.log(`[vps-agent] ${req.method} ${req.path}:`, data);
}

// ── Request Logger Middleware (log ALL requests) ────────────────────────────
app.use((req, res, next) => {
    const start = Date.now();
    const requestId = Math.random().toString(36).slice(2, 10);
    
    // Log incoming request
    console.log(`[vps-agent] → [${requestId}] ${req.method} ${req.path} from ${req.ip || req.headers['x-forwarded-for'] || req.socket.remoteAddress}`);
    
    // Log request body for provision/remove requests
    if ((req.path.includes('/create-instance') || req.path.includes('/remove-instance')) && req.body) {
        console.log(`[vps-agent] → [${requestId}] Body:`, JSON.stringify(req.body).slice(0, 200));
    }
    
    res.on('finish', () => {
        const duration = Date.now() - start;
        const status = res.statusCode;
        const flag = status >= 500 ? '✗' : status >= 400 ? '!' : '✓';
        
        console.log(`[vps-agent] ← [${requestId}] ${flag} ${req.method} ${req.path} → ${status} (${duration}ms)`);
    });
    next();
});

function requireInternal(req, res, next) {
    if (!INTERNAL_SECRET || req.headers['x-internal-secret'] !== INTERNAL_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
}

function validId(id) {
    return typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);
}

function listInstanceIds() {
    if (!fs.existsSync(INSTANCES_DIR)) return [];
    return fs.readdirSync(INSTANCES_DIR, { withFileTypes: true })
        .filter((entry) => entry.isDirectory() && validId(entry.name))
        .map((entry) => entry.name)
        .sort();
}

function run(cmd, args = [], opts = {}) {
    return new Promise((resolve, reject) => {
        execFile(cmd, args, { timeout: 30_000, ...opts }, (err, stdout, stderr) => {
            if (err) return reject(new Error(stderr || err.message));
            resolve(stdout.trim());
        });
    });
}

function shell(cmd) {
    return new Promise((resolve, reject) => {
        exec(cmd, { timeout: 15_000 }, (err, stdout, stderr) => {
            if (err) return reject(new Error(stderr || err.message));
            resolve(stdout.trim());
        });
    });
}

// ── Health ────────────────────────────────────────────────────────────────────

app.get('/healthz', (_, res) => res.json({ ok: true }));

// ── Containers status ─────────────────────────────────────────────────────────

app.get('/api/internal/containers', requireInternal, async (_, res) => {
    try {
        const raw = await run('docker', [
            'stats', '--no-stream', '--no-trunc',
            '--format', '{{json .}}',
        ]);

        const allContainers = raw
            .split('\n')
            .filter(Boolean)
            .map((line) => {
                const s = JSON.parse(line);
                const memUsageMB = parseFloat(s.MemUsage?.split('/')[0]) || 0;
                const memLimitMB = parseFloat(s.MemUsage?.split('/')[1]) || 0;
                const cpuPct = parseFloat(s.CPUPerc) || 0;
                return {
                    id: s.ID,
                    name: s.Name,
                    cpuPercent: cpuPct,
                    memUsageMB: Math.round(memUsageMB),
                    memLimitMB: Math.round(memLimitMB),
                    memPercent: parseFloat(s.MemPerc) || 0,
                    netIO: s.NetIO,
                    blockIO: s.BlockIO,
                    pids: parseInt(s.PIDs) || 0,
                    isOpenclaw: s.Name?.startsWith('openclaw-'),
                };
            });

        const openclawContainers = allContainers.filter((c) => c.isOpenclaw);
        const totalMemUsedMB = openclawContainers.reduce((sum, c) => sum + c.memUsageMB, 0);

        return res.json({
            total: openclawContainers.length,
            totalMemUsedMB,
            containers: openclawContainers,
        });
    } catch (err) {
        console.error('[vps-agent] containers:', err);
        return res.status(500).json({ error: err.message });
    }
});

// ── VPS system resources ──────────────────────────────────────────────────────

app.get('/api/internal/system', requireInternal, async (_, res) => {
    try {
        const [memInfo, cpuLoad, diskInfo] = await Promise.all([
            shell("free -m | awk 'NR==2{print $2,$3,$4}'"),
            shell("cat /proc/loadavg | awk '{print $1,$2,$3}'"),
            shell("df -BM / | awk 'NR==2{print $2,$3,$4}'"),
        ]);

        const [totalMem, usedMem, freeMem] = memInfo.split(' ').map(Number);
        const [load1, load5, load15] = cpuLoad.split(' ').map(parseFloat);
        const [diskTotalMB, diskUsedMB, diskFreeMB] = diskInfo.replace(/M/g, '').split(' ').map(Number);

        const cpuCount = parseInt(await shell("nproc"), 10);

        return res.json({
            memory: {
                totalMB: totalMem,
                usedMB: usedMem,
                freeMB: freeMem,
                usedPercent: Math.round((usedMem / totalMem) * 100),
            },
            cpu: { count: cpuCount, load1, load5, load15 },
            disk: {
                totalMB: diskTotalMB,
                usedMB: diskUsedMB,
                freeMB: diskFreeMB,
                usedPercent: Math.round((diskUsedMB / diskTotalMB) * 100),
            },
            containerRamLimitMB: CONTAINER_RAM_LIMIT_MB,
            remainingSlots: Math.floor(freeMem / CONTAINER_RAM_LIMIT_MB),
        });
    } catch (err) {
        console.error('[vps-agent] system:', err);
        return res.status(500).json({ error: err.message });
    }
});

// ── Single container stats ────────────────────────────────────────────────────

app.get('/api/internal/containers/:instanceId', requireInternal, async (req, res) => {
    const { instanceId } = req.params;
    if (!validId(instanceId)) return res.status(400).json({ error: 'Invalid instanceId' });

    try {
        const raw = await run('docker', [
            'stats', '--no-stream', '--no-trunc',
            '--format', '{{json .}}',
            `openclaw-${instanceId}`,
        ]);

        const s = JSON.parse(raw);
        const memUsageMB = parseFloat(s.MemUsage?.split('/')[0]) || 0;

        return res.json({
            instanceId,
            containerName: s.Name,
            cpuPercent: parseFloat(s.CPUPerc) || 0,
            memUsageMB: Math.round(memUsageMB),
            memPercent: parseFloat(s.MemPerc) || 0,
            netIO: s.NetIO,
            blockIO: s.BlockIO,
            pids: parseInt(s.PIDs) || 0,
        });
    } catch (err) {
        if (err.message.includes('No such container')) {
            return res.status(404).json({ error: 'Container not found' });
        }
        console.error('[vps-agent] container stats:', err);
        return res.status(500).json({ error: err.message });
    }
});

// ── Create instance (enforces RAM limit) ──────────────────────────────────────

app.post('/api/internal/create-instance', requireInternal, async (req, res) => {
    const { instanceId } = req.body;
    
    logProvisionRequest(req, { 
        action: 'create-instance-start', 
        instanceId,
        requestBody: req.body 
    });
    
    if (!validId(instanceId)) {
        logProvisionRequest(req, { action: 'create-instance-invalid-id', instanceId });
        return res.status(400).json({ error: 'Invalid instanceId' });
    }

    console.log(`[vps-agent] create-instance request for instanceId=${instanceId}`);

    const scriptPath = path.join(HOST_KIT_DIR, 'scripts', 'create-instance.sh');
    if (!fs.existsSync(scriptPath)) {
        console.error(`[vps-agent] create-instance.sh not found at ${scriptPath}`);
        return res.status(500).json({ error: `create-instance.sh not found at ${scriptPath}` });
    }

    try {
        const memRaw = await shell("free -m | awk 'NR==2{print $4}'");
        const freeMemMB = parseInt(memRaw, 10);
        console.log(`[vps-agent] free memory: ${freeMemMB}MB, required: ${CONTAINER_RAM_LIMIT_MB}MB`);

        if (freeMemMB < CONTAINER_RAM_LIMIT_MB) {
            console.warn(`[vps-agent] insufficient memory for ${instanceId}: free=${freeMemMB}MB required=${CONTAINER_RAM_LIMIT_MB}MB`);
            return res.status(507).json({
                error: 'Insufficient memory',
                freeMemMB,
                requiredMB: CONTAINER_RAM_LIMIT_MB,
            });
        }

        // Verify the runtime image exists locally before attempting container creation
        const runtimeImage = process.env.OPENCLAW_RUNTIME_IMAGE || 'openclaw-ttyd:latest';
        try {
            await run('docker', ['image', 'inspect', runtimeImage, '--format', '{{.Id}}']);
        } catch {
            const msg = `Docker image '${runtimeImage}' not found locally. Build it first: cd docker/openclaw-ttyd && docker build -t ${runtimeImage} .`;
            console.error(`[vps-agent] ${msg}`);
            return res.status(500).json({ error: msg });
        }

        console.log(`[vps-agent] running create-instance.sh for ${instanceId}...`);
        const output = await new Promise((resolve, reject) => {
            execFile('bash', [scriptPath, instanceId], {
                cwd: HOST_KIT_DIR,
                timeout: 90_000,
                env: {
                    ...process.env,
                    OPENCLAW_CONTAINER_MEMORY: `${CONTAINER_RAM_LIMIT_MB}m`,
                },
            }, (err, stdout, stderr) => {
                if (err) {
                    console.error(`[vps-agent] create-instance.sh failed for ${instanceId}:`, stderr || err.message);
                    return reject(new Error(`create-instance.sh: ${stderr || err.message}`));
                }
                console.log(`[vps-agent] create-instance.sh stdout for ${instanceId}:\n${stdout}`);
                resolve(stdout);
            });
        });

        const configPath = path.join(`/var/lib/openclaw/instances/${instanceId}`, 'openclaw.json');
        console.log(`[vps-agent] polling for gateway token at ${configPath}...`);
        let gatewayToken = null;

        // Poll for openclaw.json — the container writes it a few seconds after start
        const TOKEN_POLL_TIMEOUT_MS = 45_000;
        const TOKEN_POLL_INTERVAL_MS = 1_500;
        const pollStart = Date.now();
        while (Date.now() - pollStart < TOKEN_POLL_TIMEOUT_MS) {
            try {
                if (fs.existsSync(configPath)) {
                    const parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
                    const t = parsed?.gateway?.auth?.token;
                    if (t && typeof t === 'string' && t.trim() && t !== 'null') {
                        gatewayToken = t.trim();
                        console.log(`[vps-agent] gateway token found for ${instanceId} after ${Math.round((Date.now() - pollStart) / 1000)}s`);
                        break;
                    }
                }
            } catch (e) { /* still being written */ }
            await new Promise((r) => setTimeout(r, TOKEN_POLL_INTERVAL_MS));
        }

        if (!gatewayToken) {
            console.warn(`[vps-agent] gateway token NOT found after ${TOKEN_POLL_TIMEOUT_MS / 1000}s for ${instanceId} — container may still be starting`);
        }

        const mem0 = await ensureMem0ForInstance(instanceId);

        const result = {
            ok: true,
            instanceId,
            containerName: `openclaw-${instanceId}`,
            gatewayToken,
            mem0,
            ramLimitMB: CONTAINER_RAM_LIMIT_MB,
            output: output.trim(),
        };
        
        logProvisionRequest(req, { 
            action: 'create-instance-success', 
            instanceId,
            containerName: result.containerName,
            hasToken: !!gatewayToken,
            ramLimitMB: CONTAINER_RAM_LIMIT_MB
        });
        
        console.log(`[vps-agent] create-instance done for ${instanceId}:`, JSON.stringify({ ...result, output: '[truncated]' }));
        return res.json(result);
    } catch (err) {
        logProvisionRequest(req, { 
            action: 'create-instance-error', 
            instanceId: req.body.instanceId,
            error: err.message,
            stack: err.stack?.slice(0, 500)
        });
        
        console.error('[vps-agent] create-instance error:', err.message);
        return res.status(500).json({ error: err.message });
    }
});

app.post('/api/internal/mem0-sync', requireInternal, async (req, res) => {
    const requestedId = typeof req.body?.instanceId === 'string' ? req.body.instanceId.trim() : '';
    if (requestedId && !validId(requestedId)) {
        return res.status(400).json({ error: 'Invalid instanceId' });
    }

    const targetIds = requestedId ? [requestedId] : listInstanceIds();
    const results = [];

    for (const instanceId of targetIds) {
        try {
            const result = await ensureMem0ForInstance(instanceId);
            results.push({ instanceId, ...result });
        } catch (err) {
            results.push({ instanceId, ok: false, error: err.message });
        }
    }

    const failed = results.filter((result) => !result.ok);
    const statusCode = failed.length ? 207 : 200;
    return res.status(statusCode).json({
        ok: failed.length === 0,
        total: results.length,
        failed: failed.length,
        results,
    });
});

// ── Remove instance ───────────────────────────────────────────────────────────

app.delete('/api/internal/remove-instance/:instanceId', requireInternal, async (req, res) => {
    const { instanceId } = req.params;
    
    logProvisionRequest(req, { action: 'remove-instance-start', instanceId });
    
    if (!validId(instanceId)) {
        logProvisionRequest(req, { action: 'remove-instance-invalid-id', instanceId });
        return res.status(400).json({ error: 'Invalid instanceId' });
    }

    try {
        await run('docker', ['rm', '-f', `openclaw-${instanceId}`]);
        execFile('rm', ['-rf', `/var/lib/openclaw/instances/${instanceId}`]);
        
        logProvisionRequest(req, { action: 'remove-instance-success', instanceId });
        return res.json({ ok: true, instanceId });
    } catch (err) {
        logProvisionRequest(req, { action: 'remove-instance-error', instanceId, error: err.message });
        console.error('[vps-agent] remove-instance:', err);
        return res.status(500).json({ error: err.message });
    }
});

async function autoRegisterWithControlPlane() {
    const controlPlaneUrl = process.env.OPENCLAW_CONTROL_PLANE_URL;
    if (!controlPlaneUrl || !INTERNAL_SECRET) {
        console.warn('[vps-agent] skipping auto-register: OPENCLAW_CONTROL_PLANE_URL or INTERNAL_SECRET not set');
        return;
    }

    try {
        const ip = await shell("curl -sf https://api.ipify.org || curl -sf https://ifconfig.me");
        if (!ip) { console.warn('[vps-agent] auto-register: could not determine public IP'); return; }

        const body = {
            ip,
            shard: process.env.OPENCLAW_HOST_SHARD,
            baseDomain: process.env.OPENCLAW_BASE_DOMAIN,
            ttydSecret: process.env.OPENCLAW_TTYD_SECRET,
        };

        const res = await fetch(`${controlPlaneUrl}/api/webhooks/node-register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Internal-Secret': INTERNAL_SECRET },
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(15_000),
        });

        const data = await res.json().catch(() => ({}));
        if (res.ok) {
            console.log(`[vps-agent] auto-registered with control-plane (nodeId=${data.nodeId}, ip=${ip})`);
        } else {
            console.warn(`[vps-agent] auto-register failed: ${res.status} ${JSON.stringify(data)}`);
        }
    } catch (err) {
        console.warn(`[vps-agent] auto-register error: ${err.message}`);
    }
}

const OPENCLAW_BIN = '/home/node/.npm-global/bin/openclaw';
const INSTANCES_DIR = '/var/lib/openclaw/instances';

function readInstanceConfig(instanceId) {
    const configPath = path.join(INSTANCES_DIR, instanceId, 'openclaw.json');
    if (!fs.existsSync(configPath)) return null;
    try { return JSON.parse(fs.readFileSync(configPath, 'utf8')); } catch { return null; }
}

function ensureMem0PluginConfig(config, instanceId) {
    config.plugins = config.plugins || {};
    config.plugins.entries = config.plugins.entries || {};
    config.plugins.slots = config.plugins.slots || {};

    const previous = config.plugins.entries[MEM0_PLUGIN_KEY];
    const next = {
        ...(previous && typeof previous === 'object' ? previous : {}),
        enabled: true,
        config: {
            ...(previous?.config && typeof previous.config === 'object' ? previous.config : {}),
            apiKey: MEM0_ENV_PLACEHOLDER,
            userId: instanceId,
        },
    };

    const previousSlot = config.plugins.slots.memory;
    const changed = JSON.stringify(previous || null) !== JSON.stringify(next)
        || previousSlot !== MEM0_PLUGIN_KEY;
    config.plugins.entries[MEM0_PLUGIN_KEY] = next;
    config.plugins.slots.memory = MEM0_PLUGIN_KEY;
    return changed;
}

const wsDir = (instanceId) => path.join(INSTANCES_DIR, instanceId, 'workspace');

// ── Soul ──────────────────────────────────────────────────────────────────────
app.get('/api/internal/soul', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    const p = path.join(wsDir(instanceId), 'SOUL.md');
    try {
        res.json({ content: fs.existsSync(p) ? fs.readFileSync(p, 'utf8') : '', path: '~/.openclaw/workspace/SOUL.md' });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/internal/soul', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    const { content } = req.body || {};
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    const p = path.join(wsDir(instanceId), 'SOUL.md');
    try {
        fs.mkdirSync(path.dirname(p), { recursive: true });
        fs.writeFileSync(p, content || '', 'utf8');
        res.json({ ok: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Workspace files ───────────────────────────────────────────────────────────
app.get('/api/internal/workspace-list', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    const dir = wsDir(instanceId);
    try {
        const files = fs.existsSync(dir)
            ? fs.readdirSync(dir).filter(f => f.endsWith('.md') && f !== 'SOUL.md')
            : [];
        res.json({ files });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/internal/workspace-file', requireInternal, (req, res) => {
    const { instanceId, name } = req.query;
    if (!instanceId || !name) return res.status(400).json({ error: 'instanceId and name required' });
    const safeName = path.basename(name);
    const p = path.join(wsDir(instanceId), safeName);
    try {
        res.json({ content: fs.existsSync(p) ? fs.readFileSync(p, 'utf8') : '', path: `~/.openclaw/workspace/${safeName}` });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/internal/workspace-file', requireInternal, (req, res) => {
    const { instanceId, name } = req.query;
    const { content } = req.body || {};
    if (!instanceId || !name) return res.status(400).json({ error: 'instanceId and name required' });
    const safeName = path.basename(name);
    const p = path.join(wsDir(instanceId), safeName);
    try {
        fs.mkdirSync(path.dirname(p), { recursive: true });
        fs.writeFileSync(p, content || '', 'utf8');
        res.json({ ok: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── openclaw-config write ─────────────────────────────────────────────────────
app.put('/api/internal/openclaw-config', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    const { content } = req.body || {};
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    const configPath = path.join(INSTANCES_DIR, instanceId, 'openclaw.json');
    try {
        const incoming = JSON.parse(content);
        const existing = readInstanceConfig(instanceId) || {};
        // Preserve sensitive gateway.auth fields
        if (existing.gateway?.auth) {
            incoming.gateway = incoming.gateway || {};
            incoming.gateway.auth = existing.gateway.auth;
        }
        fs.writeFileSync(configPath, JSON.stringify(incoming, null, 2), 'utf8');
        res.json({ ok: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/internal/model-config', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const defaults = config.agents?.defaults || {};
    const primary = defaults.model?.primary || '';
    const modelsObj = defaults.models || {};
    const allowedModels = Object.keys(modelsObj);
    if (primary && !allowedModels.includes(primary)) allowedModels.push(primary);
    res.json({ primary, allowedModels, fallbacks: [] });
});

app.get('/api/internal/openclaw-config', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const safe = JSON.parse(JSON.stringify(config));
    if (safe.gateway?.auth?.token) safe.gateway.auth.token = '[redacted]';
    const configPath = path.join(INSTANCES_DIR, instanceId, 'openclaw.json');
    res.json({ content: JSON.stringify(safe, null, 2), path: configPath });
});

function runDockerExec(containerName, args, stdinData) {
    return new Promise((resolve, reject) => {
        const proc = execFile('docker', ['exec', ...(stdinData ? ['-i'] : []), containerName, OPENCLAW_BIN, ...args]);
        let stdout = '', stderr = '';
        proc.stdout.on('data', d => stdout += d);
        proc.stderr.on('data', d => stderr += d);
        if (stdinData) { proc.stdin.write(stdinData); proc.stdin.end(); }
        proc.on('close', code => {
            if (code !== 0) reject(new Error((stderr || stdout).trim().slice(0, 500)));
            else resolve((stdout || stderr).trim());
        });
    });
}

// Helper for running direct commands (not through OpenClaw CLI)
function runDockerExecDirect(containerName, args) {
    return new Promise((resolve, reject) => {
        const proc = execFile('docker', ['exec', containerName, ...args]);
        let stdout = '', stderr = '';
        proc.stdout.on('data', d => stdout += d);
        proc.stderr.on('data', d => stderr += d);
        proc.on('close', code => {
            if (code !== 0) reject(new Error((stderr || stdout).trim().slice(0, 500)));
            else resolve((stdout || stderr).trim());
        });
    });
}

function runDockerExecAsRoot(containerName, args) {
    return new Promise((resolve, reject) => {
        const proc = execFile('docker', ['exec', '-u', '0', containerName, ...args]);
        let stdout = '', stderr = '';
        proc.stdout.on('data', d => stdout += d);
        proc.stderr.on('data', d => stderr += d);
        proc.on('close', code => {
            if (code !== 0) reject(new Error((stderr || stdout).trim().slice(0, 500)));
            else resolve((stdout || stderr).trim());
        });
    });
}

// Get gateway token from container
async function getGatewayToken(containerName) {
    const script = `
const fs = require('fs');
const path = require('path');
const tokenPath = path.join(process.env.HOME, '.openclaw', 'gateway-token');
try {
    console.log(fs.readFileSync(tokenPath, 'utf8').trim());
} catch (err) {
    console.error('Token not found');
    process.exit(1);
}
`;
    return await runDockerExecDirect(containerName, ['node', '-e', script]);
}

app.post('/api/internal/configure-provider', requireInternal, async (req, res) => {
    const { instanceId, provider, token, authMethod, expiresIn, refreshToken } = req.body;
    if (!instanceId || !provider || !token) {
        return res.status(400).json({ error: 'instanceId, provider, and token are required' });
    }
    const containerName = `openclaw-${instanceId}`;
    try {
        // OpenClaw uses auth-profiles.json to store credentials and references them in openclaw.json.
        // We'll directly manipulate these files to configure the provider, mimicking what the
        // OpenClaw CLI does internally when you run `openclaw models auth paste-token`.
        
        const profileId = `${provider}:default`;
        
        // Step 1: Create/update auth-profiles.json with the credential
        const authProfileScript = `
const fs = require('fs');
const path = require('path');
const configDir = path.join(process.env.HOME, '.openclaw');
const authProfilesPath = path.join(configDir, 'auth-profiles.json');

// Read or create auth-profiles.json
let store = { version: 1, profiles: {} };
try {
    const content = fs.readFileSync(authProfilesPath, 'utf8');
    store = JSON.parse(content);
} catch (err) {
    // File doesn't exist or is invalid, start fresh
}

// Add/update the profile
store.profiles['${profileId}'] = {
    type: 'token',
    provider: '${provider}',
    token: '${token.replace(/'/g, "\\'")}',
    ${expiresIn ? `expires: ${Date.now() + parseInt(expiresIn)},` : ''}
    createdAt: ${Date.now()},
    updatedAt: ${Date.now()}
};

// Write back
fs.writeFileSync(authProfilesPath, JSON.stringify(store, null, 2));
console.log('auth-profile-updated');
`;
        
        await runDockerExecDirect(containerName, ['node', '-e', authProfileScript]);
        
        // Step 2: Update openclaw.json to reference this auth profile
        const configUpdateScript = `
const fs = require('fs');
const path = require('path');
const configPath = path.join(process.env.HOME, '.openclaw', 'openclaw.json');

// Read openclaw.json
let config = {};
try {
    const content = fs.readFileSync(configPath, 'utf8');
    config = JSON.parse(content);
} catch (err) {
    console.error('Failed to read config:', err.message);
    process.exit(1);
}

// Ensure auth structure exists
if (!config.auth) config.auth = {};
if (!config.auth.profiles) config.auth.profiles = {};

// Add profile reference
config.auth.profiles['${profileId}'] = {
    provider: '${provider}',
    mode: 'api_key'
};

// Write back
fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
console.log('config-updated');
`;
        
        await runDockerExecDirect(containerName, ['node', '-e', configUpdateScript]);
        
        // Step 3: Use gateway WebSocket to reload config (optional - gateway may not be running)
        try {
            const gatewayToken = await getGatewayToken(containerName);
            await gatewayWsExec(containerName, gatewayToken, 'config.apply', {});
        } catch (err) {
            console.warn(`[vps-agent] config.apply skipped (gateway token not found or not running): ${err.message}`);
        }
        
        // Store refresh token if provided (for future token refresh)
        if (refreshToken && authMethod === 'plugin-oauth') {
            console.log(`[vps-agent] stored refresh token for ${provider} (${authMethod})`);
        }
        
        console.log(`[vps-agent] configured provider ${provider} for ${instanceId} via auth-profiles.json (method: ${authMethod})`);
        res.json({ success: true, output: 'Provider configured', authMethod, profileId });
    } catch (err) {
        console.error(`[vps-agent] configure-provider failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/internal/set-model', requireInternal, async (req, res) => {
    const { instanceId, model } = req.body;
    if (!instanceId || !model) {
        return res.status(400).json({ error: 'instanceId and model are required' });
    }
    const containerName = `openclaw-${instanceId}`;
    try {
        const output = await runDockerExec(containerName, ['models', 'set', model]);
        // Also register the model in the agents.defaults.models config so it appears in allowedModels
        const configKey = `agents.defaults.models.${model.replace(/\//g, '.')}`;
        await runDockerExec(containerName, ['config', 'set', configKey, '{}']).catch(() => {});
        console.log(`[vps-agent] set model ${model} for ${instanceId}`);
        res.json({ success: true, output });
    } catch (err) {
        console.error(`[vps-agent] set-model failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Reserved provider keys that OpenClaw handles specially ─────────────────────
// Using these as custom provider keys causes OpenClaw to override baseUrl/auth.
const RESERVED_PROVIDER_KEYS = new Set([
    'azure', 'openai', 'anthropic', 'google', 'github-copilot',
    'openai-codex', 'opencode',
]);

function normalizeCustomProviderApi(api) {
    const value = String(api || 'openai').trim().toLowerCase();
    if (value === 'anthropic' || value === 'anthropic-messages') return 'anthropic-messages';
    if (value === 'google' || value === 'google-generative-ai') return 'google-generative-ai';
    if (value === 'openai' || value === 'openai-completions') return 'openai-completions';
    if (value === 'openai-responses') return 'openai-responses';
    return value;
}

function normalizeCustomProviderModelId(providerKey, rawId) {
    const trimmed = String(rawId || '').trim();
    if (!trimmed) return '';
    const firstSlash = trimmed.indexOf('/');
    const suffix = firstSlash >= 0 ? trimmed.slice(firstSlash + 1) : trimmed;
    return `${providerKey}/${suffix}`;
}

// ── Custom provider config ────────────────────────────────────────────────────
app.post('/api/internal/configure-custom-provider', requireInternal, async (req, res) => {
    const { instanceId, key, label, baseUrl, api, apiKey, authHeader, headers, models } = req.body;
    if (!instanceId || !key || !baseUrl) {
        return res.status(400).json({ error: 'instanceId, key, and baseUrl are required' });
    }

    if (RESERVED_PROVIDER_KEYS.has(key)) {
        return res.status(400).json({
            error: `"${key}" is a reserved provider key that OpenClaw handles internally (it overrides your baseUrl and auth settings). Use a different key like "${key}v1" or "custom-${key}" instead.`,
            code: 'RESERVED_KEY',
            suggestedKey: `${key}v1`,
        });
    }

    try {
        const config = readInstanceConfig(instanceId);
        if (!config) return res.status(404).json({ error: 'Config not found' });

        config.models = config.models || {};
        config.models.mode = config.models.mode || 'merge';
        config.models.providers = config.models.providers || {};

        const provider = {
            baseUrl: String(baseUrl).trim(),
            api: normalizeCustomProviderApi(api),
        };
        if (apiKey) provider.apiKey = apiKey;
        if (headers && typeof headers === 'object') provider.headers = headers;
        if (typeof label === 'string' && label.trim()) provider.label = label.trim();
        if (authHeader === true || String(authHeader || '').trim().toLowerCase() === 'authorization') {
            provider.authHeader = true;
        }
        if (Array.isArray(models) && models.length) {
            provider.models = models
                .map((m) => {
                    const sourceId = typeof m === 'string' ? m : m?.id;
                    const normalizedId = normalizeCustomProviderModelId(key, sourceId);
                    if (!normalizedId) return null;

                    const fallbackName = normalizedId.slice(normalizedId.indexOf('/') + 1);
                    return {
                        id: normalizedId,
                        name: (typeof m === 'object' && typeof m?.name === 'string' && m.name.trim())
                            ? m.name.trim()
                            : fallbackName
                    };
                })
                .filter(Boolean);
        }

        config.models.providers[key] = provider;

        writeInstanceConfig(instanceId, config);

        try {
            const gatewayToken = config.gateway?.auth?.token || await getGatewayToken(`openclaw-${instanceId}`);
            if (gatewayToken) {
                await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'config.apply', {});
            }
        } catch (err) {
            console.warn(`[vps-agent] custom provider config.apply skipped for ${instanceId}: ${err.message}`);
        }

        console.log(`[vps-agent] custom provider ${key} configured for ${instanceId}`);
        res.json({ success: true, providerKey: key, provider });
    } catch (err) {
        console.error(`[vps-agent] configure-custom-provider failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Helper: run a WebSocket method against the gateway inside a container ─────
function gatewayWsExec(containerName, gatewayToken, method, params, timeoutMs = 12000) {
    const connId = 'conn-' + Date.now();
    const reqId = 'req-' + Date.now();
    const paramsJson = JSON.stringify(params);
    const script = `
        let authenticated = false;
        const ws = new WebSocket('ws://localhost:18789');
        const timer = setTimeout(() => { process.stdout.write(JSON.stringify({ error: 'timeout' })); process.exit(0); }, ${timeoutMs});
        ws.addEventListener('message', (e) => {
            let parsed;
            try { parsed = JSON.parse(typeof e.data === 'string' ? e.data : ''); } catch { return; }
            if (parsed.event === 'connect.challenge') {
                ws.send(JSON.stringify({
                    type: 'req', method: 'connect', id: '${connId}',
                    params: {
                        minProtocol: 3, maxProtocol: 3,
                        client: { id: 'gateway-client', version: 'dev', platform: 'linux', mode: 'backend' },
                        caps: [], auth: { token: '${gatewayToken}' },
                        role: 'operator', scopes: ['operator.admin']
                    }
                }));
                return;
            }
            if (parsed.id === '${connId}' && !authenticated) {
                authenticated = true;
                ws.send(JSON.stringify({ type: 'req', method: '${method}', id: '${reqId}', params: ${paramsJson} }));
                return;
            }
            if (authenticated && parsed.id === '${reqId}') {
                clearTimeout(timer);
                process.stdout.write(JSON.stringify(parsed));
                ws.close();
            }
        });
        ws.addEventListener('error', (e) => {
            process.stdout.write(JSON.stringify({ error: 'ws_error', detail: e.message || 'connection failed' }));
            clearTimeout(timer);
            process.exit(0);
        });
    `.replace(/\n\s+/g, ' ');

    return run('docker', ['exec', containerName, 'node', '-e', script], { timeout: timeoutMs + 5000 })
        .then(output => { try { return JSON.parse(output); } catch { return { raw: output }; } });
}

// ── Agents list (WebSocket agents.list) ──────────────────────────────────────
app.post('/api/internal/agents-list', requireInternal, async (req, res) => {
    const { instanceId } = req.body;
    if (!instanceId) return res.status(400).json({ error: 'instanceId is required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'agents.list', {});
        const agents = result?.payload?.agents || result?.payload || [];
        res.json({ agents: Array.isArray(agents) ? agents : [] });
    } catch (err) {
        console.error(`[vps-agent] agents-list failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Agent config read (from openclaw.json) ───────────────────────────────────
app.post('/api/internal/agent-config', requireInternal, (req, res) => {
    const { instanceId, agentId } = req.body;
    if (!instanceId || !agentId) return res.status(400).json({ error: 'instanceId and agentId required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });

    const defaults = config.agents?.defaults || {};
    const agentOverrides = config.agents?.agents?.[agentId] || {};

    // Merge defaults + agent-specific overrides
    const model = agentOverrides.model || defaults.model || {};
    const identity = agentOverrides.identity || defaults.identity || {};

    res.json({
        id: agentId,
        agentId,
        model: typeof model === 'string' ? model : {
            primary: model.primary || '',
            fallbacks: model.fallbacks || []
        },
        identity: {
            name: identity.name || '',
            emoji: identity.emoji || ''
        }
    });
});

// ── Agent config update (writes to openclaw.json + config.apply via WS) ──────
app.post('/api/internal/agent-config-update', requireInternal, async (req, res) => {
    const { instanceId, agentId, updates } = req.body;
    if (!instanceId || !agentId) return res.status(400).json({ error: 'instanceId and agentId are required' });

    const configPath = path.join(INSTANCES_DIR, instanceId, 'openclaw.json');
    let config;
    try { config = JSON.parse(fs.readFileSync(configPath, 'utf8')); } catch {
        return res.status(404).json({ error: 'Config not found' });
    }

    config.agents = config.agents || {};
    config.agents.defaults = config.agents.defaults || {};

    // OpenClaw only supports agents.defaults — no per-agent overrides in config
    const target = config.agents.defaults;

    // Clean up any previously written invalid key
    if (config.agents.agents) delete config.agents.agents;

    if (updates.model) {
        target.model = target.model || {};
        if (updates.model.primary !== undefined) target.model.primary = updates.model.primary;
        if (updates.model.fallbacks !== undefined) target.model.fallbacks = updates.model.fallbacks;

        if (agentId === 'main') {
            config.agents.list = Array.isArray(config.agents.list) ? config.agents.list : [];
            const mainAgent = config.agents.list.find((agent) => agent?.id === 'main');
            if (mainAgent && updates.model.primary !== undefined) {
                mainAgent.model = updates.model.primary;
            }
        }
    }
    if (updates.identity) {
        target.identity = target.identity || {};
        if (updates.identity.name !== undefined) target.identity.name = updates.identity.name;
        if (updates.identity.emoji !== undefined) target.identity.emoji = updates.identity.emoji;
    }

    try {
        fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
        console.log(`[vps-agent] agent config updated for ${agentId} in ${instanceId}`);

        // Notify gateway to reload config
        const gatewayToken = config.gateway?.auth?.token;
        if (gatewayToken) {
            gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'config.apply', {}).catch(() => {});
        }

        res.json({ ok: true, agent: { id: agentId, model: target.model, identity: target.identity } });
    } catch (err) {
        console.error(`[vps-agent] agent-config-update write failed:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Models list (WebSocket models.list) ──────────────────────────────────────
app.post('/api/internal/models-list', requireInternal, async (req, res) => {
    const { instanceId } = req.body;
    if (!instanceId) return res.status(400).json({ error: 'instanceId is required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'models.list', {});
        res.json(result?.payload || result);
    } catch (err) {
        console.error(`[vps-agent] models-list failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Sessions list (WebSocket sessions.list → mapped to jobs) ─────────────────
app.post('/api/internal/sessions-list', requireInternal, async (req, res) => {
    const { instanceId, ids, limit, includeNarrative, includeLog } = req.body;
    if (!instanceId) return res.status(400).json({ error: 'instanceId is required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    const container = `openclaw-${instanceId}`;
    try {
        const result = await gatewayWsExec(container, gatewayToken, 'sessions.list', {});
        const sessions = result?.payload?.sessions || result?.payload || [];
        const list = Array.isArray(sessions) ? sessions : [];

        let jobs = [];
        let sessionsToEnrich = [];

        if (ids) {
            // Specific session(s) requested -- match by sessionId or key
            const idList = ids.split(',').map(s => s.trim());
            for (const id of idList) {
                const session = list.find(s => s.sessionId === id || s.key === id) || {};
                const key = session.key || id;
                sessionsToEnrich.push({ session, key });
            }
        } else {
            sessionsToEnrich = list.slice(0, limit || 100).map(s => ({ session: s, key: s.key || s.sessionKey }));
        }

        for (const { session, key } of sessionsToEnrich) {
            const job = mapSessionToJob(session, key);

            if (includeNarrative || includeLog || ids) {
                try {
                    const history = await gatewayWsExec(container, gatewayToken, 'chat.history', { sessionKey: key });
                    const messages = history?.payload?.messages || history?.payload || [];
                    if (Array.isArray(messages) && messages.length) {
                        enrichJobWithHistory(job, messages);
                    }
                } catch { /* keep basic info */ }
            }

            jobs.push(job);
        }

        res.json({ jobs });
    } catch (err) {
        console.error(`[vps-agent] sessions-list failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message, jobs: [] });
    }
});

// OpenClaw content can be: string, array of {type,text}, or empty
function extractContent(content) {
    if (typeof content === 'string') return content;
    if (Array.isArray(content)) return content.map(c => c.text || c.content || '').join('');
    return '';
}

function mapSessionToJob(session, key) {
    const sessionKey = key || session.key || '';
    const agentId = sessionKey.split(':')[1] || 'main';
    // Extract task ID from key like "agent:main:task-1234-abc" 
    const sessionPart = sessionKey.split(':')[2] || '';
    const isUiTask = sessionPart.startsWith('task-') || sessionPart.startsWith('bcast-');
    const name = session.displayName || session.origin?.label || (isUiTask ? sessionPart : agentId);

    let status = 'assigned';
    if (session.abortedLastRun) status = 'failed';
    else if (session.updatedAt && (Date.now() - session.updatedAt < 60_000)) status = 'picked_up';
    else if (session.outputTokens > 0) status = 'completed';

    const createdAt = session.updatedAt ? new Date(session.updatedAt).toISOString() : undefined;

    return {
        id: session.sessionId || sessionKey,
        name,
        status,
        agentId,
        model: session.model ? `${session.modelProvider || ''}/${session.model}` : '',
        payload: { message: '' },
        metadata: {
            status,
            agentId,
            priority: 3,
            createdAt,
            updatedAt: createdAt,
            message: '',
            channel: session.lastChannel || session.origin?.provider || 'webchat',
            inputTokens: session.inputTokens || 0,
            outputTokens: session.outputTokens || 0
        }
    };
}

// ── Chat send (generic WebSocket chat.send) ──────────────────────────────────
app.post('/api/internal/chat-send', requireInternal, async (req, res) => {
    const { instanceId, sessionKey, message, idempotencyKey } = req.body;
    if (!instanceId || !sessionKey || !message) {
        return res.status(400).json({ error: 'instanceId, sessionKey, and message are required' });
    }
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'chat.send', {
            sessionKey,
            message,
            idempotencyKey: idempotencyKey || `chat-${Date.now()}-${Math.random().toString(36).slice(2)}`
        });
        console.log(`[vps-agent] chat.send to ${sessionKey}:`, JSON.stringify(result).slice(0, 200));
        if (result.error && !result.ok) return res.status(400).json(result);
        res.json(result?.payload || result);
    } catch (err) {
        console.error(`[vps-agent] chat-send failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Agent delete (WebSocket agents.delete) ───────────────────────────────────
app.post('/api/internal/agents-delete', requireInternal, async (req, res) => {
    const { instanceId, agentId } = req.body;
    if (!instanceId || !agentId) return res.status(400).json({ error: 'instanceId and agentId are required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'agents.delete', { agentId });
        res.json(result?.payload || { ok: true });
    } catch (err) {
        console.error(`[vps-agent] agents-delete failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Sub-agent spawn (agents.create + chat.send via WebSocket) ────────────────
app.post('/api/internal/subagents-spawn', requireInternal, async (req, res) => {
    const { instanceId, task, label, model } = req.body;
    if (!instanceId || !task) {
        return res.status(400).json({ error: 'instanceId and task are required' });
    }
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });

    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    const container = `openclaw-${instanceId}`;
    const agentId = (label || 'sub-' + Date.now()).toLowerCase().replace(/[^a-z0-9-]/g, '-').slice(0, 30);

    try {
        // Step 1: Create the agent
        const createResult = await gatewayWsExec(container, gatewayToken, 'agents.create', {
            name: agentId,
            workspace: `/home/node/.openclaw/agents/${agentId}`
        });
        console.log(`[vps-agent] agent created ${agentId}:`, JSON.stringify(createResult).slice(0, 200));

        if (!createResult.ok && createResult.error) {
            return res.status(400).json({ error: createResult.error?.message || 'Failed to create agent' });
        }

        // Step 2: Send the initial task message
        const chatResult = await gatewayWsExec(container, gatewayToken, 'chat.send', {
            sessionKey: `agent:${agentId}:${agentId}`,
            message: task,
            idempotencyKey: `spawn-${Date.now()}-${Math.random().toString(36).slice(2)}`
        });
        console.log(`[vps-agent] chat.send to ${agentId}:`, JSON.stringify(chatResult).slice(0, 200));

        res.json({
            ok: true,
            agent: { id: agentId, name: label || agentId },
            chat: chatResult?.payload || chatResult
        });
    } catch (err) {
        console.error(`[vps-agent] subagents-spawn failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Channel management ────────────────────────────────────────────────────────

function writeInstanceConfig(instanceId, config) {
    const configPath = path.join(INSTANCES_DIR, instanceId, 'openclaw.json');
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
}

function isMem0PluginInstalled(instanceId) {
    const extensionDir = path.join(INSTANCES_DIR, instanceId, 'extensions', MEM0_PLUGIN_KEY);
    return fs.existsSync(extensionDir);
}

async function restartGateway(containerName) {
    await runDockerExec(containerName, ['gateway', 'restart']);
}

async function ensurePython3InContainer(containerName) {
    try {
        await runDockerExecDirect(containerName, ['python3', '--version']);
        return false;
    } catch {
        console.log(`[vps-agent] installing python3 in ${containerName}`);
        await runDockerExecAsRoot(containerName, ['sh', '-lc', 'apt-get update && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*']);
        return true;
    }
}

async function ensureMem0ForInstance(instanceId) {
    if (!validId(instanceId)) {
        throw new Error('Invalid instanceId');
    }
    if (!MEM0_API_KEY) {
        throw new Error('OPENCLAW_MEM0_API_KEY is not configured on the host');
    }

    const containerName = `openclaw-${instanceId}`;
    const config = readInstanceConfig(instanceId);
    if (!config) {
        throw new Error('Config not found');
    }

    const pythonInstalledNow = await ensurePython3InContainer(containerName);

    const alreadyInstalled = isMem0PluginInstalled(instanceId);
    let installedNow = false;
    if (!alreadyInstalled) {
        const currentSlot = config.plugins?.slots?.memory;
        if (currentSlot === MEM0_PLUGIN_KEY) {
            delete config.plugins.slots.memory;
            if (config.plugins.slots && Object.keys(config.plugins.slots).length === 0) {
                delete config.plugins.slots;
            }
            writeInstanceConfig(instanceId, config);
            console.log(`[vps-agent] cleared Mem0 memory slot before install for ${instanceId}`);
        }
        console.log(`[vps-agent] installing ${MEM0_PLUGIN_PACKAGE} in ${containerName}`);
        await runDockerExec(containerName, ['plugins', 'install', MEM0_PLUGIN_PACKAGE]);
        installedNow = true;
    }

    const configChanged = ensureMem0PluginConfig(config, instanceId);
    if (configChanged) {
        writeInstanceConfig(instanceId, config);
        console.log(`[vps-agent] wrote Mem0 config for ${instanceId}`);
    }

    if (pythonInstalledNow || installedNow || configChanged) {
        await restartGateway(containerName);
    }

    return {
        ok: true,
        installed: true,
        pythonInstalled: true,
        pythonInstalledNow,
        installedNow,
        configChanged,
        plugin: MEM0_PLUGIN_KEY,
        userId: instanceId,
    };
}

app.post('/api/internal/channels-add', requireInternal, async (req, res) => {
    const { instanceId, channel, token, slackBotToken, slackAppToken } = req.body;
    if (!instanceId || !channel) {
        return res.status(400).json({ error: 'instanceId and channel are required' });
    }
    try {
        const config = readInstanceConfig(instanceId);
        if (!config) return res.status(404).json({ error: 'Config not found' });

        config.channels = config.channels || {};

        if (channel === 'telegram') {
            config.channels.telegram = config.channels.telegram || {};
            config.channels.telegram.enabled = true;
            if (token) config.channels.telegram.botToken = token;
            config.channels.telegram.dmPolicy = config.channels.telegram.dmPolicy || 'open';
            config.channels.telegram.allowFrom = config.channels.telegram.allowFrom || ['*'];
            config.channels.telegram.groups = config.channels.telegram.groups || { '*': { requireMention: true } };
        } else if (channel === 'discord') {
            config.channels.discord = config.channels.discord || {};
            config.channels.discord.enabled = true;
            if (token) config.channels.discord.botToken = token;
        } else if (channel === 'slack') {
            config.channels.slack = config.channels.slack || {};
            config.channels.slack.enabled = true;
            if (slackBotToken) config.channels.slack.botToken = slackBotToken;
            if (slackAppToken) config.channels.slack.appToken = slackAppToken;
        } else {
            return res.status(400).json({ error: `Unsupported channel: ${channel}` });
        }

        writeInstanceConfig(instanceId, config);
        console.log(`[vps-agent] channels config written for ${channel} (${instanceId})`);

        // Restart gateway to pick up channel changes
        const containerName = `openclaw-${instanceId}`;
        try {
            await runDockerExec(containerName, ['gateway', 'restart']);
        } catch {
            // Gateway auto-reloads on config change, restart is best-effort
        }

        res.json({ success: true, output: `${channel} channel configured and gateway restarted` });
    } catch (err) {
        console.error(`[vps-agent] channels-add failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

function getChannelStatusFromConfig(instanceId) {
    const config = readInstanceConfig(instanceId);
    if (!config?.channels) return {};
    const result = {};
    for (const [ch, cfg] of Object.entries(config.channels)) {
        if (cfg && typeof cfg === 'object') {
            const hasToken = !!(cfg.botToken || cfg.token);
            result[ch] = {
                enabled: !!cfg.enabled,
                configured: cfg.enabled && hasToken,
                status: cfg.enabled && hasToken ? 'configured' : cfg.enabled ? 'enabled (no token)' : 'disabled'
            };
        }
    }
    return result;
}

app.get('/api/internal/channels-status', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    res.json({ parsed: { channels: getChannelStatusFromConfig(instanceId) } });
});

app.get('/api/internal/channels-list', requireInternal, (req, res) => {
    const { instanceId } = req.query;
    if (!instanceId) return res.status(400).json({ error: 'instanceId required' });
    res.json({ parsed: { channels: getChannelStatusFromConfig(instanceId) } });
});

app.post('/api/internal/channels-login', requireInternal, (req, res) => {
    const { instanceId, channel, verbose } = req.body;
    if (!instanceId || !channel) {
        return res.status(400).json({ error: 'instanceId and channel are required' });
    }
    const containerName = `openclaw-${instanceId}`;
    const args = ['channels', 'login', '--channel', channel];
    if (verbose) args.push('--verbose');

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Transfer-Encoding', 'chunked');

    const proc = execFile('docker', ['exec', containerName, OPENCLAW_BIN, ...args], { timeout: 120_000 });
    proc.stdout.on('data', (chunk) => res.write(chunk));
    proc.stderr.on('data', (chunk) => res.write(chunk));
    proc.on('close', () => res.end());
    proc.on('error', (err) => {
        if (!res.headersSent) res.status(500).json({ error: err.message });
        else res.end();
    });
    req.on('close', () => proc.kill());
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[vps-agent] port ${PORT}`);
    console.log(`[vps-agent] HOST_KIT_DIR=${HOST_KIT_DIR}`);
    console.log(`[vps-agent] INTERNAL_SECRET ${INTERNAL_SECRET ? 'set ✓' : 'NOT SET — all requests will be rejected!'}`);
    console.log(`[vps-agent] RUNTIME_IMAGE=${process.env.OPENCLAW_RUNTIME_IMAGE || 'openclaw-ttyd:latest (default)'}`);
    autoRegisterWithControlPlane();
});
