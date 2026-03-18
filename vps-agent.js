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
const COMPOSIO_SERVER_NAME = 'composio';
const COMPOSIO_MCP_URL = 'https://connect.composio.dev/mcp';
const COMPOSIO_HEADER_NAME = 'x-consumer-api-key';
const COMPOSIO_API_KEY = process.env.OPENCLAW_COMPOSIO_API_KEY || '';
const DEFAULT_PROVIDER_KEY = process.env.OPENCLAW_DEFAULT_PROVIDER_KEY || 'cloudflare-nemotron';
const DEFAULT_PROVIDER_BASE_URL = process.env.OPENCLAW_DEFAULT_PROVIDER_BASE_URL || '';
const DEFAULT_PROVIDER_API_KEY = process.env.OPENCLAW_DEFAULT_PROVIDER_API_KEY || '';
const DEFAULT_PROVIDER_API = process.env.OPENCLAW_DEFAULT_PROVIDER_API || 'openai-completions';
const DEFAULT_PRIMARY_MODEL_RAW = process.env.OPENCLAW_DEFAULT_PRIMARY_MODEL || `${DEFAULT_PROVIDER_KEY}/workers-ai/@cf/nvidia/nemotron-3-120b-a12b`;
const MANAGER_AGENT_ID = 'main';
const MANAGER_IDENTITY_NAME = 'Mission Manager';

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

function looksLikeTelegramBotToken(token) {
    return /^[0-9]{6,}:[A-Za-z0-9_-]{20,}$/.test(String(token || '').trim());
}

async function validateTelegramBotToken(token) {
    const trimmed = String(token || '').trim();
    if (!looksLikeTelegramBotToken(trimmed)) {
        return { ok: false, error: 'Telegram bot token format looks invalid.' };
    }

    try {
        const response = await fetch(`https://api.telegram.org/bot${trimmed}/getMe`, {
            method: 'GET',
            signal: AbortSignal.timeout(10_000),
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || data?.ok !== true) {
            return {
                ok: false,
                error: data?.description || `Telegram validation failed with HTTP ${response.status}`
            };
        }
        return { ok: true, username: data?.result?.username || '' };
    } catch (error) {
        return { ok: false, error: error?.message || 'Telegram validation request failed.' };
    }
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
        const composio = await ensureComposioForInstance(instanceId);
        const defaultModel = await ensureDefaultModelForInstance(instanceId);
        const managerConfig = readInstanceConfig(instanceId);
        if (managerConfig) {
            removeBootstrapFilesForInstance(instanceId, managerConfig);
            ensureManagerProfileForInstance(instanceId, managerConfig, { force: false });
            writeInstanceConfig(instanceId, managerConfig);
        }

        const result = {
            ok: true,
            instanceId,
            containerName: `openclaw-${instanceId}`,
            gatewayToken,
            mem0,
            composio,
            defaultModel,
            manager: { enabled: true, agentId: MANAGER_AGENT_ID, identity: MANAGER_IDENTITY_NAME },
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
const TASKS_STORE_FILE = 'mission-control-tasks.json';

function readInstanceConfig(instanceId) {
    const configPath = path.join(INSTANCES_DIR, instanceId, 'openclaw.json');
    if (!fs.existsSync(configPath)) return null;
    try {
        const parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        const { config: sanitizedConfig, changed } = sanitizeInstanceConfig(parsed);
        if (changed) {
            fs.writeFileSync(configPath, JSON.stringify(sanitizedConfig, null, 2), 'utf8');
        }
        return sanitizedConfig;
    } catch {
        return null;
    }
}

function getTasksStorePath(instanceId) {
    return path.join(INSTANCES_DIR, instanceId, TASKS_STORE_FILE);
}

function getMainWorkspace(instanceId) {
    return path.join(INSTANCES_DIR, instanceId, 'workspace');
}

function getAgentWorkspacePath(instanceId, agentId) {
    return agentId === MANAGER_AGENT_ID
        ? getMainWorkspace(instanceId)
        : getAgentHostWorkspace(instanceId, agentId);
}

function ensureDirSync(dirPath) {
    fs.mkdirSync(dirPath, { recursive: true });
}

function removeBootstrapFiles(workspacePath) {
    for (const name of ['BOOTSTRAP.md', 'bootstrap.md']) {
        const filePath = path.join(workspacePath, name);
        if (fs.existsSync(filePath)) {
            fs.rmSync(filePath, { force: true });
        }
    }
}

function removeBootstrapFilesForInstance(instanceId, config = null) {
    const resolvedConfig = config || readInstanceConfig(instanceId) || {};
    removeBootstrapFiles(getMainWorkspace(instanceId));
    const agents = Array.isArray(resolvedConfig?.agents?.list) ? resolvedConfig.agents.list : [];
    for (const agent of agents) {
        if (!agent?.id || agent.id === MANAGER_AGENT_ID) continue;
        removeBootstrapFiles(getAgentWorkspacePath(instanceId, agent.id));
    }
}

function normalizeTagList(values = []) {
    return Array.from(new Set(
        (Array.isArray(values) ? values : [])
            .map((value) => String(value || '').trim().toLowerCase())
            .filter(Boolean)
    ));
}

function normalizeObjectList(values = []) {
    return Array.isArray(values)
        ? values
            .filter((value) => value && typeof value === 'object')
            .map((value) => Object.fromEntries(
                Object.entries(value).map(([key, entry]) => [key, typeof entry === 'string' ? entry.trim() : entry])
            ))
        : [];
}

function stripMarkdown(markdown) {
    return String(markdown || '')
        .replace(/<!--[\s\S]*?-->/g, ' ')
        .replace(/```[\s\S]*?```/g, ' ')
        .replace(/`([^`]+)`/g, '$1')
        .replace(/!\[[^\]]*]\([^)]+\)/g, ' ')
        .replace(/\[[^\]]+]\([^)]+\)/g, '$1')
        .replace(/^>\s?/gm, '')
        .replace(/^#+\s*/gm, '')
        .replace(/[*_~]/g, '')
        .replace(/\s+/g, ' ')
        .trim();
}

function extractEmbeddedJson(text) {
    const normalized = String(text || '').trim();
    if (!normalized) return null;

    const direct = extractJsonObject(normalized);
    if (direct) return direct;

    const fenceMatch = normalized.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenceMatch) {
        const fromFence = extractJsonObject(fenceMatch[1]);
        if (fromFence) return fromFence;
    }

    return null;
}

function extractMissionControlProfile(markdown) {
    const match = String(markdown || '').match(/<!--\s*mission-control-profile\s*([\s\S]*?)-->/i);
    if (!match) return null;
    try {
        const parsed = JSON.parse(match[1].trim());
        return {
            ...parsed,
            capabilities: normalizeTagList(parsed?.capabilities),
            toolkits: normalizeTagList(parsed?.toolkits),
            taskTypes: normalizeTagList(parsed?.taskTypes),
            connectedApps: normalizeTagList(parsed?.connectedApps),
            responsibilities: normalizeObjectList(parsed?.responsibilities),
        };
    } catch {
        return null;
    }
}

function extractBulletValues(markdown, heading) {
    const text = String(markdown || '');
    const escapedHeading = heading.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const match = text.match(new RegExp(`^##\\s+${escapedHeading}\\s*$([\\s\\S]*?)(?=^##\\s+|$)`, 'im'));
    if (!match) return [];
    return match[1]
        .split('\n')
        .map((line) => line.trim())
        .filter((line) => line.startsWith('- '))
        .map((line) => line.slice(2).trim())
        .filter(Boolean);
}

function extractInlineProfileValues(markdown, label) {
    const text = String(markdown || '');
    const escapedLabel = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp(`^[-*]\\s+${escapedLabel}:\\s*(.+)$`, 'gim');
    const values = [];
    let match;
    while ((match = regex.exec(text)) !== null) {
        values.push(...match[1].split(',').map((value) => value.trim()).filter(Boolean));
    }
    return values;
}

function renderMissionControlProfile(profile) {
    return [
        '<!-- mission-control-profile',
        JSON.stringify(profile, null, 2),
        '-->'
    ].join('\n');
}

function readTasksStore(instanceId) {
    const storePath = getTasksStorePath(instanceId);
    if (!fs.existsSync(storePath)) return [];
    try {
        const parsed = JSON.parse(fs.readFileSync(storePath, 'utf8'));
        return Array.isArray(parsed) ? parsed : [];
    } catch {
        return [];
    }
}

function writeTasksStore(instanceId, tasks) {
    fs.writeFileSync(getTasksStorePath(instanceId), JSON.stringify(tasks, null, 2) + '\n', 'utf8');
}

function upsertTaskRecord(instanceId, taskRecord) {
    const tasks = readTasksStore(instanceId);
    const nextTasks = tasks.filter((task) => task?.id !== taskRecord.id);
    nextTasks.unshift(taskRecord);
    writeTasksStore(instanceId, nextTasks.slice(0, 1000));
}

function getTaskRecord(instanceId, taskId) {
    return readTasksStore(instanceId).find((task) => task?.id === taskId) || null;
}

function extractJsonObject(rawOutput) {
    const text = String(rawOutput || '').trim();
    if (!text) return null;

    const lines = text.split('\n');
    for (let index = 0; index < lines.length; index += 1) {
        const candidate = lines.slice(index).join('\n').trim();
        if (!candidate.startsWith('{')) continue;
        try {
            return JSON.parse(candidate);
        } catch {
            continue;
        }
    }

    return null;
}

function extractAgentRunEnvelope(parsed) {
    if (!parsed || typeof parsed !== 'object') {
        return { payloads: [], meta: {}, summary: '' };
    }

    const payloads = Array.isArray(parsed?.payloads)
        ? parsed.payloads
        : Array.isArray(parsed?.result?.payloads)
            ? parsed.result.payloads
            : [];

    const meta = parsed?.meta && typeof parsed.meta === 'object'
        ? parsed.meta
        : parsed?.result?.meta && typeof parsed.result.meta === 'object'
            ? parsed.result.meta
            : {};

    const summary = String(parsed?.summary || parsed?.result?.summary || '').trim();
    return { payloads, meta, summary };
}

const CAPABILITY_RULES = [
    { tag: 'triage', keywords: ['triage', 'route', 'assign', 'delegate', 'manager', 'coordinate', 'coordination', 'orchestrate'] },
    { tag: 'research', keywords: ['research', 'investigate', 'analyze', 'analysis', 'deep dive', 'compare', 'internet', 'web'] },
    { tag: 'writing', keywords: ['write', 'writer', 'draft', 'copy', 'content', 'article', 'blog', 'summarize', 'summary'] },
    { tag: 'email', keywords: ['email', 'mail', 'outreach', 'reply', 'inbox'] },
    { tag: 'communication', keywords: ['communication', 'message', 'comment', 'respond', 'customer', 'support'] },
    { tag: 'coding', keywords: ['code', 'implement', 'debug', 'fix', 'refactor', 'api', 'backend', 'frontend', 'javascript', 'react'] },
    { tag: 'design', keywords: ['design', 'ui', 'ux', 'figma', 'layout', 'visual'] },
    { tag: 'docs', keywords: ['docs', 'documentation', 'readme', 'spec', 'proposal', 'brief'] },
    { tag: 'operations', keywords: ['deploy', 'deployment', 'infra', 'ops', 'server', 'docker', 'monitor', 'provision'] },
    { tag: 'planning', keywords: ['plan', 'roadmap', 'strategy', 'prioritize', 'decision'] },
];

const TOOLKIT_RULES = [
    { tag: 'gmail', keywords: ['gmail', 'email', 'mail'] },
    { tag: 'slack', keywords: ['slack'] },
    { tag: 'github', keywords: ['github', 'pull request', 'issue'] },
    { tag: 'notion', keywords: ['notion'] },
    { tag: 'calendar', keywords: ['calendar', 'meeting'] },
];

const HIGH_RISK_ACTION_RULES = [
    { tag: 'send_email', keywords: ['send an email', 'write a mail', 'send mail', 'email ', 'gmail'] },
    { tag: 'publish', keywords: ['publish', 'post publicly', 'send to client', 'ship it live'] },
    { tag: 'delete', keywords: ['delete', 'remove permanently', 'destroy'] },
];

const STRICT_CONNECTION_APPS = new Set(['gmail', 'slack', 'calendar']);

function inferTagsFromText(text, rules) {
    const haystack = String(text || '').toLowerCase();
    return rules
        .filter((rule) => rule.keywords.some((keyword) => haystack.includes(keyword)))
        .map((rule) => rule.tag);
}

function inferAgentProfile(agent) {
    const combined = [
        agent?.label,
        agent?.files?.identityMd,
        agent?.files?.soulMd,
        agent?.files?.agentsMd,
    ].join('\n');
    const embedded = extractMissionControlProfile(agent?.files?.agentsMd);
    const inlineCapabilities = normalizeTagList([
        ...extractInlineProfileValues(agent?.files?.agentsMd, 'Capabilities'),
        ...extractBulletValues(agent?.files?.agentsMd, 'Capabilities')
    ]);
    const inlineToolkits = normalizeTagList([
        ...extractInlineProfileValues(agent?.files?.agentsMd, 'Tools / apps'),
        ...extractInlineProfileValues(agent?.files?.agentsMd, 'Tools/apps'),
        ...extractBulletValues(agent?.files?.agentsMd, 'Tools / Apps')
    ]);
    const inferredCapabilities = inferTagsFromText(combined, CAPABILITY_RULES);
    const inferredToolkits = inferTagsFromText(combined, TOOLKIT_RULES);
    const inferredTaskTypes = normalizeTagList([...inlineCapabilities, ...inlineToolkits, ...inferredCapabilities, ...inferredToolkits]);
    const profile = {
        role: embedded?.role || (agent?.id === MANAGER_AGENT_ID ? 'manager' : inferredCapabilities[0] || 'generalist'),
        capabilities: normalizeTagList([...(embedded?.capabilities || []), ...inlineCapabilities, ...inferredCapabilities]),
        toolkits: normalizeTagList([...(embedded?.toolkits || []), ...inlineToolkits, ...inferredToolkits]),
        taskTypes: normalizeTagList([...(embedded?.taskTypes || []), ...inferredTaskTypes]),
        connectedApps: normalizeTagList(embedded?.connectedApps || []),
        responsibilities: normalizeObjectList(embedded?.responsibilities || []),
        canTakeExternalAction: Boolean(embedded?.canTakeExternalAction) || inferredToolkits.includes('gmail') || inferredCapabilities.includes('email'),
        summary: embedded?.summary || stripMarkdown(combined).slice(0, 320),
    };

    if (agent?.id === MANAGER_AGENT_ID) {
        profile.capabilities = normalizeTagList(['triage', 'planning', 'coordination', 'review', 'research', 'writing', ...profile.capabilities]);
        profile.taskTypes = normalizeTagList(['triage', 'planning', 'research', 'writing', ...profile.taskTypes]);
    }

    return profile;
}

function inferTaskRequirements(message) {
    const raw = String(message || '').trim();
    const lower = raw.toLowerCase();
    const capabilities = normalizeTagList(inferTagsFromText(lower, CAPABILITY_RULES));
    const requiredApps = normalizeTagList(inferTagsFromText(lower, TOOLKIT_RULES));
    const highRiskActions = normalizeTagList(inferTagsFromText(lower, HIGH_RISK_ACTION_RULES));
    const needsExternalAction = requiredApps.length > 0 || highRiskActions.length > 0;

    if (needsExternalAction && !capabilities.includes('communication')) capabilities.push('communication');
    if (requiredApps.includes('gmail') && !capabilities.includes('email')) capabilities.push('email');
    if (/research|investigate|find|internet|reddit|blog|article/.test(lower) && !capabilities.includes('research')) capabilities.push('research');
    if (/write|draft|proposal|summary|mail|email/.test(lower) && !capabilities.includes('writing')) capabilities.push('writing');

    return {
        summary: raw.slice(0, 240),
        capabilities,
        requiredApps,
        highRiskActions,
        needsExternalAction,
        needsApproval: highRiskActions.some((action) => action === 'publish' || action === 'delete' || action === 'send_email'),
    };
}

function buildAgentRoster(instanceId, config) {
    const configuredAgents = Array.isArray(config?.agents?.list) ? config.agents.list : [];
    const defaultsModel = config?.agents?.defaults?.model || {};
    const rosterMap = new Map();

    const allAgents = configuredAgents.some((agent) => agent?.id === MANAGER_AGENT_ID)
        ? configuredAgents
        : [{ id: MANAGER_AGENT_ID, name: MANAGER_IDENTITY_NAME }, ...configuredAgents];

    for (const entry of allAgents) {
        if (!entry?.id) continue;
        const files = readAgentProfile(instanceId, entry.id).files;
        const model = normalizeModelOverride(entry.model, defaultsModel);
        const profile = inferAgentProfile({
            id: entry.id,
            label: entry.name || entry.id,
            files
        });
        rosterMap.set(entry.id, {
            id: entry.id,
            label: entry.name || entry.id,
            model: model.primary,
            fallbacks: model.fallbacks,
            files,
            profile
        });
    }

    return Array.from(rosterMap.values());
}

function scoreAgentForTask(agent, requirements, preferredAgentId) {
    const capabilitySet = new Set(agent.profile.capabilities || []);
    const toolkitSet = new Set([...(agent.profile.toolkits || []), ...(agent.profile.connectedApps || [])]);
    let score = 0;
    const reasons = [];

    for (const capability of requirements.capabilities) {
        if (capabilitySet.has(capability)) {
            score += 4;
            reasons.push(`matches capability "${capability}"`);
        }
    }

    for (const app of requirements.requiredApps) {
        if (toolkitSet.has(app)) {
            score += 5;
            reasons.push(`has toolkit "${app}"`);
        }
    }

    if (preferredAgentId && agent.id === preferredAgentId) {
        score += 3;
        reasons.push('requested explicitly');
    }

    if (agent.id === MANAGER_AGENT_ID) {
        score -= 1;
        reasons.push('kept slightly behind specialists to preserve manager capacity');
    }

    if (requirements.needsExternalAction && !agent.profile.canTakeExternalAction) {
        score -= 4;
    }

    return { score, reasons };
}

function chooseHeuristicAssignee(roster, requirements, preferredAgentId) {
    const ranked = roster
        .map((agent) => ({ agent, ...scoreAgentForTask(agent, requirements, preferredAgentId) }))
        .sort((left, right) => right.score - left.score);

    const top = ranked[0] || null;
    const topToolkitSet = new Set([...(top?.agent?.profile?.toolkits || []), ...(top?.agent?.profile?.connectedApps || [])]);
    const missingRequiredApp = requirements.requiredApps.some((app) => !topToolkitSet.has(app));
    const rankedSpecialists = ranked.filter((entry) => entry.agent?.id !== MANAGER_AGENT_ID);
    const topSpecialist = rankedSpecialists[0] || null;
    const specialistToolkitSet = new Set([...(topSpecialist?.agent?.profile?.toolkits || []), ...(topSpecialist?.agent?.profile?.connectedApps || [])]);
    const specialistMissingRequiredApp = requirements.requiredApps.some((app) => !specialistToolkitSet.has(app));
    if (!top || top.score < 3) {
        return {
            assignee: null,
            ranked,
            reason: requirements.needsExternalAction
                ? 'No agent currently advertises the toolkit or trust level needed for this external action.'
                : 'No specialist in the current team clearly matches this task.'
        };
    }
    if (top.agent?.id === MANAGER_AGENT_ID && topSpecialist && topSpecialist.score >= 3 && (!requirements.needsExternalAction || !specialistMissingRequiredApp)) {
        return {
            assignee: topSpecialist.agent,
            ranked,
            reason: topSpecialist.reasons[0] || 'A specialist is a better fit than keeping the task on the manager.'
        };
    }
    const requiresStrictConnection = taskRequiresStrictConnection(requirements);

    if (requiresStrictConnection && requirements.needsExternalAction && missingRequiredApp) {
        return {
            assignee: null,
            ranked,
            reason: 'No agent currently advertises the required connected app for this task.'
        };
    }

    return {
        assignee: top.agent,
        ranked,
        reason: top.reasons[0] || 'Best capability match in the current team.'
    };
}

function taskRequiresStrictConnection(requirements) {
    return requirements.highRiskActions.length > 0
        || requirements.requiredApps.some((app) => STRICT_CONNECTION_APPS.has(app));
}

function parseManagerDecision(text) {
    const parsed = extractEmbeddedJson(text);
    if (!parsed || typeof parsed !== 'object') return null;
    const decision = String(parsed.decision || '').trim().toLowerCase();
    if (!['assign', 'blocked', 'awaiting_connection', 'awaiting_approval'].includes(decision)) {
        return null;
    }
    return {
        decision,
        agentId: String(parsed.agentId || '').trim(),
        reason: String(parsed.reason || '').trim(),
        requiredCapabilities: normalizeTagList(parsed.requiredCapabilities || []),
        requiredApps: normalizeTagList(parsed.requiredApps || []),
        notes: Array.isArray(parsed.notes) ? parsed.notes.map((value) => String(value || '').trim()).filter(Boolean) : [],
    };
}

function parseTaskOutcome(text) {
    const summaryText = String(text || '').trim();
    const parsed = extractEmbeddedJson(summaryText);
    if (parsed && typeof parsed === 'object') {
        return {
            status: String(parsed.status || '').trim().toLowerCase(),
            summary: String(parsed.summary || '').trim(),
            needs: Array.isArray(parsed.needs) ? parsed.needs.map((value) => String(value || '').trim()).filter(Boolean) : [],
            evidence: Array.isArray(parsed.evidence) ? parsed.evidence.map((value) => String(value || '').trim()).filter(Boolean) : [],
            followUps: Array.isArray(parsed.followUps) ? parsed.followUps.map((value) => String(value || '').trim()).filter(Boolean) : [],
        };
    }

    const normalized = summaryText.toLowerCase();
    if (/billing error|insufficient balance|insufficient credits|out of credits|run out of credits|quota exceeded|credit balance|payment required/.test(normalized)) {
        return {
            status: 'failed',
            summary: summaryText || 'The model provider rejected the run because the API key has no available credits.',
            needs: ['Restore provider credits or switch this agent to a funded model/API key.'],
            evidence: [],
            followUps: ['Top up the provider balance or change the agent model, then retry the task.']
        };
    }
    if (/invalid api key|api key invalid|authentication failed|unauthorized|forbidden/.test(normalized) && /openrouter|openai|anthropic|provider|model/.test(normalized)) {
        return {
            status: 'failed',
            summary: summaryText || 'The model provider rejected the run because the API key or provider access is invalid.',
            needs: ['A valid provider API key with access to the selected model.'],
            evidence: [],
            followUps: ['Replace the provider API key or switch to a model with valid provider access, then retry the task.']
        };
    }
    if (/connect|authorize|not connected|missing account|authentication/.test(normalized)) {
        return { status: 'awaiting_connection', summary: summaryText, needs: [], evidence: [], followUps: [] };
    }
    if (/approve|approval|review before send|draft ready|ready for review/.test(normalized)) {
        return { status: 'awaiting_approval', summary: summaryText, needs: [], evidence: [], followUps: [] };
    }
    if (/unable|cannot|can't|blocked|no suitable/.test(normalized)) {
        return { status: 'blocked', summary: summaryText, needs: [], evidence: [], followUps: [] };
    }

    return {
        status: 'completed',
        summary: summaryText,
        needs: [],
        evidence: [],
        followUps: []
    };
}

function extractUrlsFromText(text) {
    const matches = String(text || '').match(/https?:\/\/[^\s)"'`]+/g);
    return matches ? Array.from(new Set(matches)) : [];
}

function summarizeToolFailures(output) {
    const text = String(output || '');
    const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);
    const errorLines = lines.filter((line) =>
        /MCP error|Tool .* not found|No connection found|Authentication in progress|waiting for user to complete|missing scope|failed|billing error|insufficient balance|out of credits|quota exceeded|invalid api key|unauthorized/i.test(line)
    );
    return Array.from(new Set(errorLines)).slice(-5);
}

function inferExternalActionGuards({ requirements, workerRun }) {
    const output = String(workerRun?.output || '');
    const text = String(workerRun?.text || '');
    const mergedText = `${text}\n${output}`;
    const urls = extractUrlsFromText(mergedText);
    const errors = summarizeToolFailures(output);
    const requiredApps = normalizeTagList(requirements?.requiredApps || []);
    const highRiskActions = normalizeTagList(requirements?.highRiskActions || []);
    const needsExternalAction = Boolean(requirements?.needsExternalAction);

    if (!needsExternalAction) {
        return { status: null, summary: '', evidence: [], needs: [], followUps: [] };
    }

    if (/billing error|insufficient balance|insufficient credits|out of credits|run out of credits|quota exceeded|invalid api key|unauthorized|forbidden/i.test(mergedText)) {
        return {
            status: 'failed',
            summary: 'The selected model provider failed before the agent could complete the task.',
            evidence: errors.length ? errors : [text].filter(Boolean).slice(0, 1),
            needs: ['A funded and valid provider API key for the selected model.'],
            followUps: ['Restore provider credits or switch the task to a working model, then retry.']
        };
    }

    const hasConnectionIssue = /No connection found|Authentication in progress|waiting for user to complete|authorize|not initiated/i.test(mergedText);

    if (requiredApps.includes('notion')) {
        const mentionsCreatedPage = /created?.{0,40}notion page|page creation|document created|created successfully/i.test(mergedText);
        const hasNotionUrl = urls.some((url) => /notion\.so/i.test(url));

        if (hasConnectionIssue) {
            return {
                status: 'awaiting_connection',
                summary: 'Notion is not fully connected for this user session yet.',
                evidence: errors,
                needs: ['A valid user-scoped Notion connection for this Mission Control user.'],
                followUps: ['Reconnect Notion from Mission Control and refresh the instance MCP session before retrying.']
            };
        }

        if (mentionsCreatedPage && !hasNotionUrl) {
            return {
                status: 'failed',
                summary: 'The run claimed a Notion page was created, but it did not return a Notion page URL.',
                evidence: [...errors, ...urls].slice(0, 5),
                needs: ['A verifiable Notion page URL or page ID from the tool result.'],
                followUps: ['Retry page creation and only mark the task complete after returning the Notion page URL.']
            };
        }
    }

    if ((requiredApps.length > 0 || highRiskActions.length > 0) && errors.length) {
        return {
            status: 'failed',
            summary: 'The run hit tool errors while attempting the external action.',
            evidence: errors,
            needs: requiredApps,
            followUps: ['Resolve the tool error and retry with a verified outcome.']
        };
    }

    return { status: null, summary: '', evidence: [], needs: [], followUps: [] };
}

function mapStoredTaskToJob(task, options = {}) {
    const { includeNarrative = false, includeLog = false } = options;
    const narrative = Array.isArray(task?.narrative) ? task.narrative : [];
    const log = Array.isArray(task?.log) ? task.log : [];

    const metadata = {
        status: task?.status || 'assigned',
        agentId: task?.agentId || 'main',
        managerAgentId: task?.managerAgentId || MANAGER_AGENT_ID,
        assignedAgentId: task?.assignedAgentId || task?.agentId || MANAGER_AGENT_ID,
        requestedAgentId: task?.requestedAgentId || '',
        priority: Number(task?.priority) || 3,
        createdAt: task?.createdAt || null,
        updatedAt: task?.updatedAt || null,
        message: task?.message || '',
        channel: 'mission-control',
        lastRun: task?.lastRun || null,
        requiredCapabilities: normalizeTagList(task?.requiredCapabilities || []),
        requiredApps: normalizeTagList(task?.requiredApps || []),
        manager: task?.manager || null,
        approval: task?.approval || null
    };

    if (includeNarrative) metadata.narrative = narrative;
    if (includeLog) metadata.log = log;
    if (task?.lastDecision) metadata.lastDecision = task.lastDecision;

    return {
        id: task.id,
        name: task.name || task.id,
        status: task.status || 'assigned',
        agentId: task.assignedAgentId || task.agentId || MANAGER_AGENT_ID,
        model: task.model || '',
        payload: {
            message: task.message || ''
        },
        metadata
    };
}

async function executeTaskRecord(instanceId, taskRecord) {
    const containerName = `openclaw-${instanceId}`;
    const startedAt = new Date().toISOString();
    let workingTask = { ...taskRecord };
    const config = readInstanceConfig(instanceId) || {};
    removeBootstrapFilesForInstance(instanceId, config);
    ensureManagerProfileForInstance(instanceId, config, { force: false });
    const roster = buildAgentRoster(instanceId, config);
    const requirements = inferTaskRequirements(taskRecord.message);
    const approvalGranted = String(taskRecord?.approval?.status || '').trim().toLowerCase() === 'approved';
    const effectiveRequirements = approvalGranted
        ? { ...requirements, needsApproval: false }
        : requirements;
    const preferredAgentId = approvalGranted
        ? String(taskRecord.assignedAgentId || taskRecord.requestedAgentId || '').trim()
        : String(taskRecord.requestedAgentId || '').trim();

    const managerPrompt = [
        'You are the Mission Control manager agent.',
        'Decide who should own the task from the current team.',
        'Return ONLY valid JSON with this exact shape:',
        '{"decision":"assign|blocked|awaiting_connection|awaiting_approval","agentId":"agent-id-or-empty","reason":"short explanation","requiredCapabilities":["capability"],"requiredApps":["toolkit"],"notes":["optional note"]}',
        'Rules:',
        '- Prefer the best-fit specialist instead of keeping work on main when a specialist is reasonably qualified.',
        '- If no one on the team is clearly suitable, return "blocked".',
        '- If the task needs an app connection or auth that is likely missing, return "awaiting_connection".',
        '- If the task needs human sign-off before sending/publishing/deleting, return "awaiting_approval".',
        '- Never call a task completed at this stage. You are only routing it.',
        '',
        `Task: ${taskRecord.message}`,
        `Preferred assignee: ${preferredAgentId || 'none'}`,
        `Inferred requirements: ${JSON.stringify(effectiveRequirements)}`,
        approvalGranted ? `Human approval already granted: ${JSON.stringify(taskRecord.approval)}` : '',
        `Team roster: ${JSON.stringify(roster.map((agent) => ({
            id: agent.id,
            label: agent.label,
            model: agent.model,
            capabilities: agent.profile.capabilities,
            toolkits: agent.profile.toolkits,
            canTakeExternalAction: agent.profile.canTakeExternalAction,
            summary: agent.profile.summary
        })))}`
    ].join('\n');

    const runAgentCli = async ({ agentId, sessionId, message }) => {
        const output = await runDockerExec(containerName, [
            'agent',
            '--agent', agentId,
            '--session-id', sessionId,
            '--message', message,
            '--json'
        ]);
        const parsed = extractJsonObject(output);
        const envelope = extractAgentRunEnvelope(parsed);
        const text = envelope.payloads.map((entry) => entry?.text).filter(Boolean).join('\n\n').trim()
            || envelope.meta?.lastDecision?.reason
            || envelope.summary
            || output;
        const modelProvider = envelope.meta?.agentMeta?.provider || '';
        const modelName = envelope.meta?.agentMeta?.model || '';
        return {
            output,
            parsed,
            text: String(text || '').trim(),
            model: modelProvider && modelName ? `${modelProvider}/${modelName}` : ''
        };
    };

    const heuristicDecision = chooseHeuristicAssignee(roster, effectiveRequirements, preferredAgentId);
    let managerDecision = null;

    workingTask = {
        ...workingTask,
        status: 'triage',
        agentId: MANAGER_AGENT_ID,
        managerAgentId: MANAGER_AGENT_ID,
        requestedAgentId: preferredAgentId,
        requiredCapabilities: effectiveRequirements.capabilities,
        requiredApps: effectiveRequirements.requiredApps,
        updatedAt: startedAt,
        log: [...(taskRecord.log || []), { ts: startedAt, role: 'system', text: 'Task handed to Mission Manager for triage' }]
    };
    upsertTaskRecord(instanceId, workingTask);

    try {
        const managerRun = await runAgentCli({
            agentId: MANAGER_AGENT_ID,
            sessionId: `${taskRecord.id}-manager`,
            message: managerPrompt
        }).catch(() => null);

        if (managerRun?.text) {
            managerDecision = parseManagerDecision(managerRun.text);
        }

        if (!managerDecision) {
            if (heuristicDecision.assignee) {
                managerDecision = {
                    decision: 'assign',
                    agentId: heuristicDecision.assignee.id,
                    reason: heuristicDecision.reason,
                    requiredCapabilities: effectiveRequirements.capabilities,
                    requiredApps: effectiveRequirements.requiredApps,
                    notes: ['Fallback routing logic used because manager response was not parseable.']
                };
            } else {
                managerDecision = {
                    decision: effectiveRequirements.needsExternalAction ? 'awaiting_connection' : 'blocked',
                    agentId: '',
                    reason: heuristicDecision.reason,
                    requiredCapabilities: effectiveRequirements.capabilities,
                    requiredApps: effectiveRequirements.requiredApps,
                    notes: ['Fallback routing logic used because manager response was not parseable.']
                };
            }
        }

        const rosterAgentIds = new Set(roster.map((agent) => agent.id));
        if (managerDecision.agentId && !rosterAgentIds.has(managerDecision.agentId)) {
            managerDecision.agentId = '';
        }

        if (!managerDecision.agentId
            && (managerDecision.decision === 'awaiting_approval' || managerDecision.decision === 'awaiting_connection')
            && heuristicDecision.assignee) {
            managerDecision.agentId = heuristicDecision.assignee.id;
            managerDecision.notes = [
                ...(managerDecision.notes || []),
                'Mission Control preserved the best-fit assignee so the paused task can resume with the right owner.'
            ];
        }

        if (managerDecision.decision === 'assign'
            && managerDecision.agentId === MANAGER_AGENT_ID
            && heuristicDecision.assignee
            && heuristicDecision.assignee.id !== MANAGER_AGENT_ID) {
            managerDecision.agentId = heuristicDecision.assignee.id;
            managerDecision.reason = `Main kept routing authority, but ${heuristicDecision.assignee.id} is the stronger specialist match. ${managerDecision.reason || heuristicDecision.reason}`;
            managerDecision.notes = [...(managerDecision.notes || []), 'Mission Control overrode a manager self-assignment in favor of a specialist.'];
        }

        if (managerDecision.decision !== 'assign'
            && heuristicDecision.assignee
            && !taskRequiresStrictConnection(effectiveRequirements)) {
            const previousDecision = managerDecision.decision;
            managerDecision.decision = 'assign';
            managerDecision.agentId = heuristicDecision.assignee.id;
            managerDecision.reason = `Manager marked the task as ${previousDecision}, but ${heuristicDecision.assignee.id} is a credible specialist match. ${heuristicDecision.reason}`;
            managerDecision.notes = [...(managerDecision.notes || []), 'Mission Control overrode a non-assignment because a specialist match was available.'];
        }

        if (approvalGranted && managerDecision.decision === 'awaiting_approval') {
            if (managerDecision.agentId) {
                managerDecision.decision = 'assign';
                managerDecision.reason = `Human approval has already been granted. ${managerDecision.reason || 'Continue execution with the selected agent.'}`;
                managerDecision.notes = [...(managerDecision.notes || []), 'Mission Control resumed the task after operator approval.'];
            } else if (heuristicDecision.assignee) {
                managerDecision.decision = 'assign';
                managerDecision.agentId = heuristicDecision.assignee.id;
                managerDecision.reason = `Human approval has already been granted. ${heuristicDecision.reason}`;
                managerDecision.notes = [...(managerDecision.notes || []), 'Mission Control resumed the task after operator approval.'];
            }
        }

        if (managerDecision.decision === 'assign' && !managerDecision.agentId) {
            managerDecision.decision = effectiveRequirements.needsExternalAction ? 'awaiting_connection' : 'blocked';
            managerDecision.reason = managerDecision.reason || 'Manager did not select a valid assignee.';
        }

        const routedAt = new Date().toISOString();
        const managerNarrative = {
            ts: routedAt,
            agentId: MANAGER_AGENT_ID,
            role: managerDecision.decision === 'assign' ? 'agent_message' : 'assistant',
            text: managerDecision.decision === 'assign'
                ? `Assigned to ${managerDecision.agentId}. ${managerDecision.reason}`
                : managerDecision.reason || 'Task could not be assigned.',
        };

        if (managerDecision.decision !== 'assign') {
            const blockedStatus = managerDecision.decision === 'awaiting_approval'
                ? 'awaiting_approval'
                : managerDecision.decision === 'awaiting_connection'
                    ? 'awaiting_connection'
                    : 'blocked';

            workingTask = {
                ...workingTask,
                status: blockedStatus,
                agentId: managerDecision.agentId || MANAGER_AGENT_ID,
                assignedAgentId: managerDecision.agentId || '',
                managerAgentId: MANAGER_AGENT_ID,
                requestedAgentId: preferredAgentId,
                requiredCapabilities: managerDecision.requiredCapabilities || effectiveRequirements.capabilities,
                requiredApps: managerDecision.requiredApps || effectiveRequirements.requiredApps,
                updatedAt: routedAt,
                narrative: [managerNarrative],
                manager: {
                    decision: blockedStatus,
                    agentId: managerDecision.agentId || '',
                    reason: managerDecision.reason,
                    notes: managerDecision.notes || []
                },
                lastDecision: { ts: routedAt, reason: managerDecision.reason || 'Task blocked during triage' },
                lastRun: {
                    ts: routedAt,
                    summary: managerDecision.reason || 'Task blocked during triage',
                    output: managerRun?.output || '',
                    error: null
                },
                log: [
                    ...(taskRecord.log || []),
                    { ts: startedAt, role: 'system', text: 'Task triage started' },
                    { ts: routedAt, role: 'assistant', text: managerDecision.reason || 'Task blocked during triage' }
                ]
            };
            upsertTaskRecord(instanceId, workingTask);
            return;
        }

        const assignee = roster.find((agent) => agent.id === managerDecision.agentId) || heuristicDecision.assignee;
        const assignmentAt = new Date().toISOString();
        const assignedTaskRecord = {
            ...workingTask,
            status: 'assigned',
            agentId: assignee?.id || managerDecision.agentId,
            assignedAgentId: assignee?.id || managerDecision.agentId,
            managerAgentId: MANAGER_AGENT_ID,
            requestedAgentId: preferredAgentId,
            requiredCapabilities: managerDecision.requiredCapabilities || requirements.capabilities,
            requiredApps: managerDecision.requiredApps || requirements.requiredApps,
            updatedAt: assignmentAt,
            model: assignee?.model || taskRecord.model || '',
            narrative: [managerNarrative],
            manager: {
                decision: 'assign',
                agentId: assignee?.id || managerDecision.agentId,
                reason: managerDecision.reason,
                notes: managerDecision.notes || []
            },
            lastDecision: { ts: assignmentAt, reason: managerDecision.reason || `Assigned to ${assignee?.id || managerDecision.agentId}` },
            log: [
                ...(taskRecord.log || []),
                { ts: startedAt, role: 'system', text: 'Task triage started' },
                { ts: assignmentAt, role: 'assistant', text: managerDecision.reason || `Assigned to ${assignee?.id || managerDecision.agentId}` }
            ]
        };
        workingTask = assignedTaskRecord;
        upsertTaskRecord(instanceId, assignedTaskRecord);

        const workerPrompt = [
            'You are executing a delegated Mission Control task.',
            `Manager: ${MANAGER_IDENTITY_NAME}`,
            `Assigned agent: ${assignee?.label || managerDecision.agentId}`,
            `Task: ${taskRecord.message}`,
            `Manager reason: ${managerDecision.reason || 'Best fit for the task.'}`,
            `Required capabilities: ${(managerDecision.requiredCapabilities || effectiveRequirements.capabilities).join(', ') || 'general execution'}`,
            `Required apps: ${(managerDecision.requiredApps || effectiveRequirements.requiredApps).join(', ') || 'none explicitly required'}`,
            approvalGranted ? 'Human approval has already been granted for this task. Do not return awaiting_approval solely because the task sends email or performs another already-approved action.' : '',
            'Finish with ONLY valid JSON using this shape:',
            '{"status":"completed|blocked|awaiting_connection|awaiting_approval|failed","summary":"what happened","needs":["missing dependency or approval"],"evidence":["proof points"],"followUps":["next actions"]}',
            'Rules:',
            '- Do not claim completion unless the outcome actually happened.',
            '- For any external action, include concrete evidence from the tool result.',
            '- If you create a Notion page, include the final Notion page URL in evidence.',
            '- If the same tool fails twice with the same or similar error, stop retrying and return blocked or awaiting_connection.',
            '- Do not say "in progress" or "should complete shortly". Return only the current verified state.',
            '- If a required app/account/tool is missing, use awaiting_connection.',
            '- If a human needs to review or approve before sending/posting/deleting, use awaiting_approval.',
            '- If this task should be reassigned or cannot be completed by you, use blocked and explain why.'
        ].join('\n');

        workingTask = {
            ...assignedTaskRecord,
            status: 'in_progress',
            updatedAt: assignmentAt,
            log: [
                ...assignedTaskRecord.log,
                { ts: assignmentAt, role: 'system', text: `Execution started by ${assignee?.id || managerDecision.agentId}` }
            ]
        };
        upsertTaskRecord(instanceId, workingTask);

        const workerRun = await runAgentCli({
            agentId: assignee?.id || managerDecision.agentId,
            sessionId: taskRecord.id,
            message: workerPrompt
        });
        const outcome = parseTaskOutcome(workerRun.text);
        const guard = inferExternalActionGuards({ requirements: effectiveRequirements, workerRun });
        const finishedAt = new Date().toISOString();
        const finalStatus = guard.status
            || (['completed', 'blocked', 'awaiting_connection', 'awaiting_approval', 'failed'].includes(outcome.status)
                ? outcome.status
                : 'failed');
        const finalSummary = guard.summary || outcome.summary || 'Task run finished without a verifiable structured outcome.';
        const finalNeeds = guard.needs?.length ? guard.needs : outcome.needs;
        const finalEvidence = guard.evidence?.length ? guard.evidence : outcome.evidence;
        const finalFollowUps = guard.followUps?.length ? guard.followUps : outcome.followUps;
        const narrative = [
            managerNarrative,
            {
                ts: finishedAt,
                agentId: assignee?.id || managerDecision.agentId,
                role: assignee?.id && assignee.id !== MANAGER_AGENT_ID ? 'agent_message' : 'assistant',
                text: assignee?.id && assignee.id !== MANAGER_AGENT_ID
                    ? `Reported back to Mission Manager: ${finalSummary}`
                    : finalSummary
            },
            {
                ts: finishedAt,
                agentId: assignee?.id || managerDecision.agentId,
                role: 'assistant',
                text: finalSummary
            }
        ];

        workingTask = {
            ...workingTask,
            status: finalStatus,
            updatedAt: finishedAt,
            model: workerRun.model || workingTask.model,
            narrative,
            lastDecision: { ts: finishedAt, reason: finalSummary },
            lastRun: {
                ts: finishedAt,
                summary: finalSummary,
                output: workerRun.output,
                error: null,
                needs: finalNeeds,
                evidence: finalEvidence,
                followUps: finalFollowUps
            },
            log: [
                ...assignedTaskRecord.log,
                { ts: assignmentAt, role: 'system', text: `Execution started by ${assignee?.id || managerDecision.agentId}` },
                { ts: finishedAt, role: 'assistant', text: finalSummary }
            ]
        };
        upsertTaskRecord(instanceId, workingTask);
    } catch (err) {
        const finishedAt = new Date().toISOString();
        workingTask = {
            ...workingTask,
            status: 'failed',
            agentId: workingTask.assignedAgentId || workingTask.agentId || MANAGER_AGENT_ID,
            managerAgentId: MANAGER_AGENT_ID,
            requestedAgentId: preferredAgentId,
            requiredCapabilities: workingTask.requiredCapabilities || requirements.capabilities,
            requiredApps: workingTask.requiredApps || requirements.requiredApps,
            updatedAt: finishedAt,
            lastRun: {
                ts: finishedAt,
                summary: '',
                error: err.message
            },
            log: [
                ...(workingTask.log || []),
                { ts: startedAt, role: 'system', text: 'Task execution started' },
                { ts: finishedAt, role: 'system', text: `Task failed: ${err.message}` }
            ]
        };
        upsertTaskRecord(instanceId, workingTask);
    }
}

function sanitizeAllowedModelsMap(models) {
    if (!models || typeof models !== 'object' || Array.isArray(models)) {
        return { models: {}, changed: !!models };
    }

    const nextModels = {};
    let changed = false;

    for (const [modelId, rawValue] of Object.entries(models)) {
        if (!rawValue || typeof rawValue !== 'object' || Array.isArray(rawValue)) {
            nextModels[modelId] = {};
            changed = true;
            continue;
        }

        const { enabled, ...rest } = rawValue;
        if (enabled !== undefined) changed = true;
        nextModels[modelId] = rest;
    }

    return { models: nextModels, changed };
}

function sanitizeInstanceConfig(config) {
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
        return { config, changed: false };
    }

    let changed = false;
    config.agents = config.agents || {};
    config.agents.defaults = config.agents.defaults || {};
    config.agents.list = Array.isArray(config.agents.list) ? config.agents.list : [];
    const defaults = config.agents?.defaults;
    if (defaults && defaults.models) {
        const sanitized = sanitizeAllowedModelsMap(defaults.models);
        if (sanitized.changed) {
            defaults.models = sanitized.models;
            changed = true;
        }
    }

    if (defaults && Object.prototype.hasOwnProperty.call(defaults, 'identity')) {
        const legacyIdentity = defaults.identity && typeof defaults.identity === 'object' ? defaults.identity : {};
        let mainAgent = config.agents.list.find((agent) => agent?.id === MANAGER_AGENT_ID);
        if (!mainAgent) {
            mainAgent = {
                id: MANAGER_AGENT_ID,
                workspace: '/home/node/.openclaw',
                agentDir: '/home/node/.openclaw/agent'
            };
            config.agents.list.unshift(mainAgent);
        }

        const nextName = String(legacyIdentity?.name || '').trim();
        const nextEmoji = String(legacyIdentity?.emoji || '').trim();
        if (nextName && !String(mainAgent.name || '').trim()) {
            mainAgent.name = nextName;
        }
        if (nextEmoji) {
            mainAgent.identity = mainAgent.identity || {};
            if (!String(mainAgent.identity.emoji || '').trim()) {
                mainAgent.identity.emoji = nextEmoji;
            }
        }

        delete defaults.identity;
        changed = true;
    }

    return { config, changed };
}

function normalizeModelOverride(modelValue, defaultsModel = {}) {
    const defaultPrimary = typeof defaultsModel === 'string'
        ? defaultsModel
        : String(defaultsModel?.primary || '').trim();
    const defaultFallbacks = Array.isArray(defaultsModel?.fallbacks) ? defaultsModel.fallbacks : [];

    if (typeof modelValue === 'string') {
        return {
            primary: modelValue.trim() || defaultPrimary,
            fallbacks: defaultFallbacks
        };
    }

    if (modelValue && typeof modelValue === 'object' && !Array.isArray(modelValue)) {
        const primary = String(modelValue.primary || '').trim() || defaultPrimary;
        const fallbacks = Array.isArray(modelValue.fallbacks) ? modelValue.fallbacks : defaultFallbacks;
        return { primary, fallbacks };
    }

    return {
        primary: defaultPrimary,
        fallbacks: defaultFallbacks
    };
}

function buildConfigBackedModels(config) {
    const defaults = config?.agents?.defaults || {};
    const allowedModels = new Set(Object.keys(defaults.models || {}));
    const primary = normalizeModelOverride(defaults.model || {}, defaults.model || {}).primary;
    if (primary) allowedModels.add(primary);

    const providers = config?.models?.providers || {};

    return Array.from(allowedModels)
        .filter(Boolean)
        .map((key) => {
            const [providerKey, ...restParts] = String(key).split('/');
            const rawId = restParts.join('/');
            const provider = providers?.[providerKey];
            const providerModels = Array.isArray(provider?.models) ? provider.models : [];
            const matched = providerModels.find((entry) => {
                const entryId = String(entry?.id || '');
                return entryId === key || entryId === rawId;
            });

            return {
                key,
                id: rawId || key,
                provider: providerKey || '',
                name: matched?.name || rawId || key
            };
        });
}

function listAgentWorkspaceDirs(instanceId) {
    const agentsDir = path.join(INSTANCES_DIR, instanceId, 'agents');
    if (!fs.existsSync(agentsDir)) return [];
    return fs.readdirSync(agentsDir, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => path.join(agentsDir, entry.name))
        .filter((dir) => fs.existsSync(path.join(dir, 'AGENTS.md')) || fs.existsSync(path.join(dir, 'TOOLS.md')) || fs.existsSync(path.join(dir, '.openclaw')))
        .sort();
}

function getComposioWorkspaceDirs(instanceId, extraDirs = []) {
    const dirs = [
        path.join(INSTANCES_DIR, instanceId, 'workspace'),
        ...listAgentWorkspaceDirs(instanceId),
        ...extraDirs,
    ].filter(Boolean);
    return Array.from(new Set(dirs));
}

function buildComposioServerConfig(overrides = {}) {
    const nextUrl = typeof overrides.url === 'string' && overrides.url.trim()
        ? overrides.url.trim()
        : COMPOSIO_MCP_URL;
    const nextHeaders = overrides.headers && typeof overrides.headers === 'object' && !Array.isArray(overrides.headers)
        ? Object.fromEntries(Object.entries(overrides.headers).filter(([key, value]) => key && value !== undefined && value !== null && String(value).trim() !== ''))
        : {
            [COMPOSIO_HEADER_NAME]: COMPOSIO_API_KEY,
        };
    const transport = String(overrides.transport || 'http').trim() || 'http';

    return {
        transport,
        url: nextUrl,
        headers: nextHeaders,
    };
}

function readComposioServerConfigAtWorkspace(workspaceDir) {
    const configPath = path.join(workspaceDir, 'config', 'mcporter.json');
    if (!fs.existsSync(configPath)) return null;
    try {
        const parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        const server = parsed?.mcpServers?.[COMPOSIO_SERVER_NAME];
        if (!server || typeof server !== 'object') return null;
        return {
            transport: String(server.transport || 'http').trim() || 'http',
            url: String(server.url || '').trim(),
            headers: server.headers && typeof server.headers === 'object' && !Array.isArray(server.headers)
                ? Object.fromEntries(Object.entries(server.headers).filter(([key, value]) => key && value !== undefined && value !== null && String(value).trim() !== ''))
                : {},
        };
    } catch {
        return null;
    }
}

function resolveComposioServerConfig(instanceId, overrides = {}) {
    const explicit = buildComposioServerConfig(overrides);
    if (explicit.url && explicit.headers && Object.keys(explicit.headers).length > 0 && (overrides.url || overrides.headers)) {
        return explicit;
    }

    const existingCandidates = [
        path.join(INSTANCES_DIR, instanceId, 'workspace'),
        ...listAgentWorkspaceDirs(instanceId),
    ];
    for (const workspaceDir of existingCandidates) {
        const existing = readComposioServerConfigAtWorkspace(workspaceDir);
        if (existing?.url && existing.headers && Object.keys(existing.headers).length > 0) {
            return existing;
        }
    }

    return explicit;
}

function ensureComposioConfigAtWorkspace(workspaceDir, serverConfig) {
    const configDir = path.join(workspaceDir, 'config');
    const configPath = path.join(configDir, 'mcporter.json');
    const existed = fs.existsSync(configPath);

    fs.mkdirSync(configDir, { recursive: true });

    let parsed = {};
    try {
        if (existed) {
            parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        }
    } catch {
        parsed = {};
    }

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        parsed = {};
    }
    if (!parsed.mcpServers || typeof parsed.mcpServers !== 'object' || Array.isArray(parsed.mcpServers)) {
        parsed.mcpServers = {};
    }

    const nextServer = buildComposioServerConfig(serverConfig);
    const previousServer = parsed.mcpServers[COMPOSIO_SERVER_NAME];
    const changed = !existed || JSON.stringify(previousServer || null) !== JSON.stringify(nextServer);
    parsed.mcpServers[COMPOSIO_SERVER_NAME] = nextServer;

    if (changed) {
        fs.writeFileSync(configPath, JSON.stringify(parsed, null, 2) + '\n', 'utf8');
    }

    return { changed, path: configPath };
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

function normalizeDefaultPrimaryModel(providerKey, rawModel, baseUrl) {
    const trimmed = String(rawModel || '').trim();
    if (!trimmed) return '';

    let normalized = trimmed;
    if (!normalized.startsWith(`${providerKey}/`)) {
        normalized = normalizeCustomProviderModelId(providerKey, normalized);
    }

    if (String(baseUrl || '').includes('gateway.ai.cloudflare.com')) {
        const prefix = `${providerKey}/`;
        const suffix = normalized.startsWith(prefix) ? normalized.slice(prefix.length) : normalized;
        if (suffix.startsWith('@cf/')) {
            return `${providerKey}/workers-ai/${suffix}`;
        }
    }

    return normalized;
}

function getDefaultModelDefinition() {
    if (!DEFAULT_PROVIDER_BASE_URL || !DEFAULT_PROVIDER_API_KEY) return null;

    const providerKey = String(DEFAULT_PROVIDER_KEY || '').trim();
    if (!providerKey) return null;

    const baseUrl = String(DEFAULT_PROVIDER_BASE_URL || '').trim();
    const apiKey = String(DEFAULT_PROVIDER_API_KEY || '').trim();
    if (!baseUrl || !apiKey) return null;

    const primaryModel = normalizeDefaultPrimaryModel(providerKey, DEFAULT_PRIMARY_MODEL_RAW, baseUrl);
    if (!primaryModel) return null;

    return {
        providerKey,
        baseUrl,
        api: normalizeCustomProviderApi(DEFAULT_PROVIDER_API),
        apiKey,
        primaryModel,
    };
}

async function applyConfigReload(instanceId, config = null) {
    const resolvedConfig = config || readInstanceConfig(instanceId);
    if (!resolvedConfig) return;

    try {
        const gatewayToken = resolvedConfig.gateway?.auth?.token || await getGatewayToken(`openclaw-${instanceId}`);
        if (gatewayToken) {
            await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'config.apply', {});
        }
    } catch (err) {
        console.warn(`[vps-agent] config.apply skipped for ${instanceId}: ${err.message}`);
    }
}

function upsertAllowedModel(config, modelId) {
    config.agents = config.agents || {};
    config.agents.defaults = config.agents.defaults || {};
    config.agents.defaults.models = config.agents.defaults.models || {};
    config.agents.defaults.models[modelId] = {};
}

async function ensureDefaultModelForInstance(instanceId, { force = true } = {}) {
    const definition = getDefaultModelDefinition();
    if (!definition) {
        return { enabled: false, reason: 'missing-default-model-env' };
    }

    const config = readInstanceConfig(instanceId);
    if (!config) {
        return { enabled: false, reason: 'config-not-found' };
    }

    config.models = config.models || {};
    config.models.mode = config.models.mode || 'merge';
    config.models.providers = config.models.providers || {};

    const currentPrimary = config.agents?.defaults?.model?.primary || '';
    if (!force && currentPrimary && currentPrimary !== definition.primaryModel) {
        return {
            enabled: false,
            reason: 'primary-already-set',
            currentPrimary,
        };
    }

    config.models.providers[definition.providerKey] = {
        baseUrl: definition.baseUrl,
        apiKey: definition.apiKey,
        api: definition.api,
        models: [
            {
                id: definition.primaryModel,
                name: definition.primaryModel.slice(definition.providerKey.length + 1),
            },
        ],
    };

    config.agents = config.agents || {};
    config.agents.defaults = config.agents.defaults || {};
    config.agents.defaults.model = {
        primary: definition.primaryModel,
        fallbacks: [],
    };
    upsertAllowedModel(config, definition.primaryModel);

    config.agents.list = Array.isArray(config.agents.list) ? config.agents.list : [];
    const mainAgent = config.agents.list.find((agent) => agent?.id === 'main');
    if (mainAgent) {
        mainAgent.model = definition.primaryModel;
    } else {
        config.agents.list.unshift({
            id: 'main',
            model: definition.primaryModel,
        });
    }

    writeInstanceConfig(instanceId, config);
    await applyConfigReload(instanceId, config);

    return {
        enabled: true,
        providerKey: definition.providerKey,
        primaryModel: definition.primaryModel,
    };
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

        await applyConfigReload(instanceId, config);

        console.log(`[vps-agent] custom provider ${key} configured for ${instanceId}`);
        res.json({ success: true, providerKey: key, provider });
    } catch (err) {
        console.error(`[vps-agent] configure-custom-provider failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/internal/backfill-default-model', requireInternal, async (req, res) => {
    const { instanceId, force = true } = req.body || {};

    try {
        if (instanceId) {
            const result = await ensureDefaultModelForInstance(instanceId, { force: force !== false });
            return res.json({ ok: true, updated: [result] });
        }

        const instanceIds = listInstanceIds();
        const updated = [];
        for (const currentInstanceId of instanceIds) {
            updated.push(await ensureDefaultModelForInstance(currentInstanceId, { force: force !== false }));
        }

        return res.json({ ok: true, updated });
    } catch (err) {
        console.error('[vps-agent] backfill-default-model failed:', err.message);
        return res.status(500).json({ error: err.message });
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
                        role: 'operator', scopes: ['operator.read', 'operator.write']
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
    const configuredAgents = Array.isArray(config.agents?.list)
        ? config.agents.list
            .filter((agent) => agent && typeof agent === 'object' && agent.id)
            .map((agent) => {
                const resolvedModel = normalizeModelOverride(agent.model, config.agents?.defaults?.model || {});
                const profile = inferAgentProfile({
                    id: agent.id,
                    label: agent.name || (agent.id === MANAGER_AGENT_ID ? MANAGER_IDENTITY_NAME : agent.id),
                    files: readAgentProfile(instanceId, agent.id).files
                });
                return {
                    id: agent.id,
                    name: agent.name || (agent.id === MANAGER_AGENT_ID ? MANAGER_IDENTITY_NAME : agent.id),
                    workspace: agent.workspace,
                    agentDir: agent.agentDir,
                    model: resolvedModel.primary,
                    primaryModel: resolvedModel.primary,
                    fallbacks: resolvedModel.fallbacks,
                    capabilities: profile.capabilities,
                    toolkits: profile.toolkits,
                    source: 'config'
                };
            })
        : [];

    if (!gatewayToken) {
        return res.json({ agents: configuredAgents });
    }

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'agents.list', {});
        const gatewayAgents = result?.payload?.agents || result?.payload || [];
        const merged = new Map();

        for (const agent of configuredAgents) {
            merged.set(agent.id, agent);
        }

        if (Array.isArray(gatewayAgents)) {
            for (const agent of gatewayAgents) {
                if (!agent?.id) continue;
                merged.set(agent.id, {
                    ...merged.get(agent.id),
                    ...agent,
                    source: merged.has(agent.id) ? 'config+gateway' : 'gateway'
                });
            }
        }

        res.json({ agents: Array.from(merged.values()) });
    } catch (err) {
        console.error(`[vps-agent] agents-list failed for ${instanceId}:`, err.message);
        res.json({ agents: configuredAgents, warning: err.message });
    }
});

// ── Agent config read (from openclaw.json) ───────────────────────────────────
app.post('/api/internal/agent-config', requireInternal, (req, res) => {
    const { instanceId, agentId } = req.body;
    if (!instanceId || !agentId) return res.status(400).json({ error: 'instanceId and agentId required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });

    const defaults = config.agents?.defaults || {};
    const configuredAgents = Array.isArray(config.agents?.list) ? config.agents.list : [];
    const configuredAgent = configuredAgents.find((agent) => agent?.id === agentId) || null;
    const profile = readAgentProfile(instanceId, agentId);
    const resolvedModel = normalizeModelOverride(configuredAgent?.model, defaults.model || {});
    const defaultPrimary = normalizeModelOverride(defaults.model || {}, defaults.model || {}).primary;
    const inheritsDefault = agentId !== 'main' && !configuredAgent?.model;
    const identityName = configuredAgent?.identity?.name || configuredAgent?.name || profile.identityName || '';
    const identityEmoji = configuredAgent?.identity?.emoji || '';

    res.json({
        id: agentId,
        agentId,
        workspace: configuredAgent?.workspace || getAgentContainerWorkspace(agentId),
        default: agentId === 'main',
        model: {
            primary: resolvedModel.primary,
            fallbacks: resolvedModel.fallbacks,
            inherited: inheritsDefault,
            defaultPrimary
        },
        identity: {
            name: identityName,
            emoji: identityEmoji
        },
        files: profile.files
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
    config.agents.list = Array.isArray(config.agents.list) ? config.agents.list : [];

    // Clean up any previously written invalid key
    if (config.agents.agents) delete config.agents.agents;

    if (agentId === 'main') {
        const target = config.agents.defaults;
        let mainAgent = config.agents.list.find((agent) => agent?.id === 'main');
        if (!mainAgent) {
            mainAgent = {
                id: 'main',
                name: MANAGER_IDENTITY_NAME,
                workspace: '/home/node/.openclaw',
                agentDir: '/home/node/.openclaw/agent'
            };
            config.agents.list.unshift(mainAgent);
        }
        if (updates.model) {
            target.model = target.model || {};
            if (updates.model.primary !== undefined) target.model.primary = updates.model.primary;
            if (updates.model.fallbacks !== undefined) target.model.fallbacks = updates.model.fallbacks;

            if (mainAgent && updates.model.primary !== undefined) {
                mainAgent.model = updates.model.primary;
            }
        }
        if (updates.identity) {
            mainAgent.identity = mainAgent.identity || {};
            if (updates.identity.name !== undefined) {
                const nextName = String(updates.identity.name || '').trim();
                mainAgent.name = nextName || MANAGER_IDENTITY_NAME;
                mainAgent.identity.name = nextName || MANAGER_IDENTITY_NAME;
                syncAgentIdentityName(instanceId, agentId, mainAgent.name);
            }
            if (updates.identity.emoji !== undefined) {
                const nextEmoji = String(updates.identity.emoji || '').trim();
                if (nextEmoji) mainAgent.identity.emoji = nextEmoji;
                else delete mainAgent.identity.emoji;
            }
        }
    } else {
        const agentEntry = config.agents.list.find((agent) => agent?.id === agentId);
        if (!agentEntry) {
            return res.status(404).json({ error: `Agent "${agentId}" not found` });
        }

        if (updates.model) {
            const nextPrimary = updates.model.primary !== undefined
                ? String(updates.model.primary || '').trim()
                : normalizeModelOverride(agentEntry.model, config.agents.defaults?.model || {}).primary;
            const nextFallbacks = updates.model.fallbacks !== undefined
                ? (Array.isArray(updates.model.fallbacks) ? updates.model.fallbacks : [])
                : normalizeModelOverride(agentEntry.model, config.agents.defaults?.model || {}).fallbacks;

            if (nextPrimary) {
                agentEntry.model = {
                    primary: nextPrimary,
                    fallbacks: nextFallbacks.filter((value) => value && value !== nextPrimary)
                };
            } else {
                delete agentEntry.model;
            }
        }

        if (updates.identity && updates.identity.name !== undefined) {
            const nextName = String(updates.identity.name || '').trim();
            agentEntry.name = nextName || agentEntry.id;
            syncAgentIdentityName(instanceId, agentId, agentEntry.name);
        }
    }

    try {
        fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
        console.log(`[vps-agent] agent config updated for ${agentId} in ${instanceId}`);

        // Notify gateway to reload config
        const gatewayToken = config.gateway?.auth?.token;
        if (gatewayToken) {
            gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'config.apply', {}).catch(() => {});
        }
        const refreshed = readAgentProfile(instanceId, agentId);
        const currentAgent = config.agents.list.find((agent) => agent?.id === agentId) || {};
        const defaults = config.agents.defaults || {};
        const resolvedModel = normalizeModelOverride(currentAgent.model, defaults.model || {});
        const defaultPrimary = normalizeModelOverride(defaults.model || {}, defaults.model || {}).primary;
        const inheritsDefault = agentId !== 'main' && !currentAgent.model;
        res.json({
            ok: true,
            agent: {
                id: agentId,
                model: {
                    primary: resolvedModel.primary,
                    fallbacks: resolvedModel.fallbacks,
                    inherited: inheritsDefault,
                    defaultPrimary
                },
                identity: {
                    name: currentAgent.identity?.name || currentAgent.name || refreshed.identityName || '',
                    emoji: currentAgent.identity?.emoji || ''
                },
                files: refreshed.files
            }
        });
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
    const configBackedModels = buildConfigBackedModels(config);
    if (!gatewayToken) return res.json({ models: configBackedModels });

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'models.list', {});
        if (result?.ok === false) {
            console.warn(`[vps-agent] models-list gateway fallback for ${instanceId}: ${result?.error?.message || 'unknown error'}`);
            return res.json({ models: configBackedModels, warning: result?.error?.message || 'gateway models.list failed' });
        }

        const payload = result?.payload || result;
        const models = Array.isArray(payload?.models)
            ? payload.models
            : Array.isArray(payload)
                ? payload
                : [];
        if (models.length) {
            return res.json({ models });
        }

        return res.json({ models: configBackedModels });
    } catch (err) {
        console.error(`[vps-agent] models-list failed for ${instanceId}:`, err.message);
        res.json({ models: configBackedModels, warning: err.message });
    }
});

// ── Raw chat sessions/history for Mission Control UI ─────────────────────────
app.post('/api/internal/chat-sessions', requireInternal, async (req, res) => {
    const { instanceId, limit } = req.body || {};
    if (!instanceId) return res.status(400).json({ error: 'instanceId is required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'sessions.list', {});
        const sessions = result?.payload?.sessions || result?.payload || [];
        const list = Array.isArray(sessions) ? sessions : [];
        const max = Math.max(1, Math.min(Number(limit) || 30, 200));
        const sorted = [...list].sort((a, b) => Number(b?.updatedAt || 0) - Number(a?.updatedAt || 0));
        res.json({ sessions: sorted.slice(0, max) });
    } catch (err) {
        console.error(`[vps-agent] chat-sessions failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message, sessions: [] });
    }
});

app.post('/api/internal/chat-history', requireInternal, async (req, res) => {
    const { instanceId, sessionKey, limit, includeTools } = req.body || {};
    if (!instanceId || !sessionKey) return res.status(400).json({ error: 'instanceId and sessionKey are required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    try {
        const result = await gatewayWsExec(`openclaw-${instanceId}`, gatewayToken, 'chat.history', { sessionKey });
        let messages = result?.payload?.messages || result?.payload || [];
        messages = Array.isArray(messages) ? messages : [];

        if (!includeTools) {
            messages = messages.filter((entry) => {
                const role = String(entry?.message?.role || entry?.role || '').toLowerCase();
                return role !== 'toolresult' && role !== 'tool_result' && role !== 'tool';
            });
        }

        const max = Math.max(1, Math.min(Number(limit) || 100, 500));
        if (messages.length > max) messages = messages.slice(-max);
        res.json({ messages });
    } catch (err) {
        console.error(`[vps-agent] chat-history failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message, messages: [] });
    }
});

// ── Sessions list (WebSocket sessions.list → mapped to jobs) ─────────────────
app.post('/api/internal/sessions-list', requireInternal, async (req, res) => {
    const { instanceId, ids, limit, includeNarrative, includeLog, onlyTaskSessions } = req.body;
    if (!instanceId) return res.status(400).json({ error: 'instanceId is required' });
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });
    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    const container = `openclaw-${instanceId}`;
    try {
        const result = await gatewayWsExec(container, gatewayToken, 'sessions.list', {});
        const sessions = result?.payload?.sessions || result?.payload || [];
        let list = Array.isArray(sessions) ? sessions : [];

        if (!ids && onlyTaskSessions) {
            list = list.filter((session) => {
                const key = String(session?.key || session?.sessionKey || '');
                const sessionPart = key.split(':')[2] || '';
                return sessionPart.startsWith('task-') || sessionPart.startsWith('bcast-');
            });
        }

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
                        enrichJobWithHistory(job, messages, { includeLog });
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

function normalizeTranscriptText(text) {
    return String(text || '')
        .replace(/\uFEFF/g, '')
        .replace(/^Conversation info \(untrusted metadata\):\s*```json[\s\S]*?```\s*/i, '')
        .replace(/^Sender \(untrusted metadata\):\s*```json[\s\S]*?```\s*/i, '')
        .replace(/^\[Inter-session message\]\s*/i, '')
        .trim();
}

function extractNarrativeText(content) {
    if (typeof content === 'string') return normalizeTranscriptText(content);
    if (!Array.isArray(content)) return '';

    const chunks = [];
    for (const part of content) {
        if (part === undefined || part === null) continue;
        if (typeof part === 'string') {
            chunks.push(part);
            continue;
        }
        if (typeof part !== 'object') {
            chunks.push(String(part));
            continue;
        }

        const type = String(part.type || '').toLowerCase();
        if (type === 'thinking' || type === 'reasoning' || type === 'toolcall' || type === 'tool_call' || type === 'tooluse' || type === 'tool_use') {
            continue;
        }

        if (part.text) {
            chunks.push(part.text);
            continue;
        }

        if (part.content) {
            chunks.push(extractNarrativeText(part.content));
        }
    }

    return normalizeTranscriptText(chunks.join(''));
}

function enrichJobWithHistory(job, messages, options = {}) {
    const { includeLog = false } = options;
    if (!Array.isArray(messages) || !messages.length) return job;

    const narrative = [];
    const log = [];
    let firstUserMessage = '';
    let lastAssistantMessage = '';
    let lastTimestamp = job?.metadata?.updatedAt || null;

    for (const entry of messages) {
        if (!entry || typeof entry !== 'object') continue;

        const message = entry.message && typeof entry.message === 'object' ? entry.message : entry;
        const role = String(message.role || entry.role || '').toLowerCase();
        const timestamp = entry.timestamp || message.timestamp || null;
        if (timestamp) lastTimestamp = timestamp;

        if (role === 'toolresult' || role === 'tool_result' || role === 'tool') {
            const toolText = extractContent(message.content || entry.content || '');
            if (toolText) {
                log.push({
                    ts: timestamp,
                    role: 'tool',
                    toolName: message.toolName || entry.toolName || 'tool',
                    text: toolText
                });
            }
            continue;
        }

        const text = extractNarrativeText(message.content || entry.content || '');
        if (!text) continue;

        const provenanceKind = message?.provenance?.kind || null;

        if (role === 'user') {
            if (!firstUserMessage) firstUserMessage = text;
            if (provenanceKind === 'inter_session') {
                narrative.push({
                    ts: timestamp,
                    agentId: message?.provenance?.fromAgentId || message?.provenance?.from || 'peer-agent',
                    role: 'agent_message',
                    text,
                    provenanceKind
                });
            }
            continue;
        }

        if (role === 'assistant') {
            lastAssistantMessage = text;
            narrative.push({
                ts: timestamp,
                agentId: job.agentId || 'main',
                role: 'assistant',
                text
            });
        }
    }

    job.payload = job.payload || {};
    job.metadata = job.metadata || {};

    if (firstUserMessage) {
        job.payload.message = firstUserMessage;
        job.metadata.message = firstUserMessage;
    }

    if (lastTimestamp) {
        job.metadata.updatedAt = lastTimestamp;
        if (!job.metadata.createdAt) job.metadata.createdAt = lastTimestamp;
    }

    job.metadata.narrative = narrative;
    if (includeLog && log.length) job.metadata.log = log;

    if (lastAssistantMessage) {
        job.metadata.lastDecision = {
            reason: lastAssistantMessage,
            ts: lastTimestamp
        };
        job.metadata.lastRun = {
            ...(job.metadata.lastRun || {}),
            summary: lastAssistantMessage,
            ts: lastTimestamp
        };
    }

    return job;
}

function dedupeNarrativeEntries(entries) {
    const seen = new Set();
    return entries.filter((entry) => {
        if (!entry || typeof entry !== 'object') return false;
        const key = JSON.stringify([
            entry.ts || '',
            entry.agentId || '',
            entry.role || '',
            entry.text || ''
        ]);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

function mergeNarrativeEntries(...groups) {
    return dedupeNarrativeEntries(groups.flat().filter(Boolean))
        .sort((left, right) => String(left?.ts || '').localeCompare(String(right?.ts || '')));
}

function summarizeTaskHistoryMessage({ message, fallbackAgentId }) {
    const role = String(message?.role || '').toLowerCase();
    const timestamp = message?.timestamp || null;
    const provenanceKind = message?.provenance?.kind || null;
    const fromAgentId = message?.provenance?.fromAgentId || message?.provenance?.from || fallbackAgentId || 'main';
    const text = extractNarrativeText(message?.content || '');
    if (!text) return null;

    if (role === 'user' && provenanceKind === 'inter_session') {
        return {
            ts: timestamp,
            agentId: fromAgentId,
            role: 'agent_message',
            text,
            provenanceKind
        };
    }

    if (role !== 'assistant') return null;

    if (String(fallbackAgentId || '') === MANAGER_AGENT_ID) {
        const decision = parseManagerDecision(text);
        if (decision) {
            return {
                ts: timestamp,
                agentId: MANAGER_AGENT_ID,
                role: decision.decision === 'assign' ? 'agent_message' : 'assistant',
                text: decision.decision === 'assign'
                    ? `Assigned to ${decision.agentId}. ${decision.reason}`
                    : decision.reason || 'Task triage update.',
            };
        }
    }

    const outcome = parseTaskOutcome(text);
    if (outcome?.summary) {
        return {
            ts: timestamp,
            agentId: fallbackAgentId || 'main',
            role: fallbackAgentId && fallbackAgentId !== MANAGER_AGENT_ID ? 'agent_message' : 'assistant',
            text: outcome.summary
        };
    }

    return {
        ts: timestamp,
        agentId: fallbackAgentId || 'main',
        role: fallbackAgentId && fallbackAgentId !== MANAGER_AGENT_ID ? 'agent_message' : 'assistant',
        text
    };
}

async function enrichStoredTaskWithGatewayHistory(instanceId, task, options = {}) {
    const { includeLog = false } = options;
    const config = readInstanceConfig(instanceId);
    const gatewayToken = config?.gateway?.auth?.token;
    if (!gatewayToken) return task;

    const container = `openclaw-${instanceId}`;
    const narrative = Array.isArray(task?.narrative) ? [...task.narrative] : [];
    const log = Array.isArray(task?.log) ? [...task.log] : [];
    const sessionTargets = [
        { sessionKey: `agent:${MANAGER_AGENT_ID}:${task.id}-manager`, agentId: MANAGER_AGENT_ID },
        { sessionKey: `agent:${task.assignedAgentId || task.agentId || MANAGER_AGENT_ID}:${task.id}`, agentId: task.assignedAgentId || task.agentId || MANAGER_AGENT_ID }
    ];

    for (const target of sessionTargets) {
        try {
            const history = await gatewayWsExec(container, gatewayToken, 'chat.history', { sessionKey: target.sessionKey });
            const messages = history?.payload?.messages || history?.payload || [];
            if (!Array.isArray(messages) || !messages.length) continue;

            const extractedNarrative = messages
                .map((entry) => summarizeTaskHistoryMessage({
                    message: entry?.message && typeof entry.message === 'object' ? { ...entry.message, timestamp: entry.timestamp || entry.message.timestamp } : entry,
                    fallbackAgentId: target.agentId
                }))
                .filter(Boolean);
            narrative.push(...extractedNarrative);

            if (includeLog) {
                for (const entry of messages) {
                    const message = entry?.message && typeof entry.message === 'object' ? entry.message : entry;
                    const role = String(message?.role || '').toLowerCase();
                    if (role === 'toolresult' || role === 'tool_result' || role === 'tool') {
                        const toolText = extractContent(message.content || entry.content || '');
                        if (toolText) {
                            log.push({
                                ts: entry.timestamp || message.timestamp || null,
                                role: 'tool',
                                toolName: message.toolName || entry.toolName || 'tool',
                                text: toolText
                            });
                        }
                    }
                }
            }
        } catch {
            // Keep the stored task narrative if history is unavailable.
        }
    }

    const mergedNarrative = mergeNarrativeEntries(narrative);
    const latestNarrative = mergedNarrative[mergedNarrative.length - 1] || null;

    const enriched = {
        ...task,
        narrative: mergedNarrative,
    };

    if (includeLog) {
        enriched.log = dedupeNarrativeEntries(log.map((entry) => ({
            ts: entry.ts,
            agentId: entry.agentId,
            role: entry.role,
            text: entry.text,
            toolName: entry.toolName
        }))).map((entry) => ({
            ts: entry.ts,
            role: entry.role,
            text: entry.text,
            toolName: entry.toolName
        }));
    }

    if (latestNarrative?.text) {
        enriched.lastDecision = {
            ts: latestNarrative.ts || task?.updatedAt || null,
            reason: latestNarrative.text
        };
        enriched.lastRun = {
            ...(task?.lastRun || {}),
            ts: latestNarrative.ts || task?.updatedAt || null,
            summary: latestNarrative.text
        };
    }

    return enriched;
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

app.post('/api/internal/tasks-create', requireInternal, async (req, res) => {
    const { instanceId, message, agentId, priority, name } = req.body || {};
    if (!instanceId || !message) {
        return res.status(400).json({ error: 'instanceId and message are required' });
    }

    const taskId = `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const now = new Date().toISOString();
    const config = readInstanceConfig(instanceId);
    const defaultsModel = config?.agents?.defaults?.model || {};
    const configuredAgent = Array.isArray(config?.agents?.list)
        ? config.agents.list.find((agent) => agent?.id === MANAGER_AGENT_ID)
        : null;
    const resolvedModel = normalizeModelOverride(configuredAgent?.model, defaultsModel).primary;

    const taskRecord = {
        id: taskId,
        name: String(name || `Task: ${String(message).slice(0, 60)}`).trim(),
        message: String(message).trim(),
        agentId: MANAGER_AGENT_ID,
        assignedAgentId: '',
        managerAgentId: MANAGER_AGENT_ID,
        requestedAgentId: String(agentId || '').trim() === MANAGER_AGENT_ID ? '' : String(agentId || '').trim(),
        priority: Number(priority) || 3,
        status: 'triage',
        createdAt: now,
        updatedAt: now,
        model: resolvedModel,
        narrative: [],
        log: [
            { ts: now, role: 'system', text: 'Task created from Mission Control and queued for manager triage' }
        ],
        lastRun: null,
        lastDecision: null,
        requiredCapabilities: [],
        requiredApps: [],
        manager: null
    };

    upsertTaskRecord(instanceId, taskRecord);

    queueMicrotask(() => {
        executeTaskRecord(instanceId, taskRecord).catch((err) => {
            console.error(`[vps-agent] async task execution failed for ${instanceId}/${taskId}:`, err.message);
        });
    });

    return res.json({
        ok: true,
        taskId,
        sessionKey: `agent:${MANAGER_AGENT_ID}:${taskId}`,
        status: taskRecord.status
    });
});

async function executeDirectAgentRun(instanceId, taskRecord) {
    const containerName = `openclaw-${instanceId}`;
    const startedAt = new Date().toISOString();

    upsertTaskRecord(instanceId, {
        ...taskRecord,
        status: 'in_progress',
        updatedAt: startedAt,
        log: [
            ...(taskRecord.log || []),
            { ts: startedAt, role: 'system', text: `Direct conversation started with ${taskRecord.agentId}` }
        ]
    });

    try {
        const output = await runDockerExec(containerName, [
            'agent',
            '--agent', taskRecord.agentId,
            '--session-id', taskRecord.id,
            '--message', taskRecord.message,
            '--json'
        ]);
        const parsed = extractJsonObject(output);
        const responseText = parsed?.payloads?.map((entry) => entry?.text).filter(Boolean).join('\n\n').trim()
            || parsed?.meta?.lastDecision?.reason
            || output;
        const finishedAt = new Date().toISOString();
        const modelProvider = parsed?.meta?.agentMeta?.provider || '';
        const modelName = parsed?.meta?.agentMeta?.model || '';
        const resolvedModel = modelProvider && modelName ? `${modelProvider}/${modelName}` : (taskRecord.model || '');

        upsertTaskRecord(instanceId, {
            ...taskRecord,
            status: 'completed',
            updatedAt: finishedAt,
            model: resolvedModel,
            narrative: mergeNarrativeEntries(
                Array.isArray(taskRecord.narrative) ? taskRecord.narrative : [],
                [{
                    ts: finishedAt,
                    agentId: taskRecord.agentId,
                    role: 'assistant',
                    text: String(responseText || '').trim() || 'No response returned.'
                }]
            ),
            lastDecision: {
                ts: finishedAt,
                reason: String(responseText || '').trim() || 'Conversation completed.'
            },
            lastRun: {
                ts: finishedAt,
                summary: String(responseText || '').trim() || 'Conversation completed.',
                output,
                error: null
            },
            log: [
                ...(taskRecord.log || []),
                { ts: startedAt, role: 'system', text: `Direct conversation started with ${taskRecord.agentId}` },
                { ts: finishedAt, role: 'assistant', text: String(responseText || '').trim() || 'Conversation completed.' }
            ]
        });
    } catch (err) {
        const finishedAt = new Date().toISOString();
        upsertTaskRecord(instanceId, {
            ...taskRecord,
            status: 'failed',
            updatedAt: finishedAt,
            lastRun: {
                ts: finishedAt,
                summary: '',
                error: err.message
            },
            log: [
                ...(taskRecord.log || []),
                { ts: startedAt, role: 'system', text: `Direct conversation started with ${taskRecord.agentId}` },
                { ts: finishedAt, role: 'system', text: `Conversation failed: ${err.message}` }
            ]
        });
    }
}

app.post('/api/internal/broadcast-create', requireInternal, async (req, res) => {
    const { instanceId, message, agentIds } = req.body || {};
    if (!instanceId || !message) {
        return res.status(400).json({ error: 'instanceId and message are required' });
    }

    const config = readInstanceConfig(instanceId);
    const defaultsModel = config?.agents?.defaults?.model || {};
    const requestedAgentIds = Array.isArray(agentIds) && agentIds.length
        ? agentIds.map((value) => String(value || '').trim()).filter(Boolean)
        : [MANAGER_AGENT_ID];
    const uniqueAgentIds = Array.from(new Set(requestedAgentIds));

    const tasks = [];
    for (const agentId of uniqueAgentIds) {
        const configuredAgent = Array.isArray(config?.agents?.list)
            ? config.agents.list.find((agent) => agent?.id === agentId)
            : null;
        const resolvedModel = normalizeModelOverride(configuredAgent?.model, defaultsModel).primary;
        const taskId = `bcast-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const now = new Date().toISOString();
        const taskRecord = {
            id: taskId,
            name: `Broadcast: ${String(message).slice(0, 60)}`,
            message: String(message).trim(),
            agentId,
            assignedAgentId: agentId,
            managerAgentId: '',
            requestedAgentId: agentId,
            priority: 3,
            status: 'assigned',
            createdAt: now,
            updatedAt: now,
            model: resolvedModel,
            narrative: [],
            log: [
                { ts: now, role: 'system', text: `Broadcast message queued for ${agentId}` }
            ],
            lastRun: null,
            lastDecision: null,
            requiredCapabilities: [],
            requiredApps: [],
            manager: null
        };

        upsertTaskRecord(instanceId, taskRecord);
        tasks.push({
            id: taskId,
            agentId,
            name: taskRecord.name,
            status: taskRecord.status
        });

        queueMicrotask(() => {
            executeDirectAgentRun(instanceId, taskRecord).catch((err) => {
                console.error(`[vps-agent] broadcast direct run failed for ${instanceId}/${taskId}:`, err.message);
            });
        });
    }

    return res.json({ ok: true, tasks });
});

app.post('/api/internal/tasks-list', requireInternal, async (req, res) => {
    const { instanceId, ids, limit, includeNarrative, includeLog } = req.body || {};
    if (!instanceId) return res.status(400).json({ error: 'instanceId is required' });

    const requestedIds = typeof ids === 'string' && ids.trim()
        ? new Set(ids.split(',').map((value) => value.trim()).filter(Boolean))
        : null;
    const max = Math.max(1, Math.min(Number(limit) || 100, 1000));

    let tasks = readTasksStore(instanceId);
    if (requestedIds) {
        tasks = tasks.filter((task) => requestedIds.has(task.id));
    }

    tasks = tasks
        .sort((a, b) => String(b?.updatedAt || '').localeCompare(String(a?.updatedAt || '')))
        .slice(0, max);

    const tasksToMap = [];
    for (const task of tasks) {
        if (includeNarrative || includeLog || requestedIds) {
            tasksToMap.push(await enrichStoredTaskWithGatewayHistory(instanceId, task, { includeLog }));
        } else {
            tasksToMap.push(task);
        }
    }

    const jobs = tasksToMap.map((task) => mapStoredTaskToJob(task, { includeNarrative, includeLog }));
    res.json({ jobs });
});

app.post('/api/internal/tasks-action', requireInternal, async (req, res) => {
    const { instanceId, taskId, action, note } = req.body || {};
    if (!instanceId || !taskId || !action) {
        return res.status(400).json({ error: 'instanceId, taskId, and action are required' });
    }

    const existingTask = getTaskRecord(instanceId, taskId);
    if (!existingTask) {
        return res.status(404).json({ error: `Task "${taskId}" not found` });
    }

    const now = new Date().toISOString();
    const cleanedNote = String(note || '').trim();
    const currentStatus = String(existingTask.status || '').trim().toLowerCase();

    if (action === 'approve') {
        if (currentStatus !== 'awaiting_approval') {
            return res.status(409).json({ error: `Task "${taskId}" is not awaiting approval` });
        }

        const approvedTask = {
            ...existingTask,
            status: 'triage',
            updatedAt: now,
            requestedAgentId: existingTask.assignedAgentId || existingTask.requestedAgentId || '',
            approval: {
                status: 'approved',
                approvedAt: now,
                note: cleanedNote
            },
            log: [
                ...(existingTask.log || []),
                { ts: now, role: 'system', text: cleanedNote ? `Task approved from Mission Control: ${cleanedNote}` : 'Task approved from Mission Control' }
            ]
        };
        upsertTaskRecord(instanceId, approvedTask);
        queueMicrotask(() => {
            executeTaskRecord(instanceId, approvedTask).catch((err) => {
                console.error(`[vps-agent] approved task resume failed for ${instanceId}/${taskId}:`, err.message);
            });
        });
        return res.json({ ok: true, taskId, action, status: approvedTask.status });
    }

    if (action === 'retry') {
        const resumedTask = {
            ...existingTask,
            status: 'triage',
            updatedAt: now,
            log: [
                ...(existingTask.log || []),
                { ts: now, role: 'system', text: cleanedNote ? `Task retried from Mission Control: ${cleanedNote}` : 'Task retried from Mission Control' }
            ]
        };
        upsertTaskRecord(instanceId, resumedTask);
        queueMicrotask(() => {
            executeTaskRecord(instanceId, resumedTask).catch((err) => {
                console.error(`[vps-agent] task retry failed for ${instanceId}/${taskId}:`, err.message);
            });
        });
        return res.json({ ok: true, taskId, action, status: resumedTask.status });
    }

    if (action === 'reject') {
        const rejectedTask = {
            ...existingTask,
            status: 'blocked',
            updatedAt: now,
            lastDecision: {
                ts: now,
                reason: cleanedNote || 'Operator rejected this task.'
            },
            lastRun: {
                ...(existingTask.lastRun || {}),
                ts: now,
                summary: cleanedNote || 'Operator rejected this task.'
            },
            log: [
                ...(existingTask.log || []),
                { ts: now, role: 'system', text: cleanedNote ? `Task rejected from Mission Control: ${cleanedNote}` : 'Task rejected from Mission Control' }
            ]
        };
        upsertTaskRecord(instanceId, rejectedTask);
        return res.json({ ok: true, taskId, action, status: rejectedTask.status });
    }

    return res.status(400).json({ error: `Unsupported action "${action}"` });
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
        if (result?.ok === false && String(result?.error?.message || '').includes('missing scope: operator.admin')) {
            const fallback = await deleteAgentFromConfig(instanceId, agentId);
            return res.json(fallback);
        }
        res.json(result?.payload || { ok: true });
    } catch (err) {
        console.error(`[vps-agent] agents-delete failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Sub-agent spawn (agents.create + chat.send via WebSocket) ────────────────
app.post('/api/internal/subagents-spawn', requireInternal, async (req, res) => {
    const { instanceId, initialTask, label, model, identityMd, soulMd, agentsMd } = req.body;
    if (!instanceId) {
        return res.status(400).json({ error: 'instanceId is required' });
    }
    if (![label, identityMd, soulMd, agentsMd, initialTask].some((value) => String(value || '').trim())) {
        return res.status(400).json({ error: 'Provide at least a label or one agent profile field' });
    }
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });

    const gatewayToken = config.gateway?.auth?.token;
    const container = `openclaw-${instanceId}`;
    const agentId = (label || 'sub-' + Date.now()).toLowerCase().replace(/[^a-z0-9-]/g, '-').slice(0, 30);

    try {
        const setup = await createAgentViaConfig(instanceId, agentId, {
            label,
            model,
            identityMd,
            soulMd,
            agentsMd
        });

        let chat = null;
        if (String(initialTask || '').trim()) {
            chat = await dispatchAgentTask(container, gatewayToken, agentId, initialTask);
            console.log(`[vps-agent] initial task dispatched to ${agentId}:`, JSON.stringify(chat).slice(0, 200));
        }

        res.json({
            ok: true,
            mode: 'config-fallback',
            agent: { id: agentId, name: label || agentId },
            setup,
            chat
        });
    } catch (err) {
        console.error(`[vps-agent] subagents-spawn failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Channel management ────────────────────────────────────────────────────────

function writeInstanceConfig(instanceId, config) {
    const configPath = path.join(INSTANCES_DIR, instanceId, 'openclaw.json');
    const { config: sanitizedConfig } = sanitizeInstanceConfig(config);
    fs.writeFileSync(configPath, JSON.stringify(sanitizedConfig, null, 2), 'utf8');
}

function getAgentContainerWorkspace(agentId) {
    return `/home/node/.openclaw/agents/${agentId}`;
}

function getAgentHostWorkspace(instanceId, agentId) {
    return path.join(INSTANCES_DIR, instanceId, 'agents', agentId);
}

function readTextFileIfExists(filePath) {
    try {
        return fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf8') : '';
    } catch {
        return '';
    }
}

function extractIdentityNameFromMarkdown(markdown) {
    const heading = String(markdown || '').match(/^#\s+(.+)$/m);
    return heading?.[1]?.trim() || '';
}

function buildManagerProfile() {
    const profile = {
        role: 'manager',
        capabilities: ['triage', 'planning', 'coordination', 'review', 'research', 'writing'],
        toolkits: [],
        taskTypes: ['triage', 'planning', 'research', 'writing', 'delegation'],
        connectedApps: [],
        canTakeExternalAction: false,
        responsibilities: [
            { status: 'assign', meaning: 'Choose the strongest owner and explain why.' },
            { status: 'blocked', meaning: 'No one in the team can safely own this yet.' },
            { status: 'awaiting_connection', meaning: 'A tool or app connection must be completed first.' },
            { status: 'awaiting_approval', meaning: 'Human approval is required before the final step.' }
        ],
        summary: 'Routes new tasks, keeps state honest, and only marks work complete when the outcome is verified.'
    };

    return {
        profile,
        identityMd: [
            `# ${MANAGER_IDENTITY_NAME}`,
            '',
            `You are ${MANAGER_IDENTITY_NAME}, the main agent for this workspace.`,
            'You own intake, delegation, escalation, verification, and operator-facing updates.',
            'Do not pretend work is complete when it is only planned, drafted, or waiting on a missing connection.'
        ].join('\n'),
        soulMd: [
            '# Working Style',
            '',
            'Operate like a calm operations lead.',
            'Route work deliberately, keep statuses honest, and explain blockers in plain language.',
            'Prefer explicit handoffs, verification, and useful comments over optimistic assumptions.'
        ].join('\n'),
        agentsMd: [
            '# Mission Control Charter',
            '',
            renderMissionControlProfile(profile),
            '',
            '## Core Responsibilities',
            '- Triage every new task before execution.',
            '- Assign work to the best-fit specialist when one exists.',
            '- If no specialist is a good match, either handle the task yourself when it is safe or leave a blocker comment.',
            '- For external actions such as sending email, posting, publishing, or deleting, require verification before marking a task complete.',
            '- When a task is blocked, say exactly what is missing: capability gap, missing app connection, or human approval.',
            '',
            '## Status Rules',
            '- `assigned`: the best owner has been selected.',
            '- `in_progress`: the assigned agent is actively working.',
            '- `awaiting_connection`: an app or account must be connected first.',
            '- `awaiting_approval`: a human needs to review before the last step.',
            '- `blocked`: no safe path is available with the current team.',
            '- `completed`: the requested outcome actually happened and was verified.',
            '',
            '## Team Behavior',
            '- Prefer specialists for domain work.',
            '- Keep handoffs explicit: who owns the task, why they were chosen, and what done looks like.',
            '- Leave concise comments so the dashboard reads like an operations log, not a black box.'
        ].join('\n')
    };
}

function buildSpecialistProfile({ agentId, label, identityMd, soulMd, agentsMd }) {
    const inferred = inferAgentProfile({
        id: agentId,
        label,
        files: { identityMd, soulMd, agentsMd }
    });
    const profile = {
        role: inferred.role,
        capabilities: inferred.capabilities,
        toolkits: inferred.toolkits,
        taskTypes: inferred.taskTypes,
        connectedApps: inferred.connectedApps,
        canTakeExternalAction: inferred.canTakeExternalAction,
        responsibilities: [
            { status: 'completed', meaning: 'Use only when the requested outcome really happened.' },
            { status: 'blocked', meaning: 'Use when you are not the right owner or something fundamental is missing.' },
            { status: 'awaiting_connection', meaning: 'Use when an app or account needs to be connected first.' },
            { status: 'awaiting_approval', meaning: 'Use when the work is ready but needs human sign-off.' }
        ],
        summary: inferred.summary || `${label || agentId} owns focused specialist work inside this workspace.`
    };

    return {
        profile,
        identityMd: String(identityMd || '').trim() || [
            `# ${label || 'Specialist agent'}`,
            '',
            `You are ${label || 'a specialist agent'}.`,
            'Own the domain you were created for, communicate clearly, and stay aligned with the workspace mission.'
        ].join('\n'),
        soulMd: String(soulMd || '').trim() || [
            '# Working Style',
            '',
            'Operate with calm judgment, evidence-driven thinking, and direct communication.',
            'Prefer depth over speed when the task is ambiguous, and summarize tradeoffs clearly.'
        ].join('\n'),
        agentsMd: String(agentsMd || '').trim() || [
            '# Operating Rules',
            '',
            renderMissionControlProfile(profile),
            '',
            `You are the ${label || 'specialist'} agent.`,
            'Accept delegated tasks from the main manager and report blockers explicitly.',
            'Do not claim a task is complete unless the requested outcome actually happened.',
            'If a tool, app connection, or approval is missing, say so directly and use the correct status.'
        ].join('\n')
    };
}

function ensureManagerProfileForInstance(instanceId, config, { force = false } = {}) {
    const workspace = getMainWorkspace(instanceId);
    ensureDirSync(workspace);
    removeBootstrapFiles(workspace);
    const manager = buildManagerProfile();
    const identityPath = path.join(workspace, 'IDENTITY.md');
    const soulPath = path.join(workspace, 'SOUL.md');
    const agentsPath = path.join(workspace, 'AGENTS.md');

    if (force || !fs.existsSync(identityPath) || !String(fs.readFileSync(identityPath, 'utf8')).trim()) {
        fs.writeFileSync(identityPath, `${manager.identityMd.trim()}\n`, 'utf8');
    }
    if (force || !fs.existsSync(soulPath) || !String(fs.readFileSync(soulPath, 'utf8')).trim()) {
        fs.writeFileSync(soulPath, `${manager.soulMd.trim()}\n`, 'utf8');
    }
    if (force || !fs.existsSync(agentsPath) || !String(fs.readFileSync(agentsPath, 'utf8')).trim()) {
        fs.writeFileSync(agentsPath, `${manager.agentsMd.trim()}\n`, 'utf8');
    }

    if (config && typeof config === 'object') {
        config.agents = config.agents || {};
        config.agents.defaults = config.agents.defaults || {};
        config.agents.list = Array.isArray(config.agents.list) ? config.agents.list : [];
        let mainAgent = config.agents.list.find((agent) => agent?.id === MANAGER_AGENT_ID);
        if (!mainAgent) {
            mainAgent = {
                id: MANAGER_AGENT_ID,
                name: MANAGER_IDENTITY_NAME,
                workspace: '/home/node/.openclaw',
                agentDir: '/home/node/.openclaw/agent'
            };
            config.agents.list.unshift(mainAgent);
        }
        if (force || !String(mainAgent.name || '').trim()) {
            mainAgent.name = MANAGER_IDENTITY_NAME;
        }
    }
}

function readAgentProfile(instanceId, agentId) {
    const workspace = getAgentWorkspacePath(instanceId, agentId);
    const identityMd = readTextFileIfExists(path.join(workspace, 'IDENTITY.md'));
    const soulMd = readTextFileIfExists(path.join(workspace, 'SOUL.md'));
    const agentsMd = readTextFileIfExists(path.join(workspace, 'AGENTS.md'));

    return {
        identityName: extractIdentityNameFromMarkdown(identityMd),
        files: {
            identityMd,
            soulMd,
            agentsMd
        }
    };
}

function syncAgentIdentityName(instanceId, agentId, nextName) {
    const workspace = getAgentWorkspacePath(instanceId, agentId);
    const identityPath = path.join(workspace, 'IDENTITY.md');
    const current = readTextFileIfExists(identityPath);
    if (!current) return;

    const trimmedName = String(nextName || '').trim();
    if (!trimmedName) return;

    let updated = current;
    if (/^#\s+.+$/m.test(updated)) {
        updated = updated.replace(/^#\s+.+$/m, `# ${trimmedName}`);
    } else {
        updated = `# ${trimmedName}\n\n${updated}`;
    }

    if (/^You are .+\.$/m.test(updated)) {
        updated = updated.replace(/^You are .+\.$/m, `You are ${trimmedName}.`);
    }

    fs.writeFileSync(identityPath, updated.endsWith('\n') ? updated : `${updated}\n`, 'utf8');
}

async function ensureWorkspaceOwnership(workspacePath) {
    await run('chown', ['-R', '1000:1000', workspacePath]);
}

function ensureAgentListEntry(config, agentId, label, model) {
    config.agents = config.agents || {};
    config.agents.list = Array.isArray(config.agents.list) ? config.agents.list : [];
    const existing = config.agents.list.find((agent) => agent?.id === agentId);
    if (existing) {
        throw new Error(`Agent "${agentId}" already exists`);
    }

    const next = {
        id: agentId,
        name: label || agentId,
        workspace: getAgentContainerWorkspace(agentId),
        agentDir: `${getAgentContainerWorkspace(agentId)}/agent`
    };

    if (model) next.model = model;
    config.agents.list.push(next);
}

function writeAgentProfileFiles(workspaceDir, { agentId, label, identityMd, soulMd, agentsMd }) {
    const normalizedLabel = String(label || '').trim() || 'Specialist agent';
    const specialist = buildSpecialistProfile({
        agentId: String(agentId || normalizedLabel.toLowerCase().replace(/[^a-z0-9-]/g, '-')),
        label: normalizedLabel,
        identityMd,
        soulMd,
        agentsMd
    });
    const files = [
        {
            name: 'IDENTITY.md',
            content: specialist.identityMd
        },
        {
            name: 'SOUL.md',
            content: specialist.soulMd
        },
        {
            name: 'AGENTS.md',
            content: specialist.agentsMd
        }
    ];

    for (const file of files) {
        fs.writeFileSync(path.join(workspaceDir, file.name), `${file.content.trim()}\n`, 'utf8');
    }
}

function ensureAgentWorkspaceOnDisk(instanceId, agentId) {
    const instanceRoot = path.join(INSTANCES_DIR, instanceId);
    const sourceWorkspace = path.join(instanceRoot, 'workspace');
    const targetWorkspace = getAgentHostWorkspace(instanceId, agentId);
    const mainAgentDir = path.join(instanceRoot, 'agents', 'main', 'agent');

    if (!fs.existsSync(sourceWorkspace)) {
        throw new Error(`Workspace template missing for ${instanceId}`);
    }
    if (fs.existsSync(targetWorkspace)) {
        throw new Error(`Agent workspace already exists for ${agentId}`);
    }

    fs.mkdirSync(path.dirname(targetWorkspace), { recursive: true });
    fs.cpSync(sourceWorkspace, targetWorkspace, { recursive: true, force: false, errorOnExist: true });
    removeBootstrapFiles(targetWorkspace);

    const targetAgentDir = path.join(targetWorkspace, 'agent');
    const targetSessionsDir = path.join(targetWorkspace, 'sessions');
    fs.mkdirSync(targetAgentDir, { recursive: true });
    fs.mkdirSync(targetSessionsDir, { recursive: true });

    const mainModelsPath = path.join(mainAgentDir, 'models.json');
    if (fs.existsSync(mainModelsPath)) {
        fs.copyFileSync(mainModelsPath, path.join(targetAgentDir, 'models.json'));
    }

    const authCandidates = [
        path.join(mainAgentDir, 'auth-profiles.json'),
        path.join(instanceRoot, 'auth-profiles.json'),
    ];
    const authSource = authCandidates.find((candidate) => fs.existsSync(candidate));
    if (authSource) {
        fs.copyFileSync(authSource, path.join(targetAgentDir, 'auth-profiles.json'));
    }

    return targetWorkspace;
}

async function createAgentViaConfig(instanceId, agentId, options = {}) {
    const { label, model, identityMd, soulMd, agentsMd } = options;
    const config = readInstanceConfig(instanceId);
    if (!config) throw new Error('Config not found');

    const targetWorkspace = ensureAgentWorkspaceOnDisk(instanceId, agentId);
    writeAgentProfileFiles(targetWorkspace, { agentId, label, identityMd, soulMd, agentsMd });
    await ensureWorkspaceOwnership(targetWorkspace);
    ensureAgentListEntry(config, agentId, label, model);
    writeInstanceConfig(instanceId, config);

    try {
        await ensureComposioForInstance(instanceId, [targetWorkspace]);
    } catch (err) {
        console.warn(`[vps-agent] composio setup skipped for ${instanceId}/${agentId}: ${err.message}`);
    }

    await ensureWorkspaceOwnership(targetWorkspace);

    await restartGateway(`openclaw-${instanceId}`);
    return {
        ok: true,
        workspace: getAgentContainerWorkspace(agentId),
        agentDir: `${getAgentContainerWorkspace(agentId)}/agent`,
        mode: 'config-fallback'
    };
}

async function deleteAgentFromConfig(instanceId, agentId) {
    if (agentId === 'main') {
        throw new Error('Refusing to delete main agent');
    }

    const config = readInstanceConfig(instanceId);
    if (!config) throw new Error('Config not found');

    const before = Array.isArray(config.agents?.list) ? config.agents.list.length : 0;
    config.agents = config.agents || {};
    config.agents.list = Array.isArray(config.agents.list) ? config.agents.list.filter((agent) => agent?.id !== agentId) : [];
    if (config.agents.list.length === before) {
        throw new Error(`Agent "${agentId}" not found`);
    }

    writeInstanceConfig(instanceId, config);

    const targetWorkspace = getAgentHostWorkspace(instanceId, agentId);
    if (fs.existsSync(targetWorkspace)) {
        fs.rmSync(targetWorkspace, { recursive: true, force: true });
    }

    await restartGateway(`openclaw-${instanceId}`);
    return { ok: true, deleted: true, mode: 'config-fallback', agentId };
}

async function dispatchAgentTask(containerName, gatewayToken, agentId, task) {
    const sessionKey = `agent:${agentId}:${agentId}`;

    if (gatewayToken) {
        const chatResult = await gatewayWsExec(containerName, gatewayToken, 'chat.send', {
            sessionKey,
            message: task,
            idempotencyKey: `spawn-${Date.now()}-${Math.random().toString(36).slice(2)}`
        });
        if (chatResult?.ok !== false) {
            return chatResult?.payload || chatResult;
        }

        const message = String(chatResult?.error?.message || '');
        if (!message.includes('missing scope: operator.write')) {
            return chatResult?.payload || chatResult;
        }

        console.warn(`[vps-agent] chat.send is write-gated for ${agentId}; using CLI fallback`);
    }

    const output = await runDockerExec(containerName, ['agent', '--agent', agentId, '--message', task, '--json']);
    return {
        ok: true,
        mode: 'cli-fallback',
        output,
    };
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

async function ensureMcporterInContainer(containerName) {
    let installedNow = false;
    try {
        await runDockerExecDirect(containerName, ['sh', '-lc', 'command -v mcporter >/dev/null 2>&1 || test -x /home/node/.npm-global/bin/mcporter']);
    } catch {
        console.log(`[vps-agent] installing mcporter in ${containerName}`);
        await runDockerExecDirect(containerName, ['sh', '-lc', 'npm install -g mcporter']);
        installedNow = true;
    }

    await runDockerExecAsRoot(containerName, ['sh', '-lc', 'if [ -x /home/node/.npm-global/bin/mcporter ] && [ ! -e /usr/local/bin/mcporter ]; then ln -sf /home/node/.npm-global/bin/mcporter /usr/local/bin/mcporter; fi']);

    try {
        await runDockerExecDirect(containerName, ['sh', '-lc', 'command -v mcporter >/dev/null 2>&1']);
        return installedNow;
    } catch {
        throw new Error('mcporter is installed but not executable via PATH');
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

async function ensureComposioForInstance(instanceId, extraWorkspaceDirs = [], serverConfigOverrides = {}) {
    if (!validId(instanceId)) {
        throw new Error('Invalid instanceId');
    }
    const nextServer = resolveComposioServerConfig(instanceId, serverConfigOverrides);
    const hasHeaders = nextServer.headers && Object.keys(nextServer.headers).length > 0;
    if (!hasHeaders) {
        return {
            ok: false,
            skipped: true,
            reason: 'Composio MCP session headers are not configured',
        };
    }

    const containerName = `openclaw-${instanceId}`;
    const config = readInstanceConfig(instanceId);
    if (!config) {
        throw new Error('Config not found');
    }

    const installedNow = await ensureMcporterInContainer(containerName);
    const workspaceDirs = getComposioWorkspaceDirs(instanceId, extraWorkspaceDirs);
    const writes = workspaceDirs.map((workspaceDir) => ensureComposioConfigAtWorkspace(workspaceDir, nextServer));
    const changedPaths = writes.filter((entry) => entry.changed).map((entry) => entry.path);

    return {
        ok: true,
        installed: true,
        installedNow,
        configChanged: changedPaths.length > 0,
        server: COMPOSIO_SERVER_NAME,
        workspaces: workspaceDirs,
        changedPaths,
        serverConfig: nextServer,
    };
}

app.post('/api/internal/composio-ensure', requireInternal, async (req, res) => {
    const { instanceId } = req.body;
    if (!instanceId) {
        return res.status(400).json({ error: 'instanceId is required' });
    }
    try {
        const result = await ensureComposioForInstance(instanceId);
        res.json(result);
    } catch (err) {
        console.error(`[vps-agent] composio-ensure failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/internal/composio-configure-session', requireInternal, async (req, res) => {
    const { instanceId, mcp } = req.body || {};
    if (!instanceId) {
        return res.status(400).json({ error: 'instanceId is required' });
    }
    if (!mcp || typeof mcp !== 'object') {
        return res.status(400).json({ error: 'mcp config is required' });
    }

    try {
        const result = await ensureComposioForInstance(instanceId, [], mcp);
        res.json(result);
    } catch (err) {
        console.error(`[vps-agent] composio-configure-session failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

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
            const validation = await validateTelegramBotToken(token);
            if (!validation.ok) {
                return res.status(400).json({
                    error: validation.error || 'Telegram bot token is invalid.',
                    code: 'TELEGRAM_TOKEN_INVALID'
                });
            }
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
