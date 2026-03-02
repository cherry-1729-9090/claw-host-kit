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

const CONTAINER_RAM_LIMIT_MB = parseInt(process.env.OPENCLAW_CONTAINER_RAM_MB || '5120'); // 5 GB default

function requireInternal(req, res, next) {
    if (!INTERNAL_SECRET || req.headers['x-internal-secret'] !== INTERNAL_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
}

function validId(id) {
    return typeof id === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(id);
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
    if (!validId(instanceId)) return res.status(400).json({ error: 'Invalid instanceId' });

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

        const result = {
            ok: true,
            instanceId,
            containerName: `openclaw-${instanceId}`,
            gatewayToken,
            ramLimitMB: CONTAINER_RAM_LIMIT_MB,
            output: output.trim(),
        };
        console.log(`[vps-agent] create-instance done for ${instanceId}:`, JSON.stringify({ ...result, output: '[truncated]' }));
        return res.json(result);
    } catch (err) {
        console.error('[vps-agent] create-instance error:', err.message);
        return res.status(500).json({ error: err.message });
    }
});

// ── Remove instance ───────────────────────────────────────────────────────────

app.delete('/api/internal/remove-instance/:instanceId', requireInternal, async (req, res) => {
    const { instanceId } = req.params;
    if (!validId(instanceId)) return res.status(400).json({ error: 'Invalid instanceId' });

    try {
        await run('docker', ['rm', '-f', `openclaw-${instanceId}`]);
        execFile('rm', ['-rf', `/var/lib/openclaw/instances/${instanceId}`]);
        return res.json({ ok: true, instanceId });
    } catch (err) {
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

app.post('/api/internal/configure-provider', requireInternal, async (req, res) => {
    const { instanceId, provider, token, expiresIn } = req.body;
    if (!instanceId || !provider || !token) {
        return res.status(400).json({ error: 'instanceId, provider, and token are required' });
    }
    const containerName = `openclaw-${instanceId}`;
    try {
        const args = ['models', 'auth', 'paste-token', '--provider', provider];
        if (expiresIn) args.push('--expires-in', expiresIn);
        const output = await runDockerExec(containerName, args, token);
        console.log(`[vps-agent] configured provider ${provider} for ${instanceId}`);
        res.json({ success: true, output });
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
        config.models.providers = config.models.providers || {};

        const provider = { baseUrl };
        if (apiKey) provider.apiKey = apiKey;
        if (api) provider.api = api === 'anthropic' ? 'anthropic-messages' : 'openai-completions';
        if (headers && typeof headers === 'object') provider.headers = headers;
        if (Array.isArray(models) && models.length) {
            provider.models = models.map(m => ({
                id: m.id || m,
                name: m.name || m.id || String(m)
            }));
        }

        config.models.providers[key] = provider;

        // Write auth-profiles.json so the gateway can find the API key
        if (apiKey) {
            const authDir = path.join(INSTANCES_DIR, instanceId, 'agents', 'main', 'agent');
            fs.mkdirSync(authDir, { recursive: true });
            const authPath = path.join(authDir, 'auth-profiles.json');
            let authProfiles = {};
            try { authProfiles = JSON.parse(fs.readFileSync(authPath, 'utf8')); } catch {}
            authProfiles[key] = { apiKey };
            fs.writeFileSync(authPath, JSON.stringify(authProfiles), 'utf8');
        }

        writeInstanceConfig(instanceId, config);
        console.log(`[vps-agent] custom provider ${key} configured for ${instanceId}`);
        res.json({ success: true, providerKey: key });
    } catch (err) {
        console.error(`[vps-agent] configure-custom-provider failed for ${instanceId}:`, err.message);
        res.status(500).json({ error: err.message });
    }
});

// ── Sub-agent spawn (calls gateway REST API internally) ───────────────────────
app.post('/api/internal/subagents-spawn', requireInternal, async (req, res) => {
    const { instanceId, task, label, model, agentId } = req.body;
    if (!instanceId || !task) {
        return res.status(400).json({ error: 'instanceId and task are required' });
    }
    const config = readInstanceConfig(instanceId);
    if (!config) return res.status(404).json({ error: 'Config not found' });

    const gatewayToken = config.gateway?.auth?.token;
    if (!gatewayToken) return res.status(500).json({ error: 'No gateway token found' });

    // Call the gateway's REST API from inside the host (bypasses Traefik)
    const containerName = `openclaw-${instanceId}`;
    try {
        const body = JSON.stringify({ task, label, model, agentId: agentId || 'main' });
        const args = [
            'exec', containerName,
            'curl', '-sf', '-X', 'POST',
            'http://localhost:18789/api/tasks',
            '-H', 'Content-Type: application/json',
            '-H', `Authorization: Bearer ${gatewayToken}`,
            '-d', body
        ];
        const output = await run('docker', args, { timeout: 15_000 });
        const data = JSON.parse(output);
        console.log(`[vps-agent] subagent spawned for ${instanceId}`);
        res.json(data);
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
