import express from 'express';
import { execFile, exec } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const app = express();
app.use(express.json());

const PORT = process.env.VPS_AGENT_PORT || 4444;
const INTERNAL_SECRET = process.env.OPENCLAW_INTERNAL_SECRET || '';
const HOST_KIT_DIR = process.env.OPENCLAW_HOST_KIT_DIR
    || path.dirname(fileURLToPath(import.meta.url));

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

    const scriptPath = path.join(HOST_KIT_DIR, 'scripts', 'create-instance.sh');
    if (!fs.existsSync(scriptPath)) {
        return res.status(500).json({ error: `create-instance.sh not found at ${scriptPath}` });
    }

    try {
        const memRaw = await shell("free -m | awk 'NR==2{print $4}'");
        const freeMemMB = parseInt(memRaw, 10);

        if (freeMemMB < CONTAINER_RAM_LIMIT_MB) {
            return res.status(507).json({
                error: 'Insufficient memory',
                freeMemMB,
                requiredMB: CONTAINER_RAM_LIMIT_MB,
            });
        }

        const output = await new Promise((resolve, reject) => {
            execFile('bash', [scriptPath, instanceId], {
                cwd: HOST_KIT_DIR,
                timeout: 90_000,
                env: {
                    ...process.env,
                    OPENCLAW_CONTAINER_MEMORY: `${CONTAINER_RAM_LIMIT_MB}m`,
                },
            }, (err, stdout, stderr) => {
                if (err) return reject(new Error(`create-instance.sh: ${stderr || err.message}`));
                resolve(stdout);
            });
        });

        const configPath = path.join(`/var/lib/openclaw/instances/${instanceId}`, 'openclaw.json');
        let gatewayToken = null;
        try {
            if (fs.existsSync(configPath)) {
                gatewayToken = JSON.parse(fs.readFileSync(configPath, 'utf8'))?.gateway?.auth?.token || null;
            }
        } catch { /* not yet written */ }

        return res.json({
            ok: true,
            instanceId,
            containerName: `openclaw-${instanceId}`,
            gatewayToken,
            ramLimitMB: CONTAINER_RAM_LIMIT_MB,
            output: output.trim(),
        });
    } catch (err) {
        console.error('[vps-agent] create-instance:', err);
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

app.listen(PORT, '0.0.0.0', () => console.log(`[vps-agent] port ${PORT}`));
