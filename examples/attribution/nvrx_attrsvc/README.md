# NVRX Attribution Service (nvrx-attrsvc)

FastAPI server that analyzes log files using LLM-based failure attribution.

## Quick Start

```bash
# Install
cd examples/attribution
pip install -e .

# Run
export NVRX_ATTRSVC_ALLOWED_ROOT=/path/to/logs
# API key: set env var OR create ~/.nvidia_api_key file
export NVIDIA_API_KEY=nvapi-...
nvrx-attrsvc
```

## Configuration

Environment variables (prefix: `NVRX_ATTRSVC_`):

| Variable | Default | Description |
|----------|---------|-------------|
| `ALLOWED_ROOT` | (required) | Base directory for allowed log paths |
| `HOST` | `0.0.0.0` | Listen address |
| `PORT` | `8000` | Listen port |
| `LOG_LEVEL_NAME` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |
| `CLUSTER_NAME` | `""` | Cluster name for dataflow posting |
| `DATAFLOW_INDEX` | `""` | Elasticsearch index for result posting |
| `RATE_LIMIT_SUBMIT` | `1200/minute` | Rate limit for POST /logs |
| `RATE_LIMIT_ANALYZE` | `60/minute` | Rate limit for GET /logs |
| `RATE_LIMIT_PREVIEW` | `120/minute` | Rate limit for GET /print |

**LLM Settings** (optional - library defaults used if not set):

| Variable | Description |
|----------|-------------|
| `LLM_MODEL` | LLM model identifier |
| `LLM_TEMPERATURE` | Temperature (0.0 = deterministic) |
| `LLM_TOP_P` | Top-p for nucleus sampling |
| `LLM_MAX_TOKENS` | Max tokens for response |
| `COMPUTE_TIMEOUT` | Timeout for analysis in seconds |

**NVIDIA API Key** (required, checked in order):
1. `NVIDIA_API_KEY` environment variable
2. `NVIDIA_API_KEY_FILE` environment variable (path to file)
3. `~/.nvidia_api_key` file
4. `~/.config/nvrx/nvidia_api_key` file

**Slack Notifications** (optional):

| Variable | Default | Description |
|----------|---------|-------------|
| `SLACK_BOT_TOKEN` | `""` | Slack bot OAuth token (empty = disabled) |
| `SLACK_CHANNEL` | `""` | Slack channel for terminal failure alerts |

When configured, sends alerts to Slack for jobs with `auto_resume = "STOP - DONT RESTART IMMEDIATE"`.

**Processed Files Ledger** (optional cache persistence):

| Variable | Default | Description |
|----------|---------|-------------|
| `CACHE_FILE` | `""` | Path to ledger file for persistence (empty = disabled) |
| `CACHE_GRACE_PERIOD_SECONDS` | `600` | Grace period before validating file on cache hit (10 min) |

The cache acts as a **processed files ledger** - tracking which files have been analyzed
and posted to Elasticsearch. This prevents duplicate processing after service restarts.

**Why needed:** When the service restarts, smonsvc resubmits recently completed jobs.
Without the ledger, all files would be re-analyzed and re-posted to ES. The ledger
allows the service to recognize "I've already processed this file" and skip it.

**What's stored:** `(path, mtime, size, result)` for each processed file.

**Validation strategy:**

| Phase | Behavior |
|-------|----------|
| **Grace period** (first 10 min) | Serve from cache without file validation |
| **After grace period** | `stat()` file on each hit; if `(mtime, size)` changed, invalidate and re-analyze |
| **Eviction** | Remove entries when file mtime > 14 days (safeguard against unbounded growth) |
| **On import** | Validate `(mtime, size)`; skip if file changed or > 14 days old |

The grace period absorbs straggling writes at end of file, preventing unnecessary re-analysis.

**Note:** Ledger is saved only on graceful shutdown (SIGTERM). If the service crashes or is
killed (SIGKILL), in-memory entries are lost. Use a process manager with adequate stop timeout.

Example: `NVRX_ATTRSVC_CACHE_FILE=/var/lib/nvrx/attrsvc_cache.json`

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Health check |
| `/stats` | GET | Cache and request statistics |
| `/jobs` | GET | All tracked jobs (pending, single, splitlog modes) |
| `/logs` | POST | Submit log for analysis |
| `/logs` | GET | Retrieve analysis results |
| `/print` | GET | Preview first 4KB of file |
| `/inflight` | GET | In-flight requests |
| `/docs` | GET | OpenAPI documentation |

**POST /logs** body:
```json
{
  "log_path": "/path/to/slurm-12345.out",
  "user": "alice",
  "job_id": "12345"
}
```

**GET /logs** query params:
- `log_path` (required): Path to job output file
- `file` (optional): Filename for splitlog mode
- `wl_restart` (optional): Workload restart index within file

## Resource Requirements

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 0.5 | 2 | Mostly I/O bound |
| Memory | 256MB | 1GB | Depends on MAX_JOBS and file sizes |
| Disk | 100MB | 500MB | Logs only (no persistence) |

## Deployment

See `deploy/` directory:
- **Docker**: `deploy/Dockerfile`
- **Kubernetes**: `deploy/kubernetes.yaml`
- **SLURM**: `deploy/slurm.sbatch`

For combined deployment with monitor, see `../scripts/nvrx_services.sbatch`

## Architecture

Two-layer design:

1. **Library Layer** (`nvidia_resiliency_ext.attribution`): Pure Python, no HTTP dependencies
   - `LogAnalyzer`: Main API for analyzing logs
   - Request coalescing, job tracking, splitlog management
   - Scheduler parsers (SLURM), LLM integration via MCP

2. **Service Layer** (`nvrx_attrsvc`): HTTP wrapper
   - `AttributionService`: Thin wrapper around `LogAnalyzer`
   - FastAPI routes, pydantic Settings
   - Proprietary dataflow posting

## Python API

### Library API (No HTTP Required)

```python
import asyncio
from nvidia_resiliency_ext.attribution import LogAnalyzer, AnalyzerConfig

async def main():
    config = AnalyzerConfig(allowed_root="/path/to/logs")
    analyzer = LogAnalyzer(config)
    
    result = await analyzer.submit("/path/to/slurm-12345.out", user="alice", job_id="12345")
    analysis = await analyzer.analyze("/path/to/slurm-12345.out")
    
    analyzer.shutdown()

asyncio.run(main())
```

### HTTP Service Wrapper

```python
import asyncio
from nvrx_attrsvc import AttributionService, setup

async def main():
    cfg = setup()
    service = AttributionService(cfg)
    
    result = await service.analyze_log("/path/to/job.log")
    service.shutdown()

asyncio.run(main())
```

## Files

| File | Description |
|------|-------------|
| `app.py` | FastAPI routes and middleware |
| `service.py` | `AttributionService` - wraps LogAnalyzer |
| `config.py` | `Settings` (pydantic), re-exports from lib |
| `postprocessing.py` | Configures lib with proprietary dataflow |
| `dataflow.py` | NVIDIA-proprietary Elasticsearch posting |
| `run_attrsvc.sh` | Run service with logging (background) |
| `snapshot_attrsvc.sh` | Periodic endpoint snapshot for debugging |
| `deploy/Dockerfile` | Docker build instructions |
| `deploy/kubernetes.yaml` | Kubernetes deployment manifest |
| `deploy/slurm.sbatch` | SLURM batch script |

## Documentation

See [NVRX_ATTRSVC_SPEC.md](NVRX_ATTRSVC_SPEC.md) for detailed technical specification.
