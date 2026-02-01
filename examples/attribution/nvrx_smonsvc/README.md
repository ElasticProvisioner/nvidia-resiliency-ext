# NVRX SLURM Monitor Service (nvrx-smonsvc)

SLURM job monitor that automatically submits completed job logs to the Attribution Service for analysis.

## Quick Start

```bash
# Install
cd examples/attribution
pip install -e .

# Run (requires nvrx-attrsvc running)
export NVRX_ATTRSVC_URL=http://localhost:8000
export NVRX_SMONSVC_PARTITIONS="batch batch_long"
nvrx-smonsvc
```

## Configuration

Environment variables (prefix: `NVRX_SMONSVC_`) or command-line arguments:

| Variable / Argument | Default | Description |
|---------------------|---------|-------------|
| `ATTRSVC_URL` / `--attrsvc-url` | `http://localhost:8000` | Attribution service URL |
| `PORT` / `--port` | `None` | Port for HTTP server (stats, health, jobs) |
| `INTERVAL` / `--interval` | `180` | Poll interval in seconds |
| `PARTITIONS` / `--partitions` | `batch batch_long` | SLURM partitions (space-separated) |
| `USER` / `--user` | all users | Filter jobs by user |
| `JOB_PATTERN` / `--job-pattern` | `None` | Regex to filter job names |
| `TIMEOUT` / `--timeout` | `60` | HTTP request timeout in seconds |
| `VERBOSE` / `-v, --verbose` | `false` | Enable verbose logging |
| `LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |

CLI arguments override environment variables.

## API Endpoints

When `PORT` is set, the monitor exposes an HTTP server:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Health check |
| `/stats` | GET | Monitor statistics |
| `/jobs` | GET | List tracked jobs |

## How It Works

1. Polls SLURM for completed/failed jobs in configured partitions
2. For each terminal job, extracts the output log path
3. Submits the log to the Attribution Service via POST /logs
4. Tracks job state to avoid duplicate submissions

### SLURM Job ID Handling

The monitor handles various SLURM job ID formats:

| Format | Type | Handling |
|--------|------|----------|
| `12345` | Regular job | Processed normally |
| `12345_0` | Array task | Processed normally |
| `12345+0` | Heterogeneous (het) job component | Processed (sacct uses base ID) |
| `12345[0-10]` | Array summary | Skipped (individual tasks queried separately) |

**Heterogeneous jobs**: Het job components (e.g., `1234+0`, `1234+1`) are processed. When fetching
output paths via `sacct`, the base job ID is used since SLURM associates paths with the parent job.

**Array jobs**: Array task summaries with bracket notation are skipped since they don't represent
individual runnable jobs. Individual array tasks (e.g., `12345_0`) are processed normally.

## Usage Examples

```bash
# Monitor specific partitions
nvrx-smonsvc --partitions "gpu gpu_long"

# Monitor specific user's jobs
nvrx-smonsvc --user alice

# Filter jobs by name pattern
nvrx-smonsvc --job-pattern "training_.*"

# Enable HTTP status server
nvrx-smonsvc --port 8100

# Verbose logging
nvrx-smonsvc -v
```

## Files

| File | Description |
|------|-------------|
| `__main__.py` | CLI entry point |
| `monitor.py` | Main monitor class |
| `slurm.py` | SLURM subprocess calls (squeue, scontrol, sacct) with batching and het job support |
| `attrsvc_client.py` | HTTP client for Attribution Service |
| `status_server.py` | Status server (/stats, /jobs, /healthz) |
| `models.py` | Data models (JobState, SlurmJob, MonitorState) |
| `run_smonsvc.sh` | Run service with logging (background) |
| `snapshot_smonsvc.sh` | Periodic endpoint snapshot for debugging |

## Deployment

See `deploy/` directory:
- **SLURM**: `deploy/slurm.sbatch`

For combined deployment with attrsvc, see `../scripts/nvrx_services.sbatch`
