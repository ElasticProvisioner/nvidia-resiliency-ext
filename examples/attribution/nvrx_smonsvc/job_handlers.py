#  Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
#
#  NVIDIA CORPORATION and its licensors retain all intellectual property
#  and proprietary rights in and to this software, related documentation
#  and any modifications thereto.  Any use, reproduction, disclosure or
#  distribution of this software and related documentation without an express
#  license agreement from NVIDIA CORPORATION is strictly prohibited.

"""Job submission and result handling for nvrx_smonsvc."""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .attrsvc_client import AttrsvcClient
    from .models import MonitorState, SlurmJob

logger = logging.getLogger(__name__)


def categorize_path_error(state: "MonitorState", error_msg: str) -> None:
    """
    Categorize a path error and increment the appropriate counter.

    Args:
        state: MonitorState to update counters
        error_msg: Error message to categorize
    """
    error_lower = error_msg.lower()
    if "permission denied" in error_lower:
        state.path_errors_permission += 1
    elif "not found" in error_lower or "path not found" in error_lower:
        state.path_errors_not_found += 1
    elif "empty" in error_lower or "file is empty" in error_lower:
        state.path_errors_empty += 1
    else:
        state.path_errors_other += 1


def submit_log(
    job: "SlurmJob",
    log_path: str,
    state: "MonitorState",
    attrsvc_client: "AttrsvcClient",
) -> None:
    """
    Submit a log file to the attribution service.

    Args:
        job: The SLURM job to submit log for
        log_path: Path to the log file
        state: MonitorState to update counters
        attrsvc_client: Client for attrsvc HTTP requests
    """

    def on_success(response):
        try:
            result = response.json()
            mode = result.get("mode", "single")
            if mode == "folder":
                logs_dir = result.get("logs_dir", "")
                cycles_detected = result.get("cycles_detected", 0)
                cycles_analyzed = result.get("cycles_analyzed", 0)
                logger.info(
                    f"[{job.job_id}] POST submitted (folder mode): {log_path} "
                    f"(logs_dir={logs_dir}, cycles={cycles_detected}, analyzed={cycles_analyzed})"
                )
            else:
                logger.info(f"[{job.job_id}] POST submitted: {log_path}")
        except Exception:
            logger.info(f"[{job.job_id}] POST submitted: {log_path}")
        job.log_submitted = True
        job.post_success = True
        state.total_logs_submitted += 1
        state.total_post_success += 1

    def on_client_error(error_msg: str):
        job.log_submitted = True
        state.total_logs_submitted += 1
        categorize_path_error(state, error_msg)

    def on_404():
        logger.debug(f"[{job.job_id}] POST 404 (file not found): {log_path}")
        job.log_submitted = True  # Don't retry - attrsvc received the path
        state.total_logs_submitted += 1
        state.path_errors_not_found += 1

    attrsvc_client.request_with_retry(
        method="POST",
        job_id=job.job_id,
        log_path=log_path,
        on_success=on_success,
        on_client_error=on_client_error,
        on_404=on_404,
        user=job.user,
    )


def fetch_results(
    job: "SlurmJob",
    log_path: str,
    state: "MonitorState",
    attrsvc_client: "AttrsvcClient",
) -> None:
    """
    Fetch attribution results for a completed job.

    Args:
        job: The SLURM job to fetch results for
        log_path: Path to the log file
        state: MonitorState to update counters
        attrsvc_client: Client for attrsvc HTTP requests
    """

    def on_success(response):
        try:
            result = response.json()
        except Exception as e:
            logger.warning(f"[{job.job_id}] GET JSON decode error: {e}")
            job.result_fetched = True
            state.total_results_fetched += 1
            return
        log_attribution_result(job, log_path, result)
        job.result_fetched = True
        state.total_results_fetched += 1

    def on_client_error(error_msg: str):
        job.result_fetched = True
        state.total_results_fetched += 1
        categorize_path_error(state, error_msg)

    attrsvc_client.request_with_retry(
        method="GET",
        job_id=job.job_id,
        log_path=log_path,
        on_success=on_success,
        on_client_error=on_client_error,
    )


def log_attribution_result(job: "SlurmJob", log_path: str, response: dict) -> None:
    """
    Log a summary of the attribution result to stdout.

    Args:
        job: The SLURM job
        log_path: Path to the log file
        response: Attribution result response dict (may be single-file or folder mode)
    """
    try:
        logger.debug(f"[{job.job_id}] Raw response: {response}")

        # Check for folder mode response
        mode = response.get("mode", "single")
        cycle = response.get("cycle")
        total_cycles = response.get("total_cycles")
        analyzed_log_file = response.get("log_file", "")

        inner = response.get("result", response)

        if not inner or not inner.get("module"):
            logger.warning(
                f"[{job.job_id}] Attribution result is empty or missing module: {response}"
            )
            return

        module = inner.get("module")
        state = inner.get("state", "")

        # Handle timeout results specially
        if state == "timeout":
            error_msg = inner.get("error", "LLM analysis timed out")
            logger.warning(f"[{job.job_id}] Attribution timeout: {error_msg}")
            print(
                f"[{job.job_id}] Attribution timeout:\n"
                f"  Log: {log_path}\n"
                f"  Error: {error_msg}",
                flush=True,
            )
            return

        raw_result_id = inner.get("result_id", "")
        result_id = raw_result_id[:16] + "..." if len(raw_result_id) > 16 else raw_result_id

        attribution_result = inner.get("result", "")
        if isinstance(attribution_result, list):
            attribution_text = " | ".join(str(item) for item in attribution_result)
        else:
            attribution_text = str(attribution_result) if attribution_result else ""

        if len(attribution_text) > 200:
            attribution_text = attribution_text[:200] + "..."

        # Build output message
        lines = [f"[{job.job_id}] Attribution result:"]
        if mode == "folder":
            lines.append(f"  Mode: folder (cycle {cycle}/{total_cycles})")
            lines.append(f"  Slurm output: {log_path}")
            lines.append(f"  Analyzed log: {analyzed_log_file}")
        else:
            lines.append(f"  Log: {log_path}")
        lines.append(f"  Module: {module}")
        lines.append(f"  Result ID: {result_id}")
        lines.append(f"  State: {state}")
        lines.append(f"  Attribution: {attribution_text}")

        print("\n".join(lines), flush=True)

    except Exception as e:
        logger.warning(f"[{job.job_id}] Could not parse attribution result: {e}")
