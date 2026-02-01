#  Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
#
#  NVIDIA CORPORATION and its licensors retain all intellectual property
#  and proprietary rights in and to this software, related documentation
#  and any modifications thereto.  Any use, reproduction, disclosure or
#  distribution of this software and related documentation without an express
#  license agreement from NVIDIA CORPORATION is strictly prohibited.

"""FastAPI HTTP wrapper for AttributionService."""

import json
import logging
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from .config import ErrorCode, Settings, setup
from .service import (
    AnalysisResult,
    AnalyzerError,
    AttributionService,
    SplitlogAnalysisResult,
    SubmitResult,
)

# Rate limiter instance (uses client IP as key)
limiter = Limiter(key_func=get_remote_address)


def normalize_error_message(msg: str) -> str:
    """Normalize error message to lowercase without trailing period."""
    return str(msg).lower().rstrip(".")


def json_response(data: Any, *, pretty: bool = False, indent: int | None = None) -> Response:
    """
    Create a JSON response with optional pretty-printing.

    Args:
        data: Data to serialize to JSON
        pretty: If True, use 2-space indentation
        indent: Custom indentation level (overrides pretty)

    Returns:
        FastAPI Response with JSON content
    """
    if indent is not None:
        body = json.dumps(data, indent=indent)
    elif pretty:
        body = json.dumps(data, indent=2)
    else:
        body = json.dumps(data, separators=(",", ":"))
    return Response(content=body, media_type="application/json")


# Map ErrorCode to HTTP status codes (see spec Section 7)
_ERROR_CODE_TO_HTTP_STATUS: dict[ErrorCode, int] = {
    # 400 Bad Request
    ErrorCode.INVALID_PATH: 400,
    ErrorCode.NOT_REGULAR: 400,
    ErrorCode.EMPTY_FILE: 400,
    # 403 Forbidden
    ErrorCode.OUTSIDE_ROOT: 403,
    ErrorCode.NOT_READABLE: 403,
    ErrorCode.LOGS_DIR_NOT_READABLE: 403,
    # 404 Not Found
    ErrorCode.NOT_FOUND: 404,
    # 5xx Server Errors
    ErrorCode.JOB_LIMIT_REACHED: 503,
    ErrorCode.INTERNAL_ERROR: 500,
}
_DEFAULT_HTTP_STATUS = 400  # Default to client error

logger = logging.getLogger(__name__)


# Pydantic models for HTTP-specific request/response
class ErrorResponse(BaseModel):
    """Standard error body for nvrx_attrsvc."""

    error_code: str
    message: str
    details: Any | None = None


class SubmitRequest(BaseModel):
    """Submission model for analysis requests."""

    log_path: str
    user: str = "unknown"  # Optional: SLURM job user, for dataflow records
    job_id: str | None = None  # Optional: SLURM job ID, required for split logging mode


def _raise_error(error: AnalyzerError) -> None:
    """Convert AnalyzerError to HTTPException."""
    status_code = _ERROR_CODE_TO_HTTP_STATUS.get(error.error_code, _DEFAULT_HTTP_STATUS)
    raise HTTPException(
        status_code=status_code,
        detail={"error_code": error.error_code, "message": error.message},
    )


def create_app(cfg: Settings) -> FastAPI:
    """
    Construct and return the FastAPI app for the NVRX Attribution Service.

    This is a thin HTTP wrapper around AttributionService.
    """
    app = FastAPI(
        title="NVRX Attribution Service",
        summary="nvrx_attrsvc - NVRX attribution service for artifact/log analysis",
        contact={
            "name": "NVRX Attribution Service",
            "email": "nvrx@nvidia.com",
        },
        root_path=cfg.FAST_API_ROOT_PATH,
        debug=cfg.DEBUG,
    )

    # Initialize rate limiter
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    # Initialize service in app state
    app.state.service = AttributionService(cfg)
    app.state.cache_file = cfg.CACHE_FILE  # Store for shutdown access

    @app.on_event("startup")
    async def startup_event() -> None:
        """Set event loop and load persisted cache."""
        import asyncio

        app.state.service.set_event_loop(asyncio.get_running_loop())

        # Load persisted cache if configured
        if cfg.CACHE_FILE:
            loaded = app.state.service.load_cache(cfg.CACHE_FILE)
            if loaded > 0:
                logger.info(f"Restored {loaded} cached results from {cfg.CACHE_FILE}")

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        """Save cache and cleanup."""
        # Save cache before shutdown if configured
        if app.state.cache_file:
            app.state.service.save_cache(app.state.cache_file)

    # Global exception handlers to standardize error bodies
    @app.exception_handler(HTTPException)
    async def http_exception_handler(_, exc: HTTPException) -> JSONResponse:
        detail = exc.detail
        if isinstance(detail, dict):
            error_code = str(detail.get("error_code", exc.status_code)).lower()
            message = normalize_error_message(str(detail.get("message", "error")))
            body = ErrorResponse(error_code=error_code, message=message).model_dump()
        else:
            body = ErrorResponse(
                error_code=str(exc.status_code).lower(),
                message=normalize_error_message(str(detail)),
            ).model_dump()
        return JSONResponse(status_code=exc.status_code, content=body)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(
            "Unhandled exception: %s %s",
            request.method,
            request.url.path,
            exc_info=exc,
        )
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error_code="internal_error", message="internal server error"
            ).model_dump(),
        )

    @app.get("/healthz")
    async def healthcheck(
        request: Request,
        pretty: bool = Query(default=False, description="Pretty-print JSON output"),
        indent: int = Query(default=None, description="Indentation level (overrides pretty)"),
    ) -> Response:
        """
        Health check endpoint.

        Returns status based on LLM and dataflow health:
        - "ok": All systems healthy
        - "degraded": Some issues but service is functional (20-50% error rate)
        - "fail": Critical issues (>50% error rate)
        """
        service: AttributionService = request.app.state.service
        health = await service.get_health()
        return json_response(health, pretty=pretty, indent=indent)

    @app.get("/stats")
    async def get_stats(
        request: Request,
        pretty: bool = Query(default=False, description="Pretty-print JSON output"),
        indent: int = Query(default=None, description="Indentation level (overrides pretty)"),
    ) -> Response:
        """Get cache and request coalescing statistics. See spec Section 20."""
        service: AttributionService = request.app.state.service
        stats = await service.get_stats()
        return json_response(stats, pretty=pretty, indent=indent)

    @app.get("/inflight")
    async def get_inflight(
        request: Request,
        pretty: bool = Query(default=False, description="Pretty-print JSON output"),
        indent: int = Query(default=None, description="Indentation level (overrides pretty)"),
    ) -> Response:
        """Get currently in-flight requests. See spec Section 22."""
        service: AttributionService = request.app.state.service
        inflight = await service.get_inflight()
        return json_response(inflight, pretty=pretty, indent=indent)

    @app.get("/jobs")
    async def get_all_jobs(
        request: Request,
        pretty: bool = Query(default=False, description="Pretty-print JSON output"),
        indent: int = Query(default=None, description="Indentation level (overrides pretty)"),
    ) -> Response:
        """Get all tracked jobs (pending, single-file, and splitlog modes)."""
        service: AttributionService = request.app.state.service
        jobs = service.get_all_jobs()
        return json_response(jobs, pretty=pretty, indent=indent)

    @app.post(
        "/logs",
        response_model=SubmitResult,
        responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    )
    @limiter.limit(cfg.RATE_LIMIT_SUBMIT)
    async def submit_analysis(request: Request, req: SubmitRequest) -> SubmitResult:
        """
        Submit a log file for analysis tracking.

        If job_id is provided and LOGS_DIR is found in the slurm output,
        split logging mode is enabled. In split logging mode, the service tracks multiple
        cycles and analyzes log files from the LOGS_DIR folder.
        """
        service: AttributionService = request.app.state.service
        result = await service.submit_log(req.log_path, req.user, req.job_id)
        if isinstance(result, AnalyzerError):
            _raise_error(result)
        return result

    @app.get(
        "/print",
        responses={
            200: {"content": {"text/plain": {}}},
            400: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            500: {"model": ErrorResponse},
        },
    )
    @limiter.limit(cfg.RATE_LIMIT_PREVIEW)
    async def print_log_path(
        request: Request,
        log_path: str = Query(..., description="Absolute path to a file under allowed root"),
    ) -> str:
        """Return the first 4KB of a file for preview."""
        service: AttributionService = request.app.state.service
        result = service.read_file_preview(log_path)
        if isinstance(result, AnalyzerError):
            _raise_error(result)
        return result.content

    @app.get(
        "/logs",
        responses={
            200: {"model": AnalysisResult, "description": "Single-file mode result"},
            400: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            500: {"model": ErrorResponse},
        },
    )
    @limiter.limit(cfg.RATE_LIMIT_ANALYZE)
    async def attribution_log_path(
        request: Request,
        log_path: str = Query(
            ..., min_length=1, description="Absolute path to a log file under allowed root"
        ),
        file: str | None = Query(
            default=None,
            description="Filename for splitlog mode. Use to select specific log file.",
        ),
        wl_restart: int | None = Query(
            default=None,
            ge=0,
            description="Workload restart index within file (0-indexed). See spec Section 17.",
        ),
    ) -> AnalysisResult | SplitlogAnalysisResult:
        """
        Analyze a log file and return attribution results.

        For split logging mode jobs (where LOGS_DIR was found in the slurm output):
        - Use file= to select a specific log file by filename
        - Use wl_restart= to select a specific workload restart within that file
        - Response includes mode="splitlog", sched_restarts count, and log_file path

        For single-file mode jobs:
        - The file and wl_restart parameters are ignored
        - Response includes status="completed"

        See spec Section 10 and 17 for GET flow details.
        """
        service: AttributionService = request.app.state.service
        result = await service.analyze_log(log_path, file, wl_restart)
        if isinstance(result, AnalyzerError):
            _raise_error(result)
        return result

    return app


def main() -> None:
    """Entry point for the NVRX Attribution Service."""
    cfg = setup()

    # Configure Slack integration (if token is set)
    from .postprocessing import configure_postprocessing

    configure_postprocessing(
        slack_bot_token=cfg.SLACK_BOT_TOKEN,
        slack_channel=cfg.SLACK_CHANNEL,
    )

    logger.info(f"Starting NVRX Attribution Service (nvrx_attrsvc) on {cfg.HOST}:{cfg.PORT}")
    logger.info(f"nvrx_attrsvc API Documentation: http://{cfg.HOST}:{cfg.PORT}/docs")
    uvicorn.run(
        create_app(cfg),
        host=cfg.HOST,
        port=cfg.PORT,
        access_log=False,
        timeout_graceful_shutdown=30,  # Wait up to 30s for in-flight requests on shutdown
    )
