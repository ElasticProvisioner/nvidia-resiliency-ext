#  Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
#
#  NVIDIA CORPORATION and its licensors retain all intellectual property
#  and proprietary rights in and to this software, related documentation
#  and any modifications thereto.  Any use, reproduction, disclosure or
#  distribution of this software and related documentation without an express
#  license agreement from NVIDIA CORPORATION is strictly prohibited.

"""Log analyzer module for LLM-based log analysis.

This module provides:
- LogAnalyzer: Main API for analyzing logs (usable without HTTP)
- Core configuration and error codes
- Request coalescing for deduplication and caching
- Job/FileInfo data model for tracking analysis state
- SplitlogTracker for multi-file job tracking
- Scheduler parsers (SLURM, etc.)
- LLM response parsing utilities

Example usage:
    from nvidia_resiliency_ext.attribution import LogAnalyzer, AnalyzerConfig
    
    config = AnalyzerConfig(allowed_root="/logs")
    analyzer = LogAnalyzer(config)
    result = await analyzer.analyze("/logs/slurm-12345.out")
"""

from nvidia_resiliency_ext.attribution.postprocessing import (
    DataflowStats,
    PostFunction,
    ResultPoster,
    get_dataflow_stats,
    get_default_poster,
    post_results,
    set_default_poster,
)

from .analyzer import (
    AnalysisResult,
    AnalyzerConfig,
    AnalyzerError,
    AnalyzerResult,
    FilePreviewResult,
    LogAnalyzer,
    SplitlogAnalysisResult,
    SubmitResult,
)
from .coalescer import (
    CacheResult,
    CoalescerStats,
    InflightResult,
    RequestCoalescer,
    StatsResult,
    SubmittedResult,
)
from .config import (
    DEFAULT_COMPUTE_TIMEOUT_SECONDS,
    MAX_JOBS,
    MIN_FILE_SIZE_KB,
    POLL_INTERVAL_SECONDS,
    TTL_MAX_JOB_AGE_SECONDS,
    TTL_PENDING_SECONDS,
    TTL_TERMINATED_SECONDS,
    ErrorCode,
)
from .job import FileInfo, Job, JobMode
from .parser_base import BaseParser, ParseResult
from .slurm_parser import (
    SlurmOutputInfo,
    SlurmParser,
    parse_slurm_output,
    read_and_parse_slurm_output,
)
from .splitlog import (
    CYCLE_NUM_PATTERN,
    DATE_TIME_PATTERN,
    DEFAULT_MAX_JOB_AGE_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TERMINATED_JOB_TTL_SECONDS,
    SplitlogTracker,
)
from .utils import (
    CYCLE_LOG_PATTERN,
    JOB_ID_PATTERNS,
    JobMetadata,
    ParsedLLMResponse,
    build_dataflow_record,
    extract_job_metadata,
    parse_llm_response,
)

__all__ = [
    # Main API
    "LogAnalyzer",
    "AnalyzerConfig",
    "AnalyzerError",
    "AnalyzerResult",
    "AnalysisResult",
    "SubmitResult",
    "SplitlogAnalysisResult",
    "FilePreviewResult",
    # Configuration and error codes
    "ErrorCode",
    "TTL_PENDING_SECONDS",
    "TTL_TERMINATED_SECONDS",
    "TTL_MAX_JOB_AGE_SECONDS",
    "POLL_INTERVAL_SECONDS",
    "DEFAULT_COMPUTE_TIMEOUT_SECONDS",
    "MAX_JOBS",
    "MIN_FILE_SIZE_KB",
    # Request coalescing
    "RequestCoalescer",
    "CoalescerStats",
    "StatsResult",
    "CacheResult",
    "InflightResult",
    "SubmittedResult",
    # Job data model
    "Job",
    "FileInfo",
    "JobMode",
    # Scheduler parsers
    "BaseParser",
    "ParseResult",
    "SlurmParser",
    "SlurmOutputInfo",
    "parse_slurm_output",
    "read_and_parse_slurm_output",
    # Splitlog tracking
    "SplitlogTracker",
    "CYCLE_NUM_PATTERN",
    "DATE_TIME_PATTERN",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DEFAULT_TERMINATED_JOB_TTL_SECONDS",
    "DEFAULT_MAX_JOB_AGE_SECONDS",
    # Regex patterns
    "CYCLE_LOG_PATTERN",
    "JOB_ID_PATTERNS",
    # LLM response parsing
    "ParsedLLMResponse",
    "parse_llm_response",
    # Log path metadata
    "JobMetadata",
    "extract_job_metadata",
    # Dataflow record building
    "build_dataflow_record",
    # Postprocessing
    "ResultPoster",
    "DataflowStats",
    "PostFunction",
    "get_dataflow_stats",
    "get_default_poster",
    "set_default_poster",
    "post_results",
]
