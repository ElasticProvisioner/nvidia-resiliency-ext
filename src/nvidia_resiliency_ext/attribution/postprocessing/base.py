# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generic result posting framework.

This module provides the generic framework for posting analysis results
to external systems. The actual posting implementation is injected via
a callback function, allowing proprietary integrations to be kept separate.

Example usage:
    from nvidia_resiliency_ext.attribution.postprocessing import (
        ResultPoster,
        get_dataflow_stats,
    )
    
    # Create poster with custom post function
    def my_post_fn(data: dict, index: str) -> bool:
        # Custom posting logic (e.g., Elasticsearch, database)
        return True
    
    poster = ResultPoster(post_fn=my_post_fn)
    poster.post_results(parsed, metadata, log_path, processing_time, cluster, index)
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from nvidia_resiliency_ext.attribution.log_analyzer.utils import (
    JobMetadata,
    ParsedLLMResponse,
    build_dataflow_record,
)

logger = logging.getLogger(__name__)


@dataclass
class DataflowStats:
    """Statistics for dataflow/posting operations."""

    total_posts: int = 0
    successful_posts: int = 0
    failed_posts: int = 0


# Type alias for post function signature
PostFunction = Callable[[Dict[str, Any], str], bool]


class ResultPoster:
    """
    Posts analysis results to external systems.

    The actual posting is done via an injected callback function,
    allowing proprietary implementations to be kept separate.

    Example:
        def elasticsearch_post(data: dict, index: str) -> bool:
            # Post to Elasticsearch
            return True

        poster = ResultPoster(post_fn=elasticsearch_post)
        poster.post_results(...)
    """

    def __init__(self, post_fn: Optional[PostFunction] = None):
        """
        Initialize the result poster.

        Args:
            post_fn: Function to post data. Signature: (data: dict, index: str) -> bool
                     If None, results are logged but not posted.
        """
        self._post_fn = post_fn
        self._stats = DataflowStats()

    @property
    def stats(self) -> DataflowStats:
        """Get current posting statistics."""
        return self._stats

    def post_results(
        self,
        parsed: ParsedLLMResponse,
        metadata: JobMetadata,
        log_path: str,
        processing_time: float,
        cluster_name: str,
        dataflow_index: str,
        user: str = "unknown",
    ) -> bool:
        """
        Post analysis results.

        Args:
            parsed: Parsed LLM response
            metadata: Job metadata from path
            log_path: Path to the log file
            processing_time: Time taken for analysis in seconds
            cluster_name: Cluster name for dataflow
            dataflow_index: Dataflow/elasticsearch index name
            user: Job owner

        Returns:
            True if posted successfully, False otherwise
        """
        # Build the record
        data = build_dataflow_record(
            parsed=parsed,
            metadata=metadata,
            log_path=log_path,
            processing_time=processing_time,
            cluster_name=cluster_name,
            user=user,
        )

        logger.info("jobid: %s", metadata.job_id)
        logger.info("log_path: %s", log_path)
        logger.info("auto_resume: %s", parsed.auto_resume)
        logger.info("auto_resume_explanation: %s", parsed.auto_resume_explanation)
        logger.info("attribution_text: %s", parsed.attribution_text)

        self._stats.total_posts += 1

        if self._post_fn is None:
            logger.debug("No post function configured, skipping post")
            return True

        success = self._post_fn(data, dataflow_index)
        if success:
            self._stats.successful_posts += 1
        else:
            self._stats.failed_posts += 1
        return success


# Module-level default poster (for backwards compatibility)
_default_poster: Optional[ResultPoster] = None


def get_default_poster() -> ResultPoster:
    """Get or create the default ResultPoster instance."""
    global _default_poster
    if _default_poster is None:
        _default_poster = ResultPoster()
    return _default_poster


def set_default_poster(poster: ResultPoster) -> None:
    """Set the default ResultPoster instance."""
    global _default_poster
    _default_poster = poster


def get_dataflow_stats() -> DataflowStats:
    """Get current dataflow statistics from default poster."""
    return get_default_poster().stats


def post_results(
    parsed: ParsedLLMResponse,
    metadata: JobMetadata,
    log_path: str,
    processing_time: float,
    cluster_name: str,
    dataflow_index: str,
    user: str = "unknown",
) -> bool:
    """
    Post analysis results using the default poster.

    For custom posting behavior, use ResultPoster directly or
    call set_default_poster() to configure a custom post function.
    """
    return get_default_poster().post_results(
        parsed=parsed,
        metadata=metadata,
        log_path=log_path,
        processing_time=processing_time,
        cluster_name=cluster_name,
        dataflow_index=dataflow_index,
        user=user,
    )
