# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Postprocessing for attribution results.

This module provides:
- ResultPoster: Generic framework for posting results to external systems
- Slack notifications: Alert users about terminal failures

Example usage:
    from nvidia_resiliency_ext.attribution.postprocessing import (
        ResultPoster,
        send_slack_notification,
        should_notify_slack,
    )
    
    # Custom result posting
    def my_post_fn(data: dict, index: str) -> bool:
        # Post to Elasticsearch, database, etc.
        return True
    
    poster = ResultPoster(post_fn=my_post_fn)
    
    # Slack notifications
    if should_notify_slack(auto_resume):
        send_slack_notification(data, token, channel)
"""

from .base import (
    DataflowStats,
    PostFunction,
    ResultPoster,
    get_dataflow_stats,
    get_default_poster,
    post_results,
    set_default_poster,
)
from .slack import (
    HAS_SLACK,
    SlackStats,
    get_slack_stats,
    get_slack_user_id,
    send_slack_notification,
    should_notify_slack,
)

__all__ = [
    # Base posting framework
    "DataflowStats",
    "PostFunction",
    "ResultPoster",
    "get_dataflow_stats",
    "get_default_poster",
    "post_results",
    "set_default_poster",
    # Slack notifications
    "HAS_SLACK",
    "SlackStats",
    "get_slack_stats",
    "get_slack_user_id",
    "send_slack_notification",
    "should_notify_slack",
]
