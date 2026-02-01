#  Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
#
#  NVIDIA CORPORATION and its licensors retain all intellectual property
#  and proprietary rights in and to this software, related documentation
#  and any modifications thereto.  Any use, reproduction, disclosure or
#  distribution of this software and related documentation without an express
#  license agreement from NVIDIA CORPORATION is strictly prohibited.

"""Service-layer postprocessing - configures library with proprietary dataflow and Slack.

The generic postprocessing framework is in the library:
    from nvidia_resiliency_ext.attribution.postprocessing import ResultPoster

This module configures it with:
- Proprietary dataflow.post function
- Slack notifications for terminal failures (when SLACK_BOT_TOKEN is set)
"""

import logging
from typing import Any

# Re-export from library for backwards compatibility
from nvidia_resiliency_ext.attribution.postprocessing import (
    DataflowStats,
    ResultPoster,
    get_dataflow_stats,
    get_slack_stats,
    post_results,
    send_slack_notification,
    set_default_poster,
    should_notify_slack,
)

from . import dataflow

logger = logging.getLogger(__name__)

# Slack configuration (set by configure_postprocessing from config)
_slack_bot_token: str = ""
_slack_channel: str = ""


def _post_with_slack(data: dict[str, Any], index: str) -> bool:
    """Post to dataflow and optionally send Slack notification."""
    # Post to dataflow
    success = dataflow.post(data, index)

    # Send Slack notification if configured and result is terminal
    auto_resume = data.get("s_auto_resume", "")
    if _slack_bot_token and should_notify_slack(auto_resume):
        send_slack_notification(data, _slack_bot_token, _slack_channel)

    return success


def configure_postprocessing(slack_bot_token: str = "", slack_channel: str = ""):
    """Configure postprocessing with Slack integration.

    Call this during service startup to enable Slack notifications.
    Values come from config (cfg.SLACK_BOT_TOKEN, cfg.SLACK_CHANNEL).

    Args:
        slack_bot_token: Slack bot OAuth token (empty = Slack disabled)
        slack_channel: Slack channel name or ID (from config)
    """
    global _slack_bot_token, _slack_channel, _poster
    _slack_bot_token = slack_bot_token
    _slack_channel = slack_channel

    if slack_bot_token:
        logger.info(f"Slack notifications enabled for channel: {slack_channel}")
    else:
        logger.debug("Slack notifications disabled (no token)")


# Configure the default poster with dataflow + slack
_poster = ResultPoster(post_fn=_post_with_slack)
set_default_poster(_poster)

__all__ = [
    "DataflowStats",
    "configure_postprocessing",
    "get_dataflow_stats",
    "get_slack_stats",
    "post_results",
]
