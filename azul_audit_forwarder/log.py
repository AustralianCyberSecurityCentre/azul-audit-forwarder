"""Logger configuration."""

import sys

from loguru import logger

from azul_audit_forwarder.settings import logging as config


class AuditForwarderLogger:
    """Provide logger for use in REST API."""

    def __init__(self):
        """Init."""
        logger.remove()

        self.logger = logger.bind(feed="log")

        # log all to stdout
        logger.add(
            sys.stdout,
            enqueue=True,
            level=config.log_level.upper(),
            format=config.log_format,
            diagnose=False,
        )

        # log file
        if config.log_file:
            logger.add(
                config.log_file,
                rotation=config.log_rotation,
                retention=config.log_retention,
                enqueue=True,
                level=config.log_level.upper(),
                format=config.log_format,
                filter=lambda record: record["extra"].get("feed") == "log",
                diagnose=False,
            )
