"""Settings classes using pydantic environment parsing."""

from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict


class SendLogsDestination(str, Enum):
    """Enum for log forwarding destination options."""

    CLOUDWATCH = "cloudwatch"
    SERVER = "server"
    LOG_ONLY = "log_only"


class AuditFwdSettings(BaseSettings):
    """AuditForwarder specific environment variables parsed into settings object."""

    model_config = SettingsConfigDict(env_prefix="audit_")

    # Log instead of sending a POST by default.
    # options: 'cloudwatch', 'server', 'log_only'
    send_logs_to: SendLogsDestination = SendLogsDestination.LOG_ONLY

    # Enable sending audit logs to AWS CloudWatch. This disables sending logs to the generic target_endpoint.
    # Log group - folder for related log streams
    cloudwatch_log_group: str = "azul-audit-logs"
    # Log stream - sequence of log events from the same source
    cloudwatch_log_stream: str = "azul-audit-forwarder"
    cloudwatch_region: str = "us-east-1"
    # AWS Access Key ID for CloudWatch
    cloudwatch_aws_access_key_id: str = "test"
    # AWS Secret Access Key for CloudWatch
    cloudwatch_aws_secret_access_key: str = "test"
    custom_aws_endpoint: str | None = "http://localhost:4566"  # TODO: set to None
    # location of loki server
    loki_host: str = "http://localhost:3100"
    # azul namespace to forward logs for
    azul_namespace: str = "azul"

    # To send logs to a target endpoint, send_logs_to must be set to 'server'. Target_host should be `http://audit-server:9999` in a Prod environment
    target_endpoint: str | None = None
    # Use a proxy to contact the target endpoint
    target_proxy: str | None = None
    # static key value headers to be sent when posting to target endpoint
    static_headers: dict[str, str] = {}

    send_interval: int = 5
    last_sent_file: str = "/tmp/last_sent.txt"  # nosec

    health_host: str = "0.0.0.0"  # nosec
    health_port: int = 8855

    http_client_timeout_seconds: float = 30.0


class Logging(BaseSettings):
    """Logger configuration."""

    model_config = SettingsConfigDict(env_prefix="logger_")

    log_file: str = "/tmp/azul_audit_forwarder.log"  # nosec
    log_format: str = (
        "level=<level>{level: <8}</level> time=<green>{time:YYYY-MM-DDTHH:mm:ss.SS}</green> "
        "name=<cyan>{name}</cyan> function=<cyan>{function}</cyan> {message}"
    )
    log_level: str = "info"
    log_retention: str = "1 months"
    log_rotation: str = "daily"


st = AuditFwdSettings()
logging = Logging()
