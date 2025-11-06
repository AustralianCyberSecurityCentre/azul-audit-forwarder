"""Settings classes using pydantic environment parsing."""

from pydantic_settings import BaseSettings


class AuditFwdSettings(BaseSettings):
    """AuditForwarder specific environment variables parsed into settings object."""

    # location of loki server
    loki_host: str = "http://localhost:3100"
    # azul namespace to forward logs for
    azul_namespace: str = "azul"

    # Log instead of sending a POST by default. target_host should be `http://audit-server:9999` in a Prod environment
    target_endpoint: str = "LOG_ONLY"
    # Use a proxy to contact the target endpoint
    target_proxy: str | None = None
    # static key value headers to be sent when posting to target endpoint
    static_headers: dict[str, str] = {}

    send_interval: int = 5
    last_sent_file: str = "/tmp/last_sent.txt"  # nosec

    health_host: str = "0.0.0.0"  # nosec
    health_port: int = 8855

    class Config:
        """Prefix override."""

        env_prefix = "audit_"


class Logging(BaseSettings):
    """Logger configuration."""

    log_file: str = "/tmp/azul_audit_forwarder.log"  # nosec
    log_format: str = (
        "level=<level>{level: <8}</level> time=<green>{time:YYYY-MM-DDTHH:mm:ss.SS}</green> "
        "name=<cyan>{name}</cyan> function=<cyan>{function}</cyan> {message}"
    )
    log_level: str = "info"
    log_retention: str = "1 months"
    log_rotation: str = "daily"

    class Config:
        """Config class."""

        env_prefix = "logger_"


st = AuditFwdSettings()
logging = Logging()
