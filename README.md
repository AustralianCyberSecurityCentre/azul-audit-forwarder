# Azul Audit Forwarder

Within Azul all logs from the Azul RESTAPI are forwarded to Grafana Loki. Loki is a log aggregation system designed to store and query logs and it acts as the central store for logs in Azul.

Actions users make through the Azul WebUI such as file uploads and views are captured in logs.

The Azul-Audit-Forwarder project is a python application that retrieves logs from Loki on a regular basis and forwards them to the appropriate audit system in a given environment.

Azul-Audit-Forwarder can be configured to send logs on a configurable interval, and can perform transformations to format the logs appropriately for the target Audit system.

## AWS Cloudwatch compatablity

Azul-Audit-Forwarder supports forwarding Azul audit logs to AWS Cloudwatch. Azul-Audit-Forwarder can be configured for AWS Cloudwatch or localstack through settings. 
For local testing, [localstack](https://github.com/localstack/localstack) can be utilised to simulate AWS Cloudwatch APIs. 

Localstack can be started in a docker container using the following command:

```bash
docker run -d -p 4566:4566 -p 4571:4571 -e SERVICES=logs localstack/localstack
```

## Dependency management

Dependencies are managed in the pyproject.toml and debian.txt file.

Version pinning is achieved using the `uv.lock` file.
Because the `uv.lock` file is configured to use a private UV registry, external developers using UV will need to delete the existing `uv.lock` file and update the project configuration to point to the publicly available PyPI registry instead.

To add new dependencies it's recommended to use uv with the command `uv add <new-package>`
    or for a dev package `uv add --dev <new-dev-package>`

The tool used for linting and managing styling is `ruff` and it is configured via `pyproject.toml`

The debian.txt file manages the debian dependencies that need to be installed on development systems and docker images.

Sometimes the debian.txt file is insufficient and in this case the Dockerfile may need to be modified directly to
install complex dependencies.
