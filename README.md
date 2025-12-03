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