"""Azul Audit Forwarder.

Retrieves logs from a Loki deployment over a Websocket connection.
Sends logs to a specified destination at a configurable interval.
"""

import copy
import io
import os.path
import re
import threading
import time
from datetime import datetime

import boto3
import click
import httpx
import uvicorn
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException

from azul_audit_forwarder import settings
from azul_audit_forwarder.log import AuditForwarderLogger

app = FastAPI()
healthy: bool = True
output = io.StringIO()

cloudwatch_client = boto3.client(
    "logs",
    endpoint_url=settings.st.custom_aws_endpoint,
    region_name=settings.st.cloudwatch_region,
    aws_access_key_id=settings.st.cloudwatch_aws_access_key_id,
    aws_secret_access_key=settings.st.cloudwatch_aws_secret_access_key,
)

logger = AuditForwarderLogger().logger


@app.get("/healthz", status_code=200)
def health_check() -> str:
    """Health check endpoint."""
    if healthy:
        logger.info(f"Last sent: {datetime.fromtimestamp(read_last_sent_ts()).strftime('%Y-%m-%d %H:%M:%S')}")
        return "healthy"
    else:
        raise HTTPException(status_code=500, detail="Error connecting to Loki.")


def _set_healthy(is_healthy: bool):
    global healthy
    healthy = is_healthy


def get_epoch_mins_ago(minutes: int) -> int:
    """Returns the unix epoch for `minutes` ago."""
    current_time = time.time()  # Get current Unix timestamp in seconds
    timestamp = int(current_time - (minutes * 60))  # (1 hour)
    return timestamp


def read_last_sent_ts() -> int:
    """Reads file containing a timestamp of when logs were last sent."""
    if not os.path.isfile(settings.st.last_sent_file):
        # If no value is found, return time one hour ago
        return get_epoch_mins_ago(60)

    f = open(settings.st.last_sent_file, "r")
    timestamp_str = f.read().strip()  # Read the timestamp string from the file
    if timestamp_str:
        return int(timestamp_str)
    else:
        # If no value is found, return time one hour ago
        return get_epoch_mins_ago(60)


def update_last_seen_ts(new_ts: int):
    """Updates file containing a timestamp of when logs were last sent."""
    try:
        f = open(settings.st.last_sent_file, "w")
        f.write(str(new_ts))
    except OSError as e:
        logger.error("Error while writing to last seen file")
        logger.error(e)


def parse_time_to_millis(log_line: str) -> int:
    """Extract time and return milliseconds since epoch.
    Falls back to current time in ms if parsing fails.
    """
    m = re.search(r"time=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", log_line)
    if not m:
        # Falls back to current time in ms if parsing fails
        return int(time.time() * 1000)
    ts = m.group(1)
    try:
        dt = datetime.fromisoformat(ts)
        return int(dt.timestamp() * 1000)
    except Exception as e:
        logger.error(f"Error parsing timestamp '{ts}': {e}")
        logger.error("Falling back to current time in ms.")
        return int(time.time() * 1000)


def send_logs_to_cloudwatch(last_epoch: int):
    """Send logs to AWS CloudWatch."""
    log_group = settings.st.cloudwatch_log_group
    log_stream = settings.st.cloudwatch_log_stream

    try:
        # Check if log group exists
        resp = cloudwatch_client.describe_log_groups(logGroupNamePrefix=log_group)
        groups = resp.get("logGroups", [])
        group_exists = any(g.get("logGroupName") == log_group for g in groups)
        if not group_exists:
            logger.error(f"CloudWatch Log group {log_group} does not exist.")
            return
    except ClientError as e:
        logger.error(f"Error checking log group existence: {e}")

    try:
        # Create the log stream if it does not already exist.
        resp = cloudwatch_client.describe_log_streams(logGroupName=log_group, logStreamNamePrefix=log_stream)
        streams = resp.get("logStreams", [])
        stream_exists = any(s.get("logStreamName") == log_stream for s in streams)
        if not stream_exists:
            cloudwatch_client.create_log_stream(logGroupName=log_group, logStreamName=log_stream)
    except ClientError as e:
        logger.error(f"Error describing/creating log stream exists: {e}")
        return

    # Prepare log events
    log_events = []
    # Process logs from the buffer
    processed_logs = output.getvalue()
    if len(processed_logs) > 0:
        list_logs = processed_logs.split("\n")
        logger.info(f"Preparing to send {len(list_logs)} logs to CloudWatch.")
        for log_line in list_logs:
            if log_line.strip():
                timestamp = parse_time_to_millis(log_line)
                log_events.append({"timestamp": timestamp, "message": log_line.strip()})

        if not log_events:
            logger.debug("No log events to send to CloudWatch.")
            return

        try:
            # Sort log events by timestamp
            log_events.sort(key=lambda x: x["timestamp"])
            response = cloudwatch_client.put_log_events(
                logGroupName=log_group, logStreamName=log_stream, logEvents=log_events
            )
            logger.info(f"Put log events response: {response}")
            logger.info(f"Successfully sent {len(log_events)} logs to CloudWatch.")
            update_last_seen_ts(last_epoch)
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error sending logs to CloudWatch: {e}")
    else:
        logger.debug("No logs to send to CloudWatch.")

    clear_output()


def send_logs(last_epoch: int):
    """Send logs to the specified host and port."""
    # Process logs from the buffer
    headers = copy.copy(settings.st.static_headers)
    target = settings.st.target_endpoint
    # Process logs from the buffer
    processed_logs = output.getvalue()
    # Post to target endpoint
    # Ensure empty content is not sent
    if len(processed_logs) > 0:
        if "LOG_ONLY" == target:
            logger.info("Logging all data from Loki as configured.")
            logger.info(processed_logs)
            # Update last seen timestamp once the audit logs have been successfully logged
            update_last_seen_ts(last_epoch)
        else:
            logger.info(f"Sending: {len(processed_logs)} bytes")
            logger.info(f"headers={headers}\ntarget={target}\nproxy={settings.st.target_proxy}")
            try:
                # Post to target endpoint
                resp = httpx.post(
                    target,
                    content=processed_logs,
                    headers=headers,
                    proxy=settings.st.target_proxy,
                    timeout=settings.st.http_client_timeout_seconds,
                )
                if resp.status_code == 200:
                    logger.info(f"{resp.status_code} response from posting logs.")
                    # Update last seen timestamp once the audit logs have been successfully posted
                    update_last_seen_ts(last_epoch)
                else:
                    logger.error(f"Error {resp.status_code}")
                    logger.error(f"Error {resp.content}")
                    _set_healthy(False)
            except Exception as ex:
                # Catch connection errors
                logger.error(f"Error posting logs to target: {ex}")
                _set_healthy(False)

    clear_output()


def clear_output():
    """Clear the buffer."""
    output.truncate(0)
    output.seek(0)


def send_logs_after_interval(interval=5):
    """Repeat sending of logs after specified interval."""
    threading.Timer(interval, poll_and_send_logs).start()
    # Start the loop to send after the set interval
    threading.Timer(interval, send_logs_after_interval).start()


def process_logs(content: dict) -> None:
    """Process logs returned by the /loki/api/v1/query_range endpoint."""
    num_logs = content["data"]["stats"]["summary"]["totalEntriesReturned"]
    if num_logs <= 0:
        return

    # get all lines
    for result in content["data"]["result"]:
        for value in result["values"]:
            output.write(value[1] + "\n")
    output.flush()


def poll_for_logs() -> int | None:
    """Poll for logs from Loki."""
    # The start time for the query as a nanosecond Unix epoch
    start_epoch = read_last_sent_ts()
    # Initial value for end_epoch
    end_epoch = start_epoch + (5 * 60)

    while start_epoch < get_epoch_mins_ago(1):
        # Query audit logs in 5 min intervals.
        end_epoch = start_epoch + (5 * 60)
        params = {
            "query": f'{{app="restapi-server-audit"}} | logfmt | '
            f"namespace = `{settings.st.azul_namespace}` | username != `-`",
            "limit": 5000,
            "start": start_epoch,
            "end": end_epoch,
        }
        loki_host = settings.st.loki_host
        loki_endpoint = f"{loki_host}/loki/api/v1/query_range"
        logger.debug(f"Querying {loki_endpoint} for {format(start_epoch)} to" f" {format(end_epoch)}")

        logger.info(f"Polling Loki from {start_epoch} to {end_epoch}")
        resp = None
        try:
            resp = httpx.get(url=loki_endpoint, params=params, timeout=settings.st.http_client_timeout_seconds)
            if resp.status_code == 200:
                _set_healthy(True)
                process_logs(resp.json())
                # Update start epoch for loop iteration
                start_epoch = end_epoch
            else:
                logger.error(f"Error reaching Loki: {resp.status_code}")
                logger.error(f"{resp.content}")
                _set_healthy(False)
                return None
        except Exception as ex:
            # Catch connection errors
            logger.error(f"Error connecting to: {loki_endpoint}")
            logger.error(params)
            if resp:
                logger.error(f"{resp.content}")
            logger.error(f"Error: {ex}")
            _set_healthy(False)
            return None
    return end_epoch


def poll_and_send_logs():
    """Poll for logs from Loki and send to host specified in settings."""
    last_epoch = poll_for_logs()
    if last_epoch:
        if settings.st.enable_cloudwatch_forwarding:
            send_logs_to_cloudwatch(last_epoch)
        else:
            # Send logs to target endpoint if cloudwatch forwarding is not enabled
            send_logs(last_epoch)


@click.command()
@click.option("--host", default=settings.st.health_host)
@click.option("--port", default=settings.st.health_port)
def main(host, port):
    """Run Azul Audit Forwarder from the command line."""
    send_logs_after_interval(settings.st.send_interval)
    # Start FastAPI app for health probes
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
