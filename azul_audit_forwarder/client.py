"""Azul Audit Forwarder.

Retrieves logs from a Loki deployment over a Websocket connection.
Sends logs to a specified destination at a configurable interval.
"""

import copy
import io
import math
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
LOKI_LIMIT = 5000
MAX_WINDOW_SECS = 5 * 60  # 5 minutes
MIN_WINDOW_SECS = 0.25  # 250 ms

if settings.st.send_logs_to == settings.SendLogsDestination.CLOUDWATCH:
    cloudwatch_kwargs = {
        "region_name": settings.st.cloudwatch_region,
    }

    # Only override default credentials if explicitly provided
    if settings.st.cloudwatch_aws_access_key_id:
        cloudwatch_kwargs["aws_access_key_id"] = settings.st.cloudwatch_aws_access_key_id
    if settings.st.cloudwatch_aws_secret_access_key:
        cloudwatch_kwargs["aws_secret_access_key"] = settings.st.cloudwatch_aws_secret_access_key

    if settings.st.custom_aws_endpoint:
        cloudwatch_kwargs["endpoint_url"] = settings.st.custom_aws_endpoint

    cloudwatch_client = boto3.client("logs", **cloudwatch_kwargs)  # type: ignore

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
    """Parse timestamp to milliseconds since epoch, or return current time if parsing fails."""
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

            # CloudWatch enforces a 10,000 event limit per put_log_events call
            CLOUDWATCH_MAX_EVENTS = 10000
            num_chunks = math.ceil(len(log_events) / CLOUDWATCH_MAX_EVENTS)
            logger.info(f"Sending {len(log_events)} logs to CloudWatch in {num_chunks} chunks.")

            for i in range(0, len(log_events), CLOUDWATCH_MAX_EVENTS):
                chunk = log_events[i : i + CLOUDWATCH_MAX_EVENTS]
                response = cloudwatch_client.put_log_events(
                    logGroupName=log_group, logStreamName=log_stream, logEvents=chunk
                )
                logger.debug(f"Cloudwatch Put log events response: {response}")
                if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                    logger.error("Failed to send logs to CloudWatch.")
                    return

            logger.info(f"Successfully sent {len(log_events)} log(s) to CloudWatch.")
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
    server_target_endpoint = settings.st.server_target_endpoint
    # Process logs from the buffer
    processed_logs = output.getvalue()
    # Post to target endpoint
    # Ensure empty content is not sent
    if len(processed_logs) > 0:
        if settings.st.send_logs_to == settings.SendLogsDestination.LOG_ONLY:
            logger.info("Logging all data from Loki as configured.")
            logger.info(processed_logs)
            # Update last seen timestamp once the audit logs have been successfully logged
            update_last_seen_ts(last_epoch)
        elif server_target_endpoint is not None:
            logger.info(f"Sending: {len(processed_logs)} bytes")
            logger.info(f"headers={headers}\ntarget={server_target_endpoint}\nproxy={settings.st.target_proxy}")
            try:
                # Post to target endpoint
                resp = httpx.post(
                    server_target_endpoint,
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


def send_logs_after_interval(interval: int):
    """Repeat sending of logs after specified interval."""
    while True:
        hit_limit = poll_and_send_logs()
        # Sleep for interval or 0.25 seconds if we hit the Loki limit to avoid hitting it repeatedly without delay
        time.sleep(MIN_WINDOW_SECS if hit_limit else interval)


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


def poll_for_logs() -> tuple[int | None, bool]:
    """Poll for logs from Loki using dynamic time windows.

    Halves the query window whenever the Loki limit is hit, down to 250 ms
    precision, then doubles back toward 5 minutes after a successful fetch.
    """
    current_start = float(read_last_sent_ts())
    cutoff = float(get_epoch_mins_ago(1))

    # Start with a 5 minute window
    window_secs = float(MAX_WINDOW_SECS)
    # Track the end of the last successfully processed window so we can return it at the end
    last_processed_end: float | None = None
    hit_limit = False

    while current_start < cutoff:
        current_end = min(current_start + window_secs, cutoff)

        params = {
            "query": f'{{app="restapi-server-audit"}} | namespace = `{settings.st.azul_namespace}` | username != `-`',
            "limit": LOKI_LIMIT,
            "start": current_start,
            "end": current_end,
        }
        loki_host = settings.st.loki_host
        loki_endpoint = f"{loki_host}/loki/api/v1/query_range"
        logger.info(f"Polling Loki from {current_start} to {current_end} (window={window_secs}s)")
        resp = None
        try:
            resp = httpx.get(
                url=loki_endpoint,
                params=params,
                timeout=settings.st.http_client_timeout_seconds,
            )
            if resp.status_code == 200:
                _set_healthy(True)
                data = resp.json()
                num_returned = data["data"]["stats"]["summary"]["totalEntriesReturned"]

                if num_returned >= LOKI_LIMIT and window_secs > MIN_WINDOW_SECS:
                    # Hit the limit; halve the window and retry from the same start.
                    window_secs = max(window_secs / 2, MIN_WINDOW_SECS)
                    hit_limit = True
                    logger.info(f"Hit Loki limit ({num_returned} logs), shrinking window to {window_secs}s")
                    continue

                if num_returned >= LOKI_LIMIT:
                    # At minimum window and still hitting the limit; log a warning and move on.
                    hit_limit = True
                    logger.warning(
                        f"Hit Loki limit {num_returned} logs at window {MIN_WINDOW_SECS} "
                        "some logs in this window may be dropped."
                    )

                # Successfully retrieved logs for this window; process them and move the window forward.
                process_logs(data)
                last_processed_end = current_end
                current_start = current_end

                if num_returned < LOKI_LIMIT:
                    # Under the limit; double the window.
                    window_secs = min(window_secs * 2, MAX_WINDOW_SECS)
            else:
                logger.error(f"Error reaching Loki: {resp.status_code}")
                logger.error(f"{resp.content}")
                _set_healthy(False)
                return None, False
        except Exception as ex:
            # Catch connection errors
            logger.error(f"Error connecting to: {loki_endpoint}")
            logger.error(params)
            if resp:
                logger.error(f"{resp.content}")
            logger.error(f"Error: {ex}")
            _set_healthy(False)
            return None, False

    return (int(last_processed_end) if last_processed_end is not None else None), hit_limit


def poll_and_send_logs() -> bool:
    """Poll for logs from Loki and send to host specified in settings."""
    last_epoch, hit_limit = poll_for_logs()
    if last_epoch:
        if settings.st.send_logs_to == settings.SendLogsDestination.CLOUDWATCH:
            send_logs_to_cloudwatch(last_epoch)
        else:
            # Send logs to target endpoint if cloudwatch forwarding is not enabled
            send_logs(last_epoch)
    return hit_limit


@click.command()
@click.option("--host", default=settings.st.health_host)
@click.option("--port", default=settings.st.health_port)
def main(host, port):
    """Run Azul Audit Forwarder from the command line."""
    t = threading.Thread(target=send_logs_after_interval, args=(settings.st.send_interval,), daemon=True)
    t.start()
    # Start FastAPI app for health probes
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
