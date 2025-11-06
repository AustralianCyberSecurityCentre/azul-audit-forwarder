"""Azul Audit Forwarder.

Retrieves logs from a Loki deployment over a Websocket connection.
Sends logs to a specified destination at a configurable interval.
"""

import copy
import io
import os.path
import threading
import time
from datetime import datetime

import click
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException

from azul_audit_forwarder import settings
from azul_audit_forwarder.log import AuditForwarderLogger

app = FastAPI()
healthy: bool = True
output = io.StringIO()

logger = AuditForwarderLogger().logger


@app.get("/healthz", status_code=200)
def health_check() -> str:
    """Health check endpoint."""
    if healthy:
        logger.info(f"Last sent: {datetime.fromtimestamp(read_last_sent_ts()).strftime("%Y-%m-%d %H:%M:%S")}")
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
                    target, content=processed_logs, headers=headers, proxy=settings.st.target_proxy, timeout=30.0
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

        try:
            resp = httpx.get(url=loki_endpoint, params=params, timeout=30.0)
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
            logger.error(f"{resp.content}")
            logger.error(f"Error: {ex}")
            _set_healthy(False)
            return None
    return end_epoch


def poll_and_send_logs():
    """Poll for logs from Loki and send to host specified in settings."""
    last_epoch = poll_for_logs()
    if last_epoch:
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
