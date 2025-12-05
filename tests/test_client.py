import importlib.resources
import json
import os
import time
import unittest
from typing import Any, ClassVar
from unittest.mock import Mock

import httpx

from azul_audit_forwarder import client, settings
from tests import testdata

from . import server as md


class TestSendLogsToCloudwatch(unittest.TestCase):
    def setUp(self) -> None:
        client.clear_output()
        # Save original client
        self.original_cloudwatch_client = client.cloudwatch_client
        os.environ["audit_send_logs_to"] = "cloudwatch"
        # Mock the CloudWatch client to prevent endpoint errors
        self.mock_cloudwatch = Mock()
        client.cloudwatch_client = self.mock_cloudwatch

    def tearDown(self) -> None:
        client.clear_output()
        # Restore original client
        client.cloudwatch_client = self.original_cloudwatch_client

    def test_send_logs_to_cloudwatch_success(self):
        """Test successful CloudWatch log sending."""
        # Setup
        client.output.write("time=2025-01-01T10:30:45.123 msg=test1\n")
        client.output.write("time=2025-01-01T10:30:46.456 msg=test2\n")
        timestamp = client.get_epoch_mins_ago(10)

        self.mock_cloudwatch.describe_log_groups.return_value = {
            "logGroups": [{"logGroupName": settings.st.cloudwatch_log_group}]
        }
        self.mock_cloudwatch.describe_log_streams.return_value = {
            "logStreams": [{"logStreamName": settings.st.cloudwatch_log_stream}]
        }
        self.mock_cloudwatch.put_log_events.return_value = {
            "nextSequenceToken": "12345",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        # Mock update_last_seen_ts
        client.update_last_seen_ts = Mock()

        # Execute test
        client.send_logs_to_cloudwatch(timestamp)

        self.mock_cloudwatch.put_log_events.assert_called_once()
        call_args = self.mock_cloudwatch.put_log_events.call_args
        self.assertEqual(call_args.kwargs["logGroupName"], settings.st.cloudwatch_log_group)
        self.assertEqual(call_args.kwargs["logStreamName"], settings.st.cloudwatch_log_stream)
        # Assert two log events were sent
        self.assertEqual(len(call_args.kwargs["logEvents"]), 2)
        client.update_last_seen_ts.assert_called_once_with(timestamp)
        self.assertEqual(len(client.output.getvalue()), 0)

        # Assert logs are in order by timestamp
        log_events = call_args.kwargs["logEvents"]
        self.assertLessEqual(log_events[0]["timestamp"], log_events[1]["timestamp"])
        self.assertIn("test1", log_events[0]["message"])
        self.assertIn("test2", log_events[1]["message"])

    def test_send_logs_to_cloudwatch_missing_log_group(self):
        """Test handling of missing log group."""
        client.output.write("time=2025-01-01T10:30:45 msg=test\n")
        timestamp = client.get_epoch_mins_ago(10)

        self.mock_cloudwatch.describe_log_groups.return_value = {"logGroups": []}
        client.update_last_seen_ts = Mock()

        client.send_logs_to_cloudwatch(timestamp)

        self.mock_cloudwatch.describe_log_streams.assert_not_called()
        self.mock_cloudwatch.put_log_events.assert_not_called()
        client.update_last_seen_ts.assert_not_called()


class TestNoServer(unittest.TestCase):
    def tearDown(self) -> None:
        client.clear_output()
        return super().tearDown()

    def test_parse_log(self):
        # Parse an example log line from Loki
        self.assertEqual(0, len(client.output.getvalue()))

        with importlib.resources.open_binary(testdata, "resp.json") as f:
            d = json.load(f)

        client.process_logs(d)
        processed_logs = client.output.getvalue()
        self.assertGreater(len(processed_logs), 0)
        expected_log = (
            "full_time=12/Apr/2024:10:00:47.513681 client_ip=127.0.0.6 client_port=53841 connection=- "
            "username=FAKE_USER method=POST path=/api/v0/binaries/form "
            'status=200 user_agent="python-httpx/0.27.0" referer=- '
            "duration_ms=25.69890022277832\nfull_time=12/Apr/2024:10:00:47.158444 client_ip=127.0.0.6 "
            "client_port=37439 connection=- username=FAKE_USER method=POST "
            'path=/api/v0/binaries/source status=200 user_agent="python-httpx/0.27.0" referer=- '
            "duration_ms=35.00032424926758\n"
        )
        self.assertEqual(expected_log, processed_logs)


class TestProcessLogs(unittest.TestCase):
    mock_server: ClassVar[md.MockDestination]

    def tearDown(self) -> None:
        client.clear_output()
        return super().tearDown()

    @classmethod
    def setUpClass(cls) -> None:
        cls.mock_server = md.MockDestination()
        cls.mock_server.start()
        while not cls.mock_server.is_alive():
            time.sleep(0.2)  # Wait for server to start
        cls.server = "http://%s:%s" % (cls.mock_server.host, cls.mock_server.port)
        # Wait for server to be ready to respond
        tries = 0
        while True:
            time.sleep(0.2)
            tries += 1
            try:
                _ = httpx.get(cls.server + "/info")
                break  # Exit loop if successful
            except httpx.TimeoutException:
                if tries > 20:  # Time out after about 4 seconds
                    raise RuntimeError("Timed out waiting for mock server to be ready")

        os.environ["audit_send_logs_to"] = "server"
        os.environ["audit_target_endpoint"] = cls.server + "/audit"
        os.environ["audit_static_headers"] = json.dumps(
            {
                "feed": "AZUL-V3.0-EVENTS",
                "system": "Azul3",
                "environment": "DEV",
                "myhost": "azul.example.internal",
                "myipaddress": "0.0.0.0",
            }
        )

        settings.st = settings.AuditFwdSettings()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.mock_server.kill()

    def test_post_logs(self):
        client.output.write("test\n")
        timestamp = client.get_epoch_mins_ago(10)
        client.send_logs(timestamp)
        data = httpx.get(self.server + "/info").json()
        self.assertEqual(data["body"], "test\n")
        self.assertEqual(
            data["headers"],
            {
                "feed": "AZUL-V3.0-EVENTS",
                "system": "Azul3",
                "environment": "DEV",
                "myhost": "azul.example.internal",
                "myipaddress": "0.0.0.0",
            },
        )

        client.output.write("spaghetti\n")
        timestamp = client.get_epoch_mins_ago(5)
        client.send_logs(timestamp)
        data = httpx.get(self.server + "/info").json()
        self.assertEqual(data["body"], "spaghetti\n")
        self.assertEqual(
            data["headers"],
            {
                "feed": "AZUL-V3.0-EVENTS",
                "system": "Azul3",
                "environment": "DEV",
                "myhost": "azul.example.internal",
                "myipaddress": "0.0.0.0",
            },
        )
