import importlib.resources
import json
import os
import time
import unittest
from typing import Any, ClassVar

import httpx

from azul_audit_forwarder import client, settings
from tests import testdata

from . import server as md


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
