import contextlib
import multiprocessing
import socket
from typing import Any

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.testclient import TestClient

app = FastAPI()


state = {}


@app.get("/info")
async def post_audit(request: Request, response: Response) -> dict:
    global state
    return state


@app.post("/audit")
async def post_audit(request: Request, response: Response) -> Any:
    global state
    body = await request.body()
    state["body"] = body

    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("accept", None)
    headers.pop("accept-encoding", None)
    headers.pop("connection", None)
    headers.pop("user-agent", None)
    headers.pop("content-length", None)
    state["headers"] = headers


client = TestClient(app)


class MockDestination(multiprocessing.Process):
    """Allows for mocking a real destination server."""

    def __init__(self, host="localhost", port="42081"):
        super().__init__()
        self.host = host
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            self.port = s.getsockname()[1]

        config = uvicorn.Config(app, host=host, port=self.port)
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args, **kwargs):
        self.server.run()
