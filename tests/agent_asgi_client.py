"""In-process ASGI client for sync FastAPI handlers on local Python 3.14."""

import asyncio
from unittest.mock import patch

import httpx


class LocalClient:
    def __init__(self, app):
        self.app = app

    def request(self, method, path, **kwargs):
        async def run_sync(func, *args, **_kwargs):
            return func(*args)

        async def send():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=self.app),
                base_url="http://testserver",
            ) as client:
                return await client.request(method, path, **kwargs)

        with patch("anyio.to_thread.run_sync", run_sync):
            return asyncio.run(send())

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)

    def post(self, path, **kwargs):
        return self.request("POST", path, **kwargs)
