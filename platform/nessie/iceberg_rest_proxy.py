"""Compatibility proxy for DuckDB's Iceberg REST client and Nessie warehouses."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json
import os


UPSTREAM = os.environ.get("NESSIE_UPSTREAM", "http://nessie:19120")
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "http://nessie-rest-proxy:19121")


class ProxyHandler(BaseHTTPRequestHandler):
    def _proxy(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        # DuckDB 1.4 sends Nessie's `main|warehouse` prefix verbatim. Vert.x
        # correctly rejects that raw reserved URI character, so encode it.
        url = f"{UPSTREAM}{self.path.replace('|', '%7C')}"
        request = Request(url, data=body or None, method=self.command)
        for name, value in self.headers.items():
            if name.lower() not in {"host", "content-length", "connection"}:
                request.add_header(name, value)

        try:
            response = urlopen(request)
        except HTTPError as error:
            response = error

        payload = response.read()
        content_type = response.headers.get("Content-Type", "")
        if self.path.startswith("/iceberg/v1/config") and "json" in content_type:
            catalog_config = json.loads(payload)
            serialized = json.dumps(catalog_config).replace(UPSTREAM, PUBLIC_BASE)
            payload = serialized.encode()

        self.send_response(response.status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = _proxy
    do_HEAD = _proxy
    do_POST = _proxy
    do_DELETE = _proxy


if __name__ == "__main__":
    ThreadingHTTPServer(("", 19121), ProxyHandler).serve_forever()
