"""Legacy Iceberg REST compatibility proxy for Nessie.

This proxy exists only for clients that send the Nessie Iceberg REST prefix
with a raw "|" character in the request URI.

Governed transform execution does not depend on this proxy. Transform jobs
execute through the dedicated Trino boundary.

The proxy performs two compatibility operations:

1. Encode a raw "|" in the incoming request path as "%7C" before forwarding
   the request to Nessie.

2. Rewrite Nessie's Iceberg REST base URI in the /iceberg/v1/config response
   so a client using the proxy continues subsequent requests through the
   proxy rather than bypassing it.

The proxy does not choose a Nessie branch, alter warehouse identity, or
provide transform authority.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json
import os


UPSTREAM = os.environ.get(
    "NESSIE_UPSTREAM",
    "http://nessie:19120",
).rstrip("/")

PUBLIC_BASE = os.environ.get(
    "PUBLIC_BASE",
    "http://nessie-rest-proxy:19121",
).rstrip("/")

LISTEN_HOST = os.environ.get(
    "NESSIE_REST_PROXY_HOST",
    "0.0.0.0",
)

LISTEN_PORT = int(
    os.environ.get(
        "NESSIE_REST_PROXY_PORT",
        "19121",
    )
)


class ProxyHandler(BaseHTTPRequestHandler):
    """Forward Iceberg REST requests to Nessie."""

    protocol_version = "HTTP/1.1"

    def _proxy(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length else None

        # Some REST clients send Nessie's "{ref}|{warehouse}" prefix with the
        # pipe character unescaped. Encode only that reserved character before
        # forwarding the request.
        upstream_path = self.path.replace("|", "%7C")
        upstream_url = f"{UPSTREAM}{upstream_path}"

        request = Request(
            upstream_url,
            data=body,
            method=self.command,
        )

        for name, value in self.headers.items():
            if name.lower() not in {
                "host",
                "content-length",
                "connection",
            }:
                request.add_header(name, value)

        try:
            response = urlopen(request)
        except HTTPError as error:
            response = error

        payload = response.read()
        content_type = response.headers.get(
            "Content-Type",
            "application/octet-stream",
        )

        # Nessie's config response contains catalogue endpoint information.
        # A client entering through this compatibility proxy must continue
        # using the proxy for subsequent REST requests.
        if (
            self.path.startswith("/iceberg/v1/config")
            and "json" in content_type.lower()
        ):
            try:
                catalog_config = json.loads(payload)

                serialized = json.dumps(
                    catalog_config,
                    separators=(",", ":"),
                )

                serialized = serialized.replace(
                    UPSTREAM,
                    PUBLIC_BASE,
                )

                payload = serialized.encode("utf-8")
                content_type = "application/json"
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Preserve the upstream response unchanged if Nessie returns
                # a non-JSON or malformed response.
                pass

        self.send_response(response.status)

        # Forward useful upstream response headers while allowing this proxy
        # to calculate Content-Length itself.
        excluded_response_headers = {
            "connection",
            "content-length",
            "transfer-encoding",
        }

        for name, value in response.headers.items():
            if name.lower() not in excluded_response_headers:
                self.send_header(name, value)

        self.send_header(
            "Content-Type",
            content_type,
        )
        self.send_header(
            "Content-Length",
            str(len(payload)),
        )
        self.send_header(
            "Connection",
            "close",
        )

        self.end_headers()

        if self.command != "HEAD":
            self.wfile.write(payload)

    def do_GET(self) -> None:
        self._proxy()

    def do_HEAD(self) -> None:
        self._proxy()

    def do_POST(self) -> None:
        self._proxy()

    def do_DELETE(self) -> None:
        self._proxy()


if __name__ == "__main__":
    server = ThreadingHTTPServer(
        (LISTEN_HOST, LISTEN_PORT),
        ProxyHandler,
    )

    print(
        f"Nessie Iceberg REST compatibility proxy listening on "
        f"{LISTEN_HOST}:{LISTEN_PORT}; upstream={UPSTREAM}",
        flush=True,
    )

    server.serve_forever()