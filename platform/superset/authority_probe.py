"""Runtime authority harness for the Superset ``agent_publisher`` identity.

Classifies every probed request into exactly one outcome category, so a CSRF
rejection is never mistaken for an authorization denial:

``AUTH_FAILURE``   no usable identity (401)
``CSRF_FAILURE``   valid identity, rejected before authorization by CSRF
``AUTHZ_DENIED``   reached the authorization boundary and was refused (403, or
                   404 where the endpoint is withheld from the role)
``VALIDATION``     rejected on request shape, never reaching authorization
``SUCCESS``        accepted (2xx)

The authentication and CSRF mechanism was determined from the running 6.1.0
instance rather than assumed: ``POST /api/v1/security/login`` returns a JWT
``access_token``; ``GET /api/v1/security/csrf_token/`` returns
``{"result": <token>}`` and carries ``@permission_name("read")`` on the
``security`` resource; mutating requests then require that token in the
``X-CSRFToken`` header. ``services/agent-api/bi_adapter.py`` uses exactly this
sequence, so the harness mirrors the production client.

No credential is ever printed; only HTTP status and outcome are reported.
"""

from __future__ import annotations

import http.cookiejar
import json
import urllib.error
import urllib.request

AUTH_FAILURE = "AUTH_FAILURE"
CSRF_FAILURE = "CSRF_FAILURE"
AUTHZ_DENIED = "AUTHZ_DENIED"
VALIDATION = "VALIDATION"
SUCCESS = "SUCCESS"

#: Body fragments identifying a CSRF rejection specifically. These must never
#: be reported as an authorization denial.
CSRF_MARKERS = ("csrf token is missing", "csrf")


def classify(status: int, body: str) -> str:
    """Map an HTTP status and body onto exactly one outcome category."""
    lowered = (body or "").lower()
    if any(marker in lowered for marker in CSRF_MARKERS):
        return CSRF_FAILURE
    if status == 401:
        return AUTH_FAILURE
    if status == 403:
        return AUTHZ_DENIED
    if status == 404:
        # A restricted role sees a withheld endpoint as not-found.
        return AUTHZ_DENIED
    if status == 400:
        return VALIDATION
    if 200 <= status < 300:
        return SUCCESS
    return VALIDATION


class SupersetSession:
    """Authenticated session preserving cookie and CSRF state."""

    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar)
        )
        self.token = None
        self.csrf = None

    def _call(self, path, method="GET", data=None):
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.csrf and method not in ("GET", "HEAD", "OPTIONS"):
            headers["X-CSRFToken"] = self.csrf
            headers["Referer"] = self.base
        body = json.dumps(data).encode() if data is not None else None
        request = urllib.request.Request(
            self.base + path, method=method, headers=headers, data=body
        )
        try:
            with self.opener.open(request, timeout=60) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode()
        except Exception as exc:  # noqa: BLE001
            return getattr(exc, "code", 0), str(exc)

    def login(self, username: str, password: str) -> str:
        status, body = self._call(
            "/api/v1/security/login",
            "POST",
            {"username": username, "password": password,
             "provider": "db", "refresh": True},
        )
        if status != 200:
            return AUTH_FAILURE
        self.token = json.loads(body).get("access_token")
        return SUCCESS if self.token else AUTH_FAILURE

    def obtain_csrf(self) -> str:
        """Fetch the CSRF token via the supported Superset endpoint."""
        status, body = self._call("/api/v1/security/csrf_token/")
        if status != 200:
            return classify(status, body)
        try:
            self.csrf = json.loads(body).get("result")
        except ValueError:
            self.csrf = None
        return SUCCESS if self.csrf else AUTH_FAILURE

    def probe(self, control, path, method="GET", data=None):
        """Execute one control and return its classified evidence record."""
        status, body = self._call(path, method, data)
        return {
            "control": control,
            "endpoint": f"{method} {path}",
            "authenticated": bool(self.token),
            "csrf_state": "held" if self.csrf else "absent",
            "http_status": status,
            "outcome": classify(status, body),
        }
#: ``(control, method, path, body, expected)``. ``expected`` is what the
#: publisher's authority model requires; a mismatch is reported, not assumed.
CONTROLS = (
    ("INTENDED_list_dashboards", "GET",
     "/api/v1/dashboard/?q=(page:0,page_size:1)", None, SUCCESS),
    ("INTENDED_list_charts", "GET",
     "/api/v1/chart/?q=(page:0,page_size:1)", None, SUCCESS),
    ("INTENDED_create_dashboard", "POST", "/api/v1/dashboard/",
     {"dashboard_title": "publisher-authority-probe", "published": False}, SUCCESS),
    ("SQLLAB_execute", "POST", "/api/v1/sqllab/execute/",
     {"database_id": 1, "schema": "information_schema", "sql": "SELECT 1",
      "runAsync": False, "queryLimit": 10, "async": False}, AUTHZ_DENIED),
    ("SQLLAB_history", "GET", "/api/v1/sqllab/history/", None, AUTHZ_DENIED),
    ("DATABASE_list", "GET", "/api/v1/database/", None, AUTHZ_DENIED),
    ("DATABASE_read", "GET", "/api/v1/database/1", None, AUTHZ_DENIED),
    ("DATABASE_mutate", "PUT", "/api/v1/database/1",
     {"database_name": "renamed-by-publisher"}, AUTHZ_DENIED),
    ("DATABASE_create", "POST", "/api/v1/database/",
     {"database_name": "created-by-publisher",
      "sqlalchemy_uri": "https://superset_bi:x@trino:8443/iceberg/consumption"},
     AUTHZ_DENIED),
    ("USER_ADMIN_list_users", "GET", "/api/v1/security/users/", None, AUTHZ_DENIED),
    ("ROLE_ADMIN_list_roles", "GET", "/api/v1/security/roles/", None, AUTHZ_DENIED),
    ("GUEST_TOKEN_create", "POST", "/api/v1/security/guest_token/",
     {"resources": [{"type": "dashboard", "id": 1}]}, AUTHZ_DENIED),
    ("DATASET_list", "GET",
     "/api/v1/dataset/?q=(page:0,page_size:1)", None, SUCCESS),
    ("EXPORT_chart_csv", "GET", "/api/v1/chart/1/data/?type=csv", None, AUTHZ_DENIED),
)


def run(base: str, username: str, password: str) -> dict:
    """Execute the full authority matrix for one identity."""
    session = SupersetSession(base)
    login_outcome = session.login(username, password)
    if login_outcome != SUCCESS:
        return {"login": login_outcome, "csrf": None, "controls": []}

    csrf_outcome = session.obtain_csrf()
    controls = []
    for control, method, path, data, expected in CONTROLS:
        record = session.probe(control, path, method, data)
        record["expected"] = expected
        record["status"] = "PASS" if record["outcome"] == expected else "MISMATCH"
        controls.append(record)
    return {"login": login_outcome, "csrf": csrf_outcome, "controls": controls}
def main(argv=None) -> int:
    """CLI entry point. The password is never echoed or included in output."""
    import argparse

    parser = argparse.ArgumentParser(description="Superset authority probe")
    parser.add_argument("--base", default="http://127.0.0.1:8088")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", default=None)
    args = parser.parse_args(argv)

    if not args.password:
        print("ERROR: --password is required", file=sys.stderr)
        return 2
    print(json.dumps(run(args.base, args.username, args.password), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())