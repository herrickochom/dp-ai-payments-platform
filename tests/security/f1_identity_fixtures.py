"""Deterministic F1 trusted-identity test fixtures.

All key material here is EPHEMERAL and TEST-ONLY: RSA key pairs are generated
in-process at import time and never written to disk, to `.env`, or to any
deployed configuration.  No live IdP, JWKS endpoint, Superset or Trino is
contacted.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT / "tests", ROOT, AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


from authentication import AuthenticationError


TEST_ISSUER = "https://idp.test.invalid/"
TEST_AUDIENCE = "agent-api-test"
TEST_KID = "f1-test-key-1"
ROTATED_KID = "f1-test-key-2"


def _generate_key():
    """Ephemeral TEST-ONLY RSA key pair."""

    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )


PRIVATE_KEY = _generate_key()
ROTATED_PRIVATE_KEY = _generate_key()
OTHER_PRIVATE_KEY = _generate_key()

PUBLIC_JWK = jwt.algorithms.RSAAlgorithm.to_jwk(PRIVATE_KEY.public_key())


class StaticSigningKeyResolver:
    """Offline stand-in for the JWKS resolver.

    Models the behaviours that matter for security: ``kid``-keyed selection,
    refusal to fall back to another key on an unknown ``kid``, and hard failure
    when the key set is unavailable or malformed.
    """

    def __init__(
        self,
        keys=None,
        *,
        unavailable: bool = False,
        malformed: bool = False,
    ):
        self.keys = keys if keys is not None else {TEST_KID: PRIVATE_KEY}
        self.unavailable = unavailable
        self.malformed = malformed
        self.calls = 0

    def resolve(self, token):
        self.calls += 1

        if self.unavailable:
            raise AuthenticationError(
                "SigningKeyUnavailable",
                "Signing key could not be established.",
            )

        if self.malformed:
            raise AuthenticationError(
                "SigningKeyUnavailable",
                "Signing key could not be established.",
            )

        try:
            kid = jwt.get_unverified_header(token).get("kid")

        except Exception:
            raise AuthenticationError(
                "InvalidToken",
                "Authentication is required for this resource.",
            ) from None

        key = self.keys.get(kid)

        if key is None:
            #
            # Unknown kid must NEVER fall back to an arbitrary key.
            #
            raise AuthenticationError(
                "SigningKeyUnavailable",
                "Signing key could not be established.",
            )

        return key.public_key()


def make_token(
    *,
    subject: str = "analyst-1",
    issuer: str = TEST_ISSUER,
    audience: str = TEST_AUDIENCE,
    key=PRIVATE_KEY,
    kid: str | None = TEST_KID,
    algorithm: str = "RS256",
    expires_in: int = 300,
    not_before_in: int | None = None,
    extra_claims: dict | None = None,
    omit: tuple = (),
) -> str:
    """Build a signed TEST-ONLY token.

    ``algorithm="none"`` builds a deliberately unsigned token; PyJWT requires
    an empty key for that mode, which is exactly what an attacker would send.
    """

    now = datetime.now(timezone.utc)

    claims = {
        "iss": issuer,
        "aud": audience,
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in)).timestamp()),
    }

    if not_before_in is not None:
        claims["nbf"] = int(
            (now + timedelta(seconds=not_before_in)).timestamp()
        )

    claims.update(extra_claims or {})

    for name in omit:
        claims.pop(name, None)

    headers = {"kid": kid} if kid is not None else {}

    signing_key = "" if algorithm.lower() == "none" else key

    return jwt.encode(
        claims,
        signing_key,
        algorithm=algorithm,
        headers=headers,
    )


def public_key_pem() -> bytes:
    """TEST-ONLY: the public key as PEM, for the HMAC confusion attack."""

    from cryptography.hazmat.primitives import serialization

    return PRIVATE_KEY.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def make_hmac_confusion_token(
    *,
    subject: str = "analyst-1",
    kid: str | None = TEST_KID,
) -> str:
    """Hand-craft the classic algorithm-confusion token.

    A conformant JWT library refuses to sign an HS256 token with an asymmetric
    key.  An attacker does not use a conformant library: they take the PUBLIC
    key, use its bytes as an HMAC secret, and sign ``alg: HS256`` themselves.
    This builds exactly that token so the verifier can be tested against it.
    """

    import base64
    import hashlib
    import hmac
    import json as json_module

    now = datetime.now(timezone.utc)

    header = {"alg": "HS256", "typ": "JWT"}

    if kid is not None:
        header["kid"] = kid

    payload = {
        "iss": TEST_ISSUER,
        "aud": TEST_AUDIENCE,
        "sub": subject,
        "exp": int((now + timedelta(seconds=300)).timestamp()),
    }

    def segment(data: dict) -> bytes:
        return base64.urlsafe_b64encode(
            json_module.dumps(data, separators=(",", ":")).encode()
        ).rstrip(b"=")

    signing_input = segment(header) + b"." + segment(payload)

    signature = hmac.new(
        public_key_pem(),
        signing_input,
        hashlib.sha256,
    ).digest()

    return b".".join(
        [
            signing_input,
            base64.urlsafe_b64encode(signature).rstrip(b"="),
        ]
    ).decode()


def bearer(token: str) -> str:
    return f"Bearer {token}"


ROLE_BINDINGS_YAML = """
subjects:
  f1-analyst:
    roles:
      - programme_analyst
  f1-publisher:
    roles:
      - bi_publisher
"""