"""Cryptographic bearer-token authentication (F1).

This module performs AUTHENTICATION only: it decides *who* the caller is by
cryptographically verifying an OIDC/OAuth 2.0 bearer access token against a
JWKS-published asymmetric signing key.  It performs NO authorisation.

The separation is deliberate and load-bearing:

    TokenVerifier          -> WHO      (this module)
    ServerAuthorityResolver -> WHAT    (authority.py)
    GovernancePolicyEngine -> WHETHER  (governance.py)

A verified token proves identity only.  Token claims such as ``roles``,
``permissions``, ``groups`` or ``can_publish_bi_assets`` are NEVER read here
and never reach the authority registry.  The server-owned subject-to-role
binding decides what a subject may do.

Security properties enforced here:

* verification algorithms are SERVER-PINNED; the token header cannot select
  the verification policy, so ``alg=none`` and HS/RS algorithm confusion are
  rejected before any key is resolved;
* ``iss``, ``aud``, ``exp`` and ``sub`` are required; ``nbf`` is validated
  when present;
* the signing key is resolved from the published JWKS by ``kid``; an unknown
  ``kid`` never falls back to another key;
* ANY failure - including a JWKS outage or malformed JWKS - is an
  authentication failure.  There is no unverified fallback path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import jwt


logger = logging.getLogger(__name__)


AUTHENTICATION_METHOD_OIDC_JWT = "oidc_jwt"

#
# Server-pinned default.  Asymmetric only: no `none`, and no HMAC family, so a
# token cannot downgrade to symmetric verification using the public key as a
# shared secret.
#
DEFAULT_ALLOWED_ALGORITHMS: tuple[str, ...] = ("RS256",)

REQUIRED_CLAIMS: tuple[str, ...] = ("exp", "iss", "aud", "sub")

BEARER_SCHEME = "bearer"


class AuthenticationError(RuntimeError):
    """Controlled authentication failure.

    ``category`` is safe to surface to operators.  The message never contains
    the token, a key identifier, the expected issuer or the expected audience.
    """

    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category


@dataclass(frozen=True)
class TrustedIdentity:
    """The minimum verified identity information downstream code needs.

    Deliberately excludes the token, its signature, key material, and any
    role/permission claim carried by the token.
    """

    subject_id: str
    issuer: str
    authentication_method: str = AUTHENTICATION_METHOD_OIDC_JWT
    authenticated: bool = True

    def audit_fields(self) -> dict[str, Any]:
        """Fields safe to record in audit output. Never includes a credential."""

        return {
            "subject_id": self.subject_id,
            "issuer": self.issuer,
            "authentication_method": self.authentication_method,
        }


@runtime_checkable
class SigningKeyResolver(Protocol):
    """Resolves the signing key a token was signed with."""

    def resolve(self, token: str) -> Any:
        """Return a verification key, or raise AuthenticationError."""


class JwksSigningKeyResolver:
    """Production resolver backed by the library's JWKS client.

    ``PyJWKClient`` performs ``kid``-keyed selection, caches keys, and refreshes
    the key set on an unknown ``kid`` so rotation works without a restart.  A
    JWKS outage or malformed document surfaces as an exception and becomes an
    authentication failure here.
    """

    def __init__(
        self,
        jwks_url: str,
        *,
        cache_seconds: int = 300,
        timeout_seconds: int = 5,
    ) -> None:
        self.jwks_url = jwks_url
        self._client = jwt.PyJWKClient(
            jwks_url,
            cache_keys=True,
            lifespan=cache_seconds,
            timeout=timeout_seconds,
        )

    def resolve(self, token: str) -> Any:
        try:
            signing_key = self._client.get_signing_key_from_jwt(token)

        except Exception as exc:
            #
            # Deliberately does not echo the exception: library errors can
            # embed response bodies from the key endpoint.
            #
            logger.warning(
                "jwks_signing_key_unavailable",
                extra={"error_category": type(exc).__name__},
            )
            raise AuthenticationError(
                "SigningKeyUnavailable",
                "Signing key could not be established.",
            ) from None

        return signing_key.key


def extract_bearer_token(header: str | None) -> str:
    """Parse ``Authorization: Bearer <token>`` safely."""

    if not header or not header.strip():
        raise AuthenticationError(
            "AuthenticationRequired",
            "Authentication is required for this resource.",
        )

    parts = header.split()

    if len(parts) != 2:
        raise AuthenticationError(
            "AuthenticationRequired",
            "Authorization header must contain exactly one Bearer credential.",
        )

    scheme, credential = parts

    if scheme.lower() != BEARER_SCHEME:
        raise AuthenticationError(
            "AuthenticationRequired",
            "Authorization scheme must be Bearer.",
        )

    if not credential.strip():
        raise AuthenticationError(
            "AuthenticationRequired",
            "Bearer credential is empty.",
        )

    return credential



class TokenVerifier:
    """Verify a bearer access token and produce a `TrustedIdentity`."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        signing_keys: SigningKeyResolver,
        allowed_algorithms=DEFAULT_ALLOWED_ALGORITHMS,
        clock_skew_seconds: int = 0,
    ) -> None:
        if not (issuer or "").strip():
            raise ValueError("issuer is required for trusted authentication")

        if not (audience or "").strip():
            raise ValueError("audience is required for trusted authentication")

        if not allowed_algorithms:
            raise ValueError("at least one verification algorithm is required")

        #
        # Symmetric algorithms are never acceptable on the JWKS asymmetric
        # path; reject them at construction so a misconfiguration cannot
        # silently downgrade verification.
        #
        forbidden = {"none", "HS256", "HS384", "HS512"}

        if forbidden.intersection(allowed_algorithms):
            raise ValueError(
                "symmetric and unsigned algorithms are not permitted for "
                "JWKS verification"
            )

        self.issuer = issuer.strip()
        self.audience = audience.strip()
        self.signing_keys = signing_keys
        self.allowed_algorithms = tuple(allowed_algorithms)
        self.clock_skew_seconds = max(0, int(clock_skew_seconds or 0))

    def verify(self, authorization_header: str | None) -> TrustedIdentity:
        """Verify a request's Authorization header.

        Raises `AuthenticationError` for every failure.  Never returns an
        identity derived from unverified claims.
        """

        token = extract_bearer_token(authorization_header)

        #
        # The token header is read ONLY to reject a disallowed algorithm early
        # and to let the key resolver select by kid.  It never selects the
        # verification policy: `self.allowed_algorithms` is what `jwt.decode`
        # enforces.
        #
        try:
            header = jwt.get_unverified_header(token)

        except Exception:
            raise AuthenticationError(
                "InvalidToken",
                "Authentication is required for this resource.",
            ) from None

        algorithm = header.get("alg")

        if algorithm not in self.allowed_algorithms:
            logger.warning(
                "token_algorithm_rejected",
                extra={"presented_algorithm": str(algorithm)},
            )
            raise AuthenticationError(
                "InvalidToken",
                "Authentication is required for this resource.",
            )

        key = self.signing_keys.resolve(token)

        try:
            claims = jwt.decode(
                token,
                key=key,
                algorithms=list(self.allowed_algorithms),
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.clock_skew_seconds,
                options={
                    "require": list(REQUIRED_CLAIMS),
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_aud": True,
                    "verify_iss": True,
                    "verify_nbf": True,
                },
            )

        except Exception as exc:
            logger.warning(
                "token_verification_failed",
                extra={"error_category": type(exc).__name__},
            )
            raise AuthenticationError(
                "InvalidToken",
                "Authentication is required for this resource.",
            ) from None

        subject_id = (claims.get("sub") or "").strip()

        if not subject_id:
            raise AuthenticationError(
                "InvalidToken",
                "Authentication is required for this resource.",
            )

        #
        # NOTE: no role, permission, group or scope claim is read here.
        # Authority is resolved server-side from the verified subject only.
        #
        return TrustedIdentity(
            subject_id=subject_id,
            issuer=self.issuer,
        )
