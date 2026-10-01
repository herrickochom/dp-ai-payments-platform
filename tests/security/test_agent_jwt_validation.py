"""JOB F1 - FAIL-FIRST PROOFS: bearer token validation and JWKS safety.

Every case runs offline against the real `TokenVerifier`.  No live IdP or JWKS
endpoint is contacted; signing keys are ephemeral TEST-ONLY material.

Security invariants proven here:

* a token is accepted ONLY with a valid asymmetric signature over a known kid
  and correct issuer, audience, expiry and subject;
* the token header never selects the verification policy, so ``alg=none`` and
  HS/RS algorithm confusion are rejected;
* an unknown kid, a wrong key, a JWKS outage and a malformed JWKS are all
  authentication FAILURES - there is no unverified fallback.
"""

from __future__ import annotations

import pytest

from f1_identity_fixtures import (
    OTHER_PRIVATE_KEY,
    PRIVATE_KEY,
    ROTATED_KID,
    ROTATED_PRIVATE_KEY,
    TEST_AUDIENCE,
    TEST_ISSUER,
    TEST_KID,
    StaticSigningKeyResolver,
    bearer,
    make_hmac_confusion_token,
    make_token,
)

from authentication import (
    AuthenticationError,
    TokenVerifier,
    extract_bearer_token,
)


def _verifier(resolver=None, **kwargs):
    return TokenVerifier(
        issuer=TEST_ISSUER,
        audience=TEST_AUDIENCE,
        signing_keys=resolver or StaticSigningKeyResolver(),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Authorization header handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "   ",
        "Basic dXNlcjpwYXNz",
        "Bearer",
        "Bearer ",
        "Bearer a b",
        "Bearer a Bearer b",
    ],
)
def test_missing_or_malformed_authorization_header_is_rejected(header):
    with pytest.raises(AuthenticationError):
        extract_bearer_token(header)


def test_well_formed_bearer_header_is_extracted():
    assert extract_bearer_token("Bearer abc.def.ghi") == "abc.def.ghi"


# ---------------------------------------------------------------------------
# Authentication success
# ---------------------------------------------------------------------------


def test_valid_signed_token_is_accepted():
    identity = _verifier().verify(
        bearer(make_token(subject="f1-analyst"))
    )

    assert identity.subject_id == "f1-analyst"
    assert identity.authenticated is True
    assert identity.authentication_method == "oidc_jwt"
    assert identity.issuer == TEST_ISSUER


# ---------------------------------------------------------------------------
# Token validation failures - every one must be an authentication failure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "description, token",
    [
        ("malformed", "not-a-jwt"),
        ("malformed_structured", "a.b.c"),
        ("invalid_signature", make_token(key=OTHER_PRIVATE_KEY)),
        ("expired", make_token(expires_in=-60)),
        ("wrong_issuer", make_token(issuer="https://evil.invalid/")),
        ("wrong_audience", make_token(audience="some-other-api")),
        ("missing_subject", make_token(omit=("sub",))),
        ("empty_subject", make_token(subject="")),
        ("missing_expiry", make_token(omit=("exp",))),
        ("missing_issuer", make_token(omit=("iss",))),
        ("missing_audience", make_token(omit=("aud",))),
        ("not_yet_valid", make_token(not_before_in=600)),
    ],
)
def test_invalid_token_is_rejected(description, token):
    with pytest.raises(AuthenticationError):
        _verifier().verify(bearer(token))


def test_unsigned_alg_none_token_is_rejected():
    with pytest.raises(AuthenticationError):
        _verifier().verify(
            bearer(make_token(algorithm="none"))
        )
def test_symmetric_algorithm_confusion_is_rejected():
    """An HS256 token must not verify on the asymmetric key path.

    This is the classic algorithm-confusion attack: sign with the PUBLIC key as
    an HMAC secret and hope the verifier accepts the symmetric algorithm.
    """

    with pytest.raises(AuthenticationError):
        _verifier().verify(
            bearer(make_hmac_confusion_token())
        )


@pytest.mark.parametrize("algorithm", ["HS256", "HS384", "HS512", "none"])
def test_verifier_refuses_symmetric_and_unsigned_algorithms(algorithm):
    with pytest.raises(ValueError):
        TokenVerifier(
            issuer=TEST_ISSUER,
            audience=TEST_AUDIENCE,
            signing_keys=StaticSigningKeyResolver(),
            allowed_algorithms=(algorithm,),
        )


# ---------------------------------------------------------------------------
# JWKS / signing-key safety
# ---------------------------------------------------------------------------


def test_unknown_kid_is_rejected_without_fallback():
    resolver = StaticSigningKeyResolver({TEST_KID: PRIVATE_KEY})

    with pytest.raises(AuthenticationError):
        _verifier(resolver).verify(
            bearer(make_token(kid="some-unknown-kid"))
        )


def test_wrong_key_is_rejected():
    resolver = StaticSigningKeyResolver({TEST_KID: OTHER_PRIVATE_KEY})

    with pytest.raises(AuthenticationError):
        _verifier(resolver).verify(bearer(make_token()))


@pytest.mark.parametrize("failure", ["unavailable", "malformed"])
def test_jwks_failure_never_falls_back_to_unverified(failure):
    resolver = StaticSigningKeyResolver(**{failure: True})

    with pytest.raises(AuthenticationError):
        _verifier(resolver).verify(bearer(make_token()))

    assert resolver.calls == 1, (
        "a JWKS outage or malformed key set must be an authentication "
        "failure, never a silent acceptance of the token"
    )


def test_key_rotation_recognises_the_rotated_key():
    """After rotation the published set serves a new kid for the same subject."""

    rotated = StaticSigningKeyResolver(
        {TEST_KID: PRIVATE_KEY, ROTATED_KID: ROTATED_PRIVATE_KEY}
    )

    identity = _verifier(rotated).verify(
        bearer(
            make_token(
                subject="f1-analyst",
                kid=ROTATED_KID,
                key=ROTATED_PRIVATE_KEY,
            )
        )
    )

    assert identity.subject_id == "f1-analyst"


def test_retired_key_stops_being_accepted_after_rotation():
    """Once the old key leaves the published set, its tokens must fail."""

    rotated_only = StaticSigningKeyResolver(
        {ROTATED_KID: ROTATED_PRIVATE_KEY}
    )

    with pytest.raises(AuthenticationError):
        _verifier(rotated_only).verify(bearer(make_token()))