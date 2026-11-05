"""Stage 10 execution-bound evidence contract. Offline, fail-closed.

No BigQuery table-count shortcut, no remote submission, no fabricated evidence.
An independently attested, immutable manifest is required before acceptance.
"""
from __future__ import annotations
import hashlib
import json
import re
from recovery_contracts import JobIdentity, UnsafeToDeploy, verified_evidence

SHA = re.compile(r'^[0-9a-f]{64}$')

def canonical_digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()

def validate_manifest(identity: JobIdentity, job: dict, manifest: dict, *,
                      attestation_verifier) -> dict:
    """Verify independent manifest attestation AND identity-bound output evidence.

    attestation_verifier(manifest) must validate a signature/immutable provenance
    using an independent trust anchor; returning True on a field alone is unsafe.
    """
    identity.validate()
    if not isinstance(manifest, dict):
        raise UnsafeToDeploy('manifest missing')
    if not callable(attestation_verifier) or attestation_verifier(manifest) is not True:
        raise UnsafeToDeploy('independent attestation failed')
    required = {'execution_id': identity.execution_id,
                'attempt_number': identity.attempt,
                'dataflow_job_id': job.get('id'),
                'spec_digest': identity.spec_digest}
    if any(manifest.get(k) != v for k, v in required.items()):
        raise UnsafeToDeploy('manifest identity mismatch')
    objects = manifest.get('output_objects')
    if not isinstance(objects, list) or not objects:
        raise UnsafeToDeploy('output evidence missing')
    seen = set()
    for item in objects:
        if not isinstance(item, dict): raise UnsafeToDeploy('invalid object')
        uri, digest = item.get('uri'), item.get('sha256')
        if not isinstance(uri, str) or not uri.startswith('gs://') or not SHA.fullmatch(str(digest)):
            raise UnsafeToDeploy('invalid object reference')
        if uri in seen: raise UnsafeToDeploy('duplicate output object')
        seen.add(uri)
    evidence = {k: manifest[k] for k in required}
    evidence.update(output_objects=objects, manifest_verified=True,
                    bigquery_verified=manifest.get('bigquery_verified') is True)
    return verified_evidence(identity, job, evidence)
