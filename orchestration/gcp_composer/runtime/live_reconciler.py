"""Production-oriented reconciliation tick. Never launches a second Dataflow job.

A prepared intent is marked uncertain before any possible external submission;
without a provider idempotency guarantee, UNKNOWN must remain UNKNOWN.
"""
from __future__ import annotations
from transactional_recovery import RecoveryStore

class ReconciliationFailure(RuntimeError): pass


def reconcile_once(store: RecoveryStore, provider, verifier, worker='composer-stage10'):
    claim=store.claim_due(worker)
    if claim is None: return 'IDLE'
    state=claim['state']
    if state=='PREPARED':
        store.transition(claim,state,'SUBMISSION_UNCERTAIN',delay_seconds=30)
        return 'ARMED_FOR_RECONCILIATION'
    try:
        jobs=provider.find_by_name(claim['project'],claim['region'],claim['job_name'])
    except Exception:
        jobs=None
    if not jobs:
        store.transition(claim,state,'SUBMISSION_UNCERTAIN',delay_seconds=60)
        return 'WAIT_PROVIDER_VISIBILITY'
    if len(jobs)!=1:
        store.transition(claim,state,'QUARANTINED')
        return 'QUARANTINED_DUPLICATE'
    job=jobs[0]
    if (job.get('name'),job.get('project'),job.get('region'),job.get('spec_digest')) != (claim['job_name'],claim['project'],claim['region'],claim['digest']) or not job.get('id'):
        store.transition(claim,state,'QUARANTINED')
        return 'QUARANTINED_IDENTITY'
    if job.get('state')=='JOB_STATE_DONE':
        # Verifier must independently attest the BigQuery output lineage.
        evidence=verifier.verify(claim,job)
        if not evidence:
            store.transition(claim,state,'VERIFYING',job_id=job['id'],delay_seconds=60)
            return 'WAIT_EVIDENCE'
        store.transition(claim,state,'COMPLETED',job_id=job['id'],evidence_digest=evidence)
        return 'EVIDENCE_RECORDED'
    if job.get('state') in ('JOB_STATE_FAILED','JOB_STATE_CANCELLED','JOB_STATE_DRAINED'):
        store.transition(claim,state,'QUARANTINED',job_id=job['id'])
        return 'QUARANTINED_TERMINAL'
    store.transition(claim,state,'OBSERVING',job_id=job['id'],delay_seconds=60)
    return 'OBSERVING'
