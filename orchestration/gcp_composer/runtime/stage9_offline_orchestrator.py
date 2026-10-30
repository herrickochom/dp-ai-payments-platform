"""Stage 9E offline scheduler tick. No GCP SDK, network, or background daemon.
Production scheduling must invoke tick automatically with a governed provider adapter.
"""
from stage9_transactional_recovery import RecoveryStore

class UnsafeProvider(RuntimeError): pass

def tick(store: RecoveryStore, provider, verifier, worker='stage9-reconciler'):
    """One bounded tick. Never submit without explicit provider duplicate-safety proof.
    Missing provider results are ambiguous after SUBMISSION_UNCERTAIN.
    """
    claim = store.claim_due(worker)
    if claim is None: return 'IDLE'
    state = claim['state']
    if state == 'PREPARED':
        # Crash-safe: mark uncertainty BEFORE any possible remote request.
        store.transition(claim,'PREPARED','SUBMISSION_UNCERTAIN')
        return 'ARMED_FOR_RECONCILIATION'
    jobs = provider.find_by_name(claim['project'],claim['region'],claim['job_name'])
    if jobs is None or len(jobs) == 0:
        store.transition(claim,state,'SUBMISSION_UNCERTAIN',delay_seconds=30)
        return 'WAIT_PROVIDER_VISIBILITY'
    if len(jobs) != 1:
        store.transition(claim,state,'QUARANTINED')
        return 'QUARANTINED_DUPLICATE'
    job = jobs[0]
    if (job['name'],job['project'],job['region'],job.get('spec_digest')) != (
        claim['job_name'],claim['project'],claim['region'],claim['digest']):
        store.transition(claim,state,'QUARANTINED')
        return 'QUARANTINED_IDENTITY'
    if job['state'] == 'JOB_STATE_DONE':
        digest = verifier.verify(claim,job)
        if not digest:
            store.transition(claim,state,'VERIFYING',job_id=job['id'])
            return 'WAIT_EVIDENCE'
        store.transition(claim,state,'COMPLETED',job_id=job['id'],evidence_digest=digest)
        return 'EVIDENCE_RECORDED'
    if job['state'] in {'JOB_STATE_FAILED','JOB_STATE_CANCELLED','JOB_STATE_DRAINED'}:
        store.transition(claim,state,'QUARANTINED',job_id=job['id'])
        return 'QUARANTINED_TERMINAL'
    store.transition(claim,state,'OBSERVING',job_id=job['id'])
    return 'OBSERVING'
