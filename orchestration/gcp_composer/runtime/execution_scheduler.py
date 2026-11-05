"""One bounded reconciliation tick, with no cloud submission and no manual trigger."""
from recovery_orchestrator import tick

def reconcile_once(store, provider, verifier, *, worker='stage10-scheduled-reconciler'):
    return tick(store,provider,verifier,worker=worker)
