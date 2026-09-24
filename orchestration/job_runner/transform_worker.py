"""Durable worker. Lease loss and unknown state become ORPHANED, never auto-retried."""
import os,re,socket,time
try:
    from orchestration.transform_runtime.durable_queue import LeaseLost,PostgresDurableQueue
except ImportError:
    from transform_runtime.durable_queue import LeaseLost,PostgresDurableQueue
try:
    from orchestration.transform_runtime.nessie_publication import (
        NessieConflict,
        NessieNotFound,
        NessiePublicationError,
        NessiePublisher,
        branch_for_run,
    )
except ImportError:
    from transform_runtime.nessie_publication import (
        NessieConflict,
        NessieNotFound,
        NessiePublicationError,
        NessiePublisher,
        branch_for_run,
    )
try:
    from services.shared.security.secret_provider import SecretUnavailable, resolve_secret
except ImportError:  # pragma: no cover - container layout fallback
    try:
        from secret_provider import SecretUnavailable, resolve_secret  # type: ignore[no-redef]
    except ImportError:
        SecretUnavailable = RuntimeError  # type: ignore[no-redef,assignment]

        def resolve_secret(name):  # type: ignore[misc]
            import os as _os

            return _os.environ.get(name)
from transform_execution import build_transform_command,execute

_BRANCH_RE=re.compile(r"transform_(?:tr|be)_[a-f0-9]{32}")


def _default_publisher():
    """Build the worker-side publisher with the execution-scoped token.

    The worker never holds ``NESSIE_PUBLICATION_TOKEN`` (runtime-only);
    branch assurance uses ``NESSIE_TRANSFORM_TOKEN`` so the default
    publication-token path in ``NessiePublisher`` is never taken here.
    """
    import os as _os

    try:
        token = resolve_secret("NESSIE_TRANSFORM_TOKEN")
    except Exception:
        token = _os.environ.get("NESSIE_TRANSFORM_TOKEN", "")
    return NessiePublisher(_os.getenv("NESSIE_ENDPOINT", ""), token)


def _branch_factory():
    return _default_publisher


def ensure_nessie_branch(transform_run_id, *, publisher_factory=None):
    """Ensure the execution Nessie branch exists before dbt is launched.

    Ownership stays in the control plane (worker), never in the dbt
    connection plugin: the plugin consumes an already-created branch and
    fails closed when the reference is absent.

    ``tr_`` run branches and ``be_`` execution-only fallback branches are
    both created idempotently here (via ``create_run_branch``, which treats
    an existing branch at the recorded base hash as success and raises
    ``NessieConflict`` on hash drift). This closes the gap where the worker
    derived ``DBT_NESSIE_BRANCH`` without ever creating it, so the branch
    now provably exists before ``build_transform_command``/dbt runs. The
    plugin never creates branches and never falls back to ``main``.

    Returns the branch name that dbt must consume.
    """
    factory = publisher_factory or _branch_factory()
    try:
        publisher = factory() if callable(factory) else factory
    except NessiePublicationError:
        raise
    except Exception as exc:
        raise NessiePublicationError("Nessie branch publisher unavailable") from exc
    run_id = (transform_run_id or "").strip()
    if re.fullmatch(r"(?:tr|be)_[a-f0-9]{32}", run_id or ""):
        name = branch_for_run(run_id)
        base_ref = os.getenv("NESSIE_BASE_REF", "main").strip() or "main"
        base_hash = os.getenv("NESSIE_BASE_HASH", "").strip()
        if not base_hash:
            # Resolve the immutable base hash; pinning the hash keeps creation
            # idempotent and prevents silently branching from a moved ref.
            try:
                base = publisher.get_reference(base_ref)
            except NessiePublicationError:
                raise
            except Exception as exc:
                raise NessiePublicationError("Nessie base reference unavailable") from exc
            if not isinstance(base, dict) or not base.get("hash"):
                raise NessiePublicationError("incomplete Nessie reference")
            base_hash = base["hash"]
        created = publisher.create_run_branch(run_id, base_ref, base_hash)
        if not isinstance(created, dict) or created.get("name") != name or not created.get("hash"):
            raise NessiePublicationError("incomplete Nessie reference")
        return name
    raise NessiePublicationError("invalid transform run identity")

def enabled():
    return os.getenv("PLATFORM_RUNNER_EXECUTION_ENABLED","false").lower()=="true" and os.getenv("PLATFORM_JOB_EXECUTION_ENABLED","false").lower()=="true"

def run_once(queue=None,worker=None,*,branch_factory=None):
    if not enabled(): return False
    q=queue or PostgresDurableQueue(); worker=worker or f"{socket.gethostname()}:{os.getpid()}"
    lease=int(os.getenv("TRANSFORM_WORKER_LEASE_SECONDS","30"))
    q.reconcile_expired(); row=q.claim(worker,lease)
    if not row: return False
    eid=row["batch_execution_id"]; token=row["lease_token"]
    try:
        ensure_nessie_branch(row.get("transform_run_id", eid), publisher_factory=branch_factory)
    except (NessiePublicationError, NessieConflict, NessieNotFound) as exc:
        q.orphan_if_owned(eid,worker,token,f"NESSIE_BRANCH_UNAVAILABLE:{type(exc).__name__}")
        return True
    try:
        cmd=build_transform_command(row["batch_id"],eid,transform_run_id=row["transform_run_id"])
    except ValueError as exc:
        q.orphan_if_owned(eid,worker,token,f"TRANSFORM_COMMAND_REJECTED:{exc}")
        return True
    last=[time.monotonic()]; lease_lost=[False]
    def cancel():
        if time.monotonic()-last[0]>=max(1,lease//3):
            if not q.heartbeat(eid,worker,token,lease):
                lease_lost[0]=True; return True
            last[0]=time.monotonic()
        try: return q.cancel_requested(eid,worker,token)
        except LeaseLost:
            lease_lost[0]=True; return True
    try:
        r=execute(cmd,timeout_seconds=float(os.getenv("TRANSFORM_BATCH_TIMEOUT_SECONDS","3300")),
                  termination_grace_seconds=float(os.getenv("TRANSFORM_TERMINATION_GRACE_SECONDS","15")),
                  output_cap_bytes=int(os.getenv("TRANSFORM_OUTPUT_CAP_BYTES","262144")),cancel_requested=cancel)
    except BaseException:
        q.orphan_if_owned(eid,worker,token,"WORKER_EXCEPTION_UNKNOWN_WRITE_STATE"); raise
    if lease_lost[0]:
        q.orphan_if_owned(eid,worker,token,"WORKER_LEASE_LOST_REQUIRES_RECONCILIATION")
        return True
    if r.status=="SUCCEEDED": q.finish(eid,worker,token,"SUCCEEDED",test_status="PASSED")
    elif r.status=="FAILED": q.finish(eid,worker,token,"FAILED",test_status="NOT_RUN",failure_class=r.failure_class or "DBT_BUILD_FAILED")
    elif r.status=="CANCELLED": q.finish(eid,worker,token,"CANCELLED",failure_class=r.failure_class or "OPERATOR_CANCELLED")
    else: q.finish(eid,worker,token,"ORPHANED",failure_class=r.failure_class or "UNKNOWN_WRITE_STATE_REQUIRES_RECONCILIATION")
    return True

def main():
    while True:
        if not run_once(): time.sleep(float(os.getenv("TRANSFORM_WORKER_POLL_SECONDS","1")))
if __name__=="__main__": main()
