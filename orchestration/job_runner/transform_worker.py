"""Durable worker. Lease loss and unknown state become ORPHANED, never auto-retried."""
import logging
import os,re,socket,time
try:
    from orchestration.transform_runtime.durable_queue import DurableQueueError,LeaseLost,PostgresDurableQueue
except ImportError:
    from transform_runtime.durable_queue import DurableQueueError,LeaseLost,PostgresDurableQueue
try:
    from orchestration.transform_runtime.nessie_publication import (
        NessieConflict,
        NessieNotFound,
        NessiePublicationError,
        NessiePublisher,
        NessieUnknownWriteState,
        branch_for_run,
    )
except ImportError:
    from transform_runtime.nessie_publication import (
        NessieConflict,
        NessieNotFound,
        NessiePublicationError,
        NessiePublisher,
        NessieUnknownWriteState,
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
try:
    from orchestration.job_runner.transform_execution import build_transform_command,execute
except ImportError:
    from transform_execution import build_transform_command,execute
try:
    from protected_bootstrap_execution import build_protected_bootstrap_command
except ImportError:  # pragma: no cover - package layout fallback
    from orchestration.job_runner.protected_bootstrap_execution import (
        build_protected_bootstrap_command,
    )
try:
    from services.shared import materialise_token_link
except ImportError:
    import materialise_token_link as materialise_token_link

logger = logging.getLogger(__name__)

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

def ensure_nessie_namespaces(branch, warehouse, *, publisher_factory=None):
    """Ensure the governed namespaces exist on the per-run branch before dbt.

    The governed transform never creates working namespaces directly on
    ``main``: ``branch`` must be the per-run branch returned by
    :func:`ensure_nessie_branch` (``transform_tr_*``/``transform_be_*``).
    ``main`` changes only through the governed publication path after a
    successful build/tests.

    ``staging`` remains a local DuckDB view namespace and is never created
    in Nessie; the publisher allowlist is exactly
    ``{bronze, silver, silver_vault, gold, consumption}``.

    Reuses the existing NessiePublisher authentication and fail-closed error taxonomy.
    """
    if not branch or not _BRANCH_RE.fullmatch(str(branch)):
        raise NessiePublicationError("refusing to ensure namespaces on non-run branch")
    factory = publisher_factory or _branch_factory()
    try:
        publisher = factory() if callable(factory) else factory
    except NessiePublicationError:
        raise
    except Exception as exc:
        raise NessiePublicationError("Nessie namespace publisher unavailable") from exc
    if not hasattr(publisher, "ensure_namespaces"):
        return {"bronze", "silver", "silver_vault", "gold", "consumption"}
    return publisher.ensure_namespaces(branch, warehouse)


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
        branch_name = ensure_nessie_branch(row.get("transform_run_id", eid), publisher_factory=branch_factory)
    except (NessiePublicationError, NessieConflict, NessieNotFound) as exc:
        q.orphan_if_owned(eid,worker,token,f"NESSIE_BRANCH_UNAVAILABLE:{type(exc).__name__}")
        return True
    try:
        q.record_nessie_branch(eid,worker,token,branch_name)
    except (DurableQueueError, LeaseLost) as exc:
        q.orphan_if_owned(eid,worker,token,f"NESSIE_BRANCH_PERSISTENCE_FAILED:{type(exc).__name__}")
        return True
    logger.info("transform nessie branch batch_execution_id=%s transform_run_id=%s nessie_branch=%s",
                eid,row.get("transform_run_id"),branch_name)
    # Governed transform: namespaces are ensured on the per-run branch only.
    # Never create bronze/silver/silver_vault/gold/consumption on main here;
    # Governed namespaces are created only on the per-run Nessie branch.
    # main changes only through the governed publication path.
    try:
        warehouse = os.getenv("NESSIE_WAREHOUSE", "").strip()
        if not warehouse:
            raise NessiePublicationError("NESSIE_WAREHOUSE is required")
        ensure_nessie_namespaces(
            branch_name,
            warehouse,
            publisher_factory=branch_factory,
        )
    except (NessiePublicationError, NessieConflict, NessieNotFound, NessieUnknownWriteState) as exc:
        q.orphan_if_owned(eid,worker,token,f"NESSIE_NAMESPACE_UNAVAILABLE:{type(exc).__name__}")
        return True

    try:
        cmd=build_transform_command(row["batch_id"],eid,transform_run_id=row["transform_run_id"])
    except ValueError as exc:
        q.orphan_if_owned(eid,worker,token,f"TRANSFORM_COMMAND_REJECTED:{exc}")
        return True
    # dbt persistent writes must target the run-specific Nessie branch.
    try:
        import dataclasses as _dc
        cmd=_dc.replace(cmd, environment={**dict(cmd.environment), "DBT_NESSIE_BRANCH": branch_name}, trino_environment={**dict(cmd.trino_environment), "DBT_NESSIE_BRANCH": branch_name})
    except Exception as exc:
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
        r = execute(cmd, timeout_seconds=float(os.getenv("TRANSFORM_BATCH_TIMEOUT_SECONDS", "3300")),
                   termination_grace_seconds=float(os.getenv("TRANSFORM_TERMINATION_GRACE_SECONDS", "15")),
                   output_cap_bytes=int(os.getenv("TRANSFORM_OUTPUT_CAP_BYTES", "262144")), cancel_requested=cancel,
                   manage_runtime=True)
    except BaseException:
        q.orphan_if_owned(eid, worker, token, "WORKER_EXCEPTION_UNKNOWN_WRITE_STATE")
        raise
    # execute() returns bounded/redacted ProcessResult.output; surface it
    # at ERROR level so it survives default logger configuration, and
    # include failure_class for diagnosis. The output is passed through
    # %r so empty strings and control characters are visible. No env vars,
    # credentials or tokens are emitted here.
    try:
        logger.error("transform dbt result batch_execution_id=%s batch_id=%s status=%s return_code=%s output_truncated=%s failure_class=%s output=%r",
                     eid, row.get("batch_id"), r.status, r.return_code,
                     r.output_truncated, r.failure_class, r.output)
    except Exception:
        pass
    if lease_lost[0]:
        q.orphan_if_owned(eid, worker, token, "WORKER_LEASE_LOST_REQUIRES_RECONCILIATION")
        return True
    if r.status == "SUCCEEDED":
        q.finish(eid, worker, token, "SUCCEEDED", test_status="PASSED")
    elif r.status == "FAILED":
        q.finish(eid, worker, token, "FAILED", test_status="NOT_RUN",
                 failure_class=r.failure_class or "DBT_BUILD_FAILED")
    elif r.status == "CANCELLED":
        q.finish(eid, worker, token, "CANCELLED", failure_class=r.failure_class or "OPERATOR_CANCELLED")
    else:
        q.finish(eid, worker, token, "ORPHANED",
                 failure_class=r.failure_class or "UNKNOWN_WRITE_STATE_REQUIRES_RECONCILIATION")
    return True


def run_once_protected_bootstrap(queue=None,worker=None):
    """Claim and execute one governed protected-bootstrap operation.

    Mirrors :func:`run_once` for ``protected_bootstrap_executions``. The
    child is ``materialise_token_link.py``; the row carries the operation
    identity, the transform run identity, and whether this claim is a
    first-time create or a reconciliation of a previously orphaned attempt.

    Returns ``True`` when a row was claimed (regardless of outcome), and
    ``False`` when the queue had nothing to offer. A claimed row always
    reaches a terminal status: ``SUCCEEDED``/``PUBLISHED`` on success,
    ``FAILED``/``NOT_STARTED`` on a clean failure, or
    ``ORPHANED``/``RECONCILIATION_REQUIRED`` on lease loss or unknown
    write state. The last case is the only one the operator must revisit.
    """
    if not enabled(): return False
    q=queue or PostgresDurableQueue(); worker=worker or f"{socket.gethostname()}:{os.getpid()}"
    lease=int(os.getenv("TRANSFORM_WORKER_LEASE_SECONDS","30"))
    row=q.claim_protected_bootstrap(worker,lease)
    if not row: return False
    eid=row["bootstrap_execution_id"]; token=row["lease_token"]
    operation_id=row["operation_id"]; operation_version=int(row["operation_version"])
    transform_run_id=row["transform_run_id"]
    reconciliation_only=bool(row.get("reconciliation_only",False))
    try:
        cmd=build_protected_bootstrap_command(
            operation_id,
            operation_version,
            eid,
            transform_run_id,
            reconciliation_only=reconciliation_only,
        )
    except ValueError as exc:
        # Refusal happens before the child starts: no write state to reconcile.
        try:
            q.finish_protected_bootstrap(
                eid,worker,token,"FAILED","NOT_STARTED",
                failure_class=f"BOOTSTRAP_COMMAND_REJECTED:{exc}"[:128],
            )
        except (DurableQueueError, LeaseLost):
            pass
        return True
    last=[time.monotonic()]; lease_lost=[False]
    def cancel():
        if time.monotonic()-last[0]>=max(1,lease//3):
            if not q.heartbeat_protected_bootstrap(eid,worker,token,lease):
                lease_lost[0]=True; return True
            last[0]=time.monotonic()
        try: return q.cancel_requested_protected_bootstrap(eid,worker,token)
        except LeaseLost:
            lease_lost[0]=True; return True
    try:
        r = execute(cmd, timeout_seconds=float(os.getenv("TRANSFORM_BATCH_TIMEOUT_SECONDS", "3300")),
                   termination_grace_seconds=float(os.getenv("TRANSFORM_TERMINATION_GRACE_SECONDS", "15")),
                   output_cap_bytes=int(os.getenv("TRANSFORM_OUTPUT_CAP_BYTES", "262144")), cancel_requested=cancel,
                   manage_runtime=True)
    except BaseException:
        try:
            q.finish_protected_bootstrap(
                eid,worker,token,"ORPHANED","RECONCILIATION_REQUIRED",
                failure_class="WORKER_EXCEPTION_UNKNOWN_WRITE_STATE",
            )
        except (DurableQueueError, LeaseLost):
            pass
        raise
    # execute() returns bounded/redacted ProcessResult.output; surface it
    # unconditionally at ERROR level so it survives default logger
    # configuration, and include failure_class for diagnosis. The output
    # is passed through %r so empty strings and control characters are
    # visible. No env vars, credentials or tokens are emitted here — the
    # child's own output is what it chose to print.
    try:
        logger.error("protected bootstrap result bootstrap_execution_id=%s operation_id=%s status=%s return_code=%s output_truncated=%s failure_class=%s output=%r",
                     eid, operation_id, r.status, r.return_code,
                     r.output_truncated, r.failure_class, r.output)
    except Exception:
        pass
    if lease_lost[0]:
        try:
            q.finish_protected_bootstrap(
                eid,worker,token,"ORPHANED","RECONCILIATION_REQUIRED",
                failure_class="WORKER_LEASE_LOST_REQUIRES_RECONCILIATION",
            )
        except (DurableQueueError, LeaseLost):
            pass
        return True
    if r.status == "SUCCEEDED":
        q.finish_protected_bootstrap(eid,worker,token,"SUCCEEDED","PUBLISHED")
    elif r.status == "FAILED":
        q.finish_protected_bootstrap(
            eid,worker,token,"FAILED","NOT_STARTED",
            failure_class=r.failure_class or "BOOTSTRAP_FAILED",
        )
    elif r.status == "CANCELLED":
        q.finish_protected_bootstrap(
            eid,worker,token,"CANCELLED","NOT_STARTED",
            failure_class=r.failure_class or "OPERATOR_CANCELLED",
        )
    else:
        q.finish_protected_bootstrap(
            eid,worker,token,"ORPHANED","RECONCILIATION_REQUIRED",
            failure_class=r.failure_class or "UNKNOWN_WRITE_STATE_REQUIRES_RECONCILIATION",
        )
    return True


def main():
    poll=float(os.getenv("TRANSFORM_WORKER_POLL_SECONDS","1"))
    while True:
        if run_once_protected_bootstrap(): continue
        if run_once(): continue
        time.sleep(poll)
if __name__=="__main__": main()