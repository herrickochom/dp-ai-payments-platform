"""Git evidence: HEAD state, tracked-file counts and commit history.

Read-only. No history is rewritten, reset, checked out, restored or
cleaned by any collector.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import (
    KIND_GIT,
    CollectorResult,
    EvidenceItem,
    run_git,
    safe_int,
    today_iso,
    utc_now_iso,
    register,
)

MAX_COMMITS = 200


@register("git")
def collect_git(root: Path) -> CollectorResult:
    result = CollectorResult(name="git")

    code, head_sha, _ = run_git(root, "rev-parse", "HEAD")
    if code != 0 or not head_sha.strip():
        result.errors.append("git HEAD could not be resolved")
        return result
    head = head_sha.strip()

    _, branch, _ = run_git(root, "rev-parse", "--abbrev-ref", "HEAD")
    branch_name = branch.strip() or "unknown"

    _, tracked, _ = run_git(root, "ls-files")
    tracked_files = [line for line in tracked.splitlines() if line.strip()]

    _, describe, _ = run_git(root, "describe", "--tags", "--always")
    revision = describe.strip() or head[:12]

    result.facts = {
        "head_sha": head,
        "short_sha": head[:12],
        "branch": branch_name,
        "revision": revision,
        "tracked_file_count": len(tracked_files),
        "observed_at": utc_now_iso(),
    }

    result.add(
        EvidenceItem(
            evidence_id="EV-GIT-HEAD",
            kind=KIND_GIT,
            title="Repository HEAD revision",
            location=f"{branch_name}@{head[:12]}",
            status="PRESENT",
            source_path=".git",
            observed_at=result.facts["observed_at"],
            detail={
                "git_sha": head,
                "git_branch": branch_name,
                "revision": revision,
                "tracked_file_count": len(tracked_files),
            },
        )
    )

    code, log, _ = run_git(
        root,
        "log",
        f"--max-count={MAX_COMMITS}",
        "--date=short",
        "--pretty=format:%H%x1f%ad%x1f%an%x1f%s",
    )
    if code != 0:
        result.errors.append("git log could not be read")
        return result

    commits: list[dict[str, Any]] = []
    for index, line in enumerate(log.splitlines(), start=1):
        parts = line.split("\x1f")
        if len(parts) != 4:
            continue
        sha, authored, author, summary = parts
        commits.append(
            {
                "index": index,
                "sha": sha,
                "short_sha": sha[:12],
                "author_date": authored,
                "author": author,
                "summary": summary,
            }
        )
        result.add(
            EvidenceItem(
                evidence_id=f"EV-GIT-{sha[:12]}",
                kind=KIND_GIT,
                title=summary[:160],
                location=sha[:12],
                status="PRESENT",
                source_path=".git",
                observed_at=result.facts["observed_at"],
                detail={
                    "git_sha": sha,
                    "author_date": authored,
                    "author": author,
                },
            )
        )

    dates = sorted({commit["author_date"] for commit in commits if commit["author_date"]})
    result.facts["commit_count"] = len(commits)
    result.facts["first_commit_date"] = dates[0] if dates else ""
    result.facts["last_commit_date"] = dates[-1] if dates else ""
    result.facts["evidence_date"] = today_iso()
    return result
