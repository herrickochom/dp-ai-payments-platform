"""Apply the ``agent_publisher`` permission policy to a running Superset.

Idempotent and fail-closed. The password is resolved through the shared
secret-provider contract, used in memory only, and never logged. Only the role
name and permission count are printed.
"""

from __future__ import annotations

import sys


def _resolve_password() -> str:
    """Resolve the publisher password via the shared secret provider.

    The provider package is not on this image's default ``PYTHONPATH``; it is
    copied in at ``/app/services``. Import it by path so the platform's single
    secret-resolution contract is used rather than reading the environment
    directly, which would bypass the provider.
    """
    import pathlib
    import sys

    root = pathlib.Path("/app")
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from services.shared.security.secret_provider import require_secret

    return require_secret("SUPERSET_PUBLISHER_PASSWORD")


def provision() -> int:
    """Create or converge the publisher identity and role."""
    from superset.app import create_app

    try:
        from publisher_policy import (
            PUBLISHER,
            assert_no_forbidden,
            missing_against_inventory,
            required_permissions,
        )
    except ImportError:
        import importlib.util
        import pathlib

        spec = importlib.util.spec_from_file_location(
            "publisher_policy", pathlib.Path(__file__).with_name("publisher_policy.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules["publisher_policy"] = module

    import publisher_policy as policy

    try:
        password = _resolve_password()
    except Exception:  # noqa: BLE001
        print(
            "ERROR: SUPERSET_PUBLISHER_PASSWORD is unavailable from the "
            "configured secret source",
            file=sys.stderr,
        )
        return 2

    app = create_app()
    with app.app_context():
        from flask_appbuilder.security.sqla.models import PermissionView, Role, User
        from sqlalchemy import select
        from superset import db, security_manager

        rows = db.session.execute(select(PermissionView)).scalars().all()
        inventory = {(row.view_menu.name, row.permission.name) for row in rows}

        required = policy.required_permissions()
        missing = policy.missing_against_inventory(required, inventory)
        if missing:
            # Fail closed: the installed Superset does not expose the exact
            # permissions this role is defined against.
            print(
                "ERROR: required permissions absent from installed Superset: "
                + ", ".join(f"{v}:{a}" for v, a in sorted(missing)),
                file=sys.stderr,
            )
            return 3

        policy.assert_no_forbidden(set(required))

        role = (
            db.session.execute(select(Role).where(Role.name == policy.PUBLISHER))
            .scalars()
            .first()
        )
        created = role is None
        if created:
            role = Role(name=policy.PUBLISHER)
            db.session.add(role)
            db.session.flush()

        # Converge the exact permission set: add what is missing, remove any
        # permission that is not required, so a widened role is corrected.
        granted: set[tuple[str, str]] = set()
        existing = {
            (pvm.view_menu.name, pvm.permission.name): pvm for pvm in role.permissions
        }
        for view_menu_name, action_name in required:
            pvm = existing.get((view_menu_name, action_name))
            if pvm is None:
                pvm = (
                    db.session.execute(
                        select(PermissionView)
                        .where(
                            PermissionView.view_menu.has(name=view_menu_name),
                            PermissionView.permission.has(name=action_name),
                        )
                    )
                    .scalars()
                    .first()
                )
                if pvm is None:
                    print(
                        f"ERROR: permission vanished during provisioning: "
                        f"{view_menu_name}:{action_name}",
                        file=sys.stderr,
                    )
                    return 3
                role.permissions.append(pvm)
            granted.add((view_menu_name, action_name))

        for pair, pvm in existing.items():
            if pair not in required:
                role.permissions.remove(pvm)

        policy.assert_no_forbidden(granted)

        user = (
            db.session.execute(select(User).where(User.username == policy.PUBLISHER))
            .scalars()
            .first()
        )
        if user is None:
            security_manager.add_user(
                policy.PUBLISHER, policy.PUBLISHER, policy.PUBLISHER, password, role
            )
            db.session.flush()
            user = (
                db.session.execute(select(User).where(User.username == policy.PUBLISHER))
                .scalars()
                .first()
            )
            user_action = "created"
        else:
            if [r.name for r in user.roles] != [policy.PUBLISHER]:
                security_manager.set_role(role.id, user.id)
            user_action = "converged"

        # The credential must be stored hashed. ``add_user`` writes the raw
        # value, which leaves the user unable to authenticate: observed as
        # werkzeug check_password_hash(stored, secret) == False even though the
        # user row exists, is active and holds the right role. reset_password
        # applies the configured scrypt hashing, after which the same check
        # returns True. Setting it unconditionally also converges a previously
        # mis-provisioned account onto the current secret.
        security_manager.reset_password(user.id, password)
        db.session.commit()
        print(
            f"{'created' if created else 'converged'}/{user_action} "
            f"Superset publisher: role={policy.PUBLISHER} "
            f"permissions={len(granted)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(provision())