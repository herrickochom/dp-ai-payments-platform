"""Billing observation adapter: no fabricated 'current spend' from delayed exports.

Google Cloud Billing exports can lag and may not exist. This adapter explicitly
blocks launch permission until a separate reconciled, timestamped, currency-
checked observation is available. A budget alert is not a hard spending cap.
"""
from __future__ import annotations
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
from cost_guard import CostGateBlocked

def validate_observation(*, project, currency, spend, observed_at, source,
                         expected_project='project-e48f194e-f44f-458d-b4f',
                         now=None):
    now=now or datetime.now(timezone.utc)
    if project!=expected_project or currency!='GBP':
        raise CostGateBlocked('project or currency mismatch')
    if source not in {'RECONCILED_BILLING_EXPORT'}:
        raise CostGateBlocked('untrusted billing source')
    if not isinstance(observed_at,datetime) or observed_at.tzinfo is None:
        raise CostGateBlocked('missing timestamp')
    if not isinstance(now,datetime) or now.tzinfo is None:
        raise CostGateBlocked('invalid current timestamp')
    age=(now-observed_at).total_seconds()/60
    if not 0<=age<=120:
        raise CostGateBlocked('stale billing observation')
    try:
        amount=Decimal(str(spend))
    except (InvalidOperation,ValueError):
        raise CostGateBlocked('invalid spend') from None
    if not amount.is_finite() or amount<0:
        raise CostGateBlocked('invalid spend')
    return {'reported_spend_gbp':float(amount),'spend_observed_at':observed_at,
            'source':source,'launch_authorised':False}
