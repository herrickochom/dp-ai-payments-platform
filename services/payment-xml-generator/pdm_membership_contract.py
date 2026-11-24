"""PDM SACCO and enterprise-group membership contracts.

No synthetic membership is inferred from loans or beneficiary counts.
Identity conflicts are reported, not auto-merged or deleted.
"""
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal


class MembershipContractError(ValueError):
    pass


def _unique(rows, key, label):
    ids = [str(row.get(key) or '').strip() for row in rows]
    if not all(ids) or len(set(ids)) != len(ids):
        raise MembershipContractError(f'{label}: missing or duplicate {key}')
    return set(ids)


def validate_memberships(beneficiaries, saccos, loans, memberships=(), groups=(), group_memberships=()):
    """Validate existing entities and optional explicit membership datasets.

    Membership fields: membership_id, beneficiary_id, sacco_id, status,
    effective_from, optional effective_to.
    Group fields: group_id, sacco_id.
    Group membership fields: group_membership_id, group_id, beneficiary_id.
    Returns aggregate counts only; does not reveal NINs.
    """
    for label, rows in [('beneficiaries', beneficiaries), ('saccos', saccos),
                        ('loans', loans), ('memberships', memberships),
                        ('groups', groups), ('group_memberships', group_memberships)]:
        if not isinstance(rows, (list, tuple)) or not all(isinstance(x, dict) for x in rows):
            raise MembershipContractError(f'{label}: expected sequence of objects')
    bids = _unique(beneficiaries, 'beneficiary_id', 'beneficiaries')
    sids = _unique(saccos, 'sacco_id', 'saccos')
    _unique(loans, 'loan_id', 'loans')
    for loan in loans:
        if str(loan.get('beneficiary_id') or '') not in bids:
            raise MembershipContractError('loan references unknown beneficiary')
        if str(loan.get('sacco_id') or '') not in sids:
            raise MembershipContractError('loan references unknown SACCO')
    mids = _unique(memberships, 'membership_id', 'memberships')
    gids = _unique(groups, 'group_id', 'groups')
    _unique(group_memberships, 'group_membership_id', 'group_memberships')
    by_member = defaultdict(list)
    for member in memberships:
        b, s = str(member.get('beneficiary_id') or ''), str(member.get('sacco_id') or '')
        if b not in bids or s not in sids:
            raise MembershipContractError('membership references unknown entity')
        if member.get('status') not in ('PENDING', 'ACTIVE', 'SUSPENDED', 'ENDED'):
            raise MembershipContractError('invalid membership status')
        try:
            start = date.fromisoformat(member['effective_from'])
            end = date.fromisoformat(member['effective_to']) if member.get('effective_to') else None
        except (KeyError, TypeError, ValueError) as exc:
            raise MembershipContractError('invalid membership effective date') from exc
        if end is not None and end < start:
            raise MembershipContractError('membership ends before start')
        by_member[(b,s)].append((start,end,member['status']))
    for (b,s), entries in by_member.items():
        active = sorted((start,end) for start,end,status in entries if status == 'ACTIVE')
        for previous,current in zip(active,active[1:]):
            if previous[1] is None or current[0] <= previous[1]:
                raise MembershipContractError('overlapping active memberships')
    group_saccos = {}
    for group in groups:
        sid = str(group.get('sacco_id') or '')
        if sid not in sids:
            raise MembershipContractError('group references unknown SACCO')
        group_saccos[str(group['group_id'])] = sid
    for link in group_memberships:
        gid = str(link.get('group_id') or '')
        bid = str(link.get('beneficiary_id') or '')
        if gid not in gids or bid not in bids:
            raise MembershipContractError('group membership references unknown entity')
        if not any(m.get('beneficiary_id') == bid and m.get('sacco_id') == group_saccos[gid]
                   for m in memberships):
            raise MembershipContractError('group member lacks explicit SACCO membership')
    nin_counts = Counter(str(b['nin']).strip() for b in beneficiaries if b.get('nin'))
    return {
        'beneficiaries': len(beneficiaries), 'saccos': len(saccos), 'loans': len(loans),
        'memberships': len(mids), 'enterprise_groups': len(gids),
        'group_memberships': len(group_memberships),
        'duplicate_nin_groups': sum(n > 1 for n in nin_counts.values()),
        'duplicate_nin_excess': sum(n-1 for n in nin_counts.values() if n > 1),
    }
