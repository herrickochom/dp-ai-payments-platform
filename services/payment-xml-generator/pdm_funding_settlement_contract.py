"""Stage 3: independent funding payment, payment-status and bank-credit evidence.

Pure validation and reconciliation. No transactions or bank confirmations are inferred
from PDMIS loans, allocation statuses or SACCO aggregate fields.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation

class SettlementContractError(ValueError):
    pass

def _id(value, label):
    if not isinstance(value, str) or not value.strip():
        raise SettlementContractError(f'missing {label}')
    return value.strip()

def _amount(value, label):
    if isinstance(value, bool):
        raise SettlementContractError(f'invalid {label}')
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise SettlementContractError(f'invalid {label}') from None
    if not number.is_finite() or number <= 0 or number != number.to_integral_value():
        raise SettlementContractError(f'{label} must be positive integral UGX')
    return number

def _date(value, label):
    try:
        if not isinstance(value, str):
            raise ValueError()
        return date.fromisoformat(value)
    except ValueError:
        raise SettlementContractError(f'invalid {label}') from None

def _rows(rows, label):
    if not isinstance(rows, (list, tuple)) or not all(isinstance(x, dict) for x in rows):
        raise SettlementContractError(f'{label} must be a sequence of objects')

def reconcile_funding(sacco_ids, allocations=(), instructions=(), statuses=(), credits=()):
    """Validate funding evidence and return aggregate reconciliation per allocation.

    allocations: allocation_id,sacco_id,amount_ugx,allocation_date,funding_status
    instructions: instruction_id,allocation_id,amount_ugx,instruction_date
    statuses: status_id,instruction_id,status (ACCEPTED|REJECTED|PENDING),status_date
    credits: credit_id,instruction_id,sacco_id,amount_ugx,credit_date

    A status is a message outcome, not bank settlement. Credits are independently
    evidenced and deduplicated by credit_id. No implicit credits are created.
    """
    for name, rows in [('allocations',allocations),('instructions',instructions),('statuses',statuses),('credits',credits)]:
        _rows(rows,name)
    sacco_ids={str(s) for s in sacco_ids}
    allocs={}
    for row in allocations:
        aid=_id(row.get('allocation_id'),'allocation_id')
        if aid in allocs: raise SettlementContractError('duplicate allocation_id')
        sid=_id(row.get('sacco_id'),'sacco_id')
        if sid not in sacco_ids: raise SettlementContractError('allocation references unknown SACCO')
        if row.get('funding_status') not in {'ALLOCATED','INSTRUCTED','ACCEPTED','REJECTED','CREDITED'}:
            raise SettlementContractError('invalid funding_status')
        allocs[aid]=(sid,_amount(row.get('amount_ugx'),'allocation amount'),_date(row.get('allocation_date'),'allocation date'))
    inst={}; instructed=defaultdict(lambda:Decimal(0))
    for row in instructions:
        iid=_id(row.get('instruction_id'),'instruction_id')
        if iid in inst: raise SettlementContractError('duplicate instruction_id')
        aid=_id(row.get('allocation_id'),'allocation_id')
        if aid not in allocs: raise SettlementContractError('instruction references unknown allocation')
        amt=_amount(row.get('amount_ugx'),'instruction amount')
        when=_date(row.get('instruction_date'),'instruction date')
        if when < allocs[aid][2]: raise SettlementContractError('instruction precedes allocation')
        instructed[aid]+=amt
        if instructed[aid]>allocs[aid][1]: raise SettlementContractError('instructions exceed allocation')
        inst[iid]=(aid,amt,when)
    outcomes={}; seen_status=set()
    for row in statuses:
        sid=_id(row.get('status_id'),'status_id')
        if sid in seen_status: raise SettlementContractError('duplicate status_id')
        seen_status.add(sid)
        iid=_id(row.get('instruction_id'),'instruction_id')
        if iid not in inst: raise SettlementContractError('status references unknown instruction')
        outcome=row.get('status')
        if outcome not in {'PENDING','ACCEPTED','REJECTED'}: raise SettlementContractError('invalid payment status')
        if _date(row.get('status_date'),'status date')<inst[iid][2]: raise SettlementContractError('status precedes instruction')
        if iid in outcomes and outcomes[iid]!=outcome and (outcome!='PENDING' and outcomes[iid]!='PENDING'):
            raise SettlementContractError('conflicting terminal payment statuses')
        if iid not in outcomes or outcome!='PENDING': outcomes[iid]=outcome
    seen_credit=set(); credited_by_instruction=defaultdict(lambda:Decimal(0)); credited=defaultdict(lambda:Decimal(0))
    for row in credits:
        cid=_id(row.get('credit_id'),'credit_id')
        if cid in seen_credit: raise SettlementContractError('duplicate credit_id')
        seen_credit.add(cid)
        iid=_id(row.get('instruction_id'),'instruction_id')
        if iid not in inst: raise SettlementContractError('credit references unknown instruction')
        aid,amount,instruction_date=inst[iid]
        sid=_id(row.get('sacco_id'),'sacco_id')
        if sid!=allocs[aid][0]: raise SettlementContractError('credit SACCO mismatch')
        if outcomes.get(iid)=='REJECTED': raise SettlementContractError('rejected instruction cannot have credit')
        if _date(row.get('credit_date'),'credit date')<instruction_date: raise SettlementContractError('credit precedes instruction')
        value=_amount(row.get('amount_ugx'),'credit amount')
        credited_by_instruction[iid]+=value
        if credited_by_instruction[iid]>amount: raise SettlementContractError('credits exceed instruction amount')
        credited[aid]+=value
    result={}
    for aid,(sid,allocated,_) in allocs.items():
        result[aid]={'sacco_id':sid,'allocated_ugx':int(allocated),
                     'instructed_ugx':int(instructed[aid]),
                     'credited_ugx':int(credited[aid]),
                     'uncredited_allocation_ugx':int(allocated-credited[aid])}
    return result
