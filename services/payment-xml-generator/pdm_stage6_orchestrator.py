"""Stage 6: offline, evidence-only ISO 20022 message orchestration.

No source generators, payment systems, or cloud services are invoked. XML is
returned in memory and only written by an explicitly invoked caller.
"""
from __future__ import annotations
from decimal import Decimal
from lxml import etree
import pdm_funding_iso20022 as funding
import pdm_wendi_disbursement_iso20022 as wendi
from pdm_funding_settlement_contract import reconcile_funding

class OrchestrationError(ValueError):
    pass

ISO_STATUSES = {'ACCP':'ACCEPTED','ACSP':'ACCEPTED','ACSC':'ACCEPTED','PDNG':'PENDING','RJCT':'REJECTED'}

def _rows(value, name):
    if not isinstance(value, list) or not all(isinstance(v,dict) for v in value):
        raise OrchestrationError(f'{name} must be a list of objects')
    return value

def _unique(rows, field):
    seen=set()
    for row in rows:
        val=row.get(field)
        if not isinstance(val,str) or not val.strip() or val in seen:
            raise OrchestrationError(f'missing or duplicate {field}')
        seen.add(val)

def _money(value):
    try:
        n=Decimal(str(value))
        if not n.is_finite() or n<=0 or n!=n.to_integral_value(): raise ValueError()
        return n
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise OrchestrationError('invalid positive integral UGX amount') from exc

def _xml(blob, schema):
    try:
        schema.assertValid(etree.fromstring(blob))
    except Exception as exc:
        raise OrchestrationError('ISO 20022 XSD validation failed') from exc

def build(manifest, xsd001, xsd002):
    """Return {messages, funding_reconciliation, counts} without side effects.

    Requires explicitly evidenced allocation/instruction/loan authorization.
    Does not infer settlement from PAIN.002 or disbursement from funding credit.
    """
    if not isinstance(manifest,dict): raise OrchestrationError('manifest must be an object')
    names=('sacco_ids','funding_allocations','funding_instructions','funding_statuses',
           'funding_credits','loans','disbursement_instructions','disbursement_statuses')
    for name in names: _rows(manifest.get(name),name) if name!='sacco_ids' else None
    sacco_ids=manifest.get('sacco_ids')
    if not isinstance(sacco_ids,list) or not all(isinstance(x,str) and x for x in sacco_ids) or len(set(sacco_ids))!=len(sacco_ids):
        raise OrchestrationError('sacco_ids must be a unique list of identifiers')
    allocations=manifest['funding_allocations']; instructions=manifest['funding_instructions']
    fstatuses=manifest['funding_statuses']; credits=manifest['funding_credits']
    loans=manifest['loans']; disbursements=manifest['disbursement_instructions']; dstatuses=manifest['disbursement_statuses']
    for rows,key in ((allocations,'allocation_id'),(instructions,'instruction_id'),(fstatuses,'status_id'),
                     (credits,'credit_id'),(loans,'loan_id'),(disbursements,'instruction_id'),(dstatuses,'status_id')):
        _unique(rows,key)
    alloc={r['allocation_id']:r for r in allocations}
    loan={r['loan_id']:r for r in loans}
    # Validate all funding settlement evidence and aggregate allocation limits first.
    status_rows=[]
    for s in fstatuses:
        if s.get('status') not in ISO_STATUSES: raise OrchestrationError('invalid funding ISO status')
        status_rows.append(dict(status_id=s['status_id'],instruction_id=s.get('instruction_id'),
                                status=ISO_STATUSES[s['status']],status_date=s.get('status_date')))
    recon=reconcile_funding(sacco_ids,allocations,
        [{k:r.get(k) for k in ('instruction_id','allocation_id','amount_ugx','instruction_date')} for r in instructions],
        status_rows,credits)
    messages={}; used_uetr=set(); used_iid=set()
    def add(kind,key,blob,schema):
        _xml(blob,schema)
        path=f'{kind}/{key}.xml'
        if path in messages: raise OrchestrationError('duplicate output path')
        messages[path]=blob
    for r in instructions:
        aid=r.get('allocation_id'); a=alloc.get(aid)
        if a is None: raise OrchestrationError('unknown funding allocation')
        if r.get('sacco_id')!=a['sacco_id']: raise OrchestrationError('funding SACCO mismatch')
        if r.get('instruction_date') is None: raise OrchestrationError('missing instruction date')
        if r.get('instruction_date')>r.get('execution_date',''): raise OrchestrationError('execution before instruction')
        if r.get('uetr') in used_uetr or r['instruction_id'] in used_iid: raise OrchestrationError('duplicate payment identifier')
        used_uetr.add(r.get('uetr')); used_iid.add(r['instruction_id'])
        add('funding/pain001',r['instruction_id'],funding.pain001(r),xsd001)
    fidx={r['instruction_id']:r for r in instructions}
    for s in fstatuses:
        original=fidx.get(s.get('instruction_id'))
        if original is None: raise OrchestrationError('orphan funding status')
        add('funding/pain002',s['status_id'],funding.pain002(original,s['status_id'],s['status'],s['created_at'],s.get('reason')),xsd002)
    for r in disbursements:
        l=loan.get(r.get('loan_id'))
        if l is None: raise OrchestrationError('unknown disbursement loan')
        if l.get('approval_status')!='APPROVED' or r.get('authorization')!='AUTHORIZED':
            raise OrchestrationError('loan approval and explicit payment authorization required')
        if r.get('beneficiary_id')!=l.get('beneficiary_id') or r.get('sacco_id')!=l.get('sacco_id'):
            raise OrchestrationError('disbursement loan relationship mismatch')
        if r['sacco_id'] not in sacco_ids: raise OrchestrationError('unknown disbursement SACCO')
        if _money(r.get('amount_ugx'))>_money(l.get('amount_approved_ugx')):
            raise OrchestrationError('disbursement exceeds approved loan')
        if r.get('uetr') in used_uetr or r['instruction_id'] in used_iid: raise OrchestrationError('duplicate payment identifier')
        used_uetr.add(r.get('uetr')); used_iid.add(r['instruction_id'])
        add('wendi/pain001',r['instruction_id'],wendi.pain001(r),xsd001)
    didx={r['instruction_id']:r for r in disbursements}
    for s in dstatuses:
        original=didx.get(s.get('instruction_id'))
        if original is None: raise OrchestrationError('orphan Wendi status')
        if s.get('status') not in ISO_STATUSES: raise OrchestrationError('invalid Wendi ISO status')
        add('wendi/pain002',s['status_id'],wendi.pain002(original,s['status_id'],s['status'],s['created_at'],s.get('reason')),xsd002)
    return {'messages':messages,'funding_reconciliation':recon,
            'counts':{'funding_instructions':len(instructions),'funding_statuses':len(fstatuses),
                      'wendi_instructions':len(disbursements),'wendi_statuses':len(dstatuses)}}
