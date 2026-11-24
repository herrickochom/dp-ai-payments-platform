"""Explicit-evidence Wendi beneficiary disbursement PAIN.001/PAIN.002.

Not a settlement generator. The supplied party and account details must come from
approved interface configuration; no institutional roles are inferred from loans.
"""
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from uuid import UUID
from xml.etree import ElementTree as ET

NS1 = 'urn:iso:std:iso:20022:tech:xsd:pain.001.001.09'
NS2 = 'urn:iso:std:iso:20022:tech:xsd:pain.002.001.12'

class DisbursementContractError(ValueError):
    pass

def required(d, key):
    v = d.get(key)
    if not isinstance(v, str) or not v.strip():
        raise DisbursementContractError(f'missing {key}')
    return v.strip()

def validate(d):
    if not isinstance(d, dict):
        raise DisbursementContractError('expected mapping')
    fields = ('loan_id','beneficiary_id','sacco_id','instruction_id',
              'amount_ugx','created_at','execution_date','initiating_party',
              'debtor_name','debtor_account','debtor_agent_bic',
              'creditor_name','creditor_account','creditor_agent_bic','uetr')
    for f in fields: required(d, f)
    try:
        amt = Decimal(d['amount_ugx'])
        if not amt.is_finite() or amt <= 0 or amt != amt.to_integral_value():
            raise ValueError()
        date.fromisoformat(d['execution_date'])
        datetime.fromisoformat(d['created_at'].replace('Z','+00:00'))
        UUID(d['uetr'])
    except (InvalidOperation, ValueError, TypeError) as e:
        raise DisbursementContractError('invalid amount/date/UETR') from e
    for f in ('debtor_agent_bic','creditor_agent_bic'):
        if len(d[f]) not in (8,11):
            raise DisbursementContractError('invalid BIC length')
    for f in ('instruction_id','loan_id'):
        if len(d[f]) > 35: raise DisbursementContractError('ISO identifier too long')
    return format(amt,'f')

def el(parent, name, text=None, **attrs):
    x = ET.SubElement(parent,name,attrs)
    if text is not None: x.text = str(text)
    return x

def acct(parent, number):
    el(el(el(parent,'Id'),'Othr'),'Id',number)

def pain001(d):
    amount = validate(d)
    root=ET.Element('Document',xmlns=NS1)
    init=el(root,'CstmrCdtTrfInitn')
    h=el(init,'GrpHdr')
    el(h,'MsgId','WENDI-'+d['instruction_id']);el(h,'CreDtTm',d['created_at'])
    el(h,'NbOfTxs','1');el(h,'CtrlSum',amount)
    el(el(h,'InitgPty'),'Nm',d['initiating_party'])
    p=el(init,'PmtInf')
    el(p,'PmtInfId','WENDI-'+d['instruction_id']);el(p,'PmtMtd','TRF')
    el(p,'NbOfTxs','1');el(p,'CtrlSum',amount)
    el(el(p,'ReqdExctnDt'),'Dt',d['execution_date'])
    el(el(p,'Dbtr'),'Nm',d['debtor_name']);acct(el(p,'DbtrAcct'),d['debtor_account'])
    el(el(el(p,'DbtrAgt'),'FinInstnId'),'BICFI',d['debtor_agent_bic'])
    t=el(p,'CdtTrfTxInf'); ids=el(t,'PmtId')
    el(ids,'InstrId',d['instruction_id']);el(ids,'EndToEndId',d['loan_id'])
    el(ids,'UETR',d['uetr'])
    el(el(t,'Amt'),'InstdAmt',amount,Ccy='UGX')
    el(el(el(t,'CdtrAgt'),'FinInstnId'),'BICFI',d['creditor_agent_bic'])
    el(el(t,'Cdtr'),'Nm',d['creditor_name']);acct(el(t,'CdtrAcct'),d['creditor_account'])
    el(el(t,'RmtInf'),'Ustrd','PDM loan '+d['loan_id'])
    return ET.tostring(root,encoding='utf-8',xml_declaration=True)

def pain002(d,status_id,status,created_at,reason=None):
    amount=validate(d)
    if status not in ('ACCP','ACSP','ACSC','RJCT','PDNG'):
        raise DisbursementContractError('unsupported status')
    if status=='RJCT' and not reason:
        raise DisbursementContractError('rejection reason required')
    if not isinstance(status_id,str) or not status_id.strip() or len(status_id)>35:
        raise DisbursementContractError('invalid status ID')
    try: datetime.fromisoformat(created_at.replace('Z','+00:00'))
    except (ValueError,AttributeError) as e: raise DisbursementContractError('invalid timestamp') from e
    root=ET.Element('Document',xmlns=NS2)
    r=el(root,'CstmrPmtStsRpt');h=el(r,'GrpHdr')
    el(h,'MsgId',status_id);el(h,'CreDtTm',created_at)
    g=el(r,'OrgnlGrpInfAndSts');el(g,'OrgnlMsgId','WENDI-'+d['instruction_id'])
    el(g,'OrgnlMsgNmId','pain.001.001.09');el(g,'OrgnlNbOfTxs','1')
    el(g,'OrgnlCtrlSum',amount)
    p=el(r,'OrgnlPmtInfAndSts');el(p,'OrgnlPmtInfId','WENDI-'+d['instruction_id'])
    t=el(p,'TxInfAndSts');el(t,'OrgnlInstrId',d['instruction_id'])
    el(t,'OrgnlEndToEndId',d['loan_id']);el(t,'OrgnlUETR',d['uetr'])
    el(t,'TxSts',status)
    if reason: el(el(el(t,'StsRsnInf'),'Rsn'),'Cd',reason)
    return ET.tostring(root,encoding='utf-8',xml_declaration=True)
