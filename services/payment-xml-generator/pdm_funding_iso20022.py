"""Explicit-evidence SACCO funding PAIN.001/PAIN.002 generators.

No loan-derived funding. All party/account fields supplied by caller from
approved institutional interface configuration; no institution roles inferred.
"""
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from uuid import UUID
from xml.etree import ElementTree as ET

NS1='urn:iso:std:iso:20022:tech:xsd:pain.001.001.09'
NS2='urn:iso:std:iso:20022:tech:xsd:pain.002.001.12'
class FundingMessageError(ValueError): pass

def require(x, key):
    value=x.get(key)
    if not isinstance(value,str) or not value.strip(): raise FundingMessageError(f'missing {key}')
    return value.strip()

def check(data):
    if not isinstance(data,dict): raise FundingMessageError('expected mapping')
    keys=('allocation_id','instruction_id','sacco_id','amount_ugx','instruction_date','created_at','execution_date','initiating_party','debtor_name','debtor_account','debtor_agent_bic','creditor_name','creditor_account','creditor_agent_bic','uetr')
    for k in keys: require(data,k)
    try:
        amount=Decimal(data['amount_ugx'])
        if not amount.is_finite() or amount<=0 or amount!=amount.to_integral_value(): raise ValueError()
        date.fromisoformat(data['instruction_date']); date.fromisoformat(data['execution_date'])
        datetime.fromisoformat(data['created_at'].replace('Z','+00:00'))
        UUID(data['uetr'])
    except (ValueError,InvalidOperation,TypeError) as exc: raise FundingMessageError('invalid amount/date/uetr') from exc
    if len(data['debtor_agent_bic']) not in (8,11) or len(data['creditor_agent_bic']) not in (8,11): raise FundingMessageError('invalid BIC length')
    if len(data['instruction_id'])>35 or len(data['allocation_id'])>35: raise FundingMessageError('identifier exceeds ISO field limit')
    return format(amount,'f')

def el(parent,name,text=None,**attrs):
    node=ET.SubElement(parent,name,attrs)
    if text is not None: node.text=str(text)
    return node

def account(parent,number):
    el(el(el(parent,'Id'),'Othr'),'Id',number)

def pain001(data):
    amount=check(data)
    root=ET.Element('Document',xmlns=NS1); init=el(root,'CstmrCdtTrfInitn')
    h=el(init,'GrpHdr'); el(h,'MsgId','FUND-'+data['instruction_id']); el(h,'CreDtTm',data['created_at']); el(h,'NbOfTxs','1'); el(h,'CtrlSum',amount); el(el(h,'InitgPty'),'Nm',data['initiating_party'])
    p=el(init,'PmtInf'); el(p,'PmtInfId','FUND-'+data['instruction_id']); el(p,'PmtMtd','TRF'); el(p,'NbOfTxs','1'); el(p,'CtrlSum',amount); el(el(p,'ReqdExctnDt'),'Dt',data['execution_date'])
    el(el(p,'Dbtr'),'Nm',data['debtor_name']); account(el(p,'DbtrAcct'),data['debtor_account']); el(el(el(p,'DbtrAgt'),'FinInstnId'),'BICFI',data['debtor_agent_bic'])
    tx=el(p,'CdtTrfTxInf'); pid=el(tx,'PmtId'); el(pid,'InstrId',data['instruction_id']); el(pid,'EndToEndId',data['allocation_id']); el(pid,'UETR',data['uetr']); el(el(tx,'Amt'),'InstdAmt',amount,Ccy='UGX')
    el(el(el(tx,'CdtrAgt'),'FinInstnId'),'BICFI',data['creditor_agent_bic']); el(el(tx,'Cdtr'),'Nm',data['creditor_name']); account(el(tx,'CdtrAcct'),data['creditor_account']); el(el(tx,'RmtInf'),'Ustrd','PDM SACCO funding '+data['allocation_id'])
    return ET.tostring(root,encoding='utf-8',xml_declaration=True)

def pain002(data,status_id,status,created_at,reason=None):
    check(data)
    if status not in ('ACCP','ACSP','ACSC','RJCT','PDNG'): raise FundingMessageError('unsupported ISO status')
    if status=='RJCT' and not reason: raise FundingMessageError('rejection reason required')
    if not isinstance(status_id,str) or not status_id.strip(): raise FundingMessageError('status_id required')
    datetime.fromisoformat(created_at.replace('Z','+00:00'))
    root=ET.Element('Document',xmlns=NS2); rpt=el(root,'CstmrPmtStsRpt'); h=el(rpt,'GrpHdr'); el(h,'MsgId',status_id); el(h,'CreDtTm',created_at)
    group=el(rpt,'OrgnlGrpInfAndSts'); el(group,'OrgnlMsgId','FUND-'+data['instruction_id']); el(group,'OrgnlMsgNmId','pain.001.001.09'); el(group,'OrgnlNbOfTxs','1'); el(group,'OrgnlCtrlSum',check(data))
    p=el(rpt,'OrgnlPmtInfAndSts'); el(p,'OrgnlPmtInfId','FUND-'+data['instruction_id']); tx=el(p,'TxInfAndSts'); el(tx,'OrgnlInstrId',data['instruction_id']); el(tx,'OrgnlEndToEndId',data['allocation_id']); el(tx,'OrgnlUETR',data['uetr']); el(tx,'TxSts',status)
    if reason:
        r=el(tx,'StsRsnInf'); el(el(r,'Rsn'),'Cd',reason)
    return ET.tostring(root,encoding='utf-8',xml_declaration=True)
