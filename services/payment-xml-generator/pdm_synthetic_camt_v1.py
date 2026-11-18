"""Schema-checked synthetic CAMT documents derived only from reconciled postings."""
from datetime import timedelta
from decimal import Decimal
from xml.etree import ElementTree as ET
from pdm_governed_synthetic_ledger_v1 import statement,dt,uid

def tag(parent,name,value=None,**attrs):
    node=ET.SubElement(parent,name,attrs)
    if value is not None: node.text=str(value)
    return node

def money(value): return f'{abs(Decimal(str(value))):.2f}'
def balance(parent,code,value,date):
    node=tag(parent,'Bal'); tag(tag(tag(node,'Tp'),'CdOrPrtry'),'Cd',code)
    tag(node,'Amt',money(value),Ccy='UGX'); tag(node,'CdtDbtInd','CRDT' if value>=0 else 'DBIT')
    tag(tag(node,'Dt'),'Dt',date)

def account(parent,account_id):
    acc=tag(parent,'Acct'); tag(tag(tag(acc,'Id'),'Othr'),'Id',account_id)
    tag(acc,'Ccy','UGX')

def entry(parent,p,loan_id):
    e=tag(parent,'Ntry'); tag(e,'NtryRef',p['posting_id']); tag(e,'Amt',money(p['amount_ugx']),Ccy='UGX')
    tag(e,'CdtDbtInd',p['credit_debit_indicator']); tag(tag(e,'Sts'),'Cd','BOOK')
    tag(tag(e,'BookgDt'),'Dt',p['value_date']); tag(tag(e,'ValDt'),'Dt',p['value_date'])
    tag(e,'AcctSvcrRef',p['account_servicer_reference'])
    code=p['bank_transaction_code']; b=tag(e,'BkTxCd'); d=tag(b,'Domn'); tag(d,'Cd',code['domain']); f=tag(d,'Fmly'); tag(f,'Cd',code['family']); tag(f,'SubFmlyCd',code['subfamily'])
    t=tag(tag(e,'NtryDtls'),'TxDtls'); r=tag(t,'Refs')
    tag(r,'EndToEndId',loan_id); tag(r,'TxId',p['payment_transaction_id'])
    a=tag(t,'AmtDtls'); tag(tag(a,'TxAmt'),'Amt',money(p['amount_ugx']),Ccy='UGX')
    return e

def build(context,ledger,*,kind,namespace,wallet_account,event_timestamp):
    loan=context['loan']; sacco=context['sacco']; ben=context['beneficiary']; lc=context['lifecycle']; loan_id=loan['loan_id']
    src=sacco['wendi_account']; dst=wallet_account(loan,ben)
    booked=dt(event_timestamp(loan,'disbursement')); start=booked-timedelta(seconds=2); end=booked+timedelta(seconds=1)
    account_id=src if kind=='053' else dst
    reconciled,rows=statement(ledger,account_id,start.isoformat(),end.isoformat())
    expected=[p for p in rows if p.get('loan_id')==loan_id and p['event_type']==('DISBURSEMENT' if kind=='053' else 'WALLET_CREDIT')]
    if len(expected)!=1: raise ValueError('expected exactly one loan posting')
    if kind=='053':
        root=ET.Element('Document',xmlns=namespace); msg=tag(root,'BkToCstmrStmt')
        h=tag(msg,'GrpHdr'); tag(h,'MsgId',lc['wendi_statement_message_id']); tag(h,'CreDtTm',booked.isoformat())
        container=tag(msg,'Stmt'); tag(container,'Id',uid('SYN-STMT',loan_id,account_id)); tag(container,'CreDtTm',booked.isoformat())
        fr=tag(container,'FrToDt'); tag(fr,'FrDtTm',start.isoformat()); tag(fr,'ToDtTm',end.isoformat())
        account(container,account_id)
        balance(container,'OPBD',reconciled['opening_balance_ugx'],start.date().isoformat())
        balance(container,'CLBD',reconciled['closing_balance_ugx'],end.date().isoformat())
        for p in rows: entry(container,p,loan_id if p.get('loan_id')==loan_id else p['loan_id'])
    elif kind=='054':
        root=ET.Element('Document',xmlns=namespace); msg=tag(root,'BkToCstmrDbtCdtNtfctn')
        h=tag(msg,'GrpHdr'); tag(h,'MsgId',lc['wendi_notification_message_id']); tag(h,'CreDtTm',booked.isoformat())
        container=tag(msg,'Ntfctn'); tag(container,'Id',uid('SYN-NTF',loan_id,account_id)); tag(container,'CreDtTm',booked.isoformat())
        account(container,account_id)
        entry(container,expected[0],loan_id)
    else: raise ValueError('unsupported CAMT kind')
    return ET.tostring(root,encoding='utf-8',xml_declaration=True)
