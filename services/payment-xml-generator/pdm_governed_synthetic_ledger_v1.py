"""Governed synthetic Wendi ledger V1. NOT a bank ledger."""
from datetime import datetime,timedelta
from decimal import Decimal
from hashlib import sha256
from pdm_account_posting_contract import validate_postings_v1,reconcile_statement_v1
CODE=('PMNT','RCDT','OTHR')
CODES=frozenset({CODE}) # internal governed synthetic test classification only

def uid(prefix,*values):
    if not isinstance(prefix,str) or not prefix or not prefix.isascii():
        raise ValueError('invalid synthetic identifier prefix')
    capacity=35-len(prefix)-1
    if capacity<16:
        raise ValueError('synthetic identifier prefix leaves insufficient hash capacity')
    identifier=prefix+'-'+sha256('|'.join(map(str,values)).encode()).hexdigest()[:min(24,capacity)].upper()
    if len(identifier)>35:
        raise ValueError('synthetic identifier exceeds ISO Max35Text')
    return identifier
def dt(value):
    t=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if t.tzinfo is None or t.utcoffset() is None: raise ValueError('timezone required')
    return t
def positive(value):
    d=Decimal(str(value))
    if not d.is_finite() or d<=0 or d!=d.to_integral_value(): raise ValueError('positive integral UGX required')
    return d

def build_ledger(contexts,*,wallet_account,event_timestamp,settled_status):
    owners={}; rows=[]; journals=[]; payments=[]; funding={}; loans=set(); txids=set()
    def register(account,owner):
        if not isinstance(account,str) or not account.strip() or not isinstance(owner,str) or not owner.strip(): raise ValueError('missing account ownership')
        if account in owners and owners[account]!=owner: raise ValueError('account owner conflict')
        owners[account]=owner
        return account
    issuance=register('SYN-ISSUANCE-UGX','SYN-PLATFORM-TREASURY')
    for c in contexts:
        if c['status']!=settled_status: continue
        loan=c['loan']; sacco=c['sacco']; ben=c['beneficiary']; lc=c['lifecycle']
        loan_id=str(loan['loan_id']); txid=str(lc['wendi_tx_id']); amount=positive(c['amount'])
        if not loan_id.strip() or not txid.strip() or loan_id in loans or txid in txids: raise ValueError('duplicate/blank loan or transaction')
        loans.add(loan_id); txids.add(txid)
        src=register(sacco['wendi_account'],sacco['sacco_id']); dst=register(wallet_account(loan,ben),ben['beneficiary_id'])
        if len({src,dst,issuance})!=3: raise ValueError('invalid accounts')
        booked=dt(event_timestamp(loan,'disbursement'))
        payments.append((loan_id,txid,src,dst,amount,booked))
        funding[src]=funding.get(src,Decimal(0))+amount
    def post(jid,acct,txid,event,direction,amount,when,loan_id):
        row={'posting_id':uid('SYN-PST',jid,acct,direction),'account_id':acct,'account_owner_id':owners[acct],
             'payment_transaction_id':txid,'event_type':event,'amount_ugx':str(amount),
             'credit_debit_indicator':direction,'booking_datetime':when.isoformat(),
             'value_date':when.date().isoformat(),'account_servicer_reference':uid('SYN-ASR',jid,acct),
             'bank_transaction_code':dict(zip(('domain','family','subfamily'),CODE)),
             'loan_id':loan_id,'journal_id':jid,'synthetic':True,
             'provenance':{'source':'governed_synthetic_ledger_v1','loan_id':loan_id}}
        rows.append(row); return row
    def journal(jid,kind,items):
        if len(items)!=2 or sum((Decimal(p['amount_ugx'])*(1 if p['credit_debit_indicator']=='CRDT' else -1) for p in items),Decimal(0))!=0: raise ValueError('unbalanced journal')
        journals.append({'journal_id':jid,'kind':kind,'posting_ids':[p['posting_id'] for p in items]})
    for src in sorted(funding):
        when=min(t for _,_,s,_,_,t in payments if s==src)-timedelta(seconds=1)
        jid=uid('SYN-FUND-JRN',src,str(funding[src]),when.isoformat())
        journal(jid,'SYNTHETIC_FUNDING',[
            post(jid,issuance,jid,'FUNDING_CREDIT','DBIT',funding[src],when,'SYN-FUNDING'),
            post(jid,src,jid,'FUNDING_CREDIT','CRDT',funding[src],when,'SYN-FUNDING')])
    for loan_id,txid,src,dst,amount,when in sorted(payments):
        jid=uid('SYN-PAY-JRN',loan_id,txid)
        journal(jid,'SYNTHETIC_PAYMENT',[
            post(jid,src,txid,'DISBURSEMENT','DBIT',amount,when,loan_id),
            post(jid,dst,txid,'WALLET_CREDIT','CRDT',amount,when,loan_id)])
    ids=frozenset(p['posting_id'] for p in rows)
    if len(ids)!=len(rows): raise ValueError('duplicate posting identity')
    journal_ids=[j['journal_id'] for j in journals]
    if len(set(journal_ids))!=len(journal_ids): raise ValueError('duplicate journal identity')
    references=[p['account_servicer_reference'] for p in rows]
    if len(set(references))!=len(references): raise ValueError('duplicate account servicer reference')
    for p in rows:
        for field in ('posting_id','account_servicer_reference','payment_transaction_id'):
            if len(p[field])>35: raise ValueError(f'{field} exceeds ISO Max35Text')
    validate_postings_v1(rows,governed_bank_transaction_codes=CODES,account_ownership=owners,authoritative_posting_ids=ids)
    return {'postings':rows,'journals':journals,'account_ownership':owners,
            'authoritative_posting_ids':ids,'governed_bank_transaction_codes':CODES,'synthetic':True}

def statement(ledger,account_id,start,end):
    a=dt(start); b=dt(end)
    if b<=a: raise ValueError('non-increasing statement period')
    all_rows=[p for p in ledger['postings'] if p['account_id']==account_id]
    previous=[p for p in all_rows if dt(p['booking_datetime'])<a]
    current=[p for p in all_rows if a<=dt(p['booking_datetime'])<b]
    def net(items): return sum((Decimal(p['amount_ugx'])*(1 if p['credit_debit_indicator']=='CRDT' else -1) for p in items),Decimal(0))
    opening=net(previous); closing=opening+net(current)
    reconciliation=reconcile_statement_v1(account_id=account_id,account_owner_id=ledger['account_ownership'][account_id],
        period_start=a.isoformat(),period_end=b.isoformat(),opening_balance_ugx=str(opening),closing_balance_ugx=str(closing),
        postings=current,governed_bank_transaction_codes=ledger['governed_bank_transaction_codes'],
        account_ownership=ledger['account_ownership'],authoritative_posting_ids=ledger['authoritative_posting_ids'])
    return reconciliation,current
