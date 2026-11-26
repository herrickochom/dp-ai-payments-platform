{{ config(materialized='pdm_iceberg_guarded') }}

with validation_results as (
    select
        'PAYMENT_TRANSACTION' as entity_type,
        source_system || ':' || transaction_id as entity_id,
        rule.rule_code,
        rule.rule_passed,
        rule.rule_detail
    from {{ ref('slv_pdm_payments_transactions') }} payment
    cross join unnest([struct('AMOUNT_POSITIVE' as rule_code, amount is not null and amount > 0 as rule_passed, 'Payment amount must be greater than zero' as rule_detail),
struct('CURRENCY_ISO_LENGTH' as rule_code, currency is not null and length(currency) = 3 as rule_passed, 'Currency must contain a three-character ISO code' as rule_detail)]) as rule

    union all

    select
        'PAYMENT_ENTITY_MATCH',
        source_system || ':' || transaction_id,
        'CROSS_DOMAIN_ENTITY_MATCHED',
        entity_match_status in ('MATCHED', 'NOT_APPLICABLE'),
        'Payment-to-loan, beneficiary, SACCO, business-plan and agent links must resolve'
    from {{ ref('slv_pdm_payment_entity_matches') }}

    union all

    select
        'PAYMENT_RECONCILIATION',
        status_source || ':' || status_message_id || ':'
            || coalesce(transaction_source, 'NO_TRANSACTION') || ':'
            || coalesce(transaction_id, 'NO_TRANSACTION'),
        'PAYMENT_STATUS_RECONCILED',
        reconciliation_status = 'MATCHED',
        'Payment status must match an originating message or transaction'
    from {{ ref('slv_pdm_payments_reconciliation') }}

    union all

    select
        'LOAN',
        loan_id,
        rule.rule_code,
        rule.rule_passed,
        rule.rule_detail
    from {{ ref('slv_pdm_loans') }} loan
    cross join unnest([struct('REQUESTED_AMOUNT_POSITIVE' as rule_code, amount_requested is not null and amount_requested > 0 as rule_passed, 'Requested amount must be greater than zero' as rule_detail),
struct('APPROVED_NOT_ABOVE_REQUESTED' as rule_code, amount_approved is null or
                (amount_approved >= 0 and amount_approved <= amount_requested) as rule_passed, 'Approved amount must be non-negative and not exceed requested amount' as rule_detail),
struct('DISBURSED_NOT_ABOVE_APPROVED' as rule_code, amount_disbursed is null or
                (amount_disbursed >= 0 and amount_disbursed <= amount_approved) as rule_passed, 'Disbursed amount must be non-negative and not exceed approved amount' as rule_detail),
struct('REPAID_NOT_ABOVE_DUE' as rule_code, amount_repaid is null or
                (amount_repaid >= 0 and amount_repaid <= coalesce(amount_disbursed, 0) + coalesce(interest_charged, 0)) as rule_passed, 'Repaid amount must be non-negative and not exceed principal plus charged interest' as rule_detail),
struct('REPAID_COMPONENTS_RECONCILE' as rule_code, abs(coalesce(amount_repaid, 0) - coalesce(principal_repaid, 0) - coalesce(interest_paid, 0)) <= 0.05 as rule_passed, 'Amount repaid must equal principal repaid plus interest paid within UGX 0.05' as rule_detail),
struct('OUTSTANDING_COMPONENTS_RECONCILE' as rule_code, abs(coalesce(outstanding_balance, 0) - coalesce(principal_outstanding_balance, 0)
                    - coalesce(interest_outstanding_balance, 0)) <= 0.05 as rule_passed, 'Outstanding balance must equal principal plus interest outstanding within UGX 0.05' as rule_detail),
struct('CONTRACTUAL_BALANCE_RECONCILES' as rule_code, abs(coalesce(amount_disbursed, 0) + coalesce(interest_charged, 0)
                    - coalesce(amount_repaid, 0) - coalesce(outstanding_balance, 0)) <= 0.05 as rule_passed, 'Disbursed principal plus charged interest must reconcile to repaid plus outstanding within UGX 0.05' as rule_detail),
struct('APPROVAL_DATE_ORDER' as rule_code, approval_date is null or application_date is null
                or approval_date >= application_date as rule_passed, 'Approval date cannot precede application date' as rule_detail),
struct('LIFECYCLE_DATE_ORDER' as rule_code, loan_status <> 'DISBURSED' or
                (approval_date <= verification_date and verification_date <= disbursement_date
                 and disbursement_date <= cashout_date and cashout_date <= as_of_date) as rule_passed, 'Disbursed loan lifecycle dates must be ordered through the observation date' as rule_detail)]) as rule

    union all

    select
        'LOAN',
        loan.loan_id,
        rule.rule_code,
        rule.rule_passed,
        rule.rule_detail
    from {{ ref('slv_pdm_loans') }} loan
    left join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
      on loan.loan_id = loan_link.loan_id
    left join {{ ref('slv_pdm_beneficiaries') }} beneficiary
      on loan_link.beneficiary_token = beneficiary.beneficiary_token
    left join {{ ref('slv_pdm_saccos') }} sacco
      on loan.sacco_id = sacco.sacco_id
    left join {{ ref('slv_pdm_business_plans') }} business_plan
      on loan.business_plan_id = business_plan.business_plan_id
    cross join unnest([struct('BENEFICIARY_EXISTS' as rule_code, loan_link.beneficiary_token is not null
                and beneficiary.beneficiary_token is not null as rule_passed, 'Loan beneficiary must resolve through the canonical beneficiary token' as rule_detail),
struct('SACCO_EXISTS' as rule_code, loan.sacco_id is null or sacco.sacco_id is not null as rule_passed, 'Loan SACCO must resolve when supplied' as rule_detail),
struct('BUSINESS_PLAN_EXISTS' as rule_code, loan.business_plan_id is null or business_plan.business_plan_id is not null as rule_passed, 'Loan business plan must resolve when supplied' as rule_detail)]) as rule
)
select
    entity_type,
    entity_id,
    rule_code,
    rule_passed,
    case when rule_passed then 'PASS' else 'FAIL' end as rule_status,
    rule_detail
from validation_results
