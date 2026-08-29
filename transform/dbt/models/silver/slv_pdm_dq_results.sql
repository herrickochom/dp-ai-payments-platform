{{ config(materialized='iceberg_table') }}

with validation_results as (
    select
        'PAYMENT_TRANSACTION' as entity_type,
        source_system || ':' || transaction_id as entity_id,
        rule.rule_code,
        rule.rule_passed,
        rule.rule_detail
    from {{ ref('slv_pdm_payments_transactions') }} payment
    cross join lateral (
        values
            ('AMOUNT_POSITIVE', amount is not null and amount > 0,
             'Payment amount must be greater than zero'),
            ('CURRENCY_ISO_LENGTH', currency is not null and length(currency) = 3,
             'Currency must contain a three-character ISO code')
    ) as rule(rule_code, rule_passed, rule_detail)

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
    cross join lateral (
        values
            ('REQUESTED_AMOUNT_POSITIVE', amount_requested is not null and amount_requested > 0,
             'Requested amount must be greater than zero'),
            ('APPROVED_NOT_ABOVE_REQUESTED', amount_approved is null or
                (amount_approved >= 0 and amount_approved <= amount_requested),
             'Approved amount must be non-negative and not exceed requested amount'),
            ('DISBURSED_NOT_ABOVE_APPROVED', amount_disbursed is null or
                (amount_disbursed >= 0 and amount_disbursed <= amount_approved),
             'Disbursed amount must be non-negative and not exceed approved amount'),
            ('REPAID_NOT_ABOVE_DUE', amount_repaid is null or
                (amount_repaid >= 0 and amount_repaid <= coalesce(amount_disbursed, 0) + coalesce(interest_charged, 0)),
             'Repaid amount must be non-negative and not exceed principal plus charged interest'),
            ('APPROVAL_DATE_ORDER', approval_date is null or application_date is null
                or approval_date >= application_date,
             'Approval date cannot precede application date')
    ) as rule(rule_code, rule_passed, rule_detail)

    union all

    select
        'LOAN',
        loan.loan_id,
        rule.rule_code,
        rule.rule_passed,
        rule.rule_detail
    from {{ ref('slv_pdm_loans') }} loan
    left join {{ ref('slv_pdm_beneficiaries') }} beneficiary
      on loan.beneficiary_id = beneficiary.beneficiary_id
    left join {{ ref('slv_pdm_saccos') }} sacco
      on loan.sacco_id = sacco.sacco_id
    left join {{ ref('slv_pdm_business_plans') }} business_plan
      on loan.business_plan_id = business_plan.business_plan_id
    cross join lateral (
        values
            ('BENEFICIARY_EXISTS', loan.beneficiary_id is not null and beneficiary.beneficiary_id is not null,
             'Loan beneficiary must resolve to the canonical beneficiary'),
            ('SACCO_EXISTS', loan.sacco_id is null or sacco.sacco_id is not null,
             'Loan SACCO must resolve when supplied'),
            ('BUSINESS_PLAN_EXISTS', loan.business_plan_id is null or business_plan.business_plan_id is not null,
             'Loan business plan must resolve when supplied')
    ) as rule(rule_code, rule_passed, rule_detail)
)
select
    entity_type,
    entity_id,
    rule_code,
    rule_passed,
    case when rule_passed then 'PASS' else 'FAIL' end as rule_status,
    rule_detail
from validation_results
