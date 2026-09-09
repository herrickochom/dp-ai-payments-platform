{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'executive', 'trend', 'dashboard']
) }}

-- ============================================================================
-- PDM EXECUTIVE MONTHLY TREND
-- ============================================================================
--
-- Purpose:
--   Monthly executive KPI series for the PDM National Executive
--   Intelligence Dashboard.
--
-- Important semantics:
--
--   1. Loan measures are attributed to the month in which the loan
--      was approved.
--
--   2. approved_amount, disbursed_amount, repaid_amount and
--      current_outstanding_amount are approval-cohort measures.
--
--   3. current_outstanding_amount is the current outstanding position of
--      loans belonging to that approval cohort. It is NOT a reconstructed
--      historical month-end balance.
--
--   4. Payment measures are attributed to the actual payment occurrence
--      month.
--
--   5. MoM percentages are expressed as percentage points for dashboard
--      display:
--
--          6.4  = 6.4%
--         -3.1  = -3.1%
--
--   6. The first observable month has no previous month comparison and
--      therefore returns:
--
--          previous value = NULL
--          MoM percentage = NULL
--          arrow          = '—'
--
-- ============================================================================


with loan_cohorts as (

    select
        date_trunc(
            'month',
            approval_date.calendar_date
        ) as reporting_month,

        count(
            distinct loan.loan_id
        ) as approved_loan_count,

        count(
            distinct loan.beneficiary_sk
        ) as approved_beneficiary_count,

        sum(
            loan.amount_approved
        ) as approved_amount,

        sum(
            loan.amount_disbursed
        ) as disbursed_amount,

        sum(
            loan.amount_repaid
        ) as repaid_amount,

        sum(
            loan.principal_repaid
        ) as repaid_principal_amount,

        sum(
            loan.outstanding_amount
        ) as current_outstanding_amount,

        sum(
            loan.amount_disbursed
        )
        / nullif(
            sum(loan.amount_approved),
            0
        ) as cohort_disbursement_rate,

        -- Principal-based cohort repayment (decisions 1-2): amount_repaid
        -- includes interest and is never divided by the principal denominator.
        sum(
            loan.principal_repaid
        )
        / nullif(
            sum(loan.amount_disbursed),
            0
        ) as cohort_principal_repayment_rate

    from {{ ref('gld_fct_pdm_loans') }} loan

    inner join {{ ref('gld_dim_pdm_date') }} approval_date
      on loan.approval_date_sk = approval_date.date_sk

    where approval_date.calendar_date is not null

    group by 1
),


-- ============================================================================
-- Actual monthly payment activity
-- ============================================================================

payment_activity as (

    select
        date_trunc(
            'month',
            payment_date.calendar_date
        ) as reporting_month,

        count(
            distinct payment.transaction_id
        ) as payment_count,

        sum(
            payment.payment_amount
        ) as payment_amount,

        count(
            distinct payment.transaction_id
        ) filter (
            where payment.is_reconciled
        ) as reconciled_payment_count,

        count(
            distinct payment.transaction_id
        ) filter (
            where payment.is_entity_matched
        ) as entity_matched_payment_count,

        count(
            distinct payment.transaction_id
        ) filter (
            where payment.is_account_substituted
        ) as account_substitution_count

    from {{ ref('gld_fct_pdm_payments') }} payment

    inner join {{ ref('gld_dim_pdm_date') }} payment_date
      on payment.payment_date_sk = payment_date.date_sk

    where payment_date.calendar_date is not null

    group by 1
),


-- ============================================================================
-- Combine approval-cohort and payment-activity months
-- ============================================================================

combined as (

    select
        coalesce(
            loan.reporting_month,
            payment.reporting_month
        ) as reporting_month,

        loan.approved_loan_count,
        loan.approved_beneficiary_count,

        loan.approved_amount,
        loan.disbursed_amount,
        loan.repaid_amount,
        loan.repaid_principal_amount,
        loan.current_outstanding_amount,

        loan.cohort_disbursement_rate,
        loan.cohort_principal_repayment_rate,

        payment.payment_count,
        payment.payment_amount,
        payment.reconciled_payment_count,
        payment.entity_matched_payment_count,
        payment.account_substitution_count

    from loan_cohorts loan

    full outer join payment_activity payment
      using (reporting_month)
),


-- ============================================================================
-- Normalise NULL measures to zero before applying window calculations
-- ============================================================================

normalised as (

    select
        cast(
            reporting_month as date
        ) as reporting_month,

        coalesce(
            approved_loan_count,
            0
        ) as approved_loan_count,

        coalesce(
            approved_beneficiary_count,
            0
        ) as approved_beneficiary_count,

        coalesce(
            approved_amount,
            0
        ) as approved_amount,

        coalesce(
            disbursed_amount,
            0
        ) as disbursed_amount,

        coalesce(
            repaid_amount,
            0
        ) as repaid_amount,

        coalesce(
            repaid_principal_amount,
            0
        ) as repaid_principal_amount,

        coalesce(
            current_outstanding_amount,
            0
        ) as current_outstanding_amount,

        coalesce(
            cohort_disbursement_rate,
            0
        ) as cohort_disbursement_rate,

        coalesce(
            cohort_principal_repayment_rate,
            0
        ) as cohort_principal_repayment_rate,

        coalesce(
            payment_count,
            0
        ) as payment_count,

        coalesce(
            payment_amount,
            0
        ) as payment_amount,

        coalesce(
            reconciled_payment_count,
            0
        ) as reconciled_payment_count,

        coalesce(
            entity_matched_payment_count,
            0
        ) as entity_matched_payment_count,

        coalesce(
            account_substitution_count,
            0
        ) as account_substitution_count

    from combined

    where reporting_month is not null
),


-- ============================================================================
-- Previous month values
-- ============================================================================

with_previous_month as (

    select
        normalised.*,

        lag(
            approved_amount
        ) over (
            order by reporting_month
        ) as previous_approved_amount,

        lag(
            disbursed_amount
        ) over (
            order by reporting_month
        ) as previous_disbursed_amount,

        lag(
            repaid_amount
        ) over (
            order by reporting_month
        ) as previous_repaid_amount,

        lag(
            current_outstanding_amount
        ) over (
            order by reporting_month
        ) as previous_outstanding_amount,

        lag(
            approved_beneficiary_count
        ) over (
            order by reporting_month
        ) as previous_beneficiary_count,

        lag(
            approved_loan_count
        ) over (
            order by reporting_month
        ) as previous_loan_count

    from normalised
),


-- ============================================================================
-- Month-on-month calculations
-- ============================================================================

mom_metrics as (

    select
        with_previous_month.*,


        -- --------------------------------------------------------------------
        -- TOTAL APPROVED
        -- --------------------------------------------------------------------

        approved_amount
            - previous_approved_amount
            as approved_amount_mom_change,

        case
            when previous_approved_amount is null
              or previous_approved_amount = 0
                then null

            else
                (
                    (
                        approved_amount
                        - previous_approved_amount
                    )
                    / previous_approved_amount
                ) * 100.0
        end as approved_amount_mom_pct,


        -- --------------------------------------------------------------------
        -- TOTAL DISBURSED
        -- --------------------------------------------------------------------

        disbursed_amount
            - previous_disbursed_amount
            as disbursed_amount_mom_change,

        case
            when previous_disbursed_amount is null
              or previous_disbursed_amount = 0
                then null

            else
                (
                    (
                        disbursed_amount
                        - previous_disbursed_amount
                    )
                    / previous_disbursed_amount
                ) * 100.0
        end as disbursed_amount_mom_pct,


        -- --------------------------------------------------------------------
        -- TOTAL REPAID
        -- --------------------------------------------------------------------

        repaid_amount
            - previous_repaid_amount
            as repaid_amount_mom_change,

        case
            when previous_repaid_amount is null
              or previous_repaid_amount = 0
                then null

            else
                (
                    (
                        repaid_amount
                        - previous_repaid_amount
                    )
                    / previous_repaid_amount
                ) * 100.0
        end as repaid_amount_mom_pct,


        -- --------------------------------------------------------------------
        -- TOTAL OUTSTANDING
        -- --------------------------------------------------------------------

        current_outstanding_amount
            - previous_outstanding_amount
            as outstanding_amount_mom_change,

        case
            when previous_outstanding_amount is null
              or previous_outstanding_amount = 0
                then null

            else
                (
                    (
                        current_outstanding_amount
                        - previous_outstanding_amount
                    )
                    / previous_outstanding_amount
                ) * 100.0
        end as outstanding_amount_mom_pct,


        -- --------------------------------------------------------------------
        -- BENEFICIARIES
        -- --------------------------------------------------------------------

        approved_beneficiary_count
            - previous_beneficiary_count
            as beneficiary_count_mom_change,

        case
            when previous_beneficiary_count is null
              or previous_beneficiary_count = 0
                then null

            else
                (
                    (
                        approved_beneficiary_count
                        - previous_beneficiary_count
                    )
                    / previous_beneficiary_count
                ) * 100.0
        end as beneficiary_count_mom_pct,


        -- --------------------------------------------------------------------
        -- APPROVED LOANS
        -- --------------------------------------------------------------------

        approved_loan_count
            - previous_loan_count
            as loan_count_mom_change,

        case
            when previous_loan_count is null
              or previous_loan_count = 0
                then null

            else
                (
                    (
                        approved_loan_count
                        - previous_loan_count
                    )
                    / previous_loan_count
                ) * 100.0
        end as loan_count_mom_pct

    from with_previous_month
)


-- ============================================================================
-- Final executive monthly trend dataset
-- ============================================================================

select

    reporting_month,

    year(
        reporting_month
    ) as reporting_year,

    quarter(
        reporting_month
    ) as reporting_quarter,

    month(
        reporting_month
    ) as reporting_month_number,


    -- ========================================================================
    -- KPI 1: TOTAL APPROVED
    -- ========================================================================

    approved_amount,

    previous_approved_amount,

    approved_amount_mom_change,

    approved_amount_mom_pct,

    case
        when previous_approved_amount is null then '—'
        when approved_amount > previous_approved_amount then '▲'
        when approved_amount < previous_approved_amount then '▼'
        else '—'
    end as approved_amount_mom_arrow,

    case
        when previous_approved_amount is null then 'NO PRIOR MONTH'
        when approved_amount > previous_approved_amount then 'INCREASE'
        when approved_amount < previous_approved_amount then 'DECREASE'
        else 'NO CHANGE'
    end as approved_amount_mom_direction,


    -- ========================================================================
    -- KPI 2: TOTAL DISBURSED
    -- ========================================================================

    disbursed_amount,

    previous_disbursed_amount,

    disbursed_amount_mom_change,

    disbursed_amount_mom_pct,

    case
        when previous_disbursed_amount is null then '—'
        when disbursed_amount > previous_disbursed_amount then '▲'
        when disbursed_amount < previous_disbursed_amount then '▼'
        else '—'
    end as disbursed_amount_mom_arrow,

    case
        when previous_disbursed_amount is null then 'NO PRIOR MONTH'
        when disbursed_amount > previous_disbursed_amount then 'INCREASE'
        when disbursed_amount < previous_disbursed_amount then 'DECREASE'
        else 'NO CHANGE'
    end as disbursed_amount_mom_direction,


    -- ========================================================================
    -- KPI 3: TOTAL REPAID
    -- ========================================================================

    repaid_amount,

    previous_repaid_amount,

    repaid_amount_mom_change,

    repaid_amount_mom_pct,

    case
        when previous_repaid_amount is null then '—'
        when repaid_amount > previous_repaid_amount then '▲'
        when repaid_amount < previous_repaid_amount then '▼'
        else '—'
    end as repaid_amount_mom_arrow,

    case
        when previous_repaid_amount is null then 'NO PRIOR MONTH'
        when repaid_amount > previous_repaid_amount then 'INCREASE'
        when repaid_amount < previous_repaid_amount then 'DECREASE'
        else 'NO CHANGE'
    end as repaid_amount_mom_direction,


    -- ========================================================================
    -- KPI 4: TOTAL OUTSTANDING
    -- ========================================================================

    current_outstanding_amount,

    previous_outstanding_amount,

    outstanding_amount_mom_change,

    outstanding_amount_mom_pct,

    case
        when previous_outstanding_amount is null then '—'
        when current_outstanding_amount > previous_outstanding_amount then '▲'
        when current_outstanding_amount < previous_outstanding_amount then '▼'
        else '—'
    end as outstanding_amount_mom_arrow,

    case
        when previous_outstanding_amount is null then 'NO PRIOR MONTH'
        when current_outstanding_amount > previous_outstanding_amount then 'INCREASE'
        when current_outstanding_amount < previous_outstanding_amount then 'DECREASE'
        else 'NO CHANGE'
    end as outstanding_amount_mom_direction,


    -- ========================================================================
    -- KPI 5: BENEFICIARIES
    -- ========================================================================

    approved_beneficiary_count,

    previous_beneficiary_count,

    beneficiary_count_mom_change,

    beneficiary_count_mom_pct,

    case
        when previous_beneficiary_count is null then '—'
        when approved_beneficiary_count > previous_beneficiary_count then '▲'
        when approved_beneficiary_count < previous_beneficiary_count then '▼'
        else '—'
    end as beneficiary_count_mom_arrow,

    case
        when previous_beneficiary_count is null then 'NO PRIOR MONTH'
        when approved_beneficiary_count > previous_beneficiary_count then 'INCREASE'
        when approved_beneficiary_count < previous_beneficiary_count then 'DECREASE'
        else 'NO CHANGE'
    end as beneficiary_count_mom_direction,


    -- ========================================================================
    -- KPI 6: APPROVED LOANS
    -- ========================================================================

    approved_loan_count,

    previous_loan_count,

    loan_count_mom_change,

    loan_count_mom_pct,

    case
        when previous_loan_count is null then '—'
        when approved_loan_count > previous_loan_count then '▲'
        when approved_loan_count < previous_loan_count then '▼'
        else '—'
    end as loan_count_mom_arrow,

    case
        when previous_loan_count is null then 'NO PRIOR MONTH'
        when approved_loan_count > previous_loan_count then 'INCREASE'
        when approved_loan_count < previous_loan_count then 'DECREASE'
        else 'NO CHANGE'
    end as loan_count_mom_direction,


    -- ========================================================================
    -- Supporting loan ratios
    -- ========================================================================

    cohort_disbursement_rate,

    cohort_principal_repayment_rate,


    -- ========================================================================
    -- Payment activity
    -- ========================================================================

    payment_count,

    payment_amount,

    reconciled_payment_count,

    entity_matched_payment_count,

    account_substitution_count,

    reconciled_payment_count
        / nullif(
            payment_count,
            0
        ) as payment_reconciliation_rate,

    entity_matched_payment_count
        / nullif(
            payment_count,
            0
        ) as payment_entity_match_rate,

    -- PDM POC v1 decision 6: this series describes the current state of each
    -- approval cohort. It is NOT a reconstructed month-end portfolio history.
    -- The final executive trend requires repayment-event ingestion and must
    -- not be presented as historical portfolio performance today.
    'APPROVAL_COHORT_CURRENT_STATE' as series_basis,
    'FALSE' as is_month_end_portfolio_history,
    'TRUE' as is_flagged_non_historical


from mom_metrics

order by reporting_month