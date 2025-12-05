-- Need a table storing start and end dates of all contracted stripe subscriptions in SFDC
-- This will be how we delineate between SS and contracted revenue
-- Customers can go in and out of being contracted and can also have some products contracted and some SS simultaneously
-- Hence the need for sub level breakouts and time periods
-- Customers can churn before the end of a contract so must use earliest of churn date vs contract end date

WITH 
stripe_subscription AS (
  SELECT suh.*
  FROM `gothic-avenue-392812.stripe.subscription_history` suh
  WHERE _fivetran_active IS TRUE
    AND livemode IS TRUE
    AND status != 'incomplete_expired'
),

-- get clean SFDC contract data
contract_clean AS(
  SELECT
    acc.salesforce_account_id,
    acc.account_name,
    COALESCE(con.stripe_customer_id, sub.stripe_customer_id, acc.stripe_customer_id) AS stripe_customer_id,
    COALESCE(con.stripe_subscription_id, sub.stripe_subscription_id) AS stripe_subscription_id,
    COALESCE(con.contract_effective_date, con.start_date) AS contract_effective_date_clean,
    MIN(
        DATE_SUB(
          DATE_ADD(
            COALESCE(con.contract_effective_date, con.start_date),
            INTERVAL CAST(con.contract_term AS INT64) MONTH
          ),
        INTERVAL 1 DAY
        )
    ) AS contract_end_date_clean

  FROM `analytics-448513.sources.salesforce_account` acc

  LEFT JOIN `analytics-448513.sources.salesforce_contract` con
    ON acc.salesforce_account_id = con.salesforce_account_id
    
  -- can delete this join once stripe customer id is surfaced in contract table
  LEFT JOIN `gothic-avenue-392812.salesforce.contract` con2
    ON con.salesforce_contract_id = con2.id

  LEFT JOIN `analytics-448513.sources.salesforce_subscription` sub
    ON acc.salesforce_account_id = sub.salesforce_account_id

  WHERE 
    con.status IN('activated', 'expired')
    AND con2._fivetran_deleted IS FALSE
    AND COALESCE(con.stripe_subscription_id, sub.stripe_subscription_id) IS NOT NULL

GROUP BY ALL
),

contract_periods AS(
  SELECT DISTINCT
    con.salesforce_account_id,
    con.account_name,
    stripe_customer_id,
    con.stripe_subscription_id,
    MIN(contract_effective_date_clean) AS first_contracted_date,
    MAX(
      CASE 
        WHEN sub.ended_at IS NOT NULL THEN
          LEAST(contract_end_date_clean, CAST(sub.ended_at AS DATE))
        ELSE
          contract_end_date_clean
      END
    ) AS last_contracted_date
    
  FROM contract_clean con

  LEFT JOIN stripe_subscription sub
    ON con.stripe_subscription_id = sub.id

  GROUP BY ALL
)

SELECT *
FROM contract_periods
ORDER BY 1, 4
;
