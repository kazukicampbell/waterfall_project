-- Create list of stripe customer IDs on a contract
-- stripe customer ID data hygiene not great currently so leveraging multiple SFDC objects to find it
-- ideally SFDC contract object will be only source we need to pull stripe customer ID from

WITH contracted_customers AS(
SELECT DISTINCT
    COALESCE(con2.stripe_cust_id_c, sub.stripe_customer_id, acc.stripe_customer_id) AS stripe_customer_id,
    MIN(con2.contract_effective_date_c) AS contract_first_date,
    MAX(con2.contract_end_date_c) AS contract_last_date
  FROM `analytics-448513.sources.salesforce_account` acc

  LEFT JOIN `analytics-448513.sources.salesforce_contract` con
    ON acc.salesforce_account_id = con.salesforce_account_id

  -- can delete this join once stripe customer id is surfaced in contract table
  LEFT JOIN `gothic-avenue-392812.salesforce.contract` con2
    ON con.salesforce_contract_id = con2.id
    AND con2._fivetran_deleted IS FALSE

  LEFT JOIN `analytics-448513.sources.salesforce_subscription` sub
    ON acc.salesforce_account_id = sub.salesforce_account_id
   
  WHERE con.status IN('activated', 'expired')
  GROUP BY ALL
),

-- Establish many:1 relationship between stripe customer ID and SFDC account ID (cannot be the other way round)
-- multiple stripe customers can be in one SFDC account, but not the other way around or we duplicate reporting
-- Ideally we should be able to joining via stripe subscription id in SFDC subscription object and pull stripe customer ID, but we need cleanup and process to ensure 100% data completion
-- If these is no stripe customer id in the subscription object then we pull the customer id on the acount object (mid-term solution)
-- If for some reason a stripe customer bleongs to multiple accounts we select the SFDC account with highest topline arr value (ideally won;t need this in future)


sfdc_accounts AS(
  SELECT DISTINCT
    acc.salesforce_account_id AS sfdc_account_id,
    COALESCE(con2.stripe_cust_id_c, sub.stripe_customer_id, acc.stripe_customer_id) AS stripe_customer_id,
    acc2.topline_arr_c AS sfdc_account_topline_arr

  FROM `analytics-448513.sources.salesforce_account` acc

  LEFT JOIN `analytics-448513.sources.salesforce_contract` con
    ON acc.salesforce_account_id = con.salesforce_account_id

  -- can delete this join once stripe customer id is surfaced in contract table
  LEFT JOIN `gothic-avenue-392812.salesforce.contract` con2
    ON con.salesforce_contract_id = con2.id
    AND con2._fivetran_deleted IS FALSE

  LEFT JOIN `analytics-448513.sources.salesforce_subscription` sub
    ON acc.salesforce_account_id = sub.salesforce_account_id
  
  LEFT JOIN `gothic-avenue-392812.salesforce.account` acc2
    ON acc.salesforce_account_id = acc2.id
    and acc2._fivetran_deleted IS FALSE


QUALIFY ROW_NUMBER() OVER(PARTITION BY COALESCE(con2.stripe_cust_id_c, sub.stripe_customer_id, acc.stripe_customer_id) ORDER BY acc2.topline_arr_c DESC) = 1
),
  
-- join to contracted customer CTE to flag contracted folks vs non-contracted
-- contracted foks should have revenue measured based on SFDC data entry, self-serve will be measured via stripe
-- join to SFDC acc CTE to pull SFDC account ID for future enrichment  
customer AS (
  SELECT
    cus.*
    , CASE WHEN con.stripe_customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS contracted_customer -- needs to be adjusted to factor in dates at some point
    , con.contract_first_date
    , con.contract_last_date
    , acc.sfdc_account_id

  FROM `analytics-448513.sources.stripe_customer` cus
  LEFT JOIN contracted_customers con
    ON cus.stripe_customer_id = con.stripe_customer_id

  LEFT JOIN sfdc_accounts acc
    ON cus.stripe_customer_id = acc.stripe_customer_id
  WHERE 
    --cus.is_deleted = FALSE  
    (LOWER(cus.stripe_customer_email) NOT LIKE '%@getstream.io' OR cus.stripe_customer_email IS NULL)
    --AND cus.livemode = TRUE
),

-- Create mapping of sfdc_account_id to concatenated stripe_customer_ids
account_customer_mapping AS (
  SELECT 
    sfdc_account_id,
    STRING_AGG(DISTINCT stripe_customer_id, ', ' ORDER BY stripe_customer_id) AS concat_stripe_customer_ids
  FROM customer
  WHERE sfdc_account_id IS NOT NULL
  GROUP BY sfdc_account_id
),

-- Only need the most recent version of a subscription record for this analysis
-- doesn't exist in analytics project
subscription AS (
  SELECT suh.*
  FROM `gothic-avenue-392812.stripe.subscription_history` suh
  WHERE _fivetran_active IS TRUE
    AND livemode IS TRUE
    AND status != 'incomplete_expired'
),

-- Only want to count invoices that are paid/open and subscription-related
invoice AS (
  SELECT *
  FROM `analytics-448513.sources.stripe_invoice` 
  WHERE 
    stripe_subscription_id IS NOT NULL
    AND status IN ('paid', 'open', 'uncollecible')
    --AND livemode IS TRUE
),

-- Filter to recurring subscription line items only
invoice_line_item AS (
  SELECT *
  FROM `gothic-avenue-392812.stripe.invoice_line_item` 
  WHERE type = 'subscription'
    AND amount > 0
    AND livemode IS TRUE
),

-- Filter to recurring prices only (excludes one-time, usage-based)
price AS (
  SELECT *
  FROM `gothic-avenue-392812.stripe.price`
  WHERE 1=1
    --AND type = 'recurring'
    --AND recurring_interval IS NOT NULL
    AND livemode IS TRUE
),

-- price has replaced plan, however old subscriptions need plan data for correct payment interval info
plan AS (
  SELECT *
  FROM `gothic-avenue-392812.stripe.plan`
  WHERE 1=1
    --AND type = 'recurring'
    --AND recurring_interval IS NOT NULL
    AND livemode IS TRUE
),

-- Filter products to livemode and categorize by product type
product AS (
  SELECT *,
    CASE 
      WHEN LOWER(name) LIKE '%chat%' THEN 'Chat'
      WHEN LOWER(name) LIKE '%feed%' THEN 'Feed'
      WHEN LOWER(name) LIKE '%video%' THEN 'Video'
      WHEN LOWER(name) LIKE '%moderation%' THEN 'Moderation'
      WHEN LOWER(name) LIKE 'v2_%' THEN 'Feed'
      ELSE 'Other'
    END AS product_type
  FROM `gothic-avenue-392812.stripe.product`
  WHERE livemode = TRUE
),

-- Aggregate discount information per invoice
invoice_discounts AS (
  SELECT 
    invoice_id,
    percent_off AS invoice_percent_off,
    amount_off AS invoice_amount_off
  FROM `gothic-avenue-392812.stripe.invoice_discount` id2
  LEFT JOIN `gothic-avenue-392812.stripe.coupon` c2 
    ON id2.coupon_id = c2.id
  WHERE c2.livemode = TRUE
),

-- Join all base tables together
base_joined_data AS (
  SELECT 
    COALESCE(cus.sfdc_account_id, cus.stripe_customer_id) AS grouping_key,
    acm.concat_stripe_customer_ids,
    cus.sfdc_account_id,
    cus.stripe_customer_id,
    cus.contracted_customer,
    sub.id AS subscription_id,
    sub.created AS sub_created,
    sub.status AS sub_status,
    sub.canceled_at AS sub_canceled_at,
    sub.ended_at AS sub_ended_at,
    inv.stripe_invoice_id,
    inv.created_at AS inv_created,
    inv.period_start_at AS inv_period_start,
    inv.period_end_at AS inv_period_end,
    ili.period_start AS line_item_period_start,
    ili.period_end AS line_item_period_end,
    inv.status AS inv_status,
    prd.name AS line_item_product_name,
    prd.product_type,
    COALESCE(prc.unit_amount, pln.amount) AS unit_amount,
    COALESCE(prc.recurring_interval, pln.interval) AS recurring_interval,
    COALESCE(prc.recurring_interval_count, pln.interval_count) AS recurring_interval_count,
    prc.type AS price_type,
    ili.amount AS line_item_amount,
    aid.invoice_percent_off,
    aid.invoice_amount_off,
        
    -- Calculate totals for discount allocation
    SUM(ili.amount) OVER (PARTITION BY inv.stripe_invoice_id) AS total_invoice_amount

  FROM customer cus

  INNER JOIN subscription sub
    ON cus.stripe_customer_id = sub.customer_id

  INNER JOIN invoice inv
    ON sub.id = inv.stripe_subscription_id

  INNER JOIN invoice_line_item ili
    ON inv.stripe_invoice_id = ili.invoice_id

  LEFT JOIN price prc
    ON ili.price_id = prc.id

  LEFT JOIN plan pln
    ON ili.plan_id = pln.id

  LEFT JOIN product prd
    ON prc.product_id = prd.id

  LEFT JOIN invoice_discounts aid 
    ON inv.stripe_invoice_id = aid.invoice_id
  
  LEFT JOIN account_customer_mapping acm
    ON cus.sfdc_account_id = acm.sfdc_account_id
),

-- Apply discount logic and calculate final MRR/ARR
base_data_with_discounts AS (
  SELECT 
    *,
    -- Apply discount logic
    CASE
      WHEN invoice_percent_off IS NOT NULL THEN 
        line_item_amount * (1 - invoice_percent_off / 100.0)
      WHEN invoice_amount_off IS NOT NULL THEN 
        GREATEST(0, line_item_amount - (invoice_amount_off * (line_item_amount / NULLIF(total_invoice_amount, 0))))
      ELSE line_item_amount
    END AS net_line_item_amount,
    
    -- Calculate net ARR (convert from cents to dollars)
    ROUND(
      CASE 
        WHEN recurring_interval = 'year' THEN 
          (CASE
            WHEN invoice_percent_off IS NOT NULL THEN 
              (unit_amount * (1 - invoice_percent_off / 100.0)) / recurring_interval_count
            WHEN invoice_amount_off IS NOT NULL THEN 
              GREATEST(0, unit_amount - (invoice_amount_off * (line_item_amount / NULLIF(total_invoice_amount, 0)) * (unit_amount / NULLIF(line_item_amount, 0)))) / recurring_interval_count
            ELSE unit_amount / recurring_interval_count
          END)
        WHEN recurring_interval = 'month' THEN 
          (CASE
            WHEN invoice_percent_off IS NOT NULL THEN 
              (unit_amount * (1 - invoice_percent_off / 100.0) * 12) / recurring_interval_count
            WHEN invoice_amount_off IS NOT NULL THEN 
              GREATEST(0, unit_amount - (invoice_amount_off * (line_item_amount / NULLIF(total_invoice_amount, 0)) * (unit_amount / NULLIF(line_item_amount, 0)))) * 12 / recurring_interval_count
            ELSE unit_amount * 12 / recurring_interval_count
          END)
        ELSE 0
      END / 100.0, 2
    ) AS net_arr,
    
    -- Calculate net MRR (convert from cents to dollars)
    ROUND(
      CASE 
        WHEN recurring_interval = 'year' THEN 
          (CASE
            WHEN invoice_percent_off IS NOT NULL THEN 
              (unit_amount * (1 - invoice_percent_off / 100.0)) / 12 / recurring_interval_count
            WHEN invoice_amount_off IS NOT NULL THEN 
              GREATEST(0, unit_amount - (invoice_amount_off * (line_item_amount / NULLIF(total_invoice_amount, 0)) * (unit_amount / NULLIF(line_item_amount, 0)))) / 12 / recurring_interval_count
            ELSE unit_amount / 12 / recurring_interval_count
          END)
        WHEN recurring_interval = 'month' THEN 
          (CASE
            WHEN invoice_percent_off IS NOT NULL THEN 
              (unit_amount * (1 - invoice_percent_off / 100.0)) / recurring_interval_count
            WHEN invoice_amount_off IS NOT NULL THEN 
              GREATEST(0, unit_amount - (invoice_amount_off * (line_item_amount / NULLIF(total_invoice_amount, 0)) * (unit_amount / NULLIF(line_item_amount, 0)))) / recurring_interval_count
            ELSE unit_amount / recurring_interval_count
          END)
        ELSE 0
      END / 100.0, 2
    ) AS net_mrr
    
  FROM base_joined_data
),

-- Get customer first subscription info across all products
customer_first_subscription AS (
  SELECT 
    grouping_key,
    concat_stripe_customer_ids,
    MIN(sub_created) AS customer_first_sub_date,
    MIN(DATE(DATE_TRUNC(line_item_period_start, MONTH))) AS customer_first_period
  FROM base_data_with_discounts
  WHERE line_item_amount > 0
  GROUP BY ALL
),

-- Aggregate at customer-product_type-period level (removed line_item_product_name)
customer_product_periods AS (
  SELECT 
    grouping_key,
    concat_stripe_customer_ids,
    contracted_customer,
    product_type,
    DATE(DATE_TRUNC(line_item_period_start, MONTH)) AS period_month,
    ROUND(SUM(net_arr), 2) AS period_net_arr,
    ROUND(SUM(net_mrr), 2) AS period_net_mrr,
    COUNT(DISTINCT subscription_id) AS active_subscriptions
    
  FROM base_data_with_discounts
  WHERE line_item_amount > 0
  GROUP BY ALL
),

-- Add previous period data - tracking by product_type
customer_product_with_previous AS (
  SELECT 
    cpp.*,
    cfs.customer_first_sub_date,
    cfs.customer_first_period,
    
    LAG(cpp.period_net_arr, 1) OVER (PARTITION BY cpp.grouping_key, cpp.product_type ORDER BY cpp.period_month) AS previous_period_arr,
    LAG(cpp.period_net_mrr, 1) OVER (PARTITION BY cpp.grouping_key, cpp.product_type ORDER BY cpp.period_month) previous_period_mrr,
    LAG(cpp.period_month, 1) OVER (PARTITION BY cpp.grouping_key, cpp.product_type ORDER BY cpp.period_month) AS previous_period_month,
    
    -- Check if customer had this product_type before (for reactivation detection)
    CASE 
      WHEN LAG(cpp.period_net_arr, 1) OVER (PARTITION BY cpp.grouping_key, cpp.product_type ORDER BY cpp.period_month) > 0 THEN 1
      WHEN COUNT(cpp.period_month) OVER (PARTITION BY cpp.grouping_key, cpp.product_type ORDER BY cpp.period_month ROWS UNBOUNDED PRECEDING) > 1 THEN 1
      ELSE 0 
    END AS had_revenue_before,
    
    -- Check if customer had ANY product in this product_type before (for cross-sell vs expansion)
    CASE 
      WHEN COUNT(cpp.period_month) OVER (PARTITION BY cpp.grouping_key, cpp.product_type ORDER BY cpp.period_month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) > 0 THEN 1
    ELSE 0 END AS had_product_type_before
    
  FROM customer_product_periods cpp
  LEFT JOIN customer_first_subscription cfs 
    ON cpp.grouping_key = cfs.grouping_key
),

-- Generate churn events at product_type level
churn_events AS (
  SELECT 
    grouping_key,
    concat_stripe_customer_ids,
    contracted_customer,
    product_type,
    CAST(DATE_TRUNC(sub_ended_at, MONTH) AS DATETIME) period_month,
    0.00 AS period_net_arr,
    0.00 AS period_net_mrr,
    net_arr AS previous_period_arr,
    net_mrr AS previous_period_mrr,
    MIN(sub_created) OVER(PARTITION BY grouping_key) AS customer_first_sub_date,
    MIN(DATE(DATE_TRUNC(line_item_period_start, MONTH))) OVER(PARTITION BY grouping_key) AS customer_first_period,
    'Churn' AS event_type,
    ROUND(-1 * net_arr, 2) AS arr_change_amount,
    ROUND(-1 * net_mrr, 2) AS mrr_change_amount,
    -100.0 AS arr_change_percent,
    -100.0 AS mrr_change_percent

  FROM base_data_with_discounts
  WHERE sub_status = 'canceled'
    AND line_item_amount > 0
  QUALIFY RANK() OVER(PARTITION BY subscription_id ORDER BY line_item_period_end DESC) = 1

),

-- Generate all subscription events at product_type level
active_subscription_events AS (
  SELECT 
    grouping_key,
    concat_stripe_customer_ids,
    contracted_customer,
    product_type,
    period_month,
    period_net_arr,
    period_net_mrr,
    previous_period_arr,
    previous_period_mrr,
    customer_first_sub_date,
    customer_first_period,
    
    CASE
      -- REACTIVATION: Customer had this product_type before, went to $0, now has revenue again
      WHEN period_net_arr > 0 
           AND COALESCE(previous_period_arr, 0) = 0
           AND had_revenue_before = 1
           AND period_month > customer_first_period
      THEN 'Reactivation'
      
      -- NEW SUBSCRIPTION: The first instance that a customer subscribed (any product)
      WHEN period_month = customer_first_period 
           AND previous_period_arr IS NULL 
      THEN 'New Subscription'
      
      -- CROSS-SELL: New product_type for existing customer
      WHEN previous_period_arr IS NULL 
           AND period_month > customer_first_period
           AND had_revenue_before = 0
           AND had_product_type_before = 0
      THEN 'Cross-sell'
      
      -- EXPANSION: Same product_type increased MRR
      WHEN previous_period_arr IS NOT NULL 
           AND period_net_arr > previous_period_arr
      THEN 'Expansion'
      
      -- CONTRACTION: Same product_type, ARR decreased (but still > 0)
      WHEN previous_period_arr IS NOT NULL 
           AND period_net_arr < previous_period_arr
           AND period_net_arr > 0
      THEN 'Contraction'
      
      -- MAINTENANCE: Same ARR (no change)
      WHEN previous_period_arr IS NOT NULL 
           AND period_net_arr = previous_period_arr
      THEN 'Maintenance'
      
      ELSE 'Other'
    END AS event_type,
    
    ROUND(COALESCE(period_net_arr, 0) - COALESCE(previous_period_arr, 0), 2) AS arr_change_amount,
    ROUND(COALESCE(period_net_mrr, 0) - COALESCE(previous_period_mrr, 0), 2) AS mrr_change_amount,
    
    CASE 
      WHEN previous_period_arr > 0 THEN 
        ROUND(((period_net_arr - previous_period_arr) / previous_period_arr) * 100, 2)
      ELSE NULL 
    END AS arr_change_percent,
    
    CASE 
      WHEN previous_period_mrr > 0 THEN 
        ROUND(((period_net_mrr - previous_period_mrr) / previous_period_mrr) * 100, 2)
      ELSE NULL 
    END AS mrr_change_percent

  FROM customer_product_with_previous
),

-- Combine active events and churn events
all_subscription_events AS (
  SELECT 
    grouping_key,
    concat_stripe_customer_ids,
    contracted_customer,
    product_type,
    period_month,
    event_type,
    period_net_arr,
    period_net_mrr,
    previous_period_arr,
    previous_period_mrr,
    arr_change_amount,
    mrr_change_amount,
    arr_change_percent,
    mrr_change_percent,
    customer_first_sub_date
  FROM active_subscription_events
  WHERE event_type IN ('New Subscription', 'Cross-sell', 'Expansion', 'Contraction', 'Reactivation')
  
  UNION ALL
  
  SELECT 
    grouping_key,
    concat_stripe_customer_ids,
    contracted_customer,
    product_type,
    period_month,
    event_type,
    period_net_arr,
    period_net_mrr,
    previous_period_arr,
    previous_period_mrr,
    arr_change_amount,
    mrr_change_amount,
    arr_change_percent,
    mrr_change_percent,
    customer_first_sub_date
  FROM churn_events
),

-- Final waterfall reporting at product_type level
final_waterfall AS (
  SELECT 
    grouping_key,
    concat_stripe_customer_ids,
    contracted_customer,
    product_type,
    period_month,
    event_type,
    
    -- Product_type level opening/closing metrics
    COALESCE(previous_period_mrr, 0) AS opening_product_mrr,
    COALESCE(period_net_mrr, 0) AS closing_product_mrr,
    COALESCE(previous_period_arr, 0) AS opening_product_arr,
    COALESCE(period_net_arr, 0) AS closing_product_arr,
    
    -- Product_type level current/previous
    period_net_arr,
    period_net_mrr,
    previous_period_arr,
    previous_period_mrr,
    arr_change_amount,
    mrr_change_amount,
    --arr_change_percent,
    --mrr_change_percent,
    
    -- Customer-level opening/closing aggregations (across all products)
    SUM(COALESCE(previous_period_mrr, 0)) OVER (PARTITION BY grouping_key, period_month) AS opening_customer_mrr,
    SUM(COALESCE(period_net_mrr, 0)) OVER (PARTITION BY grouping_key, period_month) AS closing_customer_mrr,
    SUM(COALESCE(previous_period_arr, 0)) OVER (PARTITION BY grouping_key, period_month) AS opening_customer_arr,
    SUM(COALESCE(period_net_arr, 0)) OVER (PARTITION BY grouping_key, period_month) AS closing_customer_arr,
    
    -- Customer-level totals
    SUM(period_net_arr) OVER (PARTITION BY grouping_key, period_month) AS customer_total_arr,
    SUM(period_net_mrr) OVER (PARTITION BY grouping_key, period_month) AS customer_total_mrr,
    SUM(arr_change_amount) OVER (PARTITION BY grouping_key, period_month) AS customer_total_arr_change,
    SUM(mrr_change_amount) OVER (PARTITION BY grouping_key, period_month) AS customer_total_mrr_change,
    
    -- Period-level opening/closing aggregations (across all customers)
    --SUM(COALESCE(previous_period_mrr, 0)) OVER (PARTITION BY period_month) AS opening_period_mrr,
    --SUM(COALESCE(period_net_mrr, 0)) OVER (PARTITION BY period_month) AS closing_period_mrr,
    --SUM(COALESCE(previous_period_arr, 0)) OVER (PARTITION BY period_month) AS opening_period_arr,
    --SUM(COALESCE(period_net_arr, 0)) OVER (PARTITION BY period_month) AS closing_period_arr,
    
    -- Additional context
    --customer_first_sub_date,
    --COUNT(*) OVER (PARTITION BY grouping_key, period_month) AS events_this_period,
    
    -- Waterfall period-over-period totals by event type
    --SUM(arr_change_amount) OVER (PARTITION BY period_month, event_type) AS period_event_arr_total,
    --SUM(mrr_change_amount) OVER (PARTITION BY period_month, event_type) AS period_event_mrr_total,
    --COUNT(*) OVER (PARTITION BY period_month, event_type) AS period_event_count

  FROM all_subscription_events
),

missing_stripe_customers AS
(
SELECT DISTINCT cus.*
FROM base_joined_data bsj

INNER JOIN customer cus
  ON bsj.grouping_key = cus.stripe_customer_id
)

SELECT *
FROM final_waterfall
;

/*
SELECT *
FROM active_subscription_events
WHERE 1=1
--AND contracted_customer IS FALSE
AND sfdc_account_id = '0015a00002gYONUAA4'
ORDER BY 1, 7;

SELECT * 
FROM missing_stripe_customers

*/
