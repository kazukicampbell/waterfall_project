
-- Configuration variable for contract grace period since this is liable to change over time
-- Just in case there's timezone issues or delays in correctly inputting contract effective dates into SFDC
DECLARE contract_grace_period_days INT64 DEFAULT 3;

-- join to contracted customer CTE to flag contracted folks vs non-contracted
-- contracted foks should have revenue measured based on SFDC data entry, self-serve will be measured via stripe
-- join to SFDC acc CTE to pull SFDC account ID for future enrichment  
WITH sfdc_account_id AS(
  SELECT DISTINCT
    sub.salesforce_account_id,
    sub.stripe_subscription_id
  FROM `analytics-448513.sources.salesforce_subscription` sub
  WHERE sub.stripe_subscription_id IS NOT NULL
), 

customer AS (
  SELECT
    cus.*
  FROM `analytics-448513.sources.stripe_customer` cus
  WHERE (LOWER(cus.stripe_customer_email) NOT LIKE '%@getstream.io' OR cus.stripe_customer_email IS NULL)
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

contracted_subscription_periods AS(
  SELECT *
  FROM `gothic-avenue-392812.waterfall.l1_contracted_subscription_periods`
),

-- Join all base tables together
base_joined_data AS (
  SELECT
    acc.salesforce_account_id,
    cus.stripe_customer_id,
    sub.id AS stripe_subscription_id,
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
  
  LEFT JOIN sfdc_account_id acc
    ON sub.id = acc.stripe_subscription_id
 
  
),

-- time to tie basee data with contracted subscription periods to label contracted periods vs non-contracted periods

-- Filter to self-serve subscriptions only
-- A subscription is self-serve if:
-- 1. It has no matching contracted periods at all, OR
-- 2. The specific invoice/line item falls outside any contracted period (with configurable grace period)
self_serve_data AS (
  SELECT 
    bjd.*,
    csp.stripe_subscription_id AS contracted_sub_id,
    csp.last_contracted_date,
    csp.first_contracted_date,
    
    -- Flag whether this specific line item falls within a contracted period (including grace period)
    CASE 
      WHEN csp.stripe_subscription_id IS NOT NULL 
        AND DATE(bjd.line_item_period_start) < DATE_ADD(csp.last_contracted_date, INTERVAL contract_grace_period_days DAY)
        AND DATE(bjd.line_item_period_end) > DATE_SUB(csp.first_contracted_date, INTERVAL contract_grace_period_days DAY)
      THEN TRUE
      ELSE FALSE
    END AS is_within_contracted_period
    
  FROM base_joined_data bjd
  
  LEFT JOIN contracted_subscription_periods csp
    ON bjd.stripe_subscription_id = csp.stripe_subscription_id
    -- Check for temporal overlap between line item period and contract period (with configurable grace period)
    AND DATE(bjd.line_item_period_start) < DATE_ADD(csp.last_contracted_date, INTERVAL contract_grace_period_days DAY)
    AND DATE(bjd.line_item_period_end) > DATE_SUB(csp.first_contracted_date, INTERVAL contract_grace_period_days DAY)
),

-- Final self-serve filter
self_serve_only AS (
  SELECT 
    * EXCEPT(contracted_sub_id, first_contracted_date, last_contracted_date, is_within_contracted_period)
  FROM self_serve_data
  WHERE is_within_contracted_period = FALSE
    OR contracted_sub_id IS NULL
),

self_serve_with_discounts AS (
  SELECT 
    *,
    
    -- Calculate net line item amount after discounts (in cents)
    CASE
      WHEN invoice_percent_off IS NOT NULL THEN 
        line_item_amount * (1 - invoice_percent_off / 100.0)
      WHEN invoice_amount_off IS NOT NULL THEN 
        GREATEST(0, line_item_amount - (invoice_amount_off * (line_item_amount / NULLIF(total_invoice_amount, 0))))
      ELSE 
        line_item_amount
    END AS net_line_item_amount
    
  FROM self_serve_only
), 

self_serve_with_mrr AS(
  SELECT *,
    -- MRR calculation from net_line_item_amount
  ROUND(
    CASE 
      WHEN recurring_interval = 'month' THEN 
        net_line_item_amount / NULLIF(recurring_interval_count, 0)
        
      WHEN recurring_interval = 'year' THEN 
        net_line_item_amount / 12 / NULLIF(recurring_interval_count, 0)
        
      ELSE 0
    END / 100.0,  -- Convert cents to dollars
    2
  ) AS mrr
  FROM self_serve_with_discounts

),

-- Add month offset arrays based on billing interval
self_serve_with_month_arrays AS (
  SELECT 
    salesforce_account_id,
    stripe_customer_id,
    stripe_subscription_id,
    product_type,
    line_item_product_name,
    line_item_period_start,
    line_item_period_end,
    mrr,
    
    -- Generate array of month offsets based on billing interval
    CASE
      -- Monthly billing: generate array for interval count months
      WHEN recurring_interval = 'month' THEN
        GENERATE_ARRAY(0, COALESCE(recurring_interval_count, 1) - 1)
        
      -- Annual billing: generate array for 12 months * interval count
      WHEN recurring_interval = 'year' THEN
        GENERATE_ARRAY(0, (12 * COALESCE(recurring_interval_count, 1)) - 1)
        
      -- Default to single month
      ELSE GENERATE_ARRAY(0, 0)
    END AS month_offsets
    
  FROM self_serve_with_mrr
  WHERE mrr > 0  -- Only include subscriptions with positive MRR
),

-- Expand to one row per month
self_serve_expanded_months AS (
  SELECT 
    salesforce_account_id,
    stripe_customer_id,
    stripe_subscription_id,
    product_type,
    line_item_product_name,
    line_item_period_start,
    line_item_period_end,
    mrr,
    month_offset
    
  FROM self_serve_with_month_arrays,
  UNNEST(month_offsets) AS month_offset
),

-- Calculate final month dates and apply period filter
self_serve_monthly_product_mrr AS (
  SELECT 
    salesforce_account_id,
    stripe_customer_id,
    stripe_subscription_id,
    product_type,
    line_item_product_name,
    
    -- Calculate month_of based on billing period and offset
    DATE_ADD(
      DATE(line_item_period_start), 
      INTERVAL month_offset MONTH
    ) AS month_of,
    
    mrr
    
  FROM self_serve_expanded_months
  
  -- Only include months within the actual billing period
  WHERE DATE_ADD(DATE(line_item_period_start), INTERVAL month_offset MONTH) < DATE(line_item_period_end)
)


SELECT *
FROM self_serve_monthly_product_mrr
WHERE 1=1
-- AND stripe_customer_id = 'cus_A7NweZCZ7ORtd8' -- had funky mrr
--AND stripe_customer_id = 'cus_PabHu8LmfxxGqg' -- annual plan
--ORDER BY inv_period_start
;
