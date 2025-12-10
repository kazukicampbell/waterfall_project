WITH account AS (
  SELECT *
  FROM `analytics-448513.sources.salesforce_account` acc
),

opportunity AS (
  SELECT *
  FROM `analytics-448513.sources.salesforce_opportunity` opp
  WHERE LOWER(opp.stage_name) LIKE '%won%'
    AND LOWER(opp.type) IN ('new business', 'expansion', 'cross-sell', 'win back', 'renewal', 'poc') 
), 

opportunity_line_item AS (
  SELECT *
  FROM `analytics-448513.sources.salesforce_opportunity_line_item` oli
), 

contract AS (
  SELECT 
    con.*,
    con2.first_billing_date_c AS contract_first_billing_date,
    con2.poc_involved_c AS poc_involved,
    con2.poc_price_c AS poc_price,
    con2.poc_start_date_c AS poc_start_date,
    con2.poc_end_date_c AS poc_end_date,
    con2.billing_frequency_c AS contract_billing_frequency,
    con2.new_mrr_c AS contract_mrr
  FROM `analytics-448513.sources.salesforce_contract` con
  INNER JOIN `gothic-avenue-392812.salesforce.contract` con2
    ON con.salesforce_contract_id = con2.id
  WHERE con.status IN ('activated', 'expired')
),

base_data AS (
  SELECT 
    acc.salesforce_account_id,
    acc.account_name,
    opp.salesforce_opportunity_id,
    opp.opportunity_name,
    con.salesforce_contract_id,
    con.stripe_subscription_id,
    COALESCE(con.contract_effective_date, con.start_date) AS contract_effective_date,
    DATE_SUB(DATE_ADD(COALESCE(con.contract_effective_date, con.start_date), INTERVAL CAST(oli.subscription_term AS INT64) MONTH), INTERVAL 1 DAY) AS contract_end_date,
    oli.product_family AS product_type,    
    oli.total_price / oli.subscription_term AS line_item_mrr
  FROM account acc
  INNER JOIN opportunity opp
    ON acc.salesforce_account_id = opp.salesforce_account_id
  INNER JOIN contract con
    ON opp.salesforce_opportunity_id = con.salesforce_opportunity_id
  INNER JOIN opportunity_line_item oli
    ON opp.salesforce_opportunity_id = oli.salesforce_opportunity_id
),

-- Calculate support MRR to allocate per contract
support_allocation AS (
  SELECT 
    salesforce_contract_id,
    SUM(CASE WHEN product_type = 'Support' THEN line_item_mrr ELSE 0 END) AS total_support_mrr,
    SUM(CASE WHEN product_type != 'Support' THEN line_item_mrr ELSE 0 END) AS total_non_support_mrr
  FROM base_data
  GROUP BY salesforce_contract_id
),

-- Allocate support MRR proportionally to other products
allocated_mrr AS (
  SELECT 
    bd.salesforce_account_id,
    bd.account_name,
    bd.salesforce_contract_id,
    bd.salesforce_opportunity_id,
    bd.opportunity_name,
    bd.stripe_subscription_id,
    bd.contract_effective_date,
    bd.contract_end_date,
    bd.product_type,
    CASE 
      WHEN sa.total_non_support_mrr > 0 THEN 
        bd.line_item_mrr + (sa.total_support_mrr * bd.line_item_mrr / sa.total_non_support_mrr)
      ELSE bd.line_item_mrr
    END AS mrr_with_support
  FROM base_data bd
  LEFT JOIN support_allocation sa
    ON bd.salesforce_contract_id = sa.salesforce_contract_id
  WHERE bd.product_type != 'Support'
),

-- Contract-level summary by product
contract_summary AS (
  SELECT 
    salesforce_account_id,
    account_name,
    salesforce_contract_id,
    salesforce_opportunity_id,
    opportunity_name,
    stripe_subscription_id,
    contract_effective_date,
    contract_end_date,
    product_type,
    SUM(mrr_with_support) AS product_mrr
  FROM allocated_mrr
  GROUP BY ALL
),

-- Generate date spine
date_spine AS (
  SELECT DATE_TRUNC(month_date, MONTH) AS fiscal_month
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      (SELECT DATE_TRUNC(MIN(contract_effective_date), MONTH) FROM contract_summary),
      DATE_TRUNC(CURRENT_DATE(), MONTH),
      INTERVAL 1 MONTH
    )
  ) AS month_date
),

-- Get all unique account/product combinations with their first start date
account_product_combos AS (
  SELECT 
    salesforce_account_id,
    account_name,
    product_type,
    MIN(contract_effective_date) AS first_contract_date
  FROM contract_summary
  GROUP BY salesforce_account_id, account_name, product_type
),

-- Create full grid: every month × every account/product
account_product_months AS (
  SELECT 
    ds.fiscal_month,
    apc.salesforce_account_id,
    apc.account_name,
    apc.product_type
  FROM date_spine ds
  CROSS JOIN account_product_combos apc
  WHERE ds.fiscal_month >= DATE_TRUNC(apc.first_contract_date, MONTH)
),

-- For each month, find ALL contracts for ranking
monthly_contract_state AS (
  SELECT 
    apm.fiscal_month,
    apm.salesforce_account_id,
    apm.account_name,
    apm.product_type,
    cs.salesforce_contract_id,
    cs.stripe_subscription_id,
    cs.salesforce_opportunity_id,
    cs.opportunity_name,
    cs.contract_effective_date,
    cs.contract_end_date,
    cs.product_mrr,
    CASE 
      WHEN cs.salesforce_contract_id IS NOT NULL 
        AND apm.fiscal_month >= DATE_TRUNC(cs.contract_effective_date, MONTH)
        AND apm.fiscal_month <= DATE_TRUNC(cs.contract_end_date, MONTH)
      THEN TRUE 
      ELSE FALSE 
    END AS is_active
  FROM account_product_months apm
  LEFT JOIN contract_summary cs
    ON apm.salesforce_account_id = cs.salesforce_account_id
    AND apm.product_type = cs.product_type
),

-- Dedupe: newer contract overwrites older for overlapping months
monthly_mrr AS (
  SELECT 
    fiscal_month,
    salesforce_account_id,
    account_name,
    product_type,
    salesforce_contract_id,
    stripe_subscription_id,
    salesforce_opportunity_id,
    opportunity_name,
    COALESCE(product_mrr, 0) AS mrr
  FROM (
    SELECT 
      fiscal_month,
      salesforce_account_id,
      account_name,
      product_type,
      salesforce_contract_id,
      stripe_subscription_id,
      salesforce_opportunity_id,
      opportunity_name,
      product_mrr,
      ROW_NUMBER() OVER (
        PARTITION BY fiscal_month, salesforce_account_id, product_type 
        ORDER BY is_active DESC, contract_effective_date DESC NULLS LAST
      ) AS contract_rank
    FROM monthly_contract_state
  )
  WHERE contract_rank = 1
)

SELECT 
  account_name,
  product_type,
  FORMAT_DATE('%Y-%m-01', fiscal_month) AS month_of,
  mrr,
  salesforce_account_id,
  salesforce_contract_id,
  stripe_subscription_id,
  salesforce_opportunity_id,
  opportunity_name
FROM monthly_mrr
ORDER BY 1, 2, 3
