-- List of subs based on high MRR

SELECT 
  acc.account_name,
  cus.stripe_customer_name,
  cus.stripe_customer_id,
  ss.stripe_subscription_id,
  ss.product_type,
  ss.product_name,
  MAX(net_mrr) AS max_mrr,
  MIN(month) AS min_month,
  MAX(month) AS max_month,
  sub.stripe_subscription_id,
  CONCAT("https://getstream.lightning.force.com/lightning/r/Account/", acc.salesforce_account_id, "/related/Contracts/view")
  

FROM derived.derived_revenue_selfserve_monthly ss

LEFT JOIN `sources.salesforce_account` acc
  ON ss.salesforce_account_id = acc.salesforce_account_id

LEFT JOIN `sources.stripe_customer` cus
  ON ss.stripe_customer_id = cus.stripe_customer_id

LEFT JOIN `sources.salesforce_subscription` sub
  ON ss.stripe_subscription_id = sub.stripe_subscription_id

WHERE ss.month >= '2020-01-01'
GROUP BY ALL
ORDER BY max_mrr DESC
;

-- high mrr
-- entperise sounding names
-- product_type other
-- non monthly or annual billing periods
