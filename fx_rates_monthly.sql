-- Historical Exchange Rates Table (2017-2024)
-- Sources: Federal Reserve, European Central Bank, Reserve Bank of India
-- Rates are end-of-month values

CREATE OR REPLACE TABLE fx_rates AS (
  SELECT * FROM (
    VALUES
    -- 2017 EUR to USD
    ('2017-01-31', 'EUR to USD', 1.0796),
    ('2017-02-28', 'EUR to USD', 1.0578),
    ('2017-03-31', 'EUR to USD', 1.0691),
    ('2017-04-30', 'EUR to USD', 1.0900),
    ('2017-05-31', 'EUR to USD', 1.1201),
    ('2017-06-30', 'EUR to USD', 1.1412),
    ('2017-07-31', 'EUR to USD', 1.1810),
    ('2017-08-31', 'EUR to USD', 1.1908),
    ('2017-09-30', 'EUR to USD', 1.1806),
    ('2017-10-31', 'EUR to USD', 1.1638),
    ('2017-11-30', 'EUR to USD', 1.1893),
    ('2017-12-31', 'EUR to USD', 1.2005),
    
    -- 2018 EUR to USD
    ('2018-01-31', 'EUR to USD', 1.2417),
    ('2018-02-28', 'EUR to USD', 1.2218),
    ('2018-03-31', 'EUR to USD', 1.2324),
    ('2018-04-30', 'EUR to USD', 1.2079),
    ('2018-05-31', 'EUR to USD', 1.1669),
    ('2018-06-30', 'EUR to USD', 1.1683),
    ('2018-07-31', 'EUR to USD', 1.1687),
    ('2018-08-31', 'EUR to USD', 1.1629),
    ('2018-09-30', 'EUR to USD', 1.1609),
    ('2018-10-31', 'EUR to USD', 1.1319),
    ('2018-11-30', 'EUR to USD', 1.1367),
    ('2018-12-31', 'EUR to USD', 1.1450),
    
    -- 2019 EUR to USD
    ('2019-01-31', 'EUR to USD', 1.1416),
    ('2019-02-28', 'EUR to USD', 1.1372),
    ('2019-03-31', 'EUR to USD', 1.1218),
    ('2019-04-30', 'EUR to USD', 1.1214),
    ('2019-05-31', 'EUR to USD', 1.1170),
    ('2019-06-30', 'EUR to USD', 1.1380),
    ('2019-07-31', 'EUR to USD', 1.1151),
    ('2019-08-31', 'EUR to USD', 1.0982),
    ('2019-09-30', 'EUR to USD', 1.0889),
    ('2019-10-31', 'EUR to USD', 1.1156),
    ('2019-11-30', 'EUR to USD', 1.1018),
    ('2019-12-31', 'EUR to USD', 1.1227),
    
    -- 2020 EUR to USD
    ('2020-01-31', 'EUR to USD', 1.1093),
    ('2020-02-29', 'EUR to USD', 1.1034),
    ('2020-03-31', 'EUR to USD', 1.1033),
    ('2020-04-30', 'EUR to USD', 1.0952),
    ('2020-05-31', 'EUR to USD', 1.1106),
    ('2020-06-30', 'EUR to USD', 1.1234),
    ('2020-07-31', 'EUR to USD', 1.1781),
    ('2020-08-31', 'EUR to USD', 1.1939),
    ('2020-09-30', 'EUR to USD', 1.1719),
    ('2020-10-31', 'EUR to USD', 1.1647),
    ('2020-11-30', 'EUR to USD', 1.1980),
    ('2020-12-31', 'EUR to USD', 1.2272),
    
    -- 2021 EUR to USD
    ('2021-01-31', 'EUR to USD', 1.2137),
    ('2021-02-28', 'EUR to USD', 1.2076),
    ('2021-03-31', 'EUR to USD', 1.1731),
    ('2021-04-30', 'EUR to USD', 1.2025),
    ('2021-05-31', 'EUR to USD', 1.2223),
    ('2021-06-30', 'EUR to USD', 1.1858),
    ('2021-07-31', 'EUR to USD', 1.1868),
    ('2021-08-31', 'EUR to USD', 1.1807),
    ('2021-09-30', 'EUR to USD', 1.1579),
    ('2021-10-31', 'EUR to USD', 1.1559),
    ('2021-11-30', 'EUR to USD', 1.1331),
    ('2021-12-31', 'EUR to USD', 1.1326),
    
    -- 2022 EUR to USD
    ('2022-01-31', 'EUR to USD', 1.1233),
    ('2022-02-28', 'EUR to USD', 1.1192),
    ('2022-03-31', 'EUR to USD', 1.1101),
    ('2022-04-30', 'EUR to USD', 1.0546),
    ('2022-05-31', 'EUR to USD', 1.0734),
    ('2022-06-30', 'EUR to USD', 1.0483),
    ('2022-07-31', 'EUR to USD', 1.0216),
    ('2022-08-31', 'EUR to USD', 1.0051),
    ('2022-09-30', 'EUR to USD', 0.9802),
    ('2022-10-31', 'EUR to USD', 0.9896),
    ('2022-11-30', 'EUR to USD', 1.0340),
    ('2022-12-31', 'EUR to USD', 1.0706),
    
    -- 2023 EUR to USD
    ('2023-01-31', 'EUR to USD', 1.0864),
    ('2023-02-28', 'EUR to USD', 1.0597),
    ('2023-03-31', 'EUR to USD', 1.0875),
    ('2023-04-30', 'EUR to USD', 1.1034),
    ('2023-05-31', 'EUR to USD', 1.0693),
    ('2023-06-30', 'EUR to USD', 1.0866),
    ('2023-07-31', 'EUR to USD', 1.1018),
    ('2023-08-31', 'EUR to USD', 1.0850),
    ('2023-09-30', 'EUR to USD', 1.0574),
    ('2023-10-31', 'EUR to USD', 1.0593),
    ('2023-11-30', 'EUR to USD', 1.0889),
    ('2023-12-31', 'EUR to USD', 1.1050),
    
    -- 2024 EUR to USD
    ('2024-01-31', 'EUR to USD', 1.0823),
    ('2024-02-29', 'EUR to USD', 1.0805),
    ('2024-03-31', 'EUR to USD', 1.0787),
    ('2024-04-30', 'EUR to USD', 1.0683),
    ('2024-05-31', 'EUR to USD', 1.0845),
    ('2024-06-30', 'EUR to USD', 1.0707),
    ('2024-07-31', 'EUR to USD', 1.0822),
    ('2024-08-31', 'EUR to USD', 1.1083),
    ('2024-09-30', 'EUR to USD', 1.1157),
    ('2024-10-31', 'EUR to USD', 1.0858),
    ('2024-11-30', 'EUR to USD', 1.0515),
    ('2024-12-31', 'EUR to USD', 1.0393),
    
    -- 2017 INR to USD (converting from USD/INR)
    ('2017-01-31', 'INR to USD', 0.01478),  -- USD/INR: 67.65
    ('2017-02-28', 'INR to USD', 0.01493),  -- USD/INR: 66.99
    ('2017-03-31', 'INR to USD', 0.01542),  -- USD/INR: 64.86
    ('2017-04-30', 'INR to USD', 0.01554),  -- USD/INR: 64.36
    ('2017-05-31', 'INR to USD', 0.01551),  -- USD/INR: 64.48
    ('2017-06-30', 'INR to USD', 0.01551),  -- USD/INR: 64.51
    ('2017-07-31', 'INR to USD', 0.01558),  -- USD/INR: 64.16
    ('2017-08-31', 'INR to USD', 0.01562),  -- USD/INR: 64.00
    ('2017-09-30', 'INR to USD', 0.01527),  -- USD/INR: 65.48
    ('2017-10-31', 'INR to USD', 0.01539),  -- USD/INR: 64.94
    ('2017-11-30', 'INR to USD', 0.01546),  -- USD/INR: 64.70
    ('2017-12-31', 'INR to USD', 0.01564),  -- USD/INR: 63.93
    
    -- 2018 INR to USD
    ('2018-01-31', 'INR to USD', 0.01570),  -- USD/INR: 63.69
    ('2018-02-28', 'INR to USD', 0.01535),  -- USD/INR: 65.11
    ('2018-03-31', 'INR to USD', 0.01536),  -- USD/INR: 65.07
    ('2018-04-30', 'INR to USD', 0.01504),  -- USD/INR: 66.50
    ('2018-05-31', 'INR to USD', 0.01480),  -- USD/INR: 67.57
    ('2018-06-30', 'INR to USD', 0.01460),  -- USD/INR: 68.49
    ('2018-07-31', 'INR to USD', 0.01459),  -- USD/INR: 68.56
    ('2018-08-31', 'INR to USD', 0.01413),  -- USD/INR: 70.77
    ('2018-09-30', 'INR to USD', 0.01377),  -- USD/INR: 72.65
    ('2018-10-31', 'INR to USD', 0.01354),  -- USD/INR: 73.85
    ('2018-11-30', 'INR to USD', 0.01431),  -- USD/INR: 69.88
    ('2018-12-31', 'INR to USD', 0.01429),  -- USD/INR: 69.98
    
    -- 2019 INR to USD
    ('2019-01-31', 'INR to USD', 0.01408),  -- USD/INR: 71.02
    ('2019-02-28', 'INR to USD', 0.01411),  -- USD/INR: 70.87
    ('2019-03-31', 'INR to USD', 0.01447),  -- USD/INR: 69.11
    ('2019-04-30', 'INR to USD', 0.01436),  -- USD/INR: 69.65
    ('2019-05-31', 'INR to USD', 0.01433),  -- USD/INR: 69.81
    ('2019-06-30', 'INR to USD', 0.01449),  -- USD/INR: 69.02
    ('2019-07-31', 'INR to USD', 0.01451),  -- USD/INR: 68.90
    ('2019-08-31', 'INR to USD', 0.01395),  -- USD/INR: 71.67
    ('2019-09-30', 'INR to USD', 0.01410),  -- USD/INR: 70.92
    ('2019-10-31', 'INR to USD', 0.01410),  -- USD/INR: 70.91
    ('2019-11-30', 'INR to USD', 0.01392),  -- USD/INR: 71.86
    ('2019-12-31', 'INR to USD', 0.01401),  -- USD/INR: 71.38
    
    -- 2020 INR to USD
    ('2020-01-31', 'INR to USD', 0.01399),  -- USD/INR: 71.51
    ('2020-02-29', 'INR to USD', 0.01393),  -- USD/INR: 71.78
    ('2020-03-31', 'INR to USD', 0.01324),  -- USD/INR: 75.52
    ('2020-04-30', 'INR to USD', 0.01307),  -- USD/INR: 76.50
    ('2020-05-31', 'INR to USD', 0.01322),  -- USD/INR: 75.66
    ('2020-06-30', 'INR to USD', 0.01322),  -- USD/INR: 75.64
    ('2020-07-31', 'INR to USD', 0.01337),  -- USD/INR: 74.82
    ('2020-08-31', 'INR to USD', 0.01369),  -- USD/INR: 73.07
    ('2020-09-30', 'INR to USD', 0.01362),  -- USD/INR: 73.42
    ('2020-10-31', 'INR to USD', 0.01339),  -- USD/INR: 74.71
    ('2020-11-30', 'INR to USD', 0.01355),  -- USD/INR: 73.78
    ('2020-12-31', 'INR to USD', 0.01368),  -- USD/INR: 73.10
    
    -- 2021 INR to USD
    ('2021-01-31', 'INR to USD', 0.01371),  -- USD/INR: 72.93
    ('2021-02-28', 'INR to USD', 0.01370),  -- USD/INR: 72.99
    ('2021-03-31', 'INR to USD', 0.01371),  -- USD/INR: 72.93
    ('2021-04-30', 'INR to USD', 0.01343),  -- USD/INR: 74.48
    ('2021-05-31', 'INR to USD', 0.01376),  -- USD/INR: 72.68
    ('2021-06-30', 'INR to USD', 0.01346),  -- USD/INR: 74.32
    ('2021-07-31', 'INR to USD', 0.01343),  -- USD/INR: 74.46
    ('2021-08-31', 'INR to USD', 0.01368),  -- USD/INR: 73.10
    ('2021-09-30', 'INR to USD', 0.01350),  -- USD/INR: 74.07
    ('2021-10-31', 'INR to USD', 0.01331),  -- USD/INR: 75.15
    ('2021-11-30', 'INR to USD', 0.01339),  -- USD/INR: 74.70
    ('2021-12-31', 'INR to USD', 0.01319),  -- USD/INR: 75.82
    
    -- 2022 INR to USD
    ('2022-01-31', 'INR to USD', 0.01333),  -- USD/INR: 75.02
    ('2022-02-28', 'INR to USD', 0.01326),  -- USD/INR: 75.45
    ('2022-03-31', 'INR to USD', 0.01317),  -- USD/INR: 75.93
    ('2022-04-30', 'INR to USD', 0.01307),  -- USD/INR: 76.52
    ('2022-05-31', 'INR to USD', 0.01289),  -- USD/INR: 77.58
    ('2022-06-30', 'INR to USD', 0.01271),  -- USD/INR: 78.67
    ('2022-07-31', 'INR to USD', 0.01260),  -- USD/INR: 79.37
    ('2022-08-31', 'INR to USD', 0.01256),  -- USD/INR: 79.62
    ('2022-09-30', 'INR to USD', 0.01223),  -- USD/INR: 81.79
    ('2022-10-31', 'INR to USD', 0.01207),  -- USD/INR: 82.88
    ('2022-11-30', 'INR to USD', 0.01229),  -- USD/INR: 81.39
    ('2022-12-31', 'INR to USD', 0.01206),  -- USD/INR: 82.91
    
    -- 2023 INR to USD
    ('2023-01-31', 'INR to USD', 0.01225),  -- USD/INR: 81.63
    ('2023-02-28', 'INR to USD', 0.01212),  -- USD/INR: 82.51
    ('2023-03-31', 'INR to USD', 0.01217),  -- USD/INR: 82.17
    ('2023-04-30', 'INR to USD', 0.01222),  -- USD/INR: 81.83
    ('2023-05-31', 'INR to USD', 0.01211),  -- USD/INR: 82.58
    ('2023-06-30', 'INR to USD', 0.01219),  -- USD/INR: 82.04
    ('2023-07-31', 'INR to USD', 0.01216),  -- USD/INR: 82.24
    ('2023-08-31', 'INR to USD', 0.01208),  -- USD/INR: 82.78
    ('2023-09-30', 'INR to USD', 0.01202),  -- USD/INR: 83.19
    ('2023-10-31', 'INR to USD', 0.01201),  -- USD/INR: 83.26
    ('2023-11-30', 'INR to USD', 0.01201),  -- USD/INR: 83.28
    ('2023-12-31', 'INR to USD', 0.01202),  -- USD/INR: 83.20
    
    -- 2024 INR to USD
    ('2024-01-31', 'INR to USD', 0.01208),  -- USD/INR: 82.78
    ('2024-02-29', 'INR to USD', 0.01207),  -- USD/INR: 82.85
    ('2024-03-31', 'INR to USD', 0.01201),  -- USD/INR: 83.25
    ('2024-04-30', 'INR to USD', 0.01199),  -- USD/INR: 83.44
    ('2024-05-31', 'INR to USD', 0.01203),  -- USD/INR: 83.12
    ('2024-06-30', 'INR to USD', 0.01199),  -- USD/INR: 83.45
    ('2024-07-31', 'INR to USD', 0.01194),  -- USD/INR: 83.76
    ('2024-08-31', 'INR to USD', 0.01191),  -- USD/INR: 83.97
    ('2024-09-30', 'INR to USD', 0.01192),  -- USD/INR: 83.91
    ('2024-10-31', 'INR to USD', 0.01188),  -- USD/INR: 84.17
    ('2024-11-30', 'INR to USD', 0.01178),  -- USD/INR: 84.89
    ('2024-12-31', 'INR to USD', 0.01172)   -- USD/INR: 85.30
    
  ) AS t(month_ending, type, exchange_rate)
);

-- Example usage to convert MRR to USD
-- Join this table to your MRR data based on month and currency type
WITH self_serve_with_usd AS (
  SELECT 
    s.*,
    
    -- Convert MRR to USD based on currency
    CASE 
      WHEN s.currency = 'eur' THEN s.mrr * fx_eur.exchange_rate
      WHEN s.currency = 'inr' THEN s.mrr * fx_inr.exchange_rate  
      WHEN s.currency = 'usd' THEN s.mrr
      ELSE s.mrr  -- Keep as-is for other currencies
    END AS mrr_usd
    
  FROM self_serve_monthly_product_mrr s
  
  -- Join EUR rates
  LEFT JOIN fx_rates fx_eur 
    ON DATE_TRUNC(s.month_of, MONTH) = DATE(fx_eur.month_ending)
    AND fx_eur.type = 'EUR to USD'
    AND s.currency = 'eur'
    
  -- Join INR rates  
  LEFT JOIN fx_rates fx_inr
    ON DATE_TRUNC(s.month_of, MONTH) = DATE(fx_inr.month_ending)
    AND fx_inr.type = 'INR to USD'
    AND s.currency = 'inr'
)
SELECT * FROM self_serve_with_usd;
