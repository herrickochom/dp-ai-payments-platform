-- Install and configure HTTPFS for S3 access
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 for MinIO
SET s3_region='us-east-1';
SET s3_endpoint='minio:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';

-- Create sample customers data
CREATE OR REPLACE TABLE sample_customers AS 
SELECT 
  id,
  'Customer_' || id::varchar as name,
  'city_' || (id % 10)::varchar as city,
  (random() * 1000)::decimal(10,2) as lifetime_value,
  CASE floor(random() * 3)
    WHEN 0 THEN 'premium'
    WHEN 1 THEN 'standard'
    WHEN 2 THEN 'basic'
  END as tier,
  date_trunc('day', current_date - (random() * 365)::integer) as signup_date
FROM generate_series(1, 1000) as t(id);

-- Create sample transactions data
CREATE OR REPLACE TABLE sample_transactions AS
SELECT
  generate_series(1, 10000) as transaction_id,
  floor(random() * 1000) + 1 as customer_id,
  date_trunc('day', current_date - (random() * 365)::integer) as transaction_date,
  (random() * 500 + 10)::decimal(10,2) as amount,
  CASE floor(random() * 4)
    WHEN 0 THEN 'purchase'
    WHEN 1 THEN 'refund' 
    WHEN 2 THEN 'exchange'
    WHEN 3 THEN 'subscription'
  END as transaction_type,
  CASE floor(random() * 5)
    WHEN 0 THEN 'electronics'
    WHEN 1 THEN 'clothing'
    WHEN 2 THEN 'food'
    WHEN 3 THEN 'services'
    WHEN 4 THEN 'entertainment'
  END as category
;

-- Export to S3 in different formats for testing
COPY sample_customers TO 's3://warehouse/bronze/customers/customers.parquet' (FORMAT PARQUET);
COPY sample_transactions TO 's3://warehouse/bronze/transactions/transactions.parquet' (FORMAT PARQUET);
COPY sample_transactions TO 's3://warehouse/raw/transactions/transactions.csv' (FORMAT CSV, HEADER);

-- Create some aggregated data in processed layer
CREATE OR REPLACE TABLE customer_metrics AS
SELECT
  c.id as customer_id,
  c.name,
  c.tier,
  c.lifetime_value,
  COUNT(t.transaction_id) as total_transactions,
  SUM(t.amount) as total_spent,
  AVG(t.amount) as avg_transaction_value
FROM sample_customers c
LEFT JOIN sample_transactions t ON c.id = t.customer_id
GROUP BY c.id, c.name, c.tier, c.lifetime_value;

COPY customer_metrics TO 's3://warehouse/processed/customer_metrics/customer_metrics.parquet' (FORMAT PARQUET);

SELECT 'Sample data created successfully!' as status;
SELECT 
  'Customers: ' || COUNT(*)::varchar as customers,
  'Transactions: ' || COUNT(*)::varchar as transactions 
FROM sample_customers
CROSS JOIN sample_transactions
LIMIT 1;
