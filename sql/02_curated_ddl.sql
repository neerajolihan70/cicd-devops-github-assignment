CREATE TABLE IF NOT EXISTS curated.sales_orders (
  order_id VARCHAR(50) PRIMARY KEY,
  order_date DATE,
  customer_id VARCHAR(50),
  amount DECIMAL(10,2)
);
