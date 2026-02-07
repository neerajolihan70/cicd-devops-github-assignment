CREATE TABLE IF NOT EXISTS staging.sales_orders (
  order_id VARCHAR(50),
  order_date DATE,
  customer_id VARCHAR(50),
  amount DECIMAL(10,2)
);
