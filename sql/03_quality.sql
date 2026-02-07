-- Null check
SELECT COUNT(*) FROM curated.sales_orders WHERE order_id IS NULL;

-- Duplicate check
SELECT order_id, COUNT(*)
FROM curated.sales_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
