TRUNCATE TABLE demo_sales_summary;

INSERT INTO demo_sales_summary (
    region,
    order_count,
    total_quantity,
    total_amount
)
SELECT
    region,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_amount
FROM demo_sales_orders
GROUP BY region
ORDER BY region;

