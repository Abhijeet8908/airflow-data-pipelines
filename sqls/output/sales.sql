SELECT
  customer_id,
  SUM(amount) AS total_sales
FROM `my-gcp-project.analytics.sales_transform`
GROUP BY customer_id