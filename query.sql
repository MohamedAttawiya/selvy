SELECT
  marketplace_id,
  gl_product_group_desc AS category,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(CAST(quantity AS DOUBLE)) AS total_units,
  SUM(CAST(our_price AS DOUBLE)) AS total_revenue
FROM "andes"."ufg_mena_bi.anow_orders_master"
WHERE CAST(order_day AS DATE) BETWEEN CAST('2026-03-16' AS DATE) AND CAST('2026-03-22' AS DATE) AND marketplace_id = 338801
  AND gl_product_group_desc IS NOT NULL
GROUP BY marketplace_id, gl_product_group_desc
ORDER BY total_units DESC, order_count DESC
LIMIT 50