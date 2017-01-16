SELECT
  COUNT(*) AS cn
FROM property.inventory
WHERE updated_at > '${date}'
;
