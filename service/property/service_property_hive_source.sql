SELECT
  inventory_id,
  inventory_type,
  CASE
    -- A 类, B 类, C 类房源 排名
    WHEN inventory_type = 'A' THEN 1
    WHEN inventory_type = 'B' THEN 2
    WHEN inventory_type = 'C' THEN 3
    WHEN inventory_type = 'D' THEN 4
    ELSE 5
  END AS inventory_type_id
FROM dw_db.dw_property_inventory_level
WHERE inventory_id IN (${ids})
