SELECT
  *
FROM (
  SELECT
    a.id
    ,a.city_id
    ,c.district_id
    ,c.block_id
    ,b.community_id
    ,e.bedrooms
    ,a.price
    ,a.price_tier
    ,a.area

    ,a.is_real
    ,a.survey_status
    ,a.verify_status
    ,a.status
    ,a.created_at
    ,a.updated_at

  FROM (
    SELECT
      a.id
      ,a.city_id
      ,a.property_id
      ,a.price
      ,a.area
      ,a.is_real
      ,a.survey_status
      ,a.source
      ,a.has_checked
      ,a.created_at
      ,a.updated_at
      ,a.verify_status
      ,a.status
      ,CASE
        WHEN a.price >= 0 AND price <= 1500000
          THEN 1
        WHEN a.price >= 1500000 AND price <= 2000000
          THEN 2
        WHEN a.price >= 2000000 AND price <= 2500000
          THEN 3
        WHEN a.price >= 2500000 AND price <= 3000000
          THEN 4
        WHEN a.price >= 3000000 AND price <= 4000000
          THEN 5
        WHEN a.price >= 4000000 AND price <= 5000000
          THEN 6
        WHEN a.price >= 5000000 AND price <= 7000000
          THEN 7
        WHEN a.price >= 7000000 AND price <= 10000000
          THEN 8
        WHEN a.price >= 10000000 AND price <= 9999999999
          THEN 9
      END AS price_tier
    FROM
        property.inventory AS a
    WHERE
        a.updated_at >= '${date}'
        -- AND a.status = 2
        AND (a.updated_at <> '0000-00-00 00:00:00' AND a.created_at <> '0000-00-00 00:00:00')
  ) AS a
  LEFT JOIN property.property AS e on a.property_id = e.id
  LEFT JOIN property.house AS b on e.house_id = b.id
  LEFT JOIN angejia.community AS c on b.community_id = c.id
) AS d
LIMIT ${offset},${limit}
;
