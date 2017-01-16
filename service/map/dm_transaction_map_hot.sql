-- 成交热度地图
DELETE FROM dm_db.dm_transaction_map_hot WHERE sheet='201608';

-- 新增当月的数据
INSERT INTO dm_db.dm_transaction_map_hot
SELECT
  a.city_id,
  b.sheet,
  b.community,
  b.volume,
  a.lat AS lat,
  a.lng AS lng,
  -- 格式化日期成为 2016M05 的格式
  CONCAT(substring(b.sheet, 1, 4), 'M', substring(b.sheet, 5, 2)) AS sheet_month
-- 小区经纬度
FROM dm_db.dm_community_geo AS a
-- 城市的成交数据
RIGHT JOIN (
    SELECT
      agj_city_id,
      sheet,
      community,
      count(*) AS volume
    FROM dw_db.dw_business
    WHERE community <> ''
    AND sheet='201608'
    GROUP BY
      agj_city_id,
      sheet,
      community
) AS b
  ON a.community = b.community
  AND a.city_id = b.agj_city_id
;


SELECT sheet,count(*) AS cn FROM dm_db.dm_transaction_map_hot GROUP BY sheet;
