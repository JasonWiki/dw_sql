add jar hdfs://Ucluster/user/jars/dw_hive_udf-1.0-SNAPSHOT.jar;
create temporary function parse_mobile_token as 'com.angejia.dw.hive.udf.parse.ParseMobileToken';
create temporary function parse_mobile_agent as 'com.angejia.dw.hive.udf.parse.ParseMobileAgent';

-- 推荐模型 - 用户访问房源历史数据,作为推荐模型基础数据
DROP TABLE IF EXISTS real_time.rt_user_inventory_history;
CREATE TABLE IF NOT EXISTS real_time.rt_user_inventory_history
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
AS
SELECT
 concat(user_id,"-",city_id) AS user_key,
 inventory_id,
 count(*) AS cnt
FROM (
 SELECT
  -- 请求用户 id , -- 解析出用户 id
  coalesce(parse_mobile_token(auth,'user_id'),0) as user_id,
  -- 解析 agent 信息
  parse_mobile_agent(mobile_agent,'ccid') as city_id,
  -- 请求地址
  request_uri,
  -- 截取房源 id
  CASE
    WHEN request_uri RLIKE '^/mobile/member/inventories/[0-9]+/[0-9]+'
      THEN regexp_extract(request_uri,'^/mobile/member/inventories/([0-9]+)/[0-9]+',1)

    WHEN request_uri RLIKE '^/mobile/member/inventory/detail/[0-9]+/[0-9]+'
      THEN regexp_extract(request_uri,'^/mobile/member/inventory/detail/([0-9]+)/[0-9]+',1)
  END AS inventory_id
 -- 实时更新的 access_log 日志表
 FROM real_time.rt_access_log
 WHERE (
   request_uri RLIKE '^/mobile/member/inventories/[0-9]+/[0-9]+'
   OR request_uri RLIKE '^/mobile/member/inventory/detail/[0-9]+/[0-9]+'
 )
 -- AND p_dt = '2016-05-10'
 AND p_dt BETWEEN DATE_SUB(${dealDate},30) AND ${dealDate}
 AND hostname = 'api.angejia.com'
) AS a
WHERE
 user_id <> ''
 AND inventory_id <> ''
GROUP BY
 concat(user_id,"-",city_id),
 inventory_id
;
