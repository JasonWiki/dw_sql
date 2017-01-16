add jar hdfs://Ucluster/user/jars/dw_hive_udf-1.0-SNAPSHOT.jar;
create temporary function parse_mobile_token as 'com.angejia.dw.hive.udf.parse.ParseMobileToken';
create temporary function parse_mobile_agent as 'com.angejia.dw.hive.udf.parse.ParseMobileAgent';

set hive.exec.compress.output=false;
set hive.default.fileformat=TEXTFILE;

-- 用户搜索小区历史
DROP TABLE IF EXISTS dw_db_temp.jason_user_community_history_detail;
CREATE TABLE IF NOT EXISTS dw_db_temp.jason_user_community_history_detail AS
SELECT
  *
FROM (
  SELECT
    -- 请求用户 id
    coalesce(parse_mobile_token(auth,'user_id'),0) as user_id,
    parse_mobile_agent(mobile_agent,'ccid') as city_id,
    parse_url(concat('http://', hostname, request_uri), 'QUERY', 'community_id') AS community_id,
    concat(server_date, ' ', server_time) AS server_time
  FROM real_time.rt_access_log
  WHERE
  -- p_dt='2016-05-15'
  p_dt BETWEEN DATE_SUB(${dealDate},30) AND ${dealDate}
  AND request_uri RLIKE '^/mobile/member/inventories/list[?](.*)'
  AND hostname = 'api.angejia.com'
) bs
WHERE bs.user_id <> 0
AND bs.community_id IS NOT NULL
;


-- 推荐模型 - 用户访问小区历史数据,作为推荐模型基础数据
DROP TABLE IF EXISTS real_time.rt_user_community_history;
CREATE TABLE IF NOT EXISTS real_time.rt_user_community_history
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
AS
SELECT
  concat(user_id,"-",city_id) AS user_key,
  community_id,
  count(*) AS cnt
FROM dw_db_temp.jason_user_community_history_detail
GROUP BY
 concat(user_id,"-",city_id),
 community_id
;
