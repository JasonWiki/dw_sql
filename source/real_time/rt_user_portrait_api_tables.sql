add jar hdfs://Ucluster/user/jars/dw_hive_udf-1.0-SNAPSHOT.jar;
create temporary function userportrait_action_needs as 'com.angejia.dw.hive.udtf.userportrait.ActionNeeds';
create temporary function json_str_to_kv as 'com.angejia.dw.hive.udtf.JsonStrToKv';


-- 用户画像 Hbase 映射表
CREATE EXTERNAL TABLE IF NOT EXISTS real_time.hb_user_portrait(
  row_key String,
  action_needs string,
  tags_city string,
  tags_district string,
  tags_block string,
  tags_community string,
  tags_bedrooms string,
  tags_price string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = "needs:actionNeeds,tags:city,tags:district,tags:block,tags:community,tags:bedrooms,tags:price")
TBLPROPERTIES (
  "hbase.table.name" = "userPortrait")
;



-- 用户画像标签组数据
DROP TABLE IF EXISTS real_time.rt_user_portrait_action_needs;
CREATE TABLE IF NOT EXISTS real_time.rt_user_portrait_action_needs AS
SELECT
  row_key AS user_id,
  tag_key, city, district, block, community, bedrooms, price, cnt
FROM
  real_time.hb_user_portrait
lateral view
  userportrait_action_needs(action_needs) now_action_needs_list AS tag_key, city, district, block, community, bedrooms, price, cnt
ORDER BY
  int(cnt) DESC
;



-- 用户标签分数表
CREATE TABLE IF NOT EXISTS real_time.rt_user_portrait_tags_score (
  user_id      string,
  tag_id     string,
  tag_score    string
) PARTITIONED BY (
  tag_type string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY '\n';

-- city 标签
INSERT OVERWRITE TABLE real_time.rt_user_portrait_tags_score PARTITION(tag_type = 'city')
SELECT
  row_key AS user_id,
  key AS tag_id,
  value AS tag_score
FROM
  real_time.hb_user_portrait
lateral view
  json_str_to_kv(tags_city) kv_list AS key, value
;

-- district 标签
INSERT OVERWRITE TABLE real_time.rt_user_portrait_tags_score PARTITION(tag_type = 'district')
SELECT
  row_key AS user_id,
  key AS tag_id,
  value AS tag_score
FROM
  real_time.hb_user_portrait
lateral view
  json_str_to_kv(tags_district) kv_list AS key, value
;

-- block 标签
INSERT OVERWRITE TABLE real_time.rt_user_portrait_tags_score PARTITION(tag_type = 'block')
SELECT
  row_key AS user_id,
  key AS tag_id,
  value AS tag_score
FROM
  real_time.hb_user_portrait
lateral view
  json_str_to_kv(tags_block) kv_list AS key, value
;

-- community 标签
INSERT OVERWRITE TABLE real_time.rt_user_portrait_tags_score PARTITION(tag_type = 'community')
SELECT
  row_key AS user_id,
  key AS tag_id,
  value AS tag_score
FROM
  real_time.hb_user_portrait
lateral view
  json_str_to_kv(tags_community) kv_list AS key, value
;

-- bedrooms 标签
INSERT OVERWRITE TABLE real_time.rt_user_portrait_tags_score PARTITION(tag_type = 'bedrooms')
SELECT
  row_key AS user_id,
  key AS tag_id,
  value AS tag_score
FROM
  real_time.hb_user_portrait
lateral view
  json_str_to_kv(tags_bedrooms) kv_list AS key, value
;

-- price 标签
INSERT OVERWRITE TABLE real_time.rt_user_portrait_tags_score PARTITION(tag_type = 'price')
SELECT
  row_key AS user_id,
  key AS tag_id,
  value AS tag_score
FROM
  real_time.hb_user_portrait
lateral view
  json_str_to_kv(tags_price) kv_list AS key, value
;
