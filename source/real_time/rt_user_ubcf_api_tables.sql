-- ubcf hbase api tables

-- Hbase 映射表, 映射算法 ubcf 计算出来的数据
CREATE EXTERNAL TABLE IF NOT EXISTS real_time.hb_user_ubcf(
  row_key String comment 'user key',
  user_relation string comment '关联用户',
  user_recommend string comment '推荐item')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = "relation:userRelation,recommend:userRecommend")
TBLPROPERTIES (
  "hbase.table.name" = "userUBCF")
;


-- 用户推荐数据
DROP TABLE IF EXISTS real_time.rt_user_recommend_ubcf;
CREATE TABLE IF NOT EXISTS real_time.rt_user_recommend_ubcf AS
SELECT
  -- user_id 和 city_id 组成的 uesr_key
  row_key AS user_key,
  -- 当前用户的 user_id 和 city_id
  split(row_key,'-')[0] AS user_id,
  split(row_key,'-')[1] AS user_city_id,
  -- 相似关联用户 user_id 和 city_id
  split(user_recommend_list,':')[1] AS relation_user_key,
  -- 相似关联用户 相似次数
  split(user_recommend_list,':')[2] AS relation_user_pf,
  -- 相似关联用户 喜欢 item_id 即 inventory_id
  split(user_recommend_list,':')[3] AS recommend_item_id,
  -- 相似关联用户 喜欢 item_id 即 inventory_id 的浏览次数
  split(user_recommend_list,':')[4] AS recommend_item_pf
FROM
  -- 这是一张 Hbase 的映射表
  real_time.hb_user_ubcf
-- 列传行
lateral view
  explode (split(user_recommend,',')) now_row_list AS user_recommend_list
;



-- 用户关联数据
DROP TABLE IF EXISTS real_time.rt_user_relation_user ;
CREATE TABLE IF NOT EXISTS real_time.rt_user_relation_user AS
SELECT
  -- user_id 和 city_id 组成的 uesr_key
  row_key AS user_key,
  -- user_id
  split(row_key,'-')[0] AS user_id,
  -- user_city_id
  split(row_key,'-')[1] AS user_city_id,
  -- 关联 user 的 user_key(user_id 和 city_id)
  split(user_relation_list,':')[1] AS relation_user_key,
  -- 关联 user 的 相似度次数
  split(user_relation_list,':')[2] AS relation_user_pf
FROM
  -- 这是一张 Hbase 的映射表
  real_time.hb_user_ubcf
-- 行转列
lateral view
  explode (split(user_relation,',')) now_row_list AS user_relation_list
;
