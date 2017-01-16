-- ibcf hbase api tables

-- Hbase 映射表, 隐射推进算法 ibcf 计算出来的数据
CREATE EXTERNAL TABLE IF NOT EXISTS real_time.hb_community_ibcf(
  row_key int,
  community_recommend string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = "recommend:communityRecommend")
TBLPROPERTIES (
  "hbase.table.name" = "communityIBCF")
;

-- 读取 Hbase Table 中, 推荐算法算出的所有推荐房源数据
DROP TABLE IF EXISTS real_time.rt_community_recommend_ibcf;
CREATE TABLE IF NOT EXISTS real_time.rt_community_recommend_ibcf AS
SELECT
  row_key AS community_id,
  split(row_list,':')[1] AS community_rs_id,
  split(row_list,':')[2] AS community_rs_pf
FROM
  -- 这是一张 Hbase 的映射表
  real_time.hb_community_ibcf
-- 列传行
lateral view
  explode (split(community_recommend,',')) now_row_list AS row_list
;
