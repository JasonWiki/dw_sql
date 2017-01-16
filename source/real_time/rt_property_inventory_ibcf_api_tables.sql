-- ibcf hbase api tables

-- Hbase 映射表, 隐射推进算法 ibcf 计算出来的数据
CREATE EXTERNAL TABLE IF NOT EXISTS real_time.hb_inventory_ibcf(
  row_key int,
  inventory_recommend string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = "recommend:inventoryRecommend")
TBLPROPERTIES (
  "hbase.table.name" = "inventoryIBCF")
;

-- 读取 Hbase Table 中, 推荐算法算出的所有推荐房源数据
DROP TABLE IF EXISTS real_time.rt_property_inventory_recommend_ibcf;
CREATE TABLE IF NOT EXISTS real_time.rt_property_inventory_recommend_ibcf AS
SELECT
  row_key AS inventory_id,
  split(row_list,':')[1] AS inventory_rs_id,
  split(row_list,':')[2] AS inventory_rs_pf
FROM
  -- 这是一张 Hbase 的映射表
  real_time.hb_inventory_ibcf
-- 行转列
lateral view
  -- explode (split(substr(inventory_recommend_inventory__inventory_ids,0,length(inventory_recommend_inventory__inventory_ids)-1),',')) now_row_list AS row_list
  explode (split(inventory_recommend,',')) now_row_list AS row_list
;
