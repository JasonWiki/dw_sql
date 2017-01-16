-- 数据指标
CREATE TABLE IF NOT EXISTS dw_db.dw_data_quality (
  `db_name` String COMMENT 'db 名称',
  `tb_name` String COMMENT 'tb 名称',
  `kpi_name` String COMMENT '指标名称',
  `kpi_introduce` String COMMENT '指标说明',
  `kpi_num` String COMMENT '指标数',
  `kpi_status` String COMMENT '数据状态 1 正常, 0 失败'
) PARTITIONED BY (
  `p_dt` String COMMENT '分区日期')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY '\n';

-- 插入数据
INSERT OVERWRITE TABLE dw_db.dw_data_quality PARTITION (p_dt = ${dealDate})

--- dw_db 层 start ---
-- dw_app_access_log
SELECT
  'dw_db' AS db_name,
  'dw_app_access_log' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_app_access_log
WHERE p_dt = ${dealDate}


-- dw_app_action_detail_log
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_app_action_detail_log' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_app_action_detail_log
WHERE p_dt = ${dealDate}


-- dw_web_action_detail_log
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_web_action_detail_log' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_web_action_detail_log
WHERE p_dt = ${dealDate}


-- dw_web_action_detail_log
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_web_visit_traffic_log' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_web_visit_traffic_log
WHERE p_dt = ${dealDate}


-- broker 主题宽表
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_broker_sd' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM
  dw_db.dw_broker_sd
WHERE
  p_dt = ${dealDate}


-- 查询有数据出现警告
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_property_inventory' AS tb_name,
  'duplicate_data' AS kpi_name,
  '重复数据' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 0
    ELSE
      1
  END AS status
FROM (
  SELECT
    inventory_id,
    count(*) AS num
  FROM
    dw_db.dw_property_inventory
  GROUP BY
    inventory_id
  HAVING
    count(*) > 1
) AS dw_inventory


-- 房源宽表
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_property_inventory_sd' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM
  dw_db.dw_property_inventory_sd
WHERE
  p_dt = ${dealDate}


-- 房源宽表 inventory_id 查询有数据出现警告
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_property_inventory_sd' AS tb_name,
  'duplicate_data' AS kpi_name,
  '重复数据' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 0
    ELSE
      1
  END AS status
FROM (
  SELECT
    inventory_id,
    count(*) AS num
  FROM
    dw_db.dw_property_inventory_sd
  WHERE
    p_dt = ${dealDate}
  GROUP BY
    inventory_id
  -- 筛选聚合后的数据
  HAVING
    COUNT(*) > 1
) AS dw_property_inventory_sd


-- 小区宽表
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_property_community_sd' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_property_community_sd
WHERE p_dt = ${dealDate}


-- 小区宽表 community_id 查询有数据出现警告
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_property_community_sd' AS tb_name,
  'duplicate_data' AS kpi_name,
  '重复数据' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 0
    ELSE
      1
  END AS status
FROM (
  SELECT
    community_id,
    count(*) AS num
  FROM
    dw_db.dw_property_community_sd
  WHERE
    p_dt = ${dealDate}
  GROUP BY
    community_id
  -- 筛选聚合后的数据
  HAVING
    COUNT(*) > 1
) AS dw_property_community



-- 检查是否有前一天的数据
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_sem_sd' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_sem_sd
WHERE p_dt = ${dealDate}


-- 检查是否有前一天的数据
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_sem_baidu_sd' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_name,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_sem_baidu_sd
WHERE p_dt = ${dealDate}


-- 检查是否有前一天的数据
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_property_inventory_recommend_d' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_name,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_property_inventory_recommend_d
WHERE p_dt = ${dealDate}


-- 检查是否有前一天的数据
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_user_recommend_d' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_name,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_user_recommend_d
WHERE p_dt = ${dealDate}


-- 检查是否有前一天的数据
UNION ALL
SELECT
  'dw_db' AS db_name,
  'dw_user_sd' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_name,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM dw_db.dw_user_sd
WHERE p_dt = ${dealDate}

--- dw_db 层 End ---





--- DM 监控 start ---

-- 顾问智能配盘
UNION ALL
SELECT
  'dm_db' AS db_name,
  'dm_broker_user_mate_inventory' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM
  dm_db.dm_broker_user_mate_inventory
WHERE
  p_dt = ${dealDate}

--- DM 监控 end ---






--- DA 监控 start ---

-- broker 业务表
UNION ALL
SELECT
  'da_db' AS db_name,
  'da_broker_summary_basis_info_daily' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  COUNT(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM
  da_db.da_broker_summary_basis_info_daily
WHERE
  p_dt = ${dealDate}


-- 查询有数据出现警告
UNION ALL
SELECT
  'da_db' AS db_name,
  'da_mobile_chat_effect_info' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM
  da_db.da_mobile_chat_effect_info
WHERE cal_dt = ${dealDate}


-- 查询有数据出现警告
UNION ALL
SELECT
  'da_db' AS db_name,
  'da_property_inventory_recommend' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM da_db.da_property_inventory_recommend


-- 查询有数据出现警告
UNION ALL
SELECT
  'da_db' AS db_name,
  'da_user_inventory_recommend_cbcf' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM db_sync.da_db__da_user_inventory_recommend_cbcf




-- 查询有数据出现警告
UNION ALL
SELECT
  'da_db' AS db_name,
  'da_user_inventory_recommend_ubcf' AS tb_name,
  'count_num_day' AS kpi_name,
  '有数据正常' AS kpi_introduce,
  count(*) AS kpi_num,
  CASE
    WHEN COUNT(*) > 0
      THEN 1
    ELSE
      0
  END AS status
FROM da_db.da_user_inventory_recommend_ubcf

--- DA 监控 end ---
;




-- 导入到 mysql，(不执行的语句不要有分号分隔符)
--export hive dw_db.dw_data_quality to mysql dw_db.dw_data_quality partition p_dt
