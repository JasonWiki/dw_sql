-- Mysql 作业
-- 统计 Hive Table 数据

-- 创建主表
CREATE TABLE IF NOT EXISTS test.hive_table_history (
  `db_id` bigint(20) NOT NULL DEFAULT '0',
  `db_name` varchar(500) NOT NULL DEFAULT '',
  `db_location_uri` varchar(4000) NOT NULL DEFAULT '',
  `db_owner` varchar(500) NOT NULL DEFAULT '',
  `db_desc` varchar(4000) NOT NULL DEFAULT '',
  `tbl_Id` bigint(20) NOT NULL DEFAULT '0',
  `sd_id` bigint(20) NOT NULL DEFAULT '0',
  `tbl_name` varchar(128) NOT NULL DEFAULT '',
  `tbl_type` varchar(128) NOT NULL DEFAULT '',
  `tbl_create_time` int(11) NOT NULL DEFAULT '0',
  `tbl_owner` varchar(500) NOT NULL DEFAULT '',
  `tbl_location` varchar(4000) NOT NULL DEFAULT '',
  `tbl_last_time_stamp` int(11) NOT NULL DEFAULT '0',
  `tbl_last_time` varchar(50) NOT NULL DEFAULT '',
  `tbl_size` varchar(50) NOT NULL DEFAULT '',
  `p_dt` char(10) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DELETE FROM test.hive_table_history WHERE p_dt = ${dealDate};
-- 创建数据
INSERT INTO test.hive_table_history
SELECT
  db_tb.DB_ID,
  db_tb.NAME,
  db_tb.DB_LOCATION_URI,
  db_tb.OWNER_NAME,
  db_tb.DESC,
  tb_tb.TBL_ID,
  tb_tb.SD_ID,
  tb_tb.TBL_NAME,
  tb_tb.TBL_TYPE,
  tb_tb.CREATE_TIME,
  tb_tb.OWNER AS tb_OWNER,
  tb_sds.LOCATION,
  tb_params_2.PARAM_VALUE,
  FROM_UNIXTIME(tb_params_2.PARAM_VALUE,'%Y-%m-%d %h:%i:%s') AS lastTime,
  tb_params_1.PARAM_VALUE,
  ${dealDate} AS p_dt
FROM
  hive.DBS AS db_tb

LEFT JOIN
  hive.TBLS AS tb_tb
ON
  db_tb.DB_ID = tb_tb.DB_ID

LEFT JOIN
  hive.SDS AS tb_sds
ON
  tb_sds.SD_ID = tb_tb.SD_ID

-- 数据表大小
LEFT JOIN
  hive.TABLE_PARAMS AS tb_params_1
ON
  tb_params_1.TBL_ID = tb_tb.TBL_ID
AND
  tb_params_1.PARAM_KEY = 'totalSize'

-- 数据表最后一次修改时间
LEFT JOIN
  hive.TABLE_PARAMS AS tb_params_2
ON
  tb_params_2.TBL_ID = tb_tb.TBL_ID
AND
  tb_params_2.PARAM_KEY = 'transient_lastDdlTime'
;
