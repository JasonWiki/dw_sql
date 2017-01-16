-- Mysql 作业

-- 获取当天运行日志
DROP TABLE IF EXISTS dw_db_temp.jason_extract_run_log_day;
CREATE TABLE IF NOT EXISTS dw_db_temp.jason_extract_run_log_day AS
SELECT *
FROM dw_service.extract_log AS log
WHERE date_format(created_at,'%Y-%m-%d') = date_format(now(),'%Y-%m-%d')
;


-- 查询抽取表详情
DROP TABLE IF EXISTS dw_db_temp.jason_extract_job_d;
CREATE TABLE IF NOT EXISTS dw_db_temp.jason_extract_job_d AS
SELECT
 -- 基础信息
 a.id, a.db_server, a.db_name, a.tb_name,

 -- 增量表信息
 b.tb_id, b.primary_key, b.incremental_field, b.incremental_val, b.conditions,

 -- 运行状态
 (SELECT code
   FROM dw_db_temp.jason_extract_run_log_day AS log
   WHERE log.db_server = a.db_server
     AND log.db_name = a.db_name
     AND log.tb_name = a.tb_name
   ORDER BY
     id DESC
   LIMIT 1
 ) AS run_code,

 -- 运行时间
 (SELECT run_time
   FROM dw_db_temp.jason_extract_run_log_day AS log
   WHERE log.db_server = a.db_server
     AND log.db_name = a.db_name
     AND log.tb_name = a.tb_name
   ORDER BY
     id DESC
   LIMIT 1
 ) AS run_time,

 -- 抽取类型
 (SELECT extract_type
   FROM dw_db_temp.jason_extract_run_log_day AS log
   WHERE log.db_server = a.db_server
     AND log.db_name = a.db_name
     AND log.tb_name = a.tb_name
   ORDER BY
     id DESC
   LIMIT 1
 ) AS extract_type,

 -- 抽取工具
 (SELECT extract_tool
   FROM dw_db_temp.jason_extract_run_log_day AS log
   WHERE log.db_server = a.db_server
     AND log.db_name = a.db_name
     AND log.tb_name = a.tb_name
   ORDER BY
     id DESC
   LIMIT 1
 ) AS extract_tool

FROM dw_service.extract_table AS a
LEFT JOIN dw_service.extract_table_ext AS b
  ON a.id=b.tb_id
WHERE a.is_delete = 0
ORDER BY
  run_time DESC
;


-- 导入数据
DELETE FROM dw_service.extract_job_sd WHERE p_dt = date_format(now(),'%Y-%m-%d');
INSERT INTO dw_service.extract_job_sd
SELECT
 a.*,
 date_format(now(),'%Y-%m-%d') AS p_dt
FROM dw_db_temp.jason_extract_job_d AS a;
