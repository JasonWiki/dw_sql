-- IP 访问统计
DROP TABLE IF EXISTS dw_temp_angejia.jason_angejia_access_log_ip_total;
CREATE TABLE dw_temp_angejia.jason_angejia_access_log_ip_total AS
SELECT
  remote_addr,
  COUNT(*) AS num
FROM
  real_time.rt_access_log
WHERE p_dt = ${dealDate}
GROUP BY
  remote_addr
ORDER BY
  num DESC
;



-- user_agent 的统计数据
DROP TABLE IF EXISTS dw_temp_angejia.jason_angejia_access_log_user_agent_total;
CREATE TABLE  IF NOT EXISTS dw_temp_angejia.jason_angejia_access_log_user_agent_total AS
SELECT
  user_agent,
  COUNT(*) as num
FROM
  real_time.rt_access_log
WHERE p_dt = ${dealDate}
GROUP BY
  user_agent
ORDER BY
  num DESC
;


export hive dw_temp_angejia.jason_angejia_access_log_ip_total to mysql dw_temp_angejia.jason_angejia_access_log_ip_total
export hive dw_temp_angejia.jason_angejia_access_log_user_agent_total to mysql dw_temp_angejia.jason_angejia_access_log_user_agent_total
