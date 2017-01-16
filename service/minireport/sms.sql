-- 组合准备分析的数据
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_monitor_daily_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_monitor_daily_info AS
SELECT
  sms.id,
  sms.phone,
  sms.biz_id,
  sms.content,
  sms.status,
  sms.created_at,
  sms.updated_at,
  sms.mt_msg_id,
  sms.deliver_at,
  sms.mt_stat,
  sms_send_channel.channel AS channel,
  sms_send_channel.phone AS channel_phone,
  sms_channel_map.channel_des AS channel_des,
  sms_biz_map.biz_des as biz_des
FROM
  db_sync.angejia__sms AS sms

-- 短信通道，可能会发送 1 次以上。所以记录到总量里面
LEFT JOIN
  db_sync.angejia__sms_send_channel AS sms_send_channel
ON
  sms.id = sms_send_channel.sms_id

-- 短信通道名称
LEFT JOIN
  db_sync.angejia__sms_channel_map AS sms_channel_map
ON
  sms_channel_map.channel = sms_send_channel.channel

-- 短信业务类型
LEFT JOIN
  db_sync.angejia__sms_biz_map AS sms_biz_map
ON
  sms_biz_map.biz_id = sms.biz_id

WHERE
  to_date(sms.created_at) = ${dealDate}
AND
  to_date(sms_send_channel.created_at)= ${dealDate}
;


-- 发送成功，有回调信息的短信
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_monitor_success_daily_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_monitor_success_daily_info AS
SELECT
  id,
  phone,
  biz_id,
  content,
  status,
  created_at,
  updated_at,
  mt_msg_id,
  deliver_at,
  mt_stat,
  channel,
  channel_des,
  biz_des,
  -- 正确的短信
  '1' AS msg_type
FROM
  dw_temp_angejia.jason_sms_monitor_daily_info
WHERE
  mt_stat IN ('DELIVRD','ET:0265')
;


-- 营销类短信，排除成功的短信
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_monitor_EMD_daily_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_monitor_EMD_daily_info AS
SELECT
  bs.id,
  bs.phone,
  bs.biz_id,
  bs.content,
  bs.status,
  bs.created_at,
  bs.updated_at,
  bs.mt_msg_id,
  bs.deliver_at,
  bs.mt_stat,
  bs.channel,
  bs.channel_des,
  bs.biz_des,
  -- 营销类短信
  '2' AS msg_type
FROM
  dw_temp_angejia.jason_sms_monitor_daily_info AS bs

-- 排除发送成功的短信
LEFT JOIN (
  SELECT id FROM dw_temp_angejia.jason_sms_monitor_success_daily_info
) AS t_1
ON
  bs.id = t_1.id
WHERE
  t_1.id IS NULL
AND
  -- 01 通道表示营销类短信
  bs.channel = '01'
;


-- 发送失败的短信
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_monitor_error_daily_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_monitor_error_daily_info AS
SELECT
  bs.id,
  bs.phone,
  bs.biz_id,
  bs.content,
  bs.status,
  bs.created_at,
  bs.updated_at,
  bs.mt_msg_id,
  bs.deliver_at,
  bs.mt_stat,
  bs.channel,
  bs.channel_des,
  bs.biz_des,
  -- 错误的短信
  '0' AS msg_type
FROM
  dw_temp_angejia.jason_sms_monitor_daily_info AS bs

-- 排除发送成功、营销类短信
LEFT JOIN (
  SELECT id FROM dw_temp_angejia.jason_sms_monitor_success_daily_info
    UNION ALL
  SELECT id FROM dw_temp_angejia.jason_sms_monitor_EMD_daily_info
) AS t_1
ON
  bs.id = t_1.id
WHERE
  t_1.id IS NULL
;



-- 汇总数据
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_monitor_summary_daily_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_monitor_summary_daily_info AS
SELECT
  *
FROM
  dw_temp_angejia.jason_sms_monitor_success_daily_info

  UNION ALL

SELECT
  *
FROM
  dw_temp_angejia.jason_sms_monitor_EMD_daily_info

  UNION ALL

SELECT
  *
FROM
  dw_temp_angejia.jason_sms_monitor_error_daily_info
;


--- 通道分析指标
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_channel_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_channel_info AS
SELECT
  channel AS channel_code,
  channel_des AS channel_name,
  -- 各个通道总数
  COUNT(*) AS channel_count,
  -- 每个通道成功数
  COUNT(
    CASE WHEN msg_type = '1'
      THEN 1
    END
  ) AS channel_success_count
FROM
  dw_temp_angejia.jason_sms_monitor_summary_daily_info
GROUP BY
  channel,
  channel_des
;


--- 各个业务 biz 指标统计
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_biz_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_biz_info AS
SELECT
  biz_id AS biz_code,
  biz_des AS biz_name,
  -- 各个通道总数
  COUNT(*) AS biz_count,
  -- 每个通道成功数
  COUNT(
    CASE WHEN msg_type = '1'
      THEN 1
    END
  ) AS biz_success_count
FROM
  dw_temp_angejia.jason_sms_monitor_summary_daily_info
GROUP BY
  biz_id,
  biz_des
ORDER BY
  int(biz_id) ASC
;


--- 用户注册短信、验证码分析
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_code_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_code_info AS
SELECT
  -- 用户注册类短信发送数
  COUNT(*) AS sms_code_count,

  -- 发送成功的
  COUNT(
    CASE WHEN msg_type = 1
      THEN 1
    END
  ) AS sms_code_success_count,

  -- 5 秒到达数总数
  COUNT(
    CASE WHEN msg_type = 1
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 5)
      THEN 1
    END
  ) AS sms_code_success_5s_count,

  -- 30 秒到达数总数
  COUNT(
    CASE WHEN msg_type = 1
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 30)
      THEN 1
    END
  ) AS sms_code_success_30s_count,

  -- 60 秒到达数总数
  COUNT(
    CASE WHEN msg_type = 1
    AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 60)
      THEN 1
    END
  ) AS sms_code_success_60s_count,


--- 移通平台 code 分析
  -- 平台发送总数
  COUNT(
    CASE WHEN channel = '00'
      THEN 1
    END
  ) AS sms_code_channel_00_count,

  -- 平台发送成功总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = '00'
      THEN 1
    END
  ) AS sms_code_channel_00_success_count,

  -- 5 秒到达率总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = '00'
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 5)
      THEN 1
    END
  ) AS sms_code_channel_00_success_5s_count,

  -- 30 秒到达率总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = '00'
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 30)
      THEN 1
    END
  ) AS sms_code_channel_00_success_30s_count,

  -- 60 秒到达率总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = '00'
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 60)
      THEN 1
    END
  ) AS sms_code_channel_00_success_60s_count,


--- 华信平台 code 分析
  -- 平台发送总数
  COUNT(
    CASE WHEN channel = 'hx'
      THEN 1
    END
  ) AS sms_code_channel_hx_count,

  -- 平台发送成功总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = 'hx'
      THEN 1
    END
  ) AS sms_code_channel_hx_success_count,

  -- 5 秒到达率总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = 'hx'
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 5)
      THEN 1
    END
  ) AS sms_code_channel_hx_success_5s_count,

  -- 30 秒到达率总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = 'hx'
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 30)
      THEN 1
    END
  ) AS sms_code_channel_hx_success_30s_count,

  -- 60 秒到达率总数
  COUNT(
    CASE WHEN msg_type = 1
      AND channel = 'hx'
      AND (unix_timestamp(deliver_at) - unix_timestamp(created_at) <= 60)
      THEN 1
    END
  ) AS sms_code_channel_hx_success_60s_count

FROM
  dw_temp_angejia.jason_sms_monitor_summary_daily_info
WHERE
  biz_id = '1'
;





--- 短信指标详情
DROP TABLE IF EXISTS dw_temp_angejia.jason_sms_detail_info;
CREATE TABLE IF NOT EXISTS dw_temp_angejia.jason_sms_detail_info AS
SELECT
--- 一 总量
  COUNT(*) AS sms_count,

--- 二 成功发送短信总量
  -- 成功总量
  COUNT (
    CASE WHEN msg_type = 1
      THEN 1
    END
  ) AS sms_success_count,

  -- 5 秒到达总数
  COUNT(
    CASE WHEN msg_type = 1 AND (unix_timestamp(deliver_at)-unix_timestamp(created_at) <= 5)
      THEN 1
    END
  ) AS sms_success_5s_count,

  -- 30 秒到达总数
  COUNT(
    CASE WHEN msg_type = 1 AND (unix_timestamp(deliver_at)-unix_timestamp(created_at) <= 30)
      THEN 1
    END
  ) AS sms_success_30s_count,

  -- 60 秒到达总数
  COUNT(
    CASE WHEN msg_type = 1 AND (unix_timestamp(deliver_at)-unix_timestamp(created_at) <= 60)
      THEN 1
    END
  ) AS sms_success_60s_count,

  -- 90 秒到达总数
  COUNT(
    CASE WHEN msg_type = 1 AND (unix_timestamp(deliver_at)-unix_timestamp(created_at) <= 90)
      THEN 1
    END
  ) AS sms_success_90s_count,


--- 三 营销通道短信
  COUNT(
    CASE WHEN msg_type = 2
      THEN 1
    END
  ) AS sms_emd_count,


-- 四 失败短信分析
  -- 失败总数
  COUNT(
    CASE WHEN msg_type = 0
      THEN 1
    END
  ) AS sms_error_count,

  -- 已发送,未到达
  COUNT(
    CASE WHEN msg_type = 0 AND mt_stat = '' AND status = 2
      THEN 1
    END
  ) AS sms_error_count_2,

  -- MSISDN 号码段不存在
  COUNT(
    CASE WHEN msg_type = 0 AND mt_stat = 'ET:0201'
      THEN 1
    END
  ) AS sms_error_count_ET0201,

  -- 配额不足
  COUNT(
    CASE WHEN msg_type = 0 AND mt_stat = 'ET:0250'
      THEN 1
    END
  ) AS sms_error_count_ET0250,

  -- 其他错误
  COUNT(
    CASE WHEN msg_type = 0 AND (mt_stat NOT IN('ET:0201','ET:0250') AND status NOT IN ('1','2'))
      THEN 1
    END
  ) AS sms_error_count_other
FROM
  dw_temp_angejia.jason_sms_monitor_summary_daily_info
;



-- 短信通道
export hive dw_temp_angejia.jason_sms_channel_info to mysql dw_temp_angejia.jason_sms_channel_info

-- 各业务 biz
export hive dw_temp_angejia.jason_sms_biz_info to mysql dw_temp_angejia.jason_sms_biz_info

-- 短信验证码分析
export hive dw_temp_angejia.jason_sms_code_info to mysql dw_temp_angejia.jason_sms_code_info

-- 短信宏观指标
export hive dw_temp_angejia.jason_sms_detail_info to mysql dw_temp_angejia.jason_sms_detail_info

-- 失败详情
export hive dw_temp_angejia.jason_sms_monitor_error_daily_info to mysql dw_temp_angejia.jason_sms_monitor_error_daily_info
