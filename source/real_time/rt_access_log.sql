-- AccessLog 实时原始日志收集表
-- 外部表
CREATE EXTERNAL TABLE IF NOT EXISTS real_time.rt_access_log(
  `request_time` string COMMENT 'from deserializer',
  `upstream_response_time` string COMMENT 'from deserializer',
  `remote_addr` string COMMENT 'from deserializer',
  `request_length` string COMMENT 'from deserializer',
  `upstream_addr` string COMMENT 'from deserializer',
  `server_date` string COMMENT 'from deserializer',
  `server_time` string COMMENT 'from deserializer',
  `hostname` string COMMENT 'from deserializer',
  `method` string COMMENT 'from deserializer',
  `request_uri` string COMMENT 'from deserializer',
  `http_code` string COMMENT 'from deserializer',
  `bytes_sent` string COMMENT 'from deserializer',
  `http_referer` string COMMENT 'from deserializer',
  `user_agent` string COMMENT 'from deserializer',
  `gzip_ratio` string COMMENT 'from deserializer',
  `http_x_forwarded_for` string COMMENT 'from deserializer',
  `auth` string COMMENT 'from deserializer',
  `mobile_agent` string COMMENT 'from deserializer'
) PARTITIONED BY (p_dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  'input.regex'='([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t\\[(.+?)T(.+?)\\+.*?\\]\\t([^\\t]*)\\t([^\\s]*)\\s([^\\s]*)\\s[^\\t]*\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*).*',
  'output.format.string'='%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s %14$s %15$s %16$s %17$s %18$s'
)
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;




-- 分区加载外部数据
-- ALTER TABLE real_time.rt_access_log ADD IF NOT EXISTS PARTITION  (p_dt = ${dealDate}) LOCATION '/flume/access_log/access_log_${baseDealDate}'
ALTER TABLE real_time.rt_access_log ADD IF NOT EXISTS PARTITION  (p_dt = ${dealDate}) LOCATION '/flume/access_log/access_log_${baseDealDate}';
