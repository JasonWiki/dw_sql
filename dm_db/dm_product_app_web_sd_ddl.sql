CREATE TABLE IF NOT EXISTS dm_db.dm_product_app_web_sd (
cal_dt string comment '业务日期',
platform string comment '平台',
version string comment '版本',
product_type string comment '产品类型',
city_id string comment '城市id',
type string comment '类型',
id string comment 'action or page id',
name string comment 'action or page name',
pv string comment 'pv',
ud string comment 'ud',
city_name string comment '城市名'
) PARTITIONED BY (p_dt STRING)
STORED AS ORC;
