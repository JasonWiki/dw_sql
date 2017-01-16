create table dw_db.dw_app_action_version_daily_summary (
  app_name string comment '产品',
  version string comment '版本',
  action_id string comment 'action id',
  action_name string comment 'action名',
  ud int comment 'ud',
  fud int comment 'fud',
  log_ud int comment '登录ud',
  log_fud int comment '登录fud',
  pv int comment 'pv',
  fpv int comment 'fpv',
  log_pv int comment '登录pv',
  log_fpv int comment '登录fpv'
) partitioned by (p_dt string comment '分区日期');
