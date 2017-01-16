create table dm_db.dm_newland_demand_process (
  platform string comment '平台ios/android',
  version string comment '版本',
  type string comment 'ud or fud',
  wechat_onview_ud int comment '登录页面ud',
  wechat_clicklogin_ud int comment '微信登录ud',
  wechat_phonelogin_ud int comment '手机登录ud',
  demand_budget_onview_ud int comment '打开预算ud',
  demand_type_onview_ud int comment '打开户型ud',
  demand_position_onview_ud int comment '打开位置ud',
  firstpage_onview_ud int comment '打开首页ud',
  firstpage_clickall_ud int comment '首页全部二手房ud',
  firstpage_no_demand_ud int comment '首页全部二手房未发需求ud',
  firstpage_send_demand_ud int comment '首页全部二手房已发需求ud',
  demand_ud int comment '提需求ud',
  connection_ud int comment '连接ud'
) partitioned by (p_dt string comment '分区日期');
