create table dw_db.dim_broker (
  broker_uid string comment '经纪人user_id',
  account_id string comment '员工账户id',
  broker_name string comment '经纪人姓名',
  on_duty_date string comment '经纪人入职日期',
  leaving_date string comment '经纪人离职日期',
  status_id string comment '经纪人在职状态id',
  status string comment '经纪人在职状态',
  type_id string comment '经纪人类型id',
  type string comment '经纪人类型',
  category string comment '新房/二手房',
  city_id string comment '经纪人所在城市id',
  city_name string comment '经纪人所在城市',
  team_id string comment '经纪人部id',
  team_name string comment '经纪人部名称',
  agent_id string comment '中心id',
  agent_name string comment '中心名称',
  company_id string comment '中心公司id',
  company_name string comment '中心公司名称',
  phone string comment '经纪人电话号码',
  work_number string comment '工号',
  email_address string comment '邮箱',
  id_number string comment '身份证号'
) partitioned by (p_dt string comment '分区日期');

alter table dw_db.dim_broker add columns(
  company_type string comment '公司类型 1直营公司 2合作公司'
);
