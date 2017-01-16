create table dw_db.dim_loupan (
  loupan_id int comment '楼盘id',
  loupan_name string comment '楼盘名',
  loupan_alias string comment '楼盘别名',
  address string comment '楼盘地址',
  city_id int comment '城市id',
  city_name string comment '城市名',
  district_id int comment '区域id',
  district_name string comment '区域名',
  block_id int comment '板块id',
  block_name string comment '板块名',
  loop_line string comment '楼盘所在环线',
  sale_status string comment '销售状态',
  display_status string comment '显示状态',
  partner_status string comment '合作状态',
  unit_price string comment '单价',
  selling_date string comment '开盘时间',
  handover_date string comment '交房日期',
  rank_level int comment '项目等级 1-三星 2-二星 3-一星 0-没有星级',
  rank_score int comment 'rank分值,分越高优先级越高',
  content_score int comment '内容得分',
  take_time string comment '拿地时间'
) partitioned by (p_dt string comment '分区日期');


alter table dw_db.dim_loupan add columns (
  content_score int comment '内容得分',
  take_time string comment '拿地时间'
);
