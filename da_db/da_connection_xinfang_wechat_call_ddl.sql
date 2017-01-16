create table da_db.da_connection_xinfang_wechat_call (
  city_id string comment '城市id',
  city_name string comment '城市名',
  center_id string comment '中心id',
  center_name string comment '中心名',
  department_id string comment '部门id',
  department_name string comment '部门名',
  account_id string comment '账户id',
  broker_id string comment '经纪人id',
  broker_name string comment '经纪人姓名',
  all_weichat_num int comment '新房总微聊对数',
  dispatch_weichat_num int comment '新房派单微聊对数',
  detail_weichat_num int comment '新房单页微聊对数',
  five_minute_reply_num int comment '5分钟回复数(9~23点)',
  thirty_minute_reply_num int comment '30分钟回复数(9~23点)',
  all_weichat_work_time_num int comment '新房总微聊对数(9~23点)',
  five_minute_reply_percentage decimal(4,3) comment '5分钟回复率(9~23点)',
  thirty_minute_reply_percentage decimal(4,3) comment '30分钟回复率(9~23点)',
  come_call_num int comment '来电数',
  hold_call_num int comment '接听数',
  call_connect_num decimal(4,3) comment '接通率',
  first_weichat_num int comment '首次微聊对数',
  weichat_buyer_num int comment '微聊-录入私客数',
  come_call_buyer_num int comment '来电-录入私客数',
  come_call_time_morethan int comment '来电-通话>=50秒',
  come_call_time_lessthan int comment '来电-通话<50秒'
) partitioned by (p_dt string comment '分区日期');


alter table da_db.da_connection_xinfang_wechat_call add columns (
  first_weichat_num int comment '首次微聊对数',
  weichat_buyer_num int comment '微聊-录入私客数',
  come_call_buyer_num int comment '来电-录入私客数',
  come_call_time_morethan int comment '来电-通话>=50秒',
  come_call_time_lessthan int comment '来电-通话<50秒'
);
