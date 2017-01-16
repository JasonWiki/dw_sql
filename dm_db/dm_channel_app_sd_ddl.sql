CREATE TABLE IF NOT EXISTS dm_db.dm_channel_app_sd (
channel_id string comment '渠道id',
channel_name string comment '渠道名',
platform string comment '平台',
city_id string comment '城市id',
city_name string comment '城市名',
td_fud bigint comment '当天fud',
td_ud bigint comment '当天ud',
td_log_fud bigint comment '当天登录fud',
td_log_ud bigint comment '当天登录ud',
td_vppv bigint comment '当天vppv',
td_vpud bigint comment '当天vpud',
td_new_conn_cnt bigint comment '日新连接数(含录入)',
td_new_conn_esf_cnt bigint comment '二手房日新连接数',
td_new_conn_xf_cnt bigint comment '新房日新连接数',
td_new_wechat_cnt bigint comment '当天首次微聊对数', --新房+二手房
td_new_wechat_esf_cnt bigint comment '二手房当天首次微聊对数',
td_new_wechat_xf_cnt bigint comment '新房当天首次微聊对数',
td_call_pairs_cnt bigint comment '当天电话对数', --新房+二手房
td_call_pairs_esf_cnt bigint comment '二手房当天电话对数',
td_call_pairs_xf_cnt bigint comment '新房当天电话对数',
td_assigned_call_buyer_cnt bigint comment '当天派电话录入私客数',
30_fud bigint comment '30天fud',
30_ud bigint comment '30天ud',
30_conn_user_cnt bigint comment '近30天连接ud',
30_new_buyer_cnt bigint comment '近30天录入ud',
30_visit_user_cnt bigint comment '近30天带看ud',
30_trans_user_cnt bigint comment '近30天成交ud',
td_new_conn_ud bigint comment '日新连接ud'
) PARTITIONED BY (p_dt STRING COMMENT '分区日期')
STORED AS ORC;

alter table dm_db.dm_channel_app_sd add columns (
  td_xf_loupan_subscribe_ud bigint comment '当天新房订阅用户数'
);
