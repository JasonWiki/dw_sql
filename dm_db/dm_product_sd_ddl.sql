CREATE TABLE IF NOT EXISTS dm_db.dm_product_sd (
cal_dt string comment '业务日期',
platform string comment '平台',
city_id string comment '城市id',
city_name string comment '城市名称',
ud string comment 'ud',
fud string comment 'fud',
log_ud string comment '登陆ud',
come_rate string comment '当日用户次日返回率',
reg_user_cnt string comment '手机注册成功人数',
list_ud string comment '列表页ud',
list_pv string comment '列表页pv',
vpud string comment '单页ud',
vppv string comment '单页pv',
list_vpud string comment '单页ud-from列表页',
list_vppv string comment '单页pv-from列表页',
vp_wechat_cnt string comment '单页连接对数',
need_user_cnt string comment '发需求用户数',
need_cnt string comment '发需求数'
)  PARTITIONED BY (p_dt STRING comment '分区日期')
STORED AS ORC;
