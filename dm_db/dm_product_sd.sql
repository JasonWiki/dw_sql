--fud
drop table if exists dw_db_temp.dm_product_sd_fud;
create table dw_db_temp.dm_product_sd_fud as
select
${dealDate} as p_dt,
count(distinct t.device_id) as fud,
count(distinct case when t.platform = 'android' then t.device_id end) as a_fud,
count(distinct case when t.platform = 'ios' then t.device_id end) as i_fud,
count(distinct case when t.city_id = 1 and t.platform = 'android' then t.device_id end) as sh_a_fud,
count(distinct case when t.city_id = 1 and t.platform = 'ios' then t.device_id end) as sh_i_fud,
count(distinct case when t.city_id = 2 and t.platform = 'android' then t.device_id end) as bj_a_fud,
count(distinct case when t.city_id = 2 and t.platform = 'ios' then t.device_id end) as bj_i_fud
from dw_db.dw_ud_fud t
where to_date(t.server_time) = ${dealDate}
--and t.city_id in (1,2) --only include SH,BJ for now
;

--ud,log_ud,pv,vpud,vppv,user_cnt
drop table if exists dw_db_temp.dm_product_sd_app_statistics;
create table dw_db_temp.dm_product_sd_app_statistics as
select
 ${dealDate} as p_dt
,count(distinct a.device_id) as ud
,count(distinct case when a.app_name = 'a-angejia' then a.device_id end) as a_ud
,count(distinct case when a.app_name = 'i-angejia' then a.device_id end) as i_ud
,count(distinct case when a.selection_city_id=1 and a.app_name = 'a-angejia' then a.device_id end) as sh_a_ud
,count(distinct case when a.selection_city_id=1 and a.app_name = 'i-angejia' then a.device_id end) as sh_i_ud
,count(distinct case when a.selection_city_id=2 and a.app_name = 'a-angejia' then a.device_id end) as bj_a_ud
,count(distinct case when a.selection_city_id=2 and a.app_name = 'i-angejia' then a.device_id end) as bj_i_ud
,count(distinct case when a.user_id <> '' then a.device_id end) as log_ud
,count(distinct case when a.user_id <> '' and a.app_name = 'a-angejia' then a.device_id end) as a_log_ud
,count(distinct case when a.user_id <> '' and a.app_name = 'i-angejia' then a.device_id end) as i_log_ud
,count(distinct case when a.user_id <> '' and a.selection_city_id=1 and a.app_name = 'a-angejia' then a.device_id end) as sh_a_log_ud
,count(distinct case when a.user_id <> '' and a.selection_city_id=1 and a.app_name = 'i-angejia' then a.device_id end) as sh_i_log_ud
,count(distinct case when a.user_id <> '' and a.selection_city_id=2 and a.app_name = 'a-angejia' then a.device_id end) as bj_a_log_ud
,count(distinct case when a.user_id <> '' and a.selection_city_id=2 and a.app_name = 'i-angejia' then a.device_id end) as bj_i_log_ud
,count(distinct case when a.request_page_id ='30005' then a.device_id end) as list_ud
,count(distinct case when a.request_page_id ='30005' and a.app_name = 'a-angejia' then a.device_id end) as a_list_ud
,count(distinct case when a.request_page_id ='30005' and a.app_name = 'i-angejia' then a.device_id end) as i_list_ud
,count(distinct case when a.request_page_id ='30005' and a.selection_city_id=1 and a.app_name = 'a-angejia' then a.device_id end) as sh_a_list_ud
,count(distinct case when a.request_page_id ='30005' and a.selection_city_id=1 and a.app_name = 'i-angejia' then a.device_id end) as sh_i_list_ud
,count(distinct case when a.request_page_id ='30005' and a.selection_city_id=2 and a.app_name = 'a-angejia' then a.device_id end) as bj_a_list_ud
,count(distinct case when a.request_page_id ='30005' and a.selection_city_id=2 and a.app_name = 'i-angejia' then a.device_id end) as bj_i_list_ud
,count(case when a.request_page_id ='30005' then a.device_id end) as list_pv
,count(case when a.request_page_id ='30005' and a.app_name = 'a-angejia'  then a.device_id end) as a_list_pv
,count(case when a.request_page_id ='30005' and a.app_name = 'i-angejia'  then a.device_id end) as i_list_pv
,count(case when a.request_page_id ='30005' and a.selection_city_id=1 and a.app_name = 'a-angejia'  then a.device_id end) as sh_a_list_pv
,count(case when a.request_page_id ='30005' and a.selection_city_id=1 and a.app_name = 'i-angejia'  then a.device_id end) as sh_i_list_pv
,count(case when a.request_page_id ='30005' and a.selection_city_id=2 and a.app_name = 'a-angejia'  then a.device_id end) as bj_a_list_pv
,count(case when a.request_page_id ='30005' and a.selection_city_id=2 and a.app_name = 'i-angejia'  then a.device_id end) as bj_i_list_pv
,count(distinct case when a.request_page_id in ('30074','30003') then a.device_id end) as vpud
,count(distinct case when a.request_page_id in ('30074','30003') and a.app_name = 'a-angejia' then a.device_id end) as a_vpud
,count(distinct case when a.request_page_id in ('30074','30003') and a.app_name = 'i-angejia' then a.device_id end) as i_vpud
,count(distinct case when a.request_page_id in ('30074','30003') and a.selection_city_id=1 and a.app_name = 'a-angejia' then a.device_id end) as sh_a_vpud
,count(distinct case when a.request_page_id in ('30074','30003') and a.selection_city_id=1 and a.app_name = 'i-angejia' then a.device_id end) as sh_i_vpud
,count(distinct case when a.request_page_id in ('30074','30003') and a.selection_city_id=2 and a.app_name = 'a-angejia' then a.device_id end) as bj_a_vpud
,count(distinct case when a.request_page_id in ('30074','30003') and a.selection_city_id=2 and a.app_name = 'i-angejia' then a.device_id end) as bj_i_vpud
,count(case when a.request_page_id in ('30074','30003') then a.device_id end) as vppv
,count(case when a.request_page_id in ('30074','30003') and a.app_name = 'a-angejia' then a.device_id end) as a_vppv
,count(case when a.request_page_id in ('30074','30003') and a.app_name = 'i-angejia' then a.device_id end) as i_vppv
,count(case when a.request_page_id in ('30074','30003') and a.selection_city_id=1 and a.app_name = 'a-angejia' then a.device_id end) as sh_a_vppv
,count(case when a.request_page_id in ('30074','30003') and a.selection_city_id=1 and a.app_name = 'i-angejia' then a.device_id end) as sh_i_vppv
,count(case when a.request_page_id in ('30074','30003') and a.selection_city_id=2 and a.app_name = 'a-angejia' then a.device_id end) as bj_a_vppv
,count(case when a.request_page_id in ('30074','30003') and a.selection_city_id=2 and a.app_name = 'i-angejia' then a.device_id end) as bj_i_vppv

,count(distinct case when b.user_id is not null then b.user_id end) as need_user_cnt
,count(distinct case when b.user_id is not null and a.app_name = 'a-angejia' then b.user_id end) as a_need_user_cnt
,count(distinct case when b.user_id is not null and a.app_name = 'i-angejia' then b.user_id end) as i_need_user_cnt
,count(distinct case when a.selection_city_id=1 and a.app_name = 'a-angejia' and b.user_id is not null then b.user_id end) as sh_a_need_user_cnt
,count(distinct case when a.selection_city_id=1 and a.app_name = 'i-angejia' and b.user_id is not null then b.user_id end) as sh_i_need_user_cnt
,count(distinct case when a.selection_city_id=2 and a.app_name = 'a-angejia' and b.user_id is not null then b.user_id end) as bj_a_need_user_cnt
,count(distinct case when a.selection_city_id=2 and a.app_name = 'i-angejia' and b.user_id is not null then b.user_id end) as bj_i_need_user_cnt
,count(distinct case when b.user_id is not null then b.log_id end) as need_cnt
,count(distinct case when b.user_id is not null and a.app_name = 'a-angejia' then b.log_id end) as a_need_cnt
,count(distinct case when b.user_id is not null and a.app_name = 'i-angejia' then b.log_id end) as i_need_cnt
,count(distinct case when a.selection_city_id=1 and a.app_name = 'a-angejia' and b.user_id is not null then b.log_id end) as sh_a_need_cnt
,count(distinct case when a.selection_city_id=1 and a.app_name = 'i-angejia' and b.user_id is not null then b.log_id end) as sh_i_need_cnt
,count(distinct case when a.selection_city_id=2 and a.app_name = 'a-angejia' and b.user_id is not null then b.log_id end) as bj_a_need_cnt
,count(distinct case when a.selection_city_id=2 and a.app_name = 'i-angejia' and b.user_id is not null then b.log_id end) as bj_i_need_cnt
from (
  select a.p_dt,
  a.app_name,
  a.user_id,
  a.selection_city_id,
  a.device_id,
  a.request_page_id
  from dw_db.dw_app_access_log a
  where a.p_dt=${dealDate}
  and a.request_uri not like '/mobile/member/configs%'
  and a.request_uri not like '/mobile/member/districts/show%'
  and a.request_uri not like '/mobile/member/inventories/searchFilters%'
  and a.request_uri not like '%/user/bind/push%'
  and a.request_uri not like '%/common/push/acks%'
  and a.hostname='api.angejia.com'
  and a.app_name in ('a-angejia','i-angejia')
) a
left outer join
(
  select to_date(a.created_at) as p_dt,
  a.user_id,
  a.log_id
  from db_sync.angejia__buyer_demand_push_batch a
  left outer join
  (
    select user_id from db_sync.angejia__buyer_type where type = 2
    union all
    select user_id from db_sync.angejia__buyer_demand_send_forbidden where deleted_at is null
  ) b on a.user_id = b.user_id
  where to_date(a.created_at) = ${dealDate} and a.type = 0 and b.user_id is null
) b on a.user_id = b.user_id
--where a.selection_city_id in (1,2) --only include SH,BJ for now
;


--app mobile register success people
drop table if exists dw_db_temp.dm_product_sd_reg_user_cnt;
create table dw_db_temp.dm_product_sd_reg_user_cnt as
select
 ${dealDate} as p_dt
,count(user_id) as reg_user_cnt
,count(case when platform = 'android' or platform is null then user_id end) as a_reg_user_cnt
,count(case when platform = 'ios' then user_id end) as i_reg_user_cnt
from dw_db.dw_user_sd
where member_source = 5 --app注册渠道
and p_dt = ${dealDate} and to_date(created_at) = ${dealDate}
;

--list to vp ud-pv
drop table if exists dw_db_temp.dm_product_sd_vpud_vppv;
create table dw_db_temp.dm_product_sd_vpud_vppv as
select
${dealDate} as p_dt,
count(distinct a.dvid) as list_vpud,
count(distinct case when a.name = 'a-angejia' then a.dvid end) as a_list_vpud,
count(distinct case when a.name = 'i-angejia' then a.dvid end) as i_list_vpud,
count(distinct case when b.selection_city_id = 1 and name = 'a-angejia' then a.dvid end) as sh_a_list_vpud,
count(distinct case when b.selection_city_id = 1 and name = 'i-angejia' then a.dvid end) as sh_i_list_vpud,
count(distinct case when b.selection_city_id = 2 and name = 'a-angejia' then a.dvid end) as bj_a_list_vpud,
count(distinct case when b.selection_city_id = 2 and name = 'i-angejia' then a.dvid end) as bj_i_list_vpud,
count(a.dvid) as list_vppv,
count(case when a.name = 'a-angejia' then a.dvid end) as a_list_vppv,
count(case when a.name = 'i-angejia' then a.dvid end) as i_list_vppv,
count(case when b.selection_city_id = 1 and a.name = 'a-angejia' then a.dvid end) as sh_a_list_vppv,
count(case when b.selection_city_id = 1 and a.name = 'i-angejia' then a.dvid end) as sh_i_list_vppv,
count(case when b.selection_city_id = 2 and a.name = 'a-angejia' then a.dvid end) as bj_a_list_vppv,
count(case when b.selection_city_id = 2 and a.name = 'i-angejia' then a.dvid end) as bj_i_list_vppv
from dw_db.dw_app_action_detail_log a
inner join (
  select distinct device_id,selection_city_id
  from dw_db.dw_app_access_log
  where selection_city_id in (1,2)
  and p_dt=${dealDate}
) b
on a.dvid=b.device_id
where a.p_dt=${dealDate} and a.action_id in ('1-100001') and a.bp_id in ('1-110000')
and a.name in ('a-angejia','i-angejia')
--and ccid in (1,2) --only include SH,BJ for now
;

--vp create connection
drop table if exists dw_db_temp.dm_product_sd_vp_wechat_cnt;
create table dw_db_temp.dm_product_sd_vp_wechat_cnt as
select
 ${dealDate} as p_dt
,count(distinct concat(a.user_id,'/',a.broker_uid)) as vp_wechat_cnt
,count(distinct case when name = 'a-angejia' then concat(a.user_id,'/',a.broker_uid) end) as a_vp_wechat_cnt
,count(distinct case when name = 'i-angejia' then concat(a.user_id,'/',a.broker_uid) end) as i_vp_wechat_cnt
,count(distinct case when ccid = 1 and name = 'a-angejia' then concat(a.user_id,'/',a.broker_uid) end) as sh_a_vp_wechat_cnt
,count(distinct case when ccid = 1 and name = 'i-angejia' then concat(a.user_id,'/',a.broker_uid) end) as sh_i_vp_wechat_cnt
,count(distinct case when ccid = 2 and name = 'a-angejia' then concat(a.user_id,'/',a.broker_uid) end) as bj_a_vp_wechat_cnt
,count(distinct case when ccid = 2 and name = 'i-angejia' then concat(a.user_id,'/',a.broker_uid) end) as bj_i_vp_wechat_cnt
from
(
  select
  a.p_dt,
  a.uid as user_id,
  a.click_time,
  a.name,
  b.selection_city_id as ccid,
  get_json_object(a.extend,'$.chatUserId') as broker_uid
  from dw_db.dw_app_action_detail_log a
  inner join (
    select distinct device_id,selection_city_id
    from dw_db.dw_app_access_log
    where selection_city_id in (1,2)
    and p_dt=${dealDate}
  ) b
  on a.dvid=b.device_id
  where a.p_dt=${dealDate} and a.current_page_id in ('1-520000')
  and a.bp_id='1-100000' and a.action_id in ('1-520001') and a.name in ('a-angejia','i-angejia')
) a
inner join db_sync.angejia__user_msg b on a.p_dt=to_date(b.created_at) and a.user_id=b.from_uid and a.broker_uid=b.to_uid
where a.click_time < b.created_at
--and a.ccid in (1,2) --only include SH,BJ for now
;


insert overwrite table dm_db.dm_product_sd partition (p_dt = ${dealDate})
select
a.p_dt as cal_dt,
'android' as platform,
'0' as city_id,
'全国' as city_name,
a.a_ud  as ud,
b.a_fud as fud,
a.a_log_ud as log_ud,
'' as come_rate,
d.a_reg_user_cnt as reg_user_cnt ,
a.a_list_ud as list_ud,
a.a_list_pv as list_pv,
a.a_vpud as vpud,
a.a_vppv as vppv,
e.a_list_vpud as list_vpud,
e.a_list_vppv as list_vppv,
f.a_vp_wechat_cnt as vp_wechat_cnt,
g.a_need_user_cnt as need_user_cnt,
g.a_need_cnt as need_cnt
from dw_db_temp.dm_product_sd_app_statistics a
left outer join dw_db_temp.dm_product_sd_fud b on a.p_dt = b.p_dt
left outer join dw_db_temp.dm_product_sd_reg_user_cnt d on a.p_dt = d.p_dt
left outer join dw_db_temp.dm_product_sd_vpud_vppv e on a.p_dt = e.p_dt
left outer join dw_db_temp.dm_product_sd_vp_wechat_cnt f on a.p_dt = f.p_dt
left outer join dw_db_temp.dm_product_sd_app_statistics g on a.p_dt = g.p_dt

union all

select
a.p_dt as cal_dt,
'ios' as platform,
'0' as city_id,
'全国' as city_name,
a.i_ud  as ud,
b.i_fud as fud,
a.i_log_ud as log_ud,
'' as come_rate,
d.i_reg_user_cnt as reg_user_cnt,
a.i_list_ud as list_ud,
a.i_list_pv as list_pv,
a.i_vpud as vpud,
a.i_vppv as vppv,
e.i_list_vpud as list_vpud,
e.i_list_vppv as list_vppv,
f.i_vp_wechat_cnt as vp_wechat_cnt,
g.i_need_user_cnt as need_user_cnt,
g.i_need_cnt as need_cnt
from dw_db_temp.dm_product_sd_app_statistics a
left outer join dw_db_temp.dm_product_sd_fud b on a.p_dt = b.p_dt
left outer join dw_db_temp.dm_product_sd_reg_user_cnt d on a.p_dt = d.p_dt
left outer join dw_db_temp.dm_product_sd_vpud_vppv e on a.p_dt = e.p_dt
left outer join dw_db_temp.dm_product_sd_vp_wechat_cnt f on a.p_dt = f.p_dt
left outer join dw_db_temp.dm_product_sd_app_statistics g on a.p_dt = g.p_dt

union all

select
a.p_dt as cal_dt,
'android'  as platform,
'1'    as city_id,
'上海' as city_name,
a.sh_a_ud  as ud,
b.sh_a_fud as fud,
a.sh_a_log_ud as log_ud,
'' as come_rate,
'' as reg_user_cnt,
a.sh_a_list_ud as list_ud,
a.sh_a_list_pv as list_pv,
a.sh_a_vpud as vpud,
a.sh_a_vppv as vppv,
e.sh_a_list_vpud as list_vpud,
e.sh_a_list_vppv as list_vppv,
f.sh_a_vp_wechat_cnt as vp_wechat_cnt,
g.sh_a_need_user_cnt as need_user_cnt,
g.sh_a_need_cnt as need_cnt
from dw_db_temp.dm_product_sd_app_statistics a
left outer join dw_db_temp.dm_product_sd_fud b on a.p_dt = b.p_dt
left outer join dw_db_temp.dm_product_sd_reg_user_cnt d on a.p_dt = d.p_dt
left outer join dw_db_temp.dm_product_sd_vpud_vppv e on a.p_dt = e.p_dt
left outer join dw_db_temp.dm_product_sd_vp_wechat_cnt f on a.p_dt = f.p_dt
left outer join dw_db_temp.dm_product_sd_app_statistics g on a.p_dt = g.p_dt

union all

select
a.p_dt as cal_dt,
'ios'  as platform,
'1'    as city_id,
'上海' as city_name,
a.sh_i_ud  as ud,
b.sh_i_fud as fud,
a.sh_i_log_ud as log_ud,
'' as come_rate,
'' as reg_user_cnt,
a.sh_i_list_ud as list_ud,
a.sh_i_list_pv as list_pv,
a.sh_i_vpud as vpud,
a.sh_i_vppv as vppv,
e.sh_i_list_vpud as list_vpud,
e.sh_i_list_vppv as list_vppv,
f.sh_i_vp_wechat_cnt as vp_wechat_cnt,
g.sh_i_need_user_cnt as need_user_cnt,
g.sh_i_need_cnt as need_cnt
from dw_db_temp.dm_product_sd_app_statistics a
left outer join dw_db_temp.dm_product_sd_fud b on a.p_dt = b.p_dt
left outer join dw_db_temp.dm_product_sd_reg_user_cnt d on a.p_dt = d.p_dt
left outer join dw_db_temp.dm_product_sd_vpud_vppv e on a.p_dt = e.p_dt
left outer join dw_db_temp.dm_product_sd_vp_wechat_cnt f on a.p_dt = f.p_dt
left outer join dw_db_temp.dm_product_sd_app_statistics g on a.p_dt = g.p_dt

union all

select
a.p_dt as cal_dt,
'android'  as platform,
'2'      as city_id,
'北京'   as city_name,
a.bj_a_ud  as ud,
b.bj_a_fud as fud,
a.bj_a_log_ud as log_ud,
'' as come_rate,
'' as reg_user_cnt,
a.bj_a_list_ud as list_ud,
a.bj_a_list_pv as list_pv,
a.bj_a_vpud as vpud,
a.bj_a_vppv as vppv,
e.bj_a_list_vpud as list_vpud,
e.bj_a_list_vppv as list_vppv,
f.bj_a_vp_wechat_cnt as vp_wechat_cnt,
g.bj_a_need_user_cnt as need_user_cnt,
g.bj_a_need_cnt as need_cnt
from dw_db_temp.dm_product_sd_app_statistics a
left outer join dw_db_temp.dm_product_sd_fud b on a.p_dt = b.p_dt
left outer join dw_db_temp.dm_product_sd_reg_user_cnt d on a.p_dt = d.p_dt
left outer join dw_db_temp.dm_product_sd_vpud_vppv e on a.p_dt = e.p_dt
left outer join dw_db_temp.dm_product_sd_vp_wechat_cnt f on a.p_dt = f.p_dt
left outer join dw_db_temp.dm_product_sd_app_statistics g on a.p_dt = g.p_dt

union all

select
a.p_dt as cal_dt,
'ios'  as platform,
'2'      as city_id,
'北京'   as city_name,
a.bj_i_ud  as ud,
b.bj_i_fud as fud,
a.bj_i_log_ud as log_ud,
'' as come_rate,
'' as reg_user_cnt,
a.bj_i_list_ud as list_ud,
a.bj_i_list_pv as list_pv,
a.bj_i_vpud as vpud,
a.bj_i_vppv as vppv,
e.bj_i_list_vpud as list_vpud,
e.bj_i_list_vppv as list_vppv,
f.bj_i_vp_wechat_cnt as vp_wechat_cnt,
g.bj_i_need_user_cnt as need_user_cnt,
g.bj_i_need_cnt as need_cnt
from dw_db_temp.dm_product_sd_app_statistics a
left outer join dw_db_temp.dm_product_sd_fud b on a.p_dt = b.p_dt
left outer join dw_db_temp.dm_product_sd_reg_user_cnt d on a.p_dt = d.p_dt
left outer join dw_db_temp.dm_product_sd_vpud_vppv e on a.p_dt = e.p_dt
left outer join dw_db_temp.dm_product_sd_vp_wechat_cnt f on a.p_dt = f.p_dt
left outer join dw_db_temp.dm_product_sd_app_statistics g on a.p_dt = g.p_dt
;
