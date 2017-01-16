--近30天录入用户
drop table if exists dw_db_temp.new_demand_user_30days;
create table dw_db_temp.new_demand_user_30days as
select b.user_id as user_id,a.city_id as city_id
from dw_db.dw_customer_demand a
inner join db_sync.angejia__broker_customer_bind_user b
on a.buyer_uid=b.broker_customer_id
where a.p_dt>=date_sub(${dealDate},29)
and to_date(a.created_at)>=date_sub(${dealDate},29)
and a.status=1
group by b.user_id,a.city_id
;

--ud,fud,log_ud,log_fud,vppv,vpud
drop table if exists dw_db_temp.channel_ud_fud_vppv_temp;
create table dw_db_temp.channel_ud_fud_vppv_temp as
select
delivery_channels as channel,
case when GROUPING__ID=1 then 'all' else selection_city_id end as city_id,
GROUPING__ID,  --0:  1:group by platform  3:group by platform,city
count(distinct case when p_dt=${dealDate} and rn=1 then device_id end) as td_fud,
count(distinct case when p_dt=${dealDate} then device_id end) as td_ud,
count(distinct case when p_dt=${dealDate} and rn=1 and max_user_id>0 then device_id end) as td_log_fud,
count(distinct case when p_dt=${dealDate} and user_id>0 then device_id end) as td_log_ud,
count(case when p_dt=${dealDate} and request_page_id in ('30074','30003') then device_id end) as td_vppv,
count(distinct case when p_dt=${dealDate} and request_page_id in ('30074','30003') then device_id end) as td_vpud,
0 as td_new_conn_cnt,
0 as td_new_conn_esf_cnt,
0 as td_new_conn_xf_cnt,
0 as td_new_wechat_cnt,
0 as td_new_wechat_esf_cnt,
0 as td_new_wechat_xf_cnt,
0 as td_call_pairs_cnt,
0 as td_call_pairs_esf_cnt,
0 as td_call_pairs_xf_cnt,
0 as td_assigned_call_buyer_cnt,
count(distinct case when (p_dt between date_sub(${dealDate},29) and ${dealDate}) and rn=1 then device_id end) as 30_fud,
count(distinct case when (p_dt between date_sub(${dealDate},29) and ${dealDate}) then device_id end) as 30_ud,
0 as 30_conn_user_cnt,
0 as 30_new_buyer_cnt,
0 as 30_visit_user_cnt,
0 as 30_trans_user_cnt,
0 as td_new_wechat_ud,
0 as td_call_pairs_esf_ud,
0 as td_call_pairs_xf_ud,
0 as td_assigned_call_buyer_ud
from (
  select *,
  row_number() over (distribute by device_id sort by server_time asc) as rn,
  max(user_id) over (distribute by p_dt,device_id) as max_user_id
  from dw_db.dw_app_access_log
  where p_dt>=date_sub(${dealDate},89) and p_dt<=${dealDate}
  and app_name in ('a-angejia','i-angejia')
  and request_uri not like '/mobile/member/configs%'
  and request_uri not like '/mobile/member/districts/show%'
  and request_uri not like '/mobile/member/inventories/searchFilters%'
  and request_uri not like '%/user/bind/push%'
  and request_uri not like '%/common/push/acks%'
  and hostname='api.angejia.com'
) t
group by delivery_channels,selection_city_id with rollup
;


drop table if exists dw_db_temp.channel_ud_fud_vppv;
create table dw_db_temp.channel_ud_fud_vppv as
select
a.channel,
a.city_id,
a.GROUPING__ID,  --0:  1:group by platform  3:group by platform,city
case when a.channel='C03' then
  case when a.city_id in ('all','2') then b.td_fud else 0 end
else a.td_fud end as td_fud,
a.td_ud,
case when a.channel='C03' then
  case when a.city_id in ('all','2') then b.td_log_fud else 0 end
else a.td_log_fud end as td_log_fud,
a.td_log_ud,
a.td_vppv,
a.td_vpud,
a.td_new_conn_cnt,
a.td_new_conn_esf_cnt,
a.td_new_conn_xf_cnt,
a.td_new_wechat_cnt,
a.td_new_wechat_esf_cnt,
a.td_new_wechat_xf_cnt,
a.td_call_pairs_cnt,
a.td_call_pairs_esf_cnt,
a.td_call_pairs_xf_cnt,
a.td_assigned_call_buyer_cnt,
case when a.channel='C03' then
  case when a.city_id in ('all','2') then b.30_fud else 0 end
else a.30_fud end as 30_fud,
a.30_ud,
a.30_conn_user_cnt,
a.30_new_buyer_cnt,
a.30_visit_user_cnt,
a.30_trans_user_cnt,
a.td_new_wechat_ud,
a.td_call_pairs_esf_ud,
a.td_call_pairs_xf_ud,
a.td_assigned_call_buyer_ud,
0 as td_xf_loupan_subscribe_ud,
0 as td_xf_loupan_subscribe_cnt
from dw_db_temp.channel_ud_fud_vppv_temp a
left join (
  select * from dw_db_temp.channel_ud_fud_vppv_temp where city_id='all'
) b
on a.channel=b.channel
;


drop table if exists dw_db_temp.dm_channel_connection_daily_temp;
create table dw_db_temp.dm_channel_connection_daily_temp as
select channel_id as channel,
case when GROUPING__ID=1 then 'all' else city_id end as city_id,
GROUPING__ID,
0 as td_fud,
0 as td_ud,
0 as td_log_fud,
0 as td_log_ud,
0 as td_vppv,
0 as td_vpud,
count(distinct concat(user_id,broker_uid,connection_type_id,nvl(user_phone,0),caller,called)) as td_new_conn_cnt,
count(distinct case when connection_type_id in (1,2,4,5,7,8,9,10,12,15) then concat(user_id,broker_uid,connection_type_id,nvl(user_phone,0),caller,called) end) as td_new_conn_esf_cnt,
count(distinct case when connection_type_id in (3,6,11,13,14) then concat(user_id,broker_uid,connection_type_id,nvl(user_phone,0),caller,called) end) as td_new_conn_xf_cnt,
count(distinct case when connection_type_id in (1,2,3,4,5,6,7,8,9) then concat(user_id,broker_uid) end) as td_new_wechat_cnt,
count(distinct case when connection_type_id in (2,4,5,7,8,9) then concat(user_id,broker_uid) end) as td_new_wechat_esf_cnt,
count(distinct case when connection_type_id in (3,6) then concat(user_id,broker_uid) end) as td_new_wechat_xf_cnt,
count(distinct case when connection_type_id in (10,11) then concat(caller,called) end) as td_call_pairs_cnt,
count(distinct case when connection_type_id in (10) then concat(caller,called) end) as td_call_pairs_esf_cnt,
count(distinct case when connection_type_id in (11) then concat(caller,called) end) as td_call_pairs_xf_cnt,
count(distinct case when connection_type_id in (12) then concat(user_id,broker_uid) end) as td_assigned_call_buyer_cnt,
0 as 30_fud,
0 as 30_ud,
0 as 30_conn_user_cnt,
0 as 30_new_buyer_cnt,
0 as 30_visit_user_cnt,
0 as 30_trans_user_cnt,
count(distinct case when connection_type_id in (1,2,3,4,5,6,7,8,9) then user_id end) as td_new_wechat_ud,
count(distinct case when connection_type_id in (10) then caller end) as td_call_pairs_esf_ud,
count(distinct case when connection_type_id in (11) then caller end) as td_call_pairs_xf_ud,
count(distinct case when connection_type_id in (12) then user_id end) as td_assigned_call_buyer_ud,
count(distinct case when connection_type_id in (14) then case when user_id=0 then user_phone else user_id end end) as td_xf_loupan_subscribe_ud,
count(case when connection_type_id in (14) then user_id end) as td_xf_loupan_subscribe_cnt
from dw_db.dw_connection_daily_summary_detail
where p_dt=${dealDate}
group by channel_id,city_id with rollup;


--近30天连接、录入、带看、成交ud
drop table if exists dw_db_temp.channel_measure_30days;
create table dw_db_temp.channel_measure_30days as
select f.channel_id as channel,
case when GROUPING__ID=1 then 'all' else a.selection_city_id end as city_id,
GROUPING__ID,
0 as td_fud,
0 as td_ud,
0 as td_log_fud,
0 as td_log_ud,
0 as td_vppv,
0 as td_vpud,
0 as td_new_conn_cnt,
0 as td_new_conn_esf_cnt,
0 as td_new_conn_xf_cnt,
0 as td_new_wechat_cnt,
0 as td_new_wechat_esf_cnt,
0 as td_new_wechat_xf_cnt,
0 as td_call_pairs_cnt,
0 as td_call_pairs_esf_cnt,
0 as td_call_pairs_xf_cnt,
0 as td_assigned_call_buyer_cnt,
0 as 30_fud,
0 as 30_ud,
count(distinct b.user_id) as 30_conn_user_cnt,
count(distinct c.user_id) as 30_new_buyer_cnt,
count(distinct d.user_id) as 30_visit_user_cnt,
count(distinct e.user_id) as 30_trans_user_cnt,
0 as td_new_wechat_ud,
0 as td_call_pairs_esf_ud,
0 as td_call_pairs_xf_ud,
0 as td_assigned_call_buyer_ud,
0 as td_xf_loupan_subscribe_ud,
0 as td_xf_loupan_subscribe_cnt
from (select delivery_channels,channel_name,platform,user_id,selection_city_id
  from dw_db.dw_app_access_log
  where p_dt>=date_sub(${dealDate},29) and p_dt<=${dealDate}
  and app_name in ('a-angejia','i-angejia')
  and request_uri not like '/mobile/member/configs%'
  and request_uri not like '/mobile/member/districts/show%'
  and request_uri not like '/mobile/member/inventories/searchFilters%'
  and request_uri not like '%/user/bind/push%'
  and request_uri not like '%/common/push/acks%'
  and hostname='api.angejia.com'
  group by delivery_channels,channel_name,platform,user_id,selection_city_id) a
left join (
  --30天微聊
  select user_id,city_id
  from (
    select user_id,broker_city_id as city_id
    from dw_db.dw_connection_wechat_detail
    where p_dt>=date_sub(${dealDate},29) and p_dt<=${dealDate}
    group by user_id,broker_city_id
  ) wechat
  union all
  --30天电话
  select user_id,city_id from dw_db.dw_connection_daily_summary_detail
  where p_dt>=date_sub(${dealDate},29) and p_dt<=${dealDate}
  and connection_type_id in (10,11)
) b
on a.user_id=b.user_id
left join dw_db_temp.new_demand_user_30days c
on a.user_id=c.user_id
  --30天带看
left join (
  select user_id,city_id
  from dw_db.dw_visit
  where to_date(visit_started_at)>=date_sub(${dealDate},29)
  and to_date(visit_started_at)<=${dealDate}
  union all
  select buyer_uid as user_id,city_id
  from dw_db.dw_xf_visit
  where to_date(visit_time)>=date_sub(${dealDate},29)
  and to_date(visit_time)<=${dealDate}
) d
on a.user_id=d.user_id
  --30天成交
left join (select user_id,inv_city_id as city_id
  from dw_db.dw_trans
  where p_dt>=date_sub(${dealDate},29) and signed_at>=date_sub(${dealDate},29)
  group by user_id,inv_city_id) e
on a.user_id=e.user_id
left join (select user_id,channel_id from dw_db.dw_user_sd where p_dt=${dealDate}) f
on a.user_id=f.user_id
group by f.channel_id,a.selection_city_id with rollup
;


drop table if exists dw_db_temp.channel_result_set;
create table dw_db_temp.channel_result_set as
select * from dw_db_temp.channel_ud_fud_vppv
union all
select * from dw_db_temp.dm_channel_connection_daily_temp
union all
select * from dw_db_temp.channel_measure_30days;

--最终结果集
insert overwrite table dm_db.dm_channel_app_sd partition (p_dt = ${dealDate})
select
basic.id as channel_id,
basic.name as channel_name,
basic.system as platform,
case when city_id='all' then 0 else t.city_id end as city_id,
case when city_id='all' then '全国' else city.name end as city_name,
sum(td_fud) as td_fud,
sum(td_ud) as td_ud,
sum(td_log_fud) as td_log_fud,
sum(td_log_ud) as td_log_ud,
sum(td_vppv) as td_vppv,
sum(td_vpud) as td_vpud,
sum(td_new_wechat_cnt)+sum(td_call_pairs_esf_cnt)+sum(td_call_pairs_xf_cnt)+sum(td_assigned_call_buyer_cnt)+sum(td_xf_loupan_subscribe_ud) as td_new_conn_cnt, --首聊+电话+派电话
sum(td_new_wechat_esf_cnt)+sum(td_call_pairs_esf_cnt)+sum(td_assigned_call_buyer_cnt) as td_new_conn_esf_cnt,
sum(td_new_wechat_xf_cnt)+sum(td_call_pairs_xf_cnt)+sum(td_xf_loupan_subscribe_cnt) as td_new_conn_xf_cnt,
sum(td_new_wechat_cnt) as td_new_wechat_cnt,
sum(td_new_wechat_esf_cnt) as td_new_wechat_esf_cnt,
sum(td_new_wechat_xf_cnt) as td_new_wechat_xf_cnt,
sum(td_call_pairs_esf_cnt)+sum(td_call_pairs_xf_cnt) as td_call_pairs_cnt,
sum(td_call_pairs_esf_cnt) as td_call_pairs_esf_cnt,
sum(td_call_pairs_xf_cnt) as td_call_pairs_xf_cnt,
sum(td_assigned_call_buyer_cnt) as td_assigned_call_buyer_cnt,
sum(30_fud) as 30_fud,
sum(30_ud) as 30_ud,
sum(30_conn_user_cnt) as 30_conn_user_cnt,
sum(30_new_buyer_cnt) as 30_new_buyer_cnt,
sum(30_visit_user_cnt) as 30_visit_user_cnt,
sum(30_trans_user_cnt) as 30_trans_user_cnt,
sum(td_new_wechat_ud)+sum(td_call_pairs_esf_ud)+sum(td_call_pairs_xf_ud)+sum(td_assigned_call_buyer_ud)+sum(td_xf_loupan_subscribe_ud) as td_new_conn_ud,
sum(td_xf_loupan_subscribe_ud) as td_xf_loupan_subscribe_ud
from dim_db.dim_app_basic_name basic
left join dw_db_temp.channel_result_set t
on basic.id=t.channel
left join dw_db.dim_city city
on t.city_id=city.id
where (GROUPING__ID=1 or (GROUPING__ID=3 and city.is_active=1))
group by basic.id,basic.name,basic.system,t.city_id,city.name
;
