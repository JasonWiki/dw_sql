
-------------------------------------------------金牌顾问指标------------------------------------------------------------
-- 顾问等级和勋章体系
-- 当月新增实勘多图房源量
drop table if exists dw_db_temp.libo_broker_medal_image_inv_cnt;
create table dw_db_temp.libo_broker_medal_image_inv_cnt as
select c.survey_broker_uid as broker_uid, count(distinct a.inventory_id) as image_inv_cnt
from db_sync.angejia__article a
left join dw_db.dw_article b
  on a.inventory_id = b.inventory_id and b.p_dt = date_sub(concat(substr(${dealDate},1,8),'01'),1) and b.quality_house=1
inner join dw_db.dw_property_inventory_sd c
  on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where a.status = 1
and a.quality_house = 1
and b.inventory_id is null
and to_date(a.created_at) <= ${dealDate}
group by c.survey_broker_uid
;


--实勘审核首次通过时间
drop table if exists dw_db_temp.libo_broker_medal_survey_inv;
create table dw_db_temp.libo_broker_medal_survey_inv as
select a.inventory_id, b.broker_uid, min(to_date(a.created_at)) as survey_date
from db_sync.angejia__inventory_log a
inner join (select distinct broker_uid,inventory_id from db_sync.angejia__survey where status = 1) b
  on a.inventory_id = b.inventory_id
where a.content rlike ".*survey_status:[1-2]->2.*"
and a.type = 5
group by a.inventory_id,b.broker_uid
having survey_date <= ${dealDate}
;


--被带看的房源量、带看次数
drop table if exists dw_db_temp.libo_broker_medal_visit_inv;
create table dw_db_temp.libo_broker_medal_visit_inv as
select
a.broker_uid,
count(distinct a.inventory_id) as visit_inv_cnt,
count(distinct a.visit_id) as visit_cnt
 from dw_db.dw_visit a
inner join dw_db_temp.libo_broker_medal_survey_inv b
  on a.inventory_id = b.inventory_id
where to_date(a.visit_started_at) >= concat(substr(${dealDate},1,8),'01')
and to_date(a.visit_started_at) < ${dealDate} and to_date(a.visit_started_at) >= b.survey_date
group by a.broker_uid
;



--首聊明细
--经纪人5分钟回复
drop table if exists dw_db_temp.libo_broker_medal_new_wechat_back;
create table dw_db_temp.libo_broker_medal_new_wechat_back as
select
 b.broker_uid
,count(distinct b.user_id) as wechat_cnt
,count(distinct case when a.account_type = 2 and (a.from_recv = 0 or c.msg_id is null) and a.created_at >= b.created_at and unix_timestamp(a.created_at)-unix_timestamp(b.created_at) between 0 and 300 then b.user_id end) as 5_min_back_cnt
from dw_db.dw_connection_new_wechat b
left join db_sync.angejia__user_msg a
  on a.from_uid = b.broker_uid and a.to_uid = b.user_id
left join db_sync.angejia__user_msg_robot c
  on a.msg_id = c.msg_id
where b.p_dt between concat(substr(${dealDate},1,8),'01') and ${dealDate}
and hour(b.created_at) between 9 and 22
group by b.broker_uid
;


-- 5分钟接单
drop table if exists dw_db_temp.libo_broker_medal_robbed;
create table dw_db_temp.libo_broker_medal_robbed as
select
 c.broker_uid
,count(distinct b.log_id) as push_cnt
,count(distinct case when c.broker_first_weichat_at not like '0000%' and unix_timestamp(c.broker_first_weichat_at)-unix_timestamp(c.created_at) between 0 and 300 then b.log_id end) as robbed_cnt
from db_sync.angejia__member_demand_log a
inner join db_sync.angejia__buyer_demand_push_batch b
  on a.id = b.log_id
left join db_sync.angejia__buyer_demand_push_detail c
  on b.id = c.batch_id
where to_date(c.created_at) between concat(substr(${dealDate},1,8),'01') and ${dealDate}
and hour(c.created_at) between 9 and 22
and b.type = 0
and c.type = 0
and c.is_update = 0
group by c.broker_uid
;


--带看转化率
drop table if exists dw_db_temp.libo_broker_medal_wechat_visit;
create table dw_db_temp.libo_broker_medal_wechat_visit as
select
 a.broker_uid
,count(distinct a.user_id) as wechat_cnt
,count(distinct case when a.created_at <= b.visit_started_at and to_date(b.visit_started_at) between a.p_dt and date_add(a.p_dt,6) then b.user_id end) as visit_cnt
from dw_db.dw_connection_new_wechat a
left join dw_db.dw_visit b
  on a.broker_uid = b.broker_uid and a.user_id = b.user_id
where (a.p_dt between concat(substr(${dealDate},1,8),'01') and date_sub(${dealDate},6) or a.p_dt between date_sub(${dealDate},6) and concat(substr(${dealDate},1,8),'01'))
and a.p_dt>=concat(substr(${dealDate},1,8),'01')
group by a.broker_uid
;



-- 成交业绩
drop table if exists dw_db_temp.libo_broker_medal_achievement;
create table dw_db_temp.libo_broker_medal_achievement as
select
 broker_uid
,sum(price) as price
from
(select broker_uid,price,created_at,deleted_at
from db_sync.retrx__commission_rate
union all
select broker_uid,price,created_at,deleted_at
from db_sync.retrx__new_commission_rate
) t
where to_date(created_at) between concat(substr(add_months(${dealDate}, -2),1,8),'01') and ${dealDate}
and deleted_at is null
group by broker_uid
;


--经纪人数据汇总
drop table if exists dw_db_temp.libo_broker_medal_broker_score_temp;
create table dw_db_temp.libo_broker_medal_broker_score_temp as
select
 a.user_id as broker_uid
,round(case when nvl(b.image_inv_cnt,0) < 8 then nvl(b.image_inv_cnt,0)/8*0.6
  when nvl(b.image_inv_cnt,0) between 8 and 12 then 0.6+(nvl(b.image_inv_cnt,0)-8)/4*0.2
  when nvl(b.image_inv_cnt,0) between 12 and 20 then 0.8+(nvl(b.image_inv_cnt,0)-12)/8*0.2
  when nvl(b.image_inv_cnt,0) > 20 then 1 else 0 end, 2)*20 as image_inv_score

,round(case when nvl(c.visit_inv_cnt,0) < 2 then nvl(c.visit_inv_cnt,0)/2*0.6
  when nvl(c.visit_inv_cnt,0) between 2 and 4 then 0.6+(nvl(c.visit_inv_cnt,0)-2)/2*0.2
  when nvl(c.visit_inv_cnt,0) between 4 and 8 then 0.8+(nvl(c.visit_inv_cnt,0)-4)/4*0.2
  when nvl(c.visit_inv_cnt,0) > 8 then 1 else 0 end, 2)*15 as visit_inv_score

,round(case when nvl((nvl(d.5_min_back_cnt,0)+nvl(e.robbed_cnt,0))/(nvl(d.wechat_cnt,0)+nvl(e.push_cnt,0)),0) < 0.7 then nvl((nvl(d.5_min_back_cnt,0)+nvl(e.robbed_cnt,0))/(nvl(d.wechat_cnt,0)+nvl(e.push_cnt,0)),0)/0.7*0.6
  when nvl((nvl(d.5_min_back_cnt,0)+nvl(e.robbed_cnt,0))/(nvl(d.wechat_cnt,0)+nvl(e.push_cnt,0)),0) between 0.7 and 0.9 then 0.6+(nvl((nvl(d.5_min_back_cnt,0)+nvl(e.robbed_cnt,0))/(nvl(d.wechat_cnt,0)+nvl(e.push_cnt,0)),0)-0.7)/0.2*0.2
  when nvl((nvl(d.5_min_back_cnt,0)+nvl(e.robbed_cnt,0))/(nvl(d.wechat_cnt,0)+nvl(e.push_cnt,0)),0) between 0.9 and 1 then 0.8+(nvl((nvl(d.5_min_back_cnt,0)+nvl(e.robbed_cnt,0))/(nvl(d.wechat_cnt,0)+nvl(e.push_cnt,0)),0)-0.9)/0.1*0.2
  when nvl((nvl(d.5_min_back_cnt,0)+nvl(e.robbed_cnt,0))/(nvl(d.wechat_cnt,0)+nvl(e.push_cnt,0)),0) = 1 then 1 else 0 end, 2)*15 as 5_min_back_score

,round(case when nvl(f.visit_cnt/f.wechat_cnt,0) < 0.03 then nvl(f.visit_cnt/f.wechat_cnt,0)/0.03*0.6
  when nvl(f.visit_cnt/f.wechat_cnt,0) between 0.03 and 0.06 then 0.6+(nvl(f.visit_cnt/f.wechat_cnt,0)-0.03)/0.03*0.2
  when nvl(f.visit_cnt/f.wechat_cnt,0) between 0.06 and 0.25 then 0.8+(nvl(f.visit_cnt/f.wechat_cnt,0)-0.06)/0.19*0.2
  when nvl(f.visit_cnt/f.wechat_cnt,0) > 0.25 then 1 else 0 end, 2)*15 as wechat_visit_score

,round(case when nvl(c.visit_cnt,0) < 3 then nvl(c.visit_cnt,0)/3*0.6
  when nvl(c.visit_cnt,0) between 3 and 6 then 0.6+(nvl(c.visit_cnt,0)-3)/3*0.2
  when nvl(c.visit_cnt,0) between 6 and 10 then 0.8+(nvl(c.visit_cnt,0)-6)/4*0.2
  when nvl(c.visit_cnt,0) > 10 then 1 else 0 end, 2)*15 as visit_score

,round(case when nvl(h.price,0) < 30000 then nvl(h.price,0)/30000*0.6
  when nvl(h.price,0) between 30000 and 60000 then 0.6+(nvl(h.price,0)-30000)/30000*0.2
  when nvl(h.price,0) between 60000 and 90000 then 0.8+(nvl(h.price,0)-60000)/30000*0.2
  when nvl(h.price,0) > 90000 then 1 else 0 end, 2)*20 as price_score

from db_sync.angejia__broker a
left join dw_db_temp.libo_broker_medal_image_inv_cnt b
  on a.user_id = b.broker_uid
left join dw_db_temp.libo_broker_medal_visit_inv c
  on a.user_id = c.broker_uid
left join dw_db_temp.libo_broker_medal_new_wechat_back d
  on a.user_id = d.broker_uid
left join dw_db_temp.libo_broker_medal_robbed e
  on a.user_id = e.broker_uid
left join dw_db_temp.libo_broker_medal_wechat_visit f
  on a.user_id = f.broker_uid
left join dw_db_temp.libo_broker_medal_achievement h
  on a.user_id = h.broker_uid
left join dw_db_temp.libo_dw_broker_summary_organization_info as i on a.user_id = i.broker_id
where i.broker_agent_id is not null
and i.broker_agent_id not in ('','46','140')
and a.type in (1,2)
and a.status = 2
;


--金牌顾问等级
drop table if exists dw_db_temp.dw_broker_medal;
create table dw_db_temp.dw_broker_medal as
select
broker_uid,
case when image_inv_score + visit_inv_score + 5_min_back_score + wechat_visit_score + visit_score + price_score >= 80 then 2
  when image_inv_score + visit_inv_score + 5_min_back_score + wechat_visit_score + visit_score + price_score between 60 and 79.99 then 1
else 0 end as level
from dw_db_temp.libo_broker_medal_broker_score_temp
;

---------------------------------------------------------------------------------------------------

--合格主营小区数
drop table if exists dw_db_temp.dw_property_address_sd_ok;
create table dw_db_temp.dw_property_address_sd_ok as
select
 concat(substr(a.month,1,4),'-',substr(a.month,5,2)) as longmonthid
,d.address_id
,count(distinct case when a.inventory_id > 0 then a.inventory_id end) as agj_num
,count(1) as sc_num
from dw_db.dw_business_records a
left join dw_db.dw_property_inventory d
  on a.inventory_id = d.inventory_id
left semi join
(select max(p_dt) as p_dt from dw_db.dw_business_records
  where p_dt >= concat(substr(add_months(${dealDate},-3),1,8),'01')
) c on a.p_dt=c.p_dt
group by concat(substr(a.month,1,4),'-',substr(a.month,5,2)),d.address_id;


drop table if exists dw_db_temp.dw_property_address_sd_ok1;
create table dw_db_temp.dw_property_address_sd_ok1 as
select
p_dt,
address_id,
case when city_id is null then 1 else city_id end as city_id,
count(distinct inventory_id) as inven_num,
count(distinct case when survey_status=2 then inventory_id end) as survey_num
 from dw_db.dw_property_inventory_sd
 where p_dt>=concat(substr(add_months(${dealDate},-3),1,8),'01') and verify_status=2
 group by p_dt,address_id,case when city_id is null then 1 else city_id end;


drop table if exists dw_db_temp.dm_kpi_sd_01;
create table dw_db_temp.dm_kpi_sd_01 as
select
k1.p_dt,
k1.city_id,
count(distinct case when k3.agj_num/k3.sc_num>=0.5 and k1.survey_num/k1.inven_num>=0.7 then k1.address_id end) as qualify_community_cnt
from dw_db_temp.dw_property_address_sd_ok1 k1
left join dw_db_temp.dw_property_address_sd_ok k3
on substr(add_months(date_sub(k1.p_dt,15),-1),1,7)=k3.longmonthid and k1.address_id=k3.address_id
inner join db_sync.angejia__community_team k4
on k1.address_id=k4.address_id and k4.deleted_at is null
where k1.p_dt>=concat(substr(add_months(${dealDate},-3),1,8),'01')
group by k1.p_dt,k1.city_id
;


--在线实勘房源数
drop table if exists dw_db_temp.dm_kpi_sd_02;
create table dw_db_temp.dm_kpi_sd_02 as
select
p_dt,
case when city_id is null then 1 else city_id end as city_id,
count(inventory_id) as survey_sale_cnt
from dw_db.dw_property_inventory_sd
where p_dt = ${dealDate} and survey_status='2' -- and verify_status = '2'
group by p_dt,case when city_id is null then 1 else city_id end
;


--app ud 分城市
drop table if exists dw_db_temp.dm_kpi_sd_03;
create table dw_db_temp.dm_kpi_sd_03 as
select
p_dt,
case when selection_city_id is not null then selection_city_id
else location_city_id
end as city_id,
count(distinct device_id) as app_ud
from dw_db.dw_app_access_log
where app_name in ('a-angejia','i-angejia')
and p_dt= ${dealDate}
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_uri not like '%/user/bind/push%'
and request_uri not like '%/common/push/acks%'
and hostname='api.angejia.com'
group by
p_dt,
case when selection_city_id is not null then selection_city_id
else location_city_id
end;


--电话对数
drop table if exists dw_db_temp.dm_kpi_sd_04;
create table dw_db_temp.dm_kpi_sd_04 as
select p_dt,
city_id,
count(distinct pair) as call_pairs_cnt
from
(
  select distinct
  to_date(start_at) as p_dt,
  city_id,
  concat(caller,called) as pair
  from dw_db.dw_connection_call
  where call_type=2  --用户->经纪人
  and server_type=2 --电话提供商 1古思 2吉亚
  and to_date(start_at)=${dealDate}
  union all
  --新房电话
  select distinct
  to_date(a.start_at) as p_dt,
  d.city_id,
  concat(a.caller,a.called) as pair
  from db_sync.angejia__call_log a
  left join db_sync.angejia__loupan_short_num c
  on a.orig_called=c.short_num
  left join db_sync.xinfang__loupan_basic d
  on c.loupan_id=d.id
  where a.inventory_number = '4008109907'
  and a.orig_channel=0
  and a.is_harass=0
  and a.server_type=2
  and to_date(a.start_at)=${dealDate}
) t
group by p_dt,city_id
;

--首聊对数
drop table if exists dw_db_temp.dm_kpi_sd_05;
create table dw_db_temp.dm_kpi_sd_05 as
select
p_dt,
broker_city_id as city_id,
count(distinct user_id,broker_uid) as first_wechat_cnt
 from dw_db.dw_connection_new_wechat
where p_dt = ${dealDate}
group by p_dt,broker_city_id;



--带看次数
drop table if exists dw_db_temp.dm_kpi_sd_06;
create table dw_db_temp.dm_kpi_sd_06 as
select
to_date(a.visit_started_at) as p_dt,
b.broker_city_id as city_id,
count(distinct a.visit_id) as visit_cnt
 from dw_db.dw_visit a
inner join dw_db.dw_broker_sd b on a.broker_uid = b.user_id and b.p_dt = ${dealDate}
where to_date(a.visit_started_at) = ${dealDate} and b.valid_broker=1 --有效经纪人
group by to_date(a.visit_started_at),b.broker_city_id
;


--经纪人数
drop table if exists dw_db_temp.dm_kpi_sd_07;
create table dw_db_temp.dm_kpi_sd_07 as
select
p_dt,
broker_city_id as city_id,
count(user_id) as broker_cnt
from dw_db.dw_broker_sd
where p_dt = ${dealDate}
and broker_duty_status_id=2
and user_id not in (3,4)
and broker_type_id in (1,2)
and broker_city_id <> 3
and agent_id is not null and agent_id not in ('','46','140')
group by p_dt,broker_city_id
;


--金牌顾问
drop table if exists dw_db_temp.dm_kpi_sd_08;
create table dw_db_temp.dm_kpi_sd_08 as
select
${dealDate} as p_dt,
b.broker_city_id as city_id,
count(distinct broker_uid) as medal_broker_cnt
from dw_db_temp.dw_broker_medal a
left join dw_db.dw_broker_sd b
on a.broker_uid=b.user_id and b.p_dt=${dealDate}
where a.level = 2
group by b.broker_city_id
;


--成交量
drop table if exists dw_db_temp.dm_kpi_sd_09;
create table dw_db_temp.dm_kpi_sd_09 as
select
to_date(signed_at) as p_dt,
city_id,
count(distinct sn) as trans_cnt
from dw_db.dw_trans
where to_date(signed_at) = ${dealDate} and is_bomb = 2
group by to_date(signed_at),city_id;


drop table if exists dw_db_temp.dm_kpi_sd_10;
create table dw_db_temp.dm_kpi_sd_10 as
select
a.p_dt as cal_dt,
a.city_id,
'' as qualify_community_cnt,
a.survey_sale_cnt,
a3.app_ud,
a4.call_pairs_cnt,
a5.first_wechat_cnt,
a6.visit_cnt,
a7.broker_cnt,
a8.medal_broker_cnt,
a9.trans_cnt
from dw_db_temp.dm_kpi_sd_02 a
left outer join dw_db_temp.dm_kpi_sd_03 a3 on a.p_dt = a3.p_dt and a.city_id = a3.city_id
left outer join dw_db_temp.dm_kpi_sd_04 a4 on a.p_dt = a4.p_dt and a.city_id = a4.city_id
left outer join dw_db_temp.dm_kpi_sd_05 a5 on a.p_dt = a5.p_dt and a.city_id = a5.city_id
left outer join dw_db_temp.dm_kpi_sd_06 a6 on a.p_dt = a6.p_dt and a.city_id = a6.city_id
left outer join dw_db_temp.dm_kpi_sd_07 a7 on a.p_dt = a7.p_dt and a.city_id = a7.city_id
left outer join dw_db_temp.dm_kpi_sd_08 a8 on a.p_dt = a8.p_dt and a.city_id = a8.city_id
left outer join dw_db_temp.dm_kpi_sd_09 a9 on a.p_dt = a9.p_dt and a.city_id = a9.city_id
where a.city_id in (1,2)
;


--以下逻辑添加于2016-08-26--
insert overwrite table dm_db.dm_kpi_sd partition (p_dt = ${dealDate})
select
${dealDate} as cal_dt,
case when GROUPING__ID=0 then 0 else city_id end as city_id,
sum(qualify_community_cnt) as qualify_community_cnt,
sum(survey_sale_cnt) as survey_sale_cnt,
sum(app_ud) as app_ud,
sum(call_pairs_cnt) as call_pairs_cnt,
sum(first_wechat_cnt) as first_wechat_cnt,
sum(visit_cnt) as visit_cnt,
sum(broker_cnt) as broker_cnt,
sum(medal_broker_cnt) as medal_broker_cnt,
sum(trans_cnt) as trans_cnt,
case when GROUPING__ID=0 then '全国' else city.name end as city_name,
sum(app_ud_past30days) as app_ud_past30days,
sum(app_fud_past30days) as app_fud_past30days,
sum(app_access_fud_past30days) as app_access_fud_past30days,
sum(app_ud_td) as app_ud_td,
sum(app_fud_td) as app_fud_td,
sum(app_access_fud_td) as app_access_fud_td,
sum(wechat_cnt_past30days) as wechat_cnt_past30days,
sum(wechat_cnt_td) as wechat_cnt_td,
sum(first_wechat_cnt_past30days) as first_wechat_cnt_past30days,
sum(first_wechat_cnt_td) as first_wechat_cnt_td,
sum(caller_cnt_past30days) as caller_cnt_past30days,
sum(first_caller_cnt_past30days) as first_caller_cnt_past30days,
sum(caller_cnt_td) as caller_cnt_td,
sum(first_caller_cnt_td) as first_caller_cnt_td,
sum(visit_ud_online_past30days) as visit_ud_online_past30days,
sum(more_visit_ud_online_past30days) as more_visit_ud_online_past30days,
sum(visit_ud_online_td) as visit_ud_online_td,
sum(trans_ud_online_past30days) as trans_ud_online_past30days,
sum(trans_ud_online_td) as trans_ud_online_td,
sum(new_demand_user_cnt_past30days) as new_demand_user_cnt_past30days,
sum(new_demand_user_cnt_td) as new_demand_user_cnt_td
from
(
select
city_id,
GROUPING__ID,
sum(qualify_community_cnt) as qualify_community_cnt,
sum(survey_sale_cnt) as survey_sale_cnt,
sum(app_ud) as app_ud,
sum(call_pairs_cnt) as call_pairs_cnt,
sum(first_wechat_cnt) as first_wechat_cnt,
sum(visit_cnt) as visit_cnt,
sum(broker_cnt) as broker_cnt,
sum(medal_broker_cnt) as medal_broker_cnt,
sum(trans_cnt) as trans_cnt,
0 as app_ud_past30days,
0 as app_fud_past30days,
0 as app_access_fud_past30days,
0 as app_ud_td,
0 as app_fud_td,
0 as app_access_fud_td,
0 as wechat_cnt_past30days,
0 as wechat_cnt_td,
0 as first_wechat_cnt_past30days,
0 as first_wechat_cnt_td,
0 as caller_cnt_past30days,
0 as first_caller_cnt_past30days,
0 as caller_cnt_td,
0 as first_caller_cnt_td,
0 as visit_ud_online_past30days,
0 as more_visit_ud_online_past30days,
0 as visit_ud_online_td,
0 as trans_ud_online_past30days,
0 as trans_ud_online_td,
0 as new_demand_user_cnt_past30days,
0 as new_demand_user_cnt_td
from dw_db_temp.dm_kpi_sd_10 kpi
group by city_id with rollup

union all
--过去30天以及当天app ud,fud,注册用户
select
city_id,
GROUPING__ID,
0 as qualify_community_cnt,
0 as survey_sale_cnt,
0 as app_ud,
0 as call_pairs_cnt,
0 as first_wechat_cnt,
0 as visit_cnt,
0 as broker_cnt,
0 as medal_broker_cnt,
0 as trans_cnt,
count(distinct case when p_dt between date_sub(${dealDate}, 29) and ${dealDate} then device_id end) as app_ud_past30days,
count(case when p_dt between date_sub(${dealDate}, 29) and ${dealDate} and rank1 = 1 then device_id end) as app_fud_past30days,
count(case when p_dt between date_sub(${dealDate}, 29) and ${dealDate} and rank2 = 1 then user_id end) as app_access_fud_past30days,
count(distinct case when p_dt = ${dealDate} then device_id end) as app_ud_td,
count(case when p_dt = ${dealDate} and rank1 = 1 then device_id end) as app_fud_td,
count(case when p_dt = ${dealDate} and rank2 = 1 then user_id end) as app_access_fud_td,
0 as wechat_cnt_past30days,
0 as wechat_cnt_td,
0 as first_wechat_cnt_past30days,
0 as first_wechat_cnt_td,
0 as caller_cnt_past30days,
0 as first_caller_cnt_past30days,
0 as caller_cnt_td,
0 as first_caller_cnt_td,
0 as visit_ud_online_past30days,
0 as more_visit_ud_online_past30days,
0 as visit_ud_online_td,
0 as trans_ud_online_past30days,
0 as trans_ud_online_td,
0 as new_demand_user_cnt_past30days,
0 as new_demand_user_cnt_td
from
(select device_id,
user_id,
p_dt,
selection_city_id as city_id,
row_number() over(partition by device_id order by p_dt) as rank1,
row_number() over(partition by user_id order by p_dt) as rank2
from dw_db.dw_app_access_log
where p_dt between date_sub(${dealDate}, 89) and ${dealDate}
and app_name in ('a-angejia', 'i-angejia')
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_uri not like '%/user/bind/push%'
and request_uri not like '%/common/push/acks%'
and hostname='api.angejia.com'
) t
where p_dt between date_sub(${dealDate}, 29) and ${dealDate}
group by city_id with rollup

union all
--过去30天微聊，当天微聊
select
broker_city_id as city_id,
GROUPING__ID,
0 as qualify_community_cnt,
0 as survey_sale_cnt,
0 as app_ud,
0 as call_pairs_cnt,
0 as first_wechat_cnt,
0 as visit_cnt,
0 as broker_cnt,
0 as medal_broker_cnt,
0 as trans_cnt,
0 as app_ud_past30days,
0 as app_fud_past30days,
0 as app_access_fud_past30days,
0 as app_ud_td,
0 as app_fud_td,
0 as app_access_fud_td,
count(distinct user_id) as wechat_cnt_past30days,
count(distinct case when p_dt=${dealDate} then user_id end) as wechat_cnt_td,
0 as first_wechat_cnt_past30days,
0 as first_wechat_cnt_td,
0 as caller_cnt_past30days,
0 as first_caller_cnt_past30days,
0 as caller_cnt_td,
0 as first_caller_cnt_td,
0 as visit_ud_online_past30days,
0 as more_visit_ud_online_past30days,
0 as visit_ud_online_td,
0 as trans_ud_online_past30days,
0 as trans_ud_online_td,
0 as new_demand_user_cnt_past30days,
0 as new_demand_user_cnt_td
from dw_db.dw_connection_wechat_detail
where p_dt between date_sub(${dealDate}, 29) and ${dealDate} and rownum=1
group by broker_city_id with rollup

union all
--过去30天首聊，当天首聊
select
broker_city_id as city_id,
GROUPING__ID,
0 as qualify_community_cnt,
0 as survey_sale_cnt,
0 as app_ud,
0 as call_pairs_cnt,
0 as first_wechat_cnt,
0 as visit_cnt,
0 as broker_cnt,
0 as medal_broker_cnt,
0 as trans_cnt,
0 as app_ud_past30days,
0 as app_fud_past30days,
0 as app_access_fud_past30days,
0 as app_ud_td,
0 as app_fud_td,
0 as app_access_fud_td,
0 as wechat_cnt_past30days,
0 as wechat_cnt_td,
count(distinct user_id) as first_wechat_cnt_past30days,
count(distinct case when p_dt=${dealDate} then user_id end) as first_wechat_cnt_td,
0 as caller_cnt_past30days,
0 as first_caller_cnt_past30days,
0 as caller_cnt_td,
0 as first_caller_cnt_td,
0 as visit_ud_online_past30days,
0 as more_visit_ud_online_past30days,
0 as visit_ud_online_td,
0 as trans_ud_online_past30days,
0 as trans_ud_online_td,
0 as new_demand_user_cnt_past30days,
0 as new_demand_user_cnt_td
from dw_db.dw_connection_new_wechat
where p_dt between date_sub(${dealDate}, 29) and ${dealDate}
group by broker_city_id with rollup

union all
--过去30天电话用户数，当天电话用户数
select city_id,
GROUPING__ID,
0 as qualify_community_cnt,
0 as survey_sale_cnt,
0 as app_ud,
0 as call_pairs_cnt,
0 as first_wechat_cnt,
0 as visit_cnt,
0 as broker_cnt,
0 as medal_broker_cnt,
0 as trans_cnt,
0 as app_ud_past30days,
0 as app_fud_past30days,
0 as app_access_fud_past30days,
0 as app_ud_td,
0 as app_fud_td,
0 as app_access_fud_td,
0 as wechat_cnt_past30days,
0 as wechat_cnt_td,
0 as first_wechat_cnt_past30days,
0 as first_wechat_cnt_td,
count(distinct caller) as caller_cnt_past30days,
count(distinct case when rank=1 then caller end) as first_caller_cnt_past30days,
count(distinct case when p_dt = ${dealDate} then caller end) as caller_cnt_td,
count(distinct case when p_dt = ${dealDate} and rank=1 then caller end) as first_caller_cnt_td,
0 as visit_ud_online_past30days,
0 as more_visit_ud_online_past30days,
0 as visit_ud_online_td,
0 as trans_ud_online_past30days,
0 as trans_ud_online_td,
0 as new_demand_user_cnt_past30days,
0 as new_demand_user_cnt_td
from
(
select to_date(start_at) as p_dt,
city_id,
caller,
called,
row_number() over(partition by caller order by start_at) as rank
from dw_db.dw_connection_call
where (call_type=2 --用户打给经纪人
or inventory_number='4008109907') --新房400
and to_date(start_at) between date_sub(${dealDate}, 89) and ${dealDate}
) t
where p_dt between date_sub(${dealDate}, 29) and ${dealDate}
group by city_id with rollup

union all
--带看和多看
select city_id,
GROUPING__ID,
0 as qualify_community_cnt,
0 as survey_sale_cnt,
0 as app_ud,
0 as call_pairs_cnt,
0 as first_wechat_cnt,
0 as visit_cnt,
0 as broker_cnt,
0 as medal_broker_cnt,
0 as trans_cnt,
0 as app_ud_past30days,
0 as app_fud_past30days,
0 as app_access_fud_past30days,
0 as app_ud_td,
0 as app_fud_td,
0 as app_access_fud_td,
0 as wechat_cnt_past30days,
0 as wechat_cnt_td,
0 as first_wechat_cnt_past30days,
0 as first_wechat_cnt_td,
0 as caller_cnt_past30days,
0 as first_caller_cnt_past30days,
0 as caller_cnt_td,
0 as first_caller_cnt_td,
count(distinct user_id) as visit_ud_online_past30days,
count(distinct case when rank>1 then user_id end) as more_visit_ud_online_past30days,
count(distinct case when p_dt = ${dealDate} then user_id end) as visit_ud_online_td,
0 as trans_ud_online_past30days,
0 as trans_ud_online_td,
0 as new_demand_user_cnt_past30days,
0 as new_demand_user_cnt_td
from
(select to_date(visit_started_at) as p_dt,user_id,inv_city_id as city_id,
rank() over(partition by nvl(user_id, buyer_uid) order by visit_id) as rank
from dw_db.dw_visit
where to_date(visit_started_at) between date_sub(${dealDate}, 29) and ${dealDate}
) t
group by city_id with rollup

union all
-- 成交ud
select
city_id,
GROUPING__ID,
0 as qualify_community_cnt,
0 as survey_sale_cnt,
0 as app_ud,
0 as call_pairs_cnt,
0 as first_wechat_cnt,
0 as visit_cnt,
0 as broker_cnt,
0 as medal_broker_cnt,
0 as trans_cnt,
0 as app_ud_past30days,
0 as app_fud_past30days,
0 as app_access_fud_past30days,
0 as app_ud_td,
0 as app_fud_td,
0 as app_access_fud_td,
0 as wechat_cnt_past30days,
0 as wechat_cnt_td,
0 as first_wechat_cnt_past30days,
0 as first_wechat_cnt_td,
0 as caller_cnt_past30days,
0 as first_caller_cnt_past30days,
0 as caller_cnt_td,
0 as first_caller_cnt_td,
0 as visit_ud_online_past30days,
0 as more_visit_ud_online_past30days,
0 as visit_ud_online_td,
count(distinct user_id) as trans_ud_online_past30days,
count(distinct case when to_date(signed_at)=${dealDate} then user_id end) as trans_ud_online_td,
0 as new_demand_user_cnt_past30days,
0 as new_demand_user_cnt_td
from dw_db.dw_trans
where to_date(signed_at) between date_sub(${dealDate}, 29) and ${dealDate}
and progress>=3
and is_bomb=2
and sn not like '%HK%'
group by city_id with rollup

union all
-- 录入私客数
select a.city_id,
GROUPING__ID,
0 as qualify_community_cnt,
0 as survey_sale_cnt,
0 as app_ud,
0 as call_pairs_cnt,
0 as first_wechat_cnt,
0 as visit_cnt,
0 as broker_cnt,
0 as medal_broker_cnt,
0 as trans_cnt,
0 as app_ud_past30days,
0 as app_fud_past30days,
0 as app_access_fud_past30days,
0 as app_ud_td,
0 as app_fud_td,
0 as app_access_fud_td,
0 as wechat_cnt_past30days,
0 as wechat_cnt_td,
0 as first_wechat_cnt_past30days,
0 as first_wechat_cnt_td,
0 as caller_cnt_past30days,
0 as first_caller_cnt_past30days,
0 as caller_cnt_td,
0 as first_caller_cnt_td,
0 as visit_ud_online_past30days,
0 as more_visit_ud_online_past30days,
0 as visit_ud_online_td,
0 as trans_ud_online_past30days,
0 as trans_ud_online_td,
count(distinct d.user_id) as new_demand_user_cnt_past30days,
count(distinct case when a.p_dt=${dealDate} and to_date(a.created_at)=${dealDate} then d.user_id end) as new_demand_user_cnt_td
from dw_db.dw_customer_demand a
inner join dw_db.dw_broker_sd c
on a.broker_uid = c.user_id and a.p_dt = c.p_dt
inner join db_sync.angejia__broker_customer_bind_user d
on a.buyer_uid = d.broker_customer_id
where a.p_dt between date_sub(${dealDate}, 29) and ${dealDate}
and to_date(a.created_at) between date_sub(${dealDate}, 29) and ${dealDate}
and a.status = 1
and c.agent_name is not null
and c.agent_name not in ('','测试中心','富阳中心')
and c.broker_type_id in (1,2)
group by a.city_id with rollup
) t
left join dw_db.dim_city city
on t.city_id=city.id
where city.is_active=1 or GROUPING__ID=0
group by case when GROUPING__ID=0 then 0 else city_id end,
case when GROUPING__ID=0 then '全国' else city.name end
;
