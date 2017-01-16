set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=false;

drop table if exists dw_db_temp.dm_platform_mth_summary_wechat;
create table dw_db_temp.dm_platform_mth_summary_wechat as
select
case when grouping__id in (0,2) then 'all' else b.city_id end as city_id,
case when grouping__id in (0,1) then 'all' else nvl(lower(d.system_classification),'pc') end as platform,
count(case when a.is_new_wechat=1 then concat(a.broker_uid,a.user_id) end) as new_wechat_cnt,
count(case when a.property_type=2 and a.source_type='需求找房' and a.is_new_wechat=1 then 1 end) as xf_demand_new_wechat_cnt,
count(case when a.property_type=2 and a.source_type<>'需求找房' and a.is_new_wechat=1 then 1 end) as xf_loupan_new_wechat_cnt,
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='需求找房' then concat(a.broker_uid,a.user_id) end) as esf_demand_new_wechat_cnt,--需求找房首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房东' then concat(a.broker_uid,a.user_id) end) as landlord_new_wechat_cnt,--房东首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='顾问单页' then concat(a.broker_uid,a.user_id) end) as broker_new_wechat_cnt,--顾问首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='推送推荐' then concat(a.broker_uid,a.user_id) end) as push_new_wechat_cnt,--推荐首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房源找房' and (f.resource=1 or f.resource is null) then concat(a.broker_uid,a.user_id) end) as promotion_new_wechat_cnt,--传统找房首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='其他' then concat(a.broker_uid,a.user_id) end) as other_new_wechat_cnt,--其他首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房源找房' and (f.resource=1 or f.resource is null) and e.survey_status=2 then concat(a.broker_uid,a.user_id) end) as survey_new_wechat_cnt,--实勘房源首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房源找房' and (f.resource=1 or f.resource is null) and nvl(e.survey_status,0)<>2 then concat(a.broker_uid,a.user_id) end) as notsurvey_new_wechat_cnt, --非实勘房源首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and f.resource=2 then concat(a.broker_uid,a.user_id) end) as marketing_inventory_new_wechat_cnt --营销房源首次微聊对数
from (
  select * from dw_db.dw_connection_wechat_sd
  where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
  and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
) a
inner join dw_db.dim_broker b
on a.broker_uid=b.broker_uid
and b.p_dt=${dealDate}
left join (
  select user_id,
  delivery_channels as channel_id,
  channel_name,
  p_dt,
  row_number() over (distribute by p_dt,user_id sort by server_time asc) rownum
  from dw_db.dw_app_access_log
  where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
  and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
) c
on a.user_id=c.user_id and a.p_dt=c.p_dt and c.rownum = 1
left join dw_db.dw_basis_dimension_delivery_channels_package d
on c.channel_id=d.channel_package_code
left join (select * from dw_db.dw_property_inventory_sd
  where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
  and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
) e
on a.inventory_id=e.inventory_id and a.p_dt=e.p_dt
left join dw_db.dw_article f
on a.article_id=f.id
and f.p_dt=${dealDate}
group by b.city_id,
nvl(lower(d.system_classification),'pc') with cube;


drop table if exists dw_db_temp.dm_platform_mth_summary_connection;
create table dw_db_temp.dm_platform_mth_summary_connection as
select case when grouping__id in (0,2) then 'all' else city_id end as city_id,
case when grouping__id in (0,1) then 'all' else platform end as platform,
count(1) as new_connection_cnt,
count(case when connection_type_id in (10,11) then 1 end) as call_pairs_cnt,
count(case when connection_type_id=10 then 1 end) as esf_call_pairs_cnt,
count(case when connection_type_id=11 then 1 end) as xf_call_pairs_cnt,
count(case when connection_type_id=12 then 1 end) as assigned_call_buyer_cnt,
count(distinct case when connection_type_id in (14,15) then user_id end) as subscribe_user_cnt,
count(distinct case when connection_type_id=14 then user_id end) as xf_subscribe_user_cnt,
count(distinct case when connection_type_id=15 then user_id end) as esf_subscribe_user_cnt
from dw_db.dw_connection_daily_summary_detail
where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
group by city_id,platform with cube;


drop table if exists dw_db_temp.dm_platform_mth_summary_demand_stats;
create table dw_db_temp.dm_platform_mth_summary_demand_stats as
select case when grouping__id in (0,2) then 'all' else a.city_id end as city_id,
case when grouping__id in (0,1) then 'all' else nvl(b.os_type,'android') end as platform,
count(distinct a.user_id) as demand_user_cnt
from db_sync.angejia__member_demand_log a
left join (
  select
     p_dt,
     user_id,
     case when app_name='a-angejia' then 'android'
          when app_name='i-angejia' then 'ios' end as os_type,
    row_number() over(partition by p_dt,user_id order by server_time desc) as rank
     from dw_db.dw_app_access_log
    where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
      and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
      and user_id>0
      and app_name in ('a-angejia', 'i-angejia')
) b
on a.user_id=b.user_id and to_date(a.created_at)=b.p_dt and b.rank=1
where year(a.created_at)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
and month(a.created_at)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
group by a.city_id,nvl(b.os_type,'android') with cube;


drop table if exists dw_db_temp.dm_platform_mth_summary_inventory;
create table dw_db_temp.dm_platform_mth_summary_inventory as
select case when grouping__id=0 then 'all' else a.city_id end as city_id,
'all' as platform,
count(distinct case when a.survey_status=2 then a.inventory_id end) as survey_cnt,
count(distinct case when a.verify_status=2 then a.inventory_id end) as total_verified_inventory_cnt
from dw_db.dw_property_inventory_sd a
inner join (
  select max(p_dt) as p_dt from dw_db.dw_property_inventory_sd
  where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
  and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end) b
on a.p_dt=b.p_dt
and year(a.p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
and month(a.p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
group by a.city_id with rollup;



drop table if exists dw_db_temp.dm_platform_mth_summary_pv;
create table dw_db_temp.dm_platform_mth_summary_pv as
select case when grouping__id in (0,2) then 'all' else city_id end as city_id,
case when grouping__id in (0,1) then 'all' else platform end as platform,
sum(uv) as uv,
sum(vppv) as vppv,
sum(vpuv) as vpuv,
sum(vcpv) as vcpv,
sum(vcuv) as vcuv
from
(
select selection_city_id as city_id,
case when app_name='a-angejia' then 'android'
     when app_name='i-angejia' then 'ios' end as platform,
count(distinct device_id) as uv,
count(case when request_page_id in ('30074','30003') then device_id end) as vppv,
count(distinct case when request_page_id in ('30074','30003') then device_id end) as vpuv,
count(case when request_page_id='30093' then device_id end) as vcpv,
count(distinct case when request_page_id='30093' then device_id end) as vcuv
from dw_db.dw_app_access_log
where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_uri not like '%/user/bind/push%'
and request_uri not like '%/common/push/acks%'
and hostname='api.angejia.com'
and app_name in ('a-angejia','i-angejia')
group by selection_city_id,
case when app_name='a-angejia' then 'android'
     when app_name='i-angejia' then 'ios' end
union all
select selection_city_id as city_id,
case when current_full_url like '%m.angejia.com%' then 'tw' else 'pc' end as platform,
count(distinct guid) as uv,
count(case when current_page_id in ('10035','10078','20008','20016') then guid end) as vppv,
count(distinct case when current_page_id in ('10035','10078','20008','20016') then guid end) as vpuv,
count(case when current_full_url rlike 'http://(sh|bj)\\.angejia\\.com/loupan/\\d+\\.html.*'
  or current_full_url rlike 'http://m\\.angejia\\.com/loupan/(sh|bj)/\\d+\\.html.*' then guid end) as vcpv,
count(distinct case when current_full_url rlike 'http://(sh|bj)\\.angejia\\.com/loupan/\\d+\\.html.*'
  or current_full_url rlike 'http://m\\.angejia\\.com/loupan/(sh|bj)/\\d+\\.html.*' then guid end) as vcuv
from dw_db.dw_web_visit_traffic_log
where year(p_dt)=case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end
and month(p_dt)=case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end
and current_full_url not like 'http://m.angejia.com/download%'
group by selection_city_id,
case when current_full_url like '%m.angejia.com%' then 'tw' else 'pc' end
) t
group by city_id, platform with cube;


drop table if exists dw_db_temp.dm_platform_mth_summary_cross;
create table dw_db_temp.dm_platform_mth_summary_cross as
select a.platform,
b.id as city_id,
b.name as city_name
from
(
  select 'all' as platform
  union all
  select 'android' as platform
  union all
  select 'ios' as platform
  union all
  select 'pc' as platform
  union all
  select 'tw' as platform
) a,
(
  select 'all' as id,'全国' as name
  union all
  select id,name from dw_db.dim_city
  where is_active=1
) b
;


insert overwrite table dm_db.dm_platform_monthly_summary partition (p_dt)
select
concat(case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end,
              case when (case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end)<10 then '0' else '' end,
              case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end) as cal_mth,
base.platform,
case when base.city_id='all' then 0 else base.city_id end as city_id,
base.city_name,
nvl(conn.new_connection_cnt,0) as new_connection_cnt,
nvl(wechat.new_wechat_cnt,0) as new_wechat_cnt,
nvl(conn.call_pairs_cnt,0) as call_pairs_cnt,
nvl(conn.assigned_call_buyer_cnt,0) as assigned_call_buyer_cnt,
nvl(conn.subscribe_user_cnt,0) as subscribe_user_cnt,
nvl(wechat.xf_loupan_new_wechat_cnt,0) as xf_loupan_new_wechat_cnt,
nvl(wechat.xf_demand_new_wechat_cnt,0) as xf_demand_new_wechat_cnt,
nvl(wechat.esf_demand_new_wechat_cnt,0) as esf_demand_new_wechat_cnt,
nvl(wechat.survey_new_wechat_cnt,0) as survey_new_wechat_cnt,
nvl(wechat.notsurvey_new_wechat_cnt,0) as notsurvey_new_wechat_cnt,
nvl(wechat.marketing_inventory_new_wechat_cnt,0) as marketing_inventory_new_wechat_cnt,
nvl(wechat.landlord_new_wechat_cnt,0) as landlord_new_wechat_cnt,
nvl(wechat.broker_new_wechat_cnt,0) as broker_new_wechat_cnt,
nvl(wechat.push_new_wechat_cnt,0) as push_new_wechat_cnt,
nvl(wechat.other_new_wechat_cnt,0) as other_new_wechat_cnt,
nvl(conn.xf_call_pairs_cnt,0) as xf_call_pairs_cnt,
nvl(conn.esf_call_pairs_cnt,0) as esf_call_pairs_cnt,
nvl(conn.xf_subscribe_user_cnt,0) as xf_subscribe_user_cnt,
nvl(conn.esf_subscribe_user_cnt,0) as esf_subscribe_user_cnt,
nvl(pv.uv,0) as uv,
nvl(pv.vpuv,0) as vpuv,
nvl(pv.vppv,0) as vppv,
nvl(pv.vcuv,0) as vcuv,
nvl(pv.vcpv,0) as vcpv,
nvl(demand.demand_user_cnt,0) as demand_user_cnt,
nvl(inv.survey_cnt,0) as survey_inventory_cnt,
nvl(inv.total_verified_inventory_cnt,0) as total_verified_inventory_cnt,
concat(case when month(${dealDate})=1 then year(${dealDate})-1 else year(${dealDate}) end,
              case when (case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end)<10 then '0' else '' end,
              case when month(${dealDate})=1 then 12 else month(${dealDate})-1 end) as cal_mth
from dw_db_temp.dm_platform_mth_summary_cross base
left join dw_db_temp.dm_platform_mth_summary_connection conn
on base.platform=conn.platform and base.city_id=conn.city_id
left join dw_db_temp.dm_platform_mth_summary_wechat wechat
on base.platform=wechat.platform and base.city_id=wechat.city_id
left join dw_db_temp.dm_platform_mth_summary_pv pv
on base.platform=pv.platform and base.city_id=pv.city_id
left join dw_db_temp.dm_platform_mth_summary_demand_stats demand
on base.platform=demand.platform and base.city_id=demand.city_id
left join dw_db_temp.dm_platform_mth_summary_inventory inv
on base.platform=inv.platform and base.city_id=inv.city_id
;
