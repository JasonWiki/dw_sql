set hive.auto.convert.join=false;
-----连接 START-----
--微聊对数
drop table if exists dw_db_temp.dm_platform_wechat_stats;
create table dw_db_temp.dm_platform_wechat_stats as
select
case when grouping__id in (0,2) then 'all' else b.city_id end as city_id,
case when grouping__id in (0,1) then 'all' else nvl(lower(d.system_classification),'pc') end as platform,
count(1) as wechat_cnt,
count(distinct a.user_id) as wechat_user_cnt,
count(case when a.property_type=1 then 1 end) as esf_wechat_cnt,
count(distinct case when a.property_type=1 then a.user_id end) as esf_wechat_user_cnt,
count(case when a.property_type=2 then 1 end) as xf_wechat_cnt,
count(distinct case when a.property_type=2 then a.user_id end) as xf_wechat_user_cnt,
count(case when a.property_type=2 and a.is_new_wechat=1 then 1 end) as xf_first_wechat_cnt,
count(case when a.property_type=2 and a.source_type='需求找房' then 1 end) as xf_paidan_wechat_cnt,
count(case when a.property_type=2 and a.source_type='需求找房' and a.is_new_wechat=1 then 1 end) as xf_paidan_first_wechat_cnt,
count(case when a.property_type=2 and a.source_type<>'需求找房' then 1 end) as xf_page_wechat_cnt,
count(case when a.property_type=2 and a.source_type<>'需求找房' and a.is_new_wechat=1 then 1 end) as xf_page_first_wechat_cnt,
count(case when a.is_new_wechat=1 then concat(a.broker_uid,a.user_id) end) as first_wechat_cnt,
count(case when a.is_new_wechat=1 and a.property_type=1 then concat(a.broker_uid,a.user_id) end) as esf_first_wechat_cnt,
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='需求找房' then concat(a.broker_uid,a.user_id) end) as demand_first_wechat_cnt,--需求找房首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房东' then concat(a.broker_uid,a.user_id) end) as landlord_first_wechat_cnt,--房东首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='顾问单页' then concat(a.broker_uid,a.user_id) end) as broker_first_wechat_cnt,--顾问首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='推送推荐' then concat(a.broker_uid,a.user_id) end) as push_first_wechat_cnt,--推荐首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房源找房' and f.resource=1 then concat(a.broker_uid,a.user_id) end) as promotion_first_wechat_cnt,--传统找房首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='其他' then concat(a.broker_uid,a.user_id) end) as other_first_wechat_cnt,--其他首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房源找房' and f.resource=1 and e.survey_status=2 then concat(a.broker_uid,a.user_id) end) as survey_first_wechat_cnt,--实勘房源首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and a.source_type='房源找房' and f.resource=1 and nvl(e.survey_status,0)<>2 then concat(a.broker_uid,a.user_id) end) as notsurvey_first_wechat_cnt, --非实勘房源首次微聊对数
count(case when a.is_new_wechat=1 and a.property_type=1 and f.resource=2 then concat(a.broker_uid,a.user_id) end) as marketing_inventory_first_wechat_cnt --营销房源首次微聊对数
from (
  select * from dw_db.dw_connection_wechat_sd
  where p_dt=${dealDate}
) a
inner join dw_db.dim_broker b
on a.broker_uid=b.broker_uid
and b.p_dt=${dealDate}
left join (
  select user_id,
  delivery_channels as channel_id,
  channel_name,
  row_number() over (distribute by user_id sort by server_time asc) rownum
  from dw_db.dw_app_access_log where p_dt = ${dealDate}
) c on a.user_id = c.user_id and c.rownum = 1
left join dw_db.dw_basis_dimension_delivery_channels_package d
on c.channel_id=d.channel_package_code
left join dw_db.dw_property_inventory_sd e
on a.inventory_id=e.inventory_id
and e.p_dt=${dealDate}
left join dw_db.dw_article f
on a.article_id=f.id
and f.p_dt=${dealDate}
group by b.city_id,
nvl(lower(d.system_classification),'pc') with cube;


--连接
drop table if exists dw_db_temp.dm_platform_connection_stats;
create table dw_db_temp.dm_platform_connection_stats as
select case when grouping__id in (0,2) then 'all' else city_id end as city_id,
case when grouping__id in (0,1) then 'all' else platform end as platform,
count(distinct concat(user_id,broker_uid,connection_type_id,nvl(user_phone,0),caller,called)) as new_connection_cnt,
count(distinct case when connection_type_id in (1,2,4,5,7,8,9,10,12,15) then concat(user_id,broker_uid,connection_type_id,nvl(user_phone,0),caller,called) end) as esf_new_connection_cnt,
count(distinct case when connection_type_id in (3,6,11,13,14) then concat(user_id,broker_uid,connection_type_id,nvl(user_phone,0),caller,called) end) as xf_new_connection_cnt,
count(distinct case when connection_type_id=10 then concat(caller,called) end) as esf_call_pairs_cnt,
count(distinct case when connection_type_id=10 then caller end) as esf_call_user_cnt,
count(distinct case when connection_type_id=11 then concat(caller,called) end) as xf_call_pairs_cnt,
count(distinct case when connection_type_id=11 then caller end) as xf_call_user_cnt,
count(distinct case when connection_type_id=12 then concat(user_id,broker_uid) end) as assigned_call_buyer_cnt,
count(distinct case when connection_type_id=14 then case when user_id=0 then user_phone else user_id end end) as xf_loupan_subscribe_user_cnt,
count(case when connection_type_id=14 then user_id end) as xf_loupan_subscribe_cnt,
count(distinct case when connection_type_id=15 then case when user_id=0 then user_phone else user_id end end) as esf_inventory_subscribe_user_cnt,
count(case when connection_type_id=15 then user_id end) as esf_inventory_subscribe_cnt
from dw_db.dw_connection_daily_summary_detail
where p_dt=${dealDate}
group by city_id,platform with cube;

-----连接 END-----


-----uv pv START-----
--request_page_id in ('30075','30076','30074','30003')
--30003 房源详情页
--30074 推广房源单页
--30075 新大陆选房单页
--30076 新大陆他人选房单页
drop table if exists dw_db_temp.dm_platform_uv_stats;
create table dw_db_temp.dm_platform_uv_stats as
select 'all' as city_id,
case when grouping__id=0 then 'all' else platform end as platform,
count(uv) as uv,
sum(vpuv) as zhaofang_vpuv,
sum(vppv) as zhaofang_vppv,
count(distinct case when t.fvppv>='5' then t.device_id end) as fvpuv
from
(
--uv,fvppv,vppv,vpuv汇总，不分城市
select
device_id,
case when app_name='a-angejia' then 'android'
     when app_name='i-angejia' then 'ios' end as platform,
count(distinct device_id) as uv,
count(case when request_page_id in ('30075','30076','30074','30003') then device_id end) as fvppv,
count(case when request_page_id in ('30074','30003') then device_id end) as vppv,
count(distinct case when request_page_id in ('30074','30003') then device_id end) as vpuv
from dw_db.dw_app_access_log
where p_dt=${dealDate}
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_uri not like '%/user/bind/push%'
and request_uri not like '%/common/push/acks%'
and hostname='api.angejia.com'
and app_name in ('a-angejia','i-angejia')
group by device_id,
case when app_name='a-angejia' then 'android'
     when app_name='i-angejia' then 'ios' end
union all
select
guid as device_id,
case when current_full_url like '%m.angejia.com%' then 'tw' else 'pc' end as platform,
count(distinct guid) as uv,
count(case when current_page_id in ('10035','10078','20008','20016') then guid end) as fvppv,
count(case when current_page_id in ('10035','10078','20008','20016') then guid end) as vppv,
count(distinct case when current_page_id in ('10035','10078','20008','20016') then guid end) as vpuv
from dw_db.dw_web_visit_traffic_log
where p_dt=${dealDate}
and current_full_url not like 'http://m.angejia.com/download%'
group by guid,
case when current_full_url like '%m.angejia.com%' then 'tw' else 'pc' end
) t
group by t.platform with rollup

union all
--uv,fvppv,vppv,vpuv细分,分城市,平台
select city_id,
case when grouping__id=1 then 'all' else platform end as platform,
count(uv) as uv,
sum(vpuv) as zhaofang_vpuv,
sum(vppv) as zhaofang_vppv,
count(distinct case when t.fvppv>='5' then t.device_id end) as fvpuv
from
(
select selection_city_id as city_id,
case when app_name='a-angejia' then 'android'
     when app_name='i-angejia' then 'ios' end as platform,
device_id,
count(distinct device_id) as uv,
count(case when request_page_id in ('30075','30076','30074','30003') then device_id end) as fvppv,
count(case when request_page_id in ('30074','30003') then device_id end) as vppv,
count(distinct case when request_page_id in ('30074','30003') then device_id end) as vpuv
from dw_db.dw_app_access_log
where p_dt=${dealDate}
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_uri not like '%/user/bind/push%'
and request_uri not like '%/common/push/acks%'
and hostname='api.angejia.com'
and app_name in ('a-angejia','i-angejia')
group by device_id,
selection_city_id,
case when app_name='a-angejia' then 'android'
     when app_name='i-angejia' then 'ios' end
union all
select selection_city_id as city_id,
case when current_full_url like '%m.angejia.com%' then 'tw' else 'pc' end as platform,
guid as device_id,
count(distinct guid) as uv,
count(case when current_page_id in ('10035','10078','20008','20016') then guid end) as fvppv,
count(case when current_page_id in ('10035','10078','20008','20016') then guid end) as vppv,
count(distinct case when current_page_id in ('10035','10078','20008','20016') then guid end) as vpuv
from dw_db.dw_web_visit_traffic_log
where p_dt=${dealDate}
and current_full_url not like 'http://m.angejia.com/download%'
group by guid,
selection_city_id,
case when current_full_url like '%m.angejia.com%' then 'tw' else 'pc' end
) t
group by city_id,platform grouping sets((city_id,platform),(city_id));

--fud
drop table if exists dw_db_temp.dm_platform_fud_stats;
create table dw_db_temp.dm_platform_fud_stats as
select
case when grouping__id in (0,2) then 'all' else city_id end as city_id,
case when grouping__id in (0,1) then 'all' else platform end as platform,
count(distinct case when p_dt=${dealDate} and rank1=1 then device_id end) as fud,
count(distinct case when p_dt=${dealDate} and rank2=1 then user_id end) as reg_user_cnt
from
(select device_id,
user_id,
p_dt,
selection_city_id as city_id,
case when app_name='a-angejia' then 'android'
     when app_name='i-angejia' then 'ios' end as platform,
row_number() over(distribute by device_id sort by p_dt) as rank1,
row_number() over(distribute by user_id sort by p_dt) as rank2
from dw_db.dw_app_access_log
where (p_dt between date_sub(${dealDate}, 89) and ${dealDate})
and app_name in ('a-angejia', 'i-angejia')
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_uri not like '%/user/bind/push%'
and request_uri not like '%/common/push/acks%'
and hostname='api.angejia.com'
) t
where p_dt=${dealDate}
group by city_id,platform with cube;

--首页和新房
drop table if exists dw_db_temp.dm_platform_xf_action_ud_stats;
create table dw_db_temp.dm_platform_xf_action_ud_stats as
select case when grouping__id in (0,2) then 'all' else city_id end as city_id
,case when grouping__id in (0,1) then 'all' else platform end as platform
,sum(main_page_pv) as main_page_pv
,sum(main_page_ud) as main_page_ud
,sum(xf_main_page_pv) as xf_main_page_pv
,sum(xf_main_page_ud) as xf_main_page_ud
,sum(xf_list_pv) as xf_list_pv
,sum(xf_list_ud) as xf_list_ud
,sum(xf_vcpv) as xf_vcpv
,sum(xf_vcud) as xf_vcud
,sum(xf_loupan_call_pv) as xf_loupan_call_pv
,sum(xf_loupan_call_ud) as xf_loupan_call_ud
,sum(xf_room_pv) as xf_room_pv
,sum(xf_room_ud) as xf_room_ud
,sum(xf_room_call_pv) as xf_room_call_pv
,sum(xf_room_call_ud) as xf_room_call_ud
from
(select ccid as city_id,
case when name='a-angejia' then 'android' when name='i-angejia' then 'ios' end as platform,
count(case when current_page_id='1-960000' and action_id in ('1-960036') then dvid end) as main_page_pv, --首页pv
count(distinct case when current_page_id='1-960000' and action_id in ('1-960036') then dvid end) as main_page_ud, --首页ud
count(case when current_page_id='1-960000' and action_id in ('1-960089') then dvid end) as xf_main_page_pv, --新房首页pv
count(distinct case when current_page_id='1-960000' and action_id in ('1-960089') then dvid end) as xf_main_page_ud, --新房首页ud
count(case when current_page_id='1-4800000' and action_id in ('1-4800001') then dvid end) as xf_list_pv, --新房列表页pv
count(distinct case when current_page_id='1-4800000' and action_id in ('1-4800001') then dvid end) as xf_list_ud, --新房列表页ud
count(case when current_page_id='1-4900000' and action_id in ('1-4900001') then dvid end) as xf_vcpv, --新房楼盘单页pv
count(distinct case when current_page_id='1-4900000' and action_id in ('1-4900001') then dvid end) as xf_vcud, --新房楼盘单页ud
count(case when current_page_id='1-4900000' and action_id in ('1-4900017') then dvid end) as xf_loupan_call_pv, --新房电话点击pv
count(distinct case when current_page_id='1-4900000' and action_id in ('1-4900017') then dvid end) as xf_loupan_call_ud, --新房电话点击ud
count(case when current_page_id='1-5000000' and action_id in ('1-5000001') then dvid end) as xf_room_pv, --新房户型单页pv
count(distinct case when current_page_id='1-5000000' and action_id in ('1-5000001') then dvid end) as xf_room_ud, --新房户型单页ud
count(case when current_page_id='1-5000000' and action_id in ('1-5000006') then dvid end) as xf_room_call_pv, --新房户型单页电话点击pv
count(distinct case when current_page_id='1-5000000' and action_id in ('1-5000006') then dvid end) as xf_room_call_ud --新房户型单页电话点击ud
from dw_db.dw_app_action_detail_log
where p_dt=${dealDate}
and current_page_id in ('1-960000',--首页
  '1-4800000',--新房列表页
  '1-4900000',--楼盘单页
  '1-5000000'--户型单页
)
and name in ('a-angejia','i-angejia')
group by ccid,case when name='a-angejia' then 'android' when name='i-angejia' then 'ios' end

union all

select case when current_full_url rlike 'http://sh\\.angejia\\.com.*' or current_full_url rlike 'http://m\\.angejia\\.com/loupan/sh.*' then '1'
  when current_full_url rlike 'http://bj\\.angejia\\.com.*' or current_full_url rlike 'http://m\\.angejia\\.com/loupan/bj.*' then '2' end as city_id
  ,case when current_full_url rlike 'http://m\\.angejia\\.com.*' then 'tw'
  when current_full_url rlike 'http://(sh|bj)\\.angejia\\.com.*' then 'pc' end as platform
,0 as main_page_pv
,0 as main_page_ud
,0 as xf_main_page_pv
,0 as xf_main_page_ud
,0 as xf_list_pv
,0 as xf_list_ud,
count(case when current_full_url rlike 'http://(sh|bj)\\.angejia\\.com/loupan/\\d+\\.html.*'
  or current_full_url rlike 'http://m\\.angejia\\.com/loupan/(sh|bj)/\\d+\\.html.*' then guid end) as xf_vcpv,
count(distinct case when current_full_url rlike 'http://(sh|bj)\\.angejia\\.com/loupan/\\d+\\.html.*'
  or current_full_url rlike 'http://m\\.angejia\\.com/loupan/(sh|bj)/\\d+\\.html.*' then guid end) as xf_vcud
,0 as xf_loupan_call_pv
,0 as xf_loupan_call_ud
,0 as xf_room_pv
,0 as xf_room_ud
,0 as xf_room_call_pv
,0 as xf_room_call_ud
from dw_db.dw_web_visit_traffic_log
where p_dt=${dealDate}
and (current_full_url rlike 'http://m\\.angejia\\.com.*'
  or current_full_url rlike 'http://(sh|bj)\\.angejia\\.com.*')
group by case when current_full_url rlike 'http://sh\\.angejia\\.com.*' or current_full_url rlike 'http://m\\.angejia\\.com/loupan/sh.*' then '1'
  when current_full_url rlike 'http://bj\\.angejia\\.com.*' or current_full_url rlike 'http://m\\.angejia\\.com/loupan/bj.*' then '2' end
  ,case when current_full_url rlike 'http://m\\.angejia\\.com.*' then 'tw'
  when current_full_url rlike 'http://(sh|bj)\\.angejia\\.com.*' then 'pc' end
) t
group by city_id, platform with cube
;

-----uv pv END-----


-----其他指标 START-----
---选房单量
drop table if exists dw_db_temp.dm_platform_xuanfangdan_stats;
create table dw_db_temp.dm_platform_xuanfangdan_stats as
select case when grouping__id in (0,2) then 'all' else c.city_id end as city_id,
case when grouping__id in (0,1) then 'all' else
  case when b.os_type='ios' then 'ios' else 'android' end
      end as platform,
count(distinct a.id) as xuanfang_cnt
from db_sync.angejia__buyer_push_recommend_inventory_batch a
left join
(select user_id,
   case when app_name='a-angejia' then 'android'
        when app_name='i-angejia' then 'ios' end as os_type,
  row_number() over(partition by user_id order by server_time desc) as rank
   from dw_db.dw_app_access_log
  where p_dt between date_sub(${dealDate},29) and ${dealDate}
    and user_id>0
    and app_name in ('a-angejia', 'i-angejia')
) b
on a.user_id=b.user_id and b.rank=1
left join db_sync.angejia__broker c
on a.broker_uid=c.user_id
where to_date(a.created_at)=${dealDate}
group by c.city_id,
case when b.os_type='ios' then 'ios' else 'android' end with cube;

--发需求用户数
drop table if exists dw_db_temp.dm_platform_demand_stats;
create table dw_db_temp.dm_platform_demand_stats as
select case when grouping__id in (0,2) then 'all' else a.city_id end as city_id,
case when grouping__id in (0,1) then 'all' else nvl(b.os_type,'android') end as platform,
count(distinct a.user_id) as demand_user_cnt
from db_sync.angejia__member_demand_log a
left join
(select user_id,
   case when app_name='a-angejia' then 'android'
        when app_name='i-angejia' then 'ios' end as os_type,
  row_number() over(partition by user_id order by server_time desc) as rank
   from dw_db.dw_app_access_log
  where p_dt=${dealDate}
    and user_id>0
    and app_name in ('a-angejia', 'i-angejia')
) b
on a.user_id=b.user_id and b.rank=1
where to_date(a.created_at)=${dealDate}
group by a.city_id,nvl(b.os_type,'android') with cube;

--总房源、实勘房源
drop table if exists dw_db_temp.dm_platform_inv_stats;
create table dw_db_temp.dm_platform_inv_stats as
select case when grouping__id=0 then 'all' else city_id end as city_id,
'all' as platform,
count(distinct case when survey_status=2 then inventory_id end) as survey_cnt,
count(distinct case when survey_status<>2 then inventory_id end) as non_survey_cnt,
count(distinct case when verify_status=2 then inventory_id end) as total_verified_inventory_cnt,
count(distinct inventory_id) as total_online_inv_cnt
from dw_db.dw_property_inventory_sd
where p_dt=${dealDate}
group by city_id with rollup;

--二手房录入用户数
drop table if exists dw_db_temp.dm_platform_esf_new_demand_user;
create table dw_db_temp.dm_platform_esf_new_demand_user as
select case when grouping__id in (0,2) then 'all' else a.city_id end as city_id,
case when grouping__id in (0,1) then 'all' else c.platform end as platform,
count(distinct b.user_id) as esf_buyer_cnt
from db_sync.angejia__demand a
inner join db_sync.angejia__broker_customer_bind_user b
on a.buyer_uid=b.broker_customer_id
left join (select * from dw_db.dw_user_sd where p_dt=${dealDate}) c
on b.user_id=c.user_id
where to_date(a.created_at)=${dealDate}
and a.status=1
group by a.city_id,c.platform with cube;

--新房录入用户数
drop table if exists dw_db_temp.dm_platform_xf_new_demand_user;
create table dw_db_temp.dm_platform_xf_new_demand_user as
select case when grouping__id in (0,2) then 'all' else a.city_id end as city_id,
case when grouping__id in (0,1) then 'all' else c.platform end as platform,
count(distinct b.user_id) as xf_buyer_cnt
from db_sync.xinfang__buyer a
inner join db_sync.angejia__broker_customer_bind_user b
on a.customer_id=b.broker_customer_id
left join (select * from dw_db.dw_user_sd where p_dt=${dealDate}) c
on b.user_id=c.user_id
where to_date(a.created_at)=${dealDate}
group by a.city_id,c.platform with cube;

-----其他指标 END-----

-----带看 START-----
--线上来源带看
drop table if exists dw_db_temp.dm_platform_visit_stats;
create table dw_db_temp.dm_platform_visit_stats as
select case when grouping__id in (0,2) then 'all' else a.inv_city_id end as city_id,
case when grouping__id in (0,1) then 'all' else b.platform end as platform,
count(distinct concat(a.buyer_uid,a.broker_uid)) as online_visit_cnt,
count(distinct a.user_id) as online_visit_user_cnt,
count(distinct case when a.type=1 then concat(a.buyer_uid,a.broker_uid) end) as esf_online_visit_cnt,
count(distinct case when a.type=1 then a.user_id end) as esf_online_visit_user_cnt,
count(distinct case when a.type=2 then concat(a.buyer_uid,a.broker_uid) end) as xf_online_visit_cnt,
count(distinct case when a.type=2 then a.user_id end) as xf_online_visit_user_cnt
from dw_db.dw_visit a
inner join (
  select user_id,lower(platform) as platform
  from dw_db.dw_user_sd
  where p_dt=${dealDate}
) b
on a.user_id=b.user_id
where to_date(a.visit_started_at)=${dealDate}
group by a.inv_city_id,b.platform with cube;

-----带看 END-----

-----成交 START-----
--线上来源成交
drop table if exists dw_db_temp.dm_platform_trans_stats;
create table dw_db_temp.dm_platform_trans_stats as
select case when grouping__id in (0,2) then 'all' else a.inv_city_id end as city_id,
case when grouping__id in (0,1) then 'all' else b.platform end as platform,
count(distinct a.sn) as online_trans_cnt,
count(distinct case when a.transaction_type=1 then a.sn end) as esf_online_trans_cnt,
count(distinct case when a.transaction_type=3 then a.sn end) as xf_online_trans_cnt,
count(distinct a.user_id) as online_trans_user_cnt,
count(distinct case when a.transaction_type=1 then a.user_id end) as esf_online_trans_user_cnt,
count(distinct case when a.transaction_type=3 then a.user_id end) as xf_online_trans_user_cnt
from dw_db.dw_trans a
inner join (
  select user_id,case when platform='android' then 'android' when platform='iOS' then 'ios' end as platform
  from dw_db.dw_user_sd
  where p_dt=${dealDate}
) b
on a.user_id=b.user_id
and to_date(a.signed_at)=${dealDate}
and a.is_bomb<>1
group by a.inv_city_id,b.platform with cube;

-----成交 END-----

-----30天 START-----
--近30天二手房录入用户
drop table if exists dw_db_temp.dm_platform_new_demand_user_30days;
create table dw_db_temp.dm_platform_new_demand_user_30days as
select b.user_id as user_id,a.city_id as city_id
from dw_db.dw_customer_demand a
inner join db_sync.angejia__broker_customer_bind_user b
on a.buyer_uid=b.broker_customer_id
where a.p_dt>=date_sub(${dealDate},29)
and to_date(a.created_at)>=date_sub(${dealDate},29)
and a.status=1
group by b.user_id,a.city_id
;

--近30天连接、录入、带看、成交ud
drop table if exists dw_db_temp.dm_platform_measure_30days;
create table dw_db_temp.dm_platform_measure_30days as
select case when grouping__id in (0,2) then 'all' else a.selection_city_id end as city_id,
case when grouping__id in (0,1) then 'all' else a.platform end as platform,
count(distinct b.user_id) as 30_conn_user_cnt,
count(distinct c.user_id) as 30_new_buyer_cnt,
count(distinct d.user_id) as 30_visit_user_cnt,
count(distinct e.user_id) as 30_trans_user_cnt
from (select distinct case when platform='android' then 'android' when platform='iOS' then 'ios' end as platform,
  user_id,
  selection_city_id
  from dw_db.dw_app_access_log
  where p_dt>=date_sub(${dealDate},29) and p_dt<=${dealDate}
  and app_name in ('a-angejia','i-angejia')
  and request_uri not like '/mobile/member/configs%'
  and request_uri not like '/mobile/member/districts/show%'
  and request_uri not like '/mobile/member/inventories/searchFilters%'
  and request_uri not like '%/user/bind/push%'
  and request_uri not like '%/common/push/acks%'
  and hostname='api.angejia.com') a
left join (
  --30天微聊
  select user_id,city_id from (select a.user_id,b.city_id as city_id
  from dw_db.dw_connection_wechat_sd a
  inner join dw_db.dim_broker b
  on a.broker_uid=b.broker_uid
  and b.p_dt=${dealDate}
  and a.p_dt>=date_sub(${dealDate},29) and a.p_dt<=${dealDate}
  group by a.user_id,b.city_id) wechat
  union all
  select user_id,city_id
  from dw_db.dw_connection_daily_summary_detail
  where p_dt>=date_sub(${dealDate},29) and p_dt<=${dealDate}
  and connection_type_id in (10,11,12,13,14,15)
) b
on a.user_id=b.user_id
left join dw_db_temp.dm_platform_new_demand_user_30days c
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
group by a.selection_city_id,a.platform with cube
;
-----30天 END-----


--平台 城市cross join
drop table if exists dw_db_temp.dm_platform_cross;
create table dw_db_temp.dm_platform_cross as
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


insert overwrite table dm_db.dm_platform_sd partition (p_dt = ${dealDate})
select
${dealDate} as cal_dt,
base.platform,
case when base.city_id='all' then 0 else base.city_id end as city_id,
base.city_name as city_name,
nvl(wechat.wechat_cnt,0)+nvl(conn.esf_call_pairs_cnt,0)+nvl(conn.xf_call_pairs_cnt,0)+nvl(conn.assigned_call_buyer_cnt,0) as connection_cnt,
nvl(wechat.wechat_cnt,0) as wechat_cnt,
nvl(conn.esf_call_pairs_cnt,0)+nvl(conn.xf_call_pairs_cnt,0) as call_pairs_cnt,
nvl(wechat.first_wechat_cnt,0) as first_wechat_cnt,
nvl(wechat.xf_first_wechat_cnt,0) as xinfang_first_wechat_cnt,
nvl(wechat.demand_first_wechat_cnt,0) as demand_first_wechat_cnt,
0 as promotion_survey_first_wechat_cnt, --2016-12-15废弃
0 as promotion_notsurvey_first_wechat_cnt, --2016-12-15废弃
0 as notpromotion_survey_first_wechat_cnt, --2016-12-15废弃
0 as notpromotion_notsurvey_first_wechat_cnt, --2016-12-15废弃
nvl(wechat.landlord_first_wechat_cnt,0) as landlord_first_wechat_cnt,
nvl(wechat.broker_first_wechat_cnt,0) as broker_first_wechat_cnt,
nvl(wechat.push_first_wechat_cnt,0) as push_first_wechat_cnt,
nvl(wechat.other_first_wechat_cnt,0) as other_first_wechat_cnt,
nvl(uv.uv,0) as uv,
nvl(uv.zhaofang_vpuv,0) as zhaofang_vpuv,
nvl(uv.zhaofang_vppv,0) as zhaofang_vppv,
nvl(uv.fvpuv,0) as fvpuv,
nvl(xuanfang.xuanfang_cnt,0) as xuanfang_cnt,
nvl(demand.demand_user_cnt,0) as demand_user_cnt,
nvl(inv.survey_cnt,0) as survey_cnt,
nvl(inv.total_verified_inventory_cnt,0) as total_verified_inventory_cnt,
nvl(inv.non_survey_cnt,0) as non_survey_cnt,
nvl(inv.total_online_inv_cnt,0) as total_online_inv_cnt,
nvl(wechat.promotion_first_wechat_cnt,0) as promotion_first_wechat_cnt,
nvl(trans.online_trans_cnt,0) as online_trans_cnt,
nvl(trans.esf_online_trans_cnt,0) as esf_online_trans_cnt,
nvl(trans.xf_online_trans_cnt,0) as xf_online_trans_cnt,
nvl(trans.online_trans_user_cnt,0) as online_trans_user_cnt,
nvl(trans.esf_online_trans_user_cnt,0) as esf_online_trans_user_cnt,
nvl(trans.xf_online_trans_user_cnt,0) as xf_online_trans_user_cnt,
nvl(visit.online_visit_cnt,0) as online_visit_cnt,
nvl(visit.esf_online_visit_cnt,0) as esf_online_visit_cnt,
nvl(visit.xf_online_visit_cnt,0) as xf_online_visit_cnt,
nvl(visit.online_visit_user_cnt,0) as online_visit_user_cnt,
nvl(visit.esf_online_visit_user_cnt,0) as esf_online_visit_user_cnt,
nvl(visit.xf_online_visit_user_cnt,0) as xf_online_visit_user_cnt,
nvl(conn.new_connection_cnt,0) as new_connection_cnt,
nvl(conn.esf_new_connection_cnt,0) as esf_new_connection_cnt,
nvl(conn.xf_new_connection_cnt,0) as xf_new_connection_cnt,
nvl(fud.reg_user_cnt,0) as reg_user_cnt,
nvl(fud.fud,0) as fud,
nvl(wechat.wechat_user_cnt,0) as wechat_user_cnt,
nvl(wechat.esf_wechat_user_cnt,0) as esf_wechat_user_cnt,
nvl(wechat.xf_wechat_user_cnt,0) as xf_wechat_user_cnt,
nvl(conn.esf_call_pairs_cnt,0) as esf_call_pairs_cnt,
nvl(conn.xf_call_pairs_cnt,0) as xf_call_pairs_cnt,
nvl(conn.esf_call_user_cnt,0) as esf_call_user_cnt,
nvl(conn.xf_call_user_cnt,0) as xf_call_user_cnt,
nvl(conn.assigned_call_buyer_cnt,0) as assigned_call_buyer_cnt,
nvl(wechat.xf_page_wechat_cnt,0)+nvl(wechat.xf_paidan_wechat_cnt,0) as xf_total_wechat_cnt,
nvl(wechat.xf_page_wechat_cnt,0) as xf_page_wechat_cnt,
nvl(wechat.xf_paidan_wechat_cnt,0) as xf_paidan_wechat_cnt,
nvl(wechat.xf_page_first_wechat_cnt,0)+nvl(wechat.xf_paidan_first_wechat_cnt,0) as xf_total_first_wechat_cnt,
nvl(wechat.xf_page_first_wechat_cnt,0) as xf_page_first_wechat_cnt,
nvl(wechat.xf_paidan_first_wechat_cnt,0) as xf_paidan_first_wechat_cnt,
nvl(wechat.esf_wechat_user_cnt,0)+nvl(conn.esf_call_user_cnt,0)+nvl(conn.assigned_call_buyer_cnt,0) as esf_connection_user_cnt,
nvl(wechat.xf_wechat_user_cnt,0)+nvl(conn.xf_call_user_cnt,0) as xf_connection_user_cnt,
nvl(esfbuyer.esf_buyer_cnt,0) as esf_buyer_cnt,
nvl(xfbuyer.xf_buyer_cnt,0) as xf_buyer_cnt,
nvl(xfud.xf_vcpv,0) as xf_vcpv,
nvl(xfud.xf_vcud,0) as xf_vcud,
nvl(xf.xf_online_loupan_cnt,0) as xf_online_loupan_cnt,
nvl(xf.xf_coop_loupan_cnt,0) as xf_coop_loupan_cnt,
nvl(30days.30_conn_user_cnt,0) as esf_30_conn_user_cnt,
nvl(30days.30_new_buyer_cnt,0) as esf_30_new_buyer_cnt,
nvl(30days.30_visit_user_cnt,0) as esf_30_visit_user_cnt,
nvl(30days.30_trans_user_cnt,0) as esf_30_trans_user_cnt,
nvl(broker.on_duty_broker_cnt,0) as on_duty_broker_cnt,
nvl(conn.xf_loupan_subscribe_user_cnt,0) as xf_loupan_subscribe_user_cnt,
nvl(conn.xf_loupan_subscribe_cnt,0) as xf_loupan_subscribe_cnt,
nvl(wechat.survey_first_wechat_cnt,0) as survey_first_wechat_cnt,
nvl(wechat.notsurvey_first_wechat_cnt,0) as notsurvey_first_wechat_cnt,
nvl(wechat.marketing_inventory_first_wechat_cnt,0) as marketing_inventory_first_wechat_cnt,
nvl(conn.esf_inventory_subscribe_user_cnt,0) as esf_inventory_subscribe_user_cnt,
nvl(conn.esf_inventory_subscribe_cnt,0) as esf_inventory_subscribe_cnt
from dw_db_temp.dm_platform_cross base
left join dw_db_temp.dm_platform_wechat_stats wechat  --微聊
on base.city_id=wechat.city_id and base.platform=wechat.platform
left join dw_db_temp.dm_platform_uv_stats uv --uv
on base.city_id=uv.city_id and base.platform=uv.platform
left join dw_db_temp.dm_platform_xuanfangdan_stats xuanfang --选房单
on base.city_id=xuanfang.city_id and base.platform=xuanfang.platform
left join dw_db_temp.dm_platform_demand_stats demand --发需求
on base.city_id=demand.city_id and base.platform=demand.platform
left join dw_db_temp.dm_platform_inv_stats inv --房源
on base.city_id=inv.city_id and base.platform=inv.platform
left join dw_db_temp.dm_platform_visit_stats visit --带看
on base.city_id=visit.city_id and base.platform=visit.platform
left join dw_db_temp.dm_platform_trans_stats trans --成交
on base.city_id=trans.city_id and base.platform=trans.platform
left join dw_db_temp.dm_platform_fud_stats fud --fud
on base.city_id=fud.city_id and base.platform=fud.platform
left join dw_db_temp.dm_platform_esf_new_demand_user esfbuyer --二手房录入
on base.city_id=esfbuyer.city_id and base.platform=esfbuyer.platform
left join dw_db_temp.dm_platform_xf_new_demand_user xfbuyer --新房录入
on base.city_id=xfbuyer.city_id and base.platform=xfbuyer.platform
left join dw_db_temp.dm_platform_xf_action_ud_stats xfud --vcpv
on base.city_id=xfud.city_id and base.platform=xfud.platform
left join dw_db_temp.dm_platform_connection_stats conn
on base.city_id=conn.city_id and base.platform=conn.platform
left join (
  select case when grouping__id=0 then 'all' else city_id end as city_id,
  'all' as platform,
  count(distinct case when display_status=2 then id end) as xf_online_loupan_cnt,
  count(distinct case when partner_status=1 and display_status=2 then id end) as xf_coop_loupan_cnt
  from db_sync.xinfang__loupan_basic
  group by city_id with rollup
) xf --新房
on base.city_id=xf.city_id and base.platform=xf.platform
left join dw_db_temp.dm_platform_measure_30days 30days
on base.city_id=30days.city_id and base.platform=30days.platform
left join (
  select case when grouping__id=0 then 'all' else city_id end as city_id,
  'all' as platform,
  count(distinct broker_uid) as on_duty_broker_cnt
  from dw_db.dim_broker
  where p_dt=${dealDate}
  and status_id=2
  group by city_id with rollup
) broker
on base.city_id=broker.city_id and base.platform=broker.platform
;
