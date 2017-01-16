-- 自营销效果 vs. 非自营销效果 日报
-- 经纪人自营销房源id,broker_id
drop table if exists dw_temp_angejia.zhiwen_promotion_effect_inventory;
create table dw_temp_angejia.zhiwen_promotion_effect_inventory as 
select distinct inventory_id
from db_sync.angejia__article 
where status = 1 
and to_date(spread_at) <= ${dealDate}
and broker_uid>0
;


-- 在职安家顾问broker_id
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_broker_id;
create table dw_temp_angejia.zhiwen_promotion_2_effect_broker_id as
select distinct user_id
from dw_db.dw_broker_sd
where p_dt = ${dealDate}
and agent_name is not null
and agent_name not like '%测试%'
and broker_duty_status_id = '2'
and broker_type_id in ('1','2')
;


-- 联系顾问
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_connect_broker_cnt;
create table dw_temp_angejia.zhiwen_promotion_2_effect_connect_broker_cnt as
select 
    -- 1 pc , 2 tw
    type
    ,case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,count(distinct a.user_phone,a.inventory_id,a.broker_uid) as connect_broker_cnt
from db_sync.angejia__page_information_statistics a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b
    on a.inventory_id = b.inventory_id
inner join dw_temp_angejia.zhiwen_promotion_2_effect_broker_id c
    on a.broker_uid = c.user_id
where to_date(a.created_at) = ${dealDate}
and a.action_type = 9
group by a.type,case when b.inventory_id is not null then '1' else '0' end
;


-- 微聊
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt;
create table dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt as
select 
    case when wl_type <> '3' and device_from = '1' then '1'   -- android 房源单页
        when wl_type <> '3' and device_from = '2' then '2'    -- ios 房源单页
        when wl_type = '3' and device_from = '1' then '3'     -- android 经纪人单页
        when wl_type = '3' and device_from = '2' then '4'     -- ios 经纪人单页
        when device_from = '3' then '5' end as type
    ,case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,concat(a.user_id,'-',a.broker_id,'-',to_date(a.created_at)) as wechat_key
    ,a.broker_id
    ,a.user_id
    ,to_date(a.created_at) as p_dt
    ,count(distinct a.inventory_id) as wechat_cnt
    ,min(a.created_at) as send_click_time
from db_sync.angejia__wl_statistics a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b
    on a.inventory_id = b.inventory_id
inner join dw_temp_angejia.zhiwen_promotion_2_effect_broker_id c
    on a.broker_id = c.user_id
where to_date(a.created_at) = ${dealDate}
and a.source = 0
group by case when wl_type <> '3' and device_from = '1' then '1'
    when wl_type <> '3' and device_from = '2' then '2'
    when wl_type = '3' and device_from = '1' then '3' 
    when wl_type = '3' and device_from = '2' then '4'
    when device_from = '3' then '5' end
    ,case when b.inventory_id is not null then '1' else '0' end
    ,a.broker_id, a.user_id, to_date(a.created_at)
;


-- 微聊对,设[用户id]-[经纪人id]-[日期]为唯一键，用户发送，设from_uid-to_uid-p_dt为微聊key, 否则设to_uid-from_uid-p_dt为微聊key
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_wechat_today;
create table dw_temp_angejia.zhiwen_promotion_2_effect_wechat_today as
select case when account_type = 1 then concat(from_uid,'-',to_uid,'-',p_dt) else concat(to_uid,'-',from_uid,'-',p_dt) end as wechat_key
    ,from_uid
    ,to_uid
    ,account_type
    ,content
    ,created_at
from dw_db.dw_connection_wechat
where p_dt = ${dealDate}
and account_type in (1,2)
;


drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back_1;   -- 剔除用户发起前的微聊记录
create table dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back_1 as
select 
    a.wechat_key
    ,count(case when a.account_type=1 then a.wechat_key end) as user_wechat_cnt   -- 用户回复内容条数
    ,min(case when a.account_type=1 then a.created_at end) as min_user_send_time    -- 用户最小发送时间
from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_today a 
inner join 
    (   -- 微聊对微聊内容，以用户发起最小时间开始计
    select wechat_key
        ,min(send_click_time) as first_send_click_time
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt
    group by wechat_key
    ) b on a.wechat_key = b.wechat_key
    -- a表部分微聊内容发生在上述时间之后，增加60秒
where unix_timestamp(a.created_at)+60 > unix_timestamp(b.first_send_click_time)
group by a.wechat_key
;


drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back_2;   -- 经纪人回复最小时间
create table dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back_2 as
select 
a.wechat_key
,min(case when a.account_type=2 then unix_timestamp(a.created_at) - unix_timestamp(b.min_user_send_time) end) as broker_back_time
from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_today a
inner join dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back_1 b
    on a.wechat_key=b.wechat_key
where a.created_at >= b.min_user_send_time
group by a.wechat_key
;


drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back;   -- 微聊深度、经纪人有回复数、5分钟回复次数
create table dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back as
select 
    a.wechat_key
    ,a.type
    ,a.promotion_check
    ,a.wechat_cnt       -- 微聊对数
    ,nvl(b.user_wechat_cnt,0) as user_wechat_cnt        -- 用户微聊深度
    ,case when c.broker_back_time is not null then a.wechat_cnt else '0' end as broker_back_cnt        -- 有回复微聊对数
    ,case when c.broker_back_time<=300 then a.wechat_cnt else '0' end as 5_min_back_cnt         -- 5分钟回复对数
from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt a
left join dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back_1 b 
    on a.wechat_key = b.wechat_key
left join dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back_2 c 
    on a.wechat_key = c.wechat_key
;


-- 电话click[当前无法区分具体的用户来电来源，使用click做区分]
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_call_click;
create table dw_temp_angejia.zhiwen_promotion_2_effect_call_click as
select 
    case when name='a-angejia' then 'android' else 'ios' end as type
    ,get_json_object(extend, '$.vpid') as inventory_id
    ,click_time
from dw_db.dw_app_action_detail_log
where p_dt = ${dealDate}
and action_id = '1-100008'

union all
select 
    'tw' as type
    ,get_json_object(page_param, '$.inventoryId') as inventory_id
    ,server_time as click_time
from dw_db.dw_web_action_detail_log
where p_dt = ${dealDate}
and action_id = '4-320002'
;

drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_call_click_2;
create table dw_temp_angejia.zhiwen_promotion_2_effect_call_click_2 as
select a.type
    ,a.inventory_id
from dw_temp_angejia.zhiwen_promotion_2_effect_call_click a
left join (select inventory_id,max(click_time) as max_click_time from dw_temp_angejia.zhiwen_promotion_2_effect_call_click group by inventory_id) b
    on a.inventory_id = b.inventory_id and a.click_time = b.max_click_time      -- 以最后一次点击时间为准，判断电话来源的用户端
where b.inventory_id is not null
group by a.type,a.inventory_id
;


-- 自营销和非自营销电话连接数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_call_cnt;
create table dw_temp_angejia.zhiwen_promotion_2_effect_call_cnt as
select 
    case when c.inventory_id is not null and d.broker_duty_status_id = '2' and d.agent_name is not null and d.agent_name not like '%测试%' then '1'       -- 自营销房源电话
        when c.inventory_id is null and d.broker_duty_status_id = '2' and d.agent_name is not null and d.agent_name not like '%测试%' and d.user_id is not null then '0'      -- 非自营销房源，用户打经纪人电话
        when c.inventory_id is null and d.user_id is null then '2'        -- 非自营销房源，用户打房东电话
        end as promotion_check
    ,case when e.type is not null then e.type else 'pc' end as type
    ,count(distinct b.called_uid, b.caller, a.inventory_id) as call_cnt
from db_sync.angejia__call_relation_with_inventory a
left join db_sync.angejia__call_log b
    on a.call_log_id = b.id
left join dw_temp_angejia.zhiwen_promotion_effect_inventory c
    on a.inventory_id = c.inventory_id
left join dw_db.dw_broker_sd d
    on b.called_uid = d.user_id and d.p_dt = ${dealDate}
left join dw_temp_angejia.zhiwen_promotion_2_effect_call_click_2 e 
    on a.inventory_id = e.inventory_id
where to_date(a.created_at) = ${dealDate}
and b.call_type = 2
group by case when c.inventory_id is not null and d.broker_duty_status_id = '2' and d.agent_name is not null and d.agent_name not like '%测试%' then '1'       -- 自营销房源电话
        when c.inventory_id is null and d.broker_duty_status_id = '2' and d.agent_name is not null and d.agent_name not like '%测试%' and d.user_id is not null then '0'      -- 非自营销房源，用户打经纪人电话
        when c.inventory_id is null and d.user_id is null then '2'        -- 非自营销房源，用户打房东电话
        end
    ,case when e.type is not null then e.type else 'pc' end
;


-- 自营销房源量
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_id;
create table dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_id as
select distinct a.inventory_id
from db_sync.angejia__article a
inner join dw_temp_angejia.zhiwen_promotion_2_effect_broker_id b
    on a.broker_uid = b.user_id
left join dw_db.dw_property_inventory_sd c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where a.status = 1 
and to_date(a.spread_at) <= ${dealDate}
and a.broker_uid>0
and c.inventory_id is not null
;


-- 自营销和非自营销房源量
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_cnt;
create table dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_cnt as
select count(case when b.inventory_id is not null then a.inventory_id end) as promotion_inventory_cnt
,count(case when b.inventory_id is null then a.inventory_id end) as not_promotion_inventory_cnt
,count(*) as inventory_cnt
from dw_db.dw_property_inventory_sd a
left join dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_id b 
    on a.inventory_id = b.inventory_id
where a.p_dt = ${dealDate}
;


-- VPPV
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_vppv;
create table dw_temp_angejia.zhiwen_promotion_2_effect_vppv as
select 
    case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,sum(a.pc_vppv) as pc_vppv
    ,sum(a.touch_vppv + a.wechat_public_num_vppv) as tw_vppv
    ,sum(a.app_vppv_android) as android_vppv
    ,sum(a.app_vppv_ios) as ios_vppv
    ,count(case when a.pc_vppv>0 then a.inventory_id end) as pc_inventory_cnt
    ,count(case when a.touch_vppv + a.wechat_public_num_vppv>0 then a.inventory_id end) as tw_inventory_cnt
    ,count(case when a.app_vppv_android>0 then a.inventory_id end) as android_inventory_cnt
    ,count(case when a.app_vppv_ios>0 then a.inventory_id end) as ios_inventory_cnt
    ,count(case when a.pc_vppv+a.touch_vppv + a.wechat_public_num_vppv+a.app_vppv_android+a.app_vppv_ios>0 then a.inventory_id end) as total_inventory_cnt
from dw_db.dw_property_inventory_sd a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b
    on a.inventory_id = b.inventory_id
where a.p_dt = ${dealDate}
group by case when b.inventory_id is not null then '1' else '0' end
;


-- guid访问inventory_id
drop table if exists dw_temp_angejia.zhiwen_guid_visit_inventory;
create table dw_temp_angejia.zhiwen_guid_visit_inventory as 
select 
  case when current_page_id='10035' then 'tw' else 'pc' end as type   -- 旧入口进来vppv
  ,guid
  ,case when current_page_id = '10035' then regexp_replace(regexp_replace(current_page, '/sale/sh/', '') , '.html', '') 
    else regexp_replace(regexp_replace(current_page, '/', ''), '.html', '') end as inventory_id
from dw_db.dw_web_visit_traffic_log
where p_dt = ${dealDate}
and current_page_id in ('10035', '20008')    -- 旧的房源
union all 

select 
  a.type
  ,a.guid
  ,b.inventory_id
from
(select 
  case when current_page_id='10078' then 'tw' else 'pc' end as type   -- 新入口进来vppv
  ,guid
  ,case when current_page_id = '10078' then regexp_replace(regexp_replace(current_page, '/sale/sh/a', '') , '.html', '') 
    else regexp_replace(regexp_replace(current_page, '/a', ''), '.html', '') end as article_id
from dw_db.dw_web_visit_traffic_log
where p_dt = ${dealDate}
and current_page_id in ('10078','20016')    -- 推广房源
) a
left join db_sync.angejia__article b
  on a.article_id = b.id 
union all

select 
  case when app_name='i-angejia' then 'ios' else 'android' end as type   -- 旧版本vppv
  ,device_id as guid
  ,regexp_extract(request_uri,'/mobile/member/inventories/([0-9]+)', 1) as inventory_id
from dw_db.dw_app_access_log
where p_dt = ${dealDate}
and app_name in ('i-angejia','a-angejia')  
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_page_id = '30003'
union all

select 
  a.type
  ,a.guid
  ,b.inventory_id
from
(select 
  case when app_name='i-angejia' then 'ios' else 'android' end as type    -- 新版本vppv
  ,device_id as guid
  ,regexp_extract(request_uri,'/mobile/member/inventories/([0-9]+)/([0-9]+)',2) as article_id
from dw_db.dw_app_access_log
where p_dt = ${dealDate}
and app_name in ('i-angejia','a-angejia')  
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_page_id = '30074'
) a 
left join db_sync.angejia__article b
  on a.article_id = b.id 
;


-- 计算vpud
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_vpud;
create table dw_temp_angejia.zhiwen_promotion_2_effect_vpud as
select 
    case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,a.type
    ,count(distinct guid) as vpud
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_inventory_sd c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, case when b.inventory_id is not null then '1' else '0' end
;


-- 计算fvpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_fvpuv;
create table dw_temp_angejia.zhiwen_promotion_2_effect_fvpuv as
select a.type
    ,case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_inventory_sd c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid, case when b.inventory_id is not null then '1' else '0' end
having vppv >= 5
;


-- 计算evpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_evpuv;
create table dw_temp_angejia.zhiwen_promotion_2_effect_evpuv as
select a.type
    ,case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_inventory_sd c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid, case when b.inventory_id is not null then '1' else '0' end
having vppv >= 3
;


-- 全站访问数据vpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_vpud_total;
create table dw_temp_angejia.zhiwen_promotion_2_effect_vpud_total as
select 
    a.type
    ,count(distinct guid) as vpud
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_inventory_sd c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type
;

-- 全站访问fvpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_fvpuv_total;
create table dw_temp_angejia.zhiwen_promotion_2_effect_fvpuv_total as
select a.type
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_inventory_sd c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid
having vppv >= 5
;

-- 全站访问evpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_evpuv_total;
create table dw_temp_angejia.zhiwen_promotion_2_effect_evpuv_total as
select a.type
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join dw_temp_angejia.zhiwen_promotion_effect_inventory b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_inventory_sd c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid
having vppv >= 3
;

-- 各个需要union数据
--  vppv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_1;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_1 as 
select 
promotion_check
,sum(android_vppv + ios_vppv + pc_vppv + tw_vppv) as total
,sum(android_vppv) as android
,sum(ios_vppv) as ios
,sum(pc_vppv) as pc
,sum(tw_vppv) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_vppv
group by promotion_check
;

-- vpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_2;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_2 as 
select 
promotion_check
,sum(vpud) as total
,sum(case when type = 'android' then vpud end) as android
,sum(case when type = 'ios' then vpud end) as ios
,sum(case when type = 'pc' then vpud end) as pc
,sum(case when type = 'tw' then vpud end) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_vpud
group by promotion_check
;

-- evpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_3;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_3 as 
select 
promotion_check
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_evpuv
group by promotion_check
;

-- fvpuv
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_3_1;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_3_1 as 
select 
promotion_check
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_fvpuv
group by promotion_check
;

-- 有vppv房源量
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_4;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_4 as 
select 
promotion_check
,sum(total_inventory_cnt) as total
,sum(android_inventory_cnt) as android
,sum(ios_inventory_cnt) as ios
,sum(pc_inventory_cnt) as pc
,sum(tw_inventory_cnt) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_vppv
group by promotion_check
;

-- 微聊连接对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_6;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_6 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0)+nvl(pc,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type in ('1','3') then wechat_cnt end) as android
    ,sum(case when type in ('2','4') then wechat_cnt end) as ios
    ,sum(case when type=5 then wechat_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 来电量
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_7;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_7 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0)+nvl(pc,0)+nvl(tw,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,nvl(tw,0) as tw
from
(select promotion_check
    ,sum(case when type = 'android' then call_cnt end) as android
    ,sum(case when type = 'ios' then call_cnt end) as ios
    ,sum(case when type = 'pc' then call_cnt end) as pc
    ,sum(case when type = 'tw' then call_cnt end) as tw
    from dw_temp_angejia.zhiwen_promotion_2_effect_call_cnt
    group by promotion_check) t
;

-- 联系顾问量
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_8;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_8 as 
select 
promotion_check
,nvl(connect_broker_cnt,0) as total
,'' as android
,'' as ios
,nvl(connect_broker_cnt,0) as pc
,'' as tw
from (
    select promotion_check
    ,sum(connect_broker_cnt) as connect_broker_cnt 
    from dw_temp_angejia.zhiwen_promotion_2_effect_connect_broker_cnt
    group by promotion_check) t
;

-- 连接量
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_5;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_5 as 
select case when t.promotion_check = 1 then t.promotion_check else 0 end as promotion_check
,sum(nvl(total,0)) as total
,sum(nvl(android,0)) as android
,sum(nvl(ios,0)) as ios
,sum(nvl(pc,0)) as pc
,sum(nvl(tw,0)) as tw
from
(select * from dw_temp_angejia.zhiwen_promotion_2_effect_report_6
union all
select * from dw_temp_angejia.zhiwen_promotion_2_effect_report_7
union all
select * from dw_temp_angejia.zhiwen_promotion_2_effect_report_8) t
group by case when t.promotion_check = 1 then t.promotion_check else 0 end
;


-- 有回复的对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_10;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_10 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0)+nvl(pc,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type in ('1','3') then broker_back_cnt end) as android
    ,sum(case when type in ('2','4') then broker_back_cnt end) as ios
    ,sum(case when type=5 then broker_back_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 无回复的对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_11;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_11 as 
select promotion_check
,nvl(android,0)+nvl(ios,0)+nvl(pc,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type in ('1','3') then wechat_cnt-broker_back_cnt end) as android
    ,sum(case when type in ('2','4') then wechat_cnt-broker_back_cnt end) as ios
    ,sum(case when type=5 then wechat_cnt-broker_back_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 对话深度
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_12;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_12 as 
select 
promotion_check
,nvl((nvl(android,0)*nvl(android_cnt,0)+nvl(ios,0)*nvl(ios_cnt,0)+nvl(pc,0)*nvl(pc_cnt,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)+nvl(pc_cnt,0)),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type in ('1','3') then broker_back_cnt end) as android_cnt
    ,sum(case when type in ('2','4') then broker_back_cnt end) as ios_cnt
    ,sum(case when type=5 then broker_back_cnt end) as pc_cnt
    ,avg(case when type in ('1','3') then user_wechat_cnt end) as android
    ,avg(case when type in ('2','4') then user_wechat_cnt end) as ios
    ,avg(case when type=5 then user_wechat_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 5分钟回复率
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_13;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_13 as 
select 
promotion_check
,nvl((nvl(android,0)+nvl(ios,0)+nvl(pc,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)+nvl(pc_cnt,0)),0) as total
,nvl(nvl(android,0)/nvl(android_cnt,0),0) as android
,nvl(nvl(ios,0)/nvl(ios_cnt,0),0) as ios
,nvl(nvl(pc,0)/nvl(pc_cnt,0),0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type in ('1','3') then broker_back_cnt end) as android_cnt
    ,sum(case when type in ('2','4') then broker_back_cnt end) as ios_cnt
    ,sum(case when type=5 then broker_back_cnt end) as pc_cnt
    ,sum(case when type in ('1','3') then 5_min_back_cnt end) as android
    ,sum(case when type in ('2','4') then 5_min_back_cnt end) as ios
    ,sum(case when type=5 then 5_min_back_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 房源单页微聊连接对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_14;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_14 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0)+nvl(pc,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type = 1 then wechat_cnt end) as android
    ,sum(case when type = 2 then wechat_cnt end) as ios
    ,sum(case when type = 5 then wechat_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 有回复的对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_15;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_15 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0)+nvl(pc,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=1 then broker_back_cnt end) as android
    ,sum(case when type=2 then broker_back_cnt end) as ios
    ,sum(case when type=5 then broker_back_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 无回复的对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_16;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_16 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0)+nvl(pc,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=1 then wechat_cnt-broker_back_cnt end) as android
    ,sum(case when type=2 then wechat_cnt-broker_back_cnt end) as ios
    ,sum(case when type=5 then wechat_cnt-broker_back_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 对话深度
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_17;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_17 as 
select 
promotion_check
,nvl((nvl(android,0)*nvl(android_cnt,0)+nvl(ios,0)*nvl(ios_cnt,0)+nvl(pc,0)*nvl(pc_cnt,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)+nvl(pc_cnt,0)),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=1 then broker_back_cnt end) as android_cnt
    ,sum(case when type=2 then broker_back_cnt end) as ios_cnt
    ,sum(case when type=5 then broker_back_cnt end) as pc_cnt
    ,avg(case when type=1 then user_wechat_cnt end) as android
    ,avg(case when type=2 then user_wechat_cnt end) as ios
    ,avg(case when type=5 then user_wechat_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 5分钟回复率
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_18;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_18 as 
select 
promotion_check
,nvl((nvl(android,0)+nvl(ios,0)+nvl(pc,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)+nvl(pc_cnt,0)),0) as total
,nvl(nvl(android,0)/nvl(android_cnt,0),0) as android
,nvl(nvl(ios,0)/nvl(ios_cnt,0),0) as ios
,nvl(nvl(pc,0)/nvl(pc_cnt,0),0) as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=1 then broker_back_cnt end) as android_cnt
    ,sum(case when type=2 then broker_back_cnt end) as ios_cnt
    ,sum(case when type=5 then broker_back_cnt end) as pc_cnt
    ,sum(case when type=1 then 5_min_back_cnt end) as android
    ,sum(case when type=2 then 5_min_back_cnt end) as ios
    ,sum(case when type=5 then 5_min_back_cnt end) as pc
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 经纪人单页微聊连接对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_19;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_19 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type = 3 then wechat_cnt end) as android
    ,sum(case when type = 4 then wechat_cnt end) as ios
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 有回复的对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_20;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_20 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=3 then broker_back_cnt end) as android
    ,sum(case when type=4 then broker_back_cnt end) as ios
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 无回复的对话数
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_21;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_21 as 
select 
promotion_check
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=3 then wechat_cnt-broker_back_cnt end) as android
    ,sum(case when type=4 then wechat_cnt-broker_back_cnt end) as ios
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 对话深度
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_22;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_22 as 
select 
promotion_check
,nvl((nvl(android,0)*nvl(android_cnt,0)+nvl(ios,0)*nvl(ios_cnt,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=3 then broker_back_cnt end) as android_cnt
    ,sum(case when type=4 then broker_back_cnt end) as ios_cnt
    ,avg(case when type=3 then user_wechat_cnt end) as android
    ,avg(case when type=4 then user_wechat_cnt end) as ios
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;

-- 5分钟回复率
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_report_23;
create table dw_temp_angejia.zhiwen_promotion_2_effect_report_23 as 
select 
promotion_check
,nvl((nvl(android,0)+nvl(ios,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)),0) as total
,nvl(nvl(android,0)/nvl(android_cnt,0),0) as android
,nvl(nvl(ios,0)/nvl(ios_cnt,0),0) as ios
,'' as pc
,'' as tw
from
(select promotion_check
    ,sum(case when type=3 then broker_back_cnt end) as android_cnt
    ,sum(case when type=4 then broker_back_cnt end) as ios_cnt
    ,sum(case when type=3 then 5_min_back_cnt end) as android
    ,sum(case when type=4 then 5_min_back_cnt end) as ios
    from dw_temp_angejia.zhiwen_promotion_2_effect_wechat_cnt_back
    group by promotion_check) t
;



---------------------------------自营销内容------------------------------------------
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_end_1;
create table dw_temp_angejia.zhiwen_promotion_2_effect_end_1 as 
select 
0 as rank
,'房源量' as type 
,promotion_inventory_cnt as total
,promotion_inventory_cnt as android
,promotion_inventory_cnt as ios
,promotion_inventory_cnt as pc
,promotion_inventory_cnt as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_cnt
union all
select 1 as rank,'VPPV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_1 where promotion_check=1
union all
select 2 as rank,'VPUV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_2 where promotion_check=1
union all
select 3 as rank,'EVPUV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_3 where promotion_check=1
union all
select 4 as rank,'FVPUV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_3_1 where promotion_check=1
union all
select 5 as rank,'有vppv房源数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_4 where promotion_check=1
union all
select 6 as rank,'连数接' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_5 where promotion_check=1
union all
select 7 as rank,'微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_6 where promotion_check=1
union all
select 8 as rank,'来电数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check=1
union all
select 9 as rank,'联系顾问数' as type,nvl(total,0) as total,'' as android,'' as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_8 where promotion_check=1
union all
select 10 as rank,'微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_6 where promotion_check=1
union all
select 11 as rank,'有回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_10 where promotion_check=1
union all
select 12 as rank,'无回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_11 where promotion_check=1
union all
select 13 as rank,'对话深度' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_12 where promotion_check=1
union all
select 14 as rank,'5分钟回复率' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_13 where promotion_check=1
union all
select 15 as rank,'房源单页微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_14 where promotion_check=1
union all
select 16 as rank,'有回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_15 where promotion_check=1
union all
select 17 as rank,'无回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_16 where promotion_check=1
union all
select 18 as rank,'对话深度' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_17 where promotion_check=1
union all
select 19 as rank,'5分钟回复率' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_18 where promotion_check=1
union all
select 20 as rank,'经纪人单页微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,'' as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_19 where promotion_check=1
union all
select 21 as rank,'有回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_20 where promotion_check=1
union all
select 22 as rank,'无回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_21 where promotion_check=1
union all
select 23 as rank,'对话深度' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_22 where promotion_check=1
union all
select 24 as rank,'5分钟回复率' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_23 where promotion_check=1
union all
select 25 as rank,'来电数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check=1
union all
select 26 as rank,'房源单页来电数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check=1
union all
select 27 as rank,'顾问单页来电数' as type,'' as total,'' as android,'' as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check=1
;

drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_end;
create table dw_temp_angejia.zhiwen_promotion_2_effect_end as
select a.rank
,a.type
,case when a.total is null then '' else nvl(b.total,0) end as total
,case when a.android is null then '' else nvl(b.android,0) end as android
,case when a.ios is null then '' else nvl(b.ios,0) end as ios
,case when a.pc is null then '' else nvl(b.pc,0) end as pc
,case when a.tw is null then '' else nvl(b.tw,0) end as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_moder a
left join dw_temp_angejia.zhiwen_promotion_2_effect_end_1 b
    on a.rank = b.rank
;

-- export hive dw_temp_angejia.zhiwen_promotion_2_effect_end to mysql dw_temp_angejia.zhiwen_promotion_2_effect_end



---------------------------------非自营销内容------------------------------------------
drop table if exists dw_temp_angejia.zhiwen_not_promotion_2_effect_end_1;
create table dw_temp_angejia.zhiwen_not_promotion_2_effect_end_1 as 
select 
0 as rank
,'房源量' as type 
,not_promotion_inventory_cnt as total
,not_promotion_inventory_cnt as android
,not_promotion_inventory_cnt as ios
,not_promotion_inventory_cnt as pc
,not_promotion_inventory_cnt as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_cnt
union all
select 1 as rank,'VPPV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_1 where promotion_check=0
union all
select 2 as rank,'VPUV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_2 where promotion_check=0
union all
select 3 as rank,'EVPUV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_3 where promotion_check=0
union all
select 4 as rank,'FVPUV' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_3_1 where promotion_check=0
union all
select 5 as rank,'有vppv房源数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_4 where promotion_check=0
union all
select 6 as rank,'连数接' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_5 where promotion_check = 0
union all
select 7 as rank,'微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_6 where promotion_check=0
union all
select 8 as rank,'来电数' as type,sum(nvl(total,0)) as total,sum(nvl(android,0)) as android,sum(nvl(ios,0)) as ios,sum(nvl(pc,0)) as pc,sum(nvl(tw,0)) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check in (0,2)
union all
select 9 as rank,'联系顾问数' as type,nvl(total,0) as total,'' as android,'' as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_8 where promotion_check=0
union all
select 10 as rank,'微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_6 where promotion_check=0
union all
select 11 as rank,'有回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_10 where promotion_check=0
union all
select 12 as rank,'无回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_11 where promotion_check=0
union all
select 13 as rank,'对话深度' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_12 where promotion_check=0
union all
select 14 as rank,'5分钟回复率' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_13 where promotion_check=0
union all
select 15 as rank,'房源单页微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_14 where promotion_check=0
union all
select 16 as rank,'有回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_15 where promotion_check=0
union all
select 17 as rank,'无回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_16 where promotion_check=0
union all
select 18 as rank,'对话深度' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_17 where promotion_check=0
union all
select 19 as rank,'5分钟回复率' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_18 where promotion_check=0
union all
select 20 as rank,'经纪人单页微聊连接对话数' as type,nvl(total,0) as total,nvl(android,0) as android,'' as ios,nvl(pc,0) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_19 where promotion_check=0
union all
select 21 as rank,'有回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_20 where promotion_check=0
union all
select 22 as rank,'无回复对话数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_21 where promotion_check=0
union all
select 23 as rank,'对话深度' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_22 where promotion_check=0
union all
select 24 as rank,'5分钟回复率' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,'' as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_23 where promotion_check=0
union all
select 25 as rank,'来电数' as type,sum(nvl(total,0)) as total,sum(nvl(android,0)) as android,sum(nvl(ios,0)) as ios,sum(nvl(pc,0)) as pc,sum(nvl(tw,0)) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check in (0,2)
union all
select 26 as rank,'用户打经纪人来电数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check=0
union all
select 27 as rank,'用户打房东来电数' as type,nvl(total,0) as total,nvl(android,0) as android,nvl(ios,0) as ios,nvl(pc,0) as pc,nvl(tw,0) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7 where promotion_check=2
;

drop table if exists dw_temp_angejia.zhiwen_not_promotion_2_effect_end;
create table dw_temp_angejia.zhiwen_not_promotion_2_effect_end as
select a.rank
,a.type
,case when a.total is null then '' else nvl(b.total,0) end as total
,case when a.android is null then '' else nvl(b.android,0) end as android
,case when a.ios is null then '' else nvl(b.ios,0) end as ios
,case when a.pc is null then '' else nvl(b.pc,0) end as pc
,case when a.tw is null then '' else nvl(b.tw,0) end as tw
from dw_temp_angejia.zhiwen_not_promotion_2_effect_moder a
left join dw_temp_angejia.zhiwen_not_promotion_2_effect_end_1 b
    on a.rank = b.rank
;

-- export hive dw_temp_angejia.zhiwen_not_promotion_2_effect_end to mysql dw_temp_angejia.zhiwen_not_promotion_2_effect_end




-------------------------------全站访问效果日报---------------------------------
drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_end_total_1;
create table dw_temp_angejia.zhiwen_promotion_2_effect_end_total_1 as 
select 
0 as rank
,'房源量' as type 
,inventory_cnt as total
,inventory_cnt as android
,inventory_cnt as ios
,inventory_cnt as pc
,inventory_cnt as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_promotion_inventory_cnt
union all

select 
1 as rank
,'VPPV' as type
,sum(android_vppv + ios_vppv + pc_vppv + tw_vppv) as total
,sum(android_vppv) as android
,sum(ios_vppv) as ios
,sum(pc_vppv) as pc
,sum(tw_vppv) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_vppv
union all

select 
2 as rank
,'VPUV' as type
,sum(vpud) as total
,sum(case when type = 'android' then vpud end) as android
,sum(case when type = 'ios' then vpud end) as ios
,sum(case when type = 'pc' then vpud end) as pc
,sum(case when type = 'tw' then vpud end) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_vpud_total
union all

select 
3 as rank
,'EVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_evpuv_total
union all


select 
4 as rank
,'FVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_fvpuv_total
union all

select 
5 as rank
,'有vppv房源量' as type
,sum(total_inventory_cnt) as total
,sum(android_inventory_cnt) as android
,sum(ios_inventory_cnt) as ios
,sum(pc_inventory_cnt) as pc
,sum(tw_inventory_cnt) as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_vppv
union all

select 6 as rank,'连接数' as type,sum(nvl(total,0)) as total,sum(nvl(android,0)) as android,sum(nvl(ios,0)) as ios,sum(nvl(pc,0)) as pc,sum(nvl(tw,0)) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_5

union all

select 7 as rank,'微聊连接对话数' as type,sum(nvl(total,0)) as total,sum(nvl(android,0)) as android,sum(nvl(ios,0)) as ios,sum(nvl(pc,0)) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_6
union all

select 8 as rank,'来电数' as type,sum(nvl(total,0)) as total,sum(nvl(android,0)) as android,sum(nvl(ios,0)) as ios,sum(nvl(pc,0)) as pc,sum(nvl(tw,0)) as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_7
union all

select 9 as rank,'联系顾问数' as type,sum(nvl(total,0)) as total,'' as android,'' as ios,sum(nvl(pc,0)) as pc,'' as tw 
from dw_temp_angejia.zhiwen_promotion_2_effect_report_8
;

drop table if exists dw_temp_angejia.zhiwen_promotion_2_effect_end_total;
create table dw_temp_angejia.zhiwen_promotion_2_effect_end_total as
select a.rank
,a.type
,case when a.total is null then '' else nvl(b.total,0) end as total
,case when a.android is null then '' else nvl(b.android,0) end as android
,case when a.ios is null then '' else nvl(b.ios,0) end as ios
,case when a.pc is null then '' else nvl(b.pc,0) end as pc
,case when a.tw is null then '' else nvl(b.tw,0) end as tw
from dw_temp_angejia.zhiwen_promotion_2_effect_end_total_moder a
left join dw_temp_angejia.zhiwen_promotion_2_effect_end_total_1 b
    on a.rank = b.rank
;

-- export hive dw_temp_angejia.zhiwen_promotion_2_effect_end_total to mysql dw_temp_angejia.zhiwen_promotion_2_effect_end_total
