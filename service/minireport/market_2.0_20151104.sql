-- 自营销效果 vs. 非自营销效果 日报

-- 自营销‘联系顾问’连接数据
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_connect_broker_cnt;
create table dw_temp_angejia.zhiwen_promotion2_effect_connect_broker_cnt as
select type     -- 1 pc , 2 tw
    ,case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,count(distinct a.user_phone,a.inventory_id,a.broker_uid) as connect_broker_cnt
from db_sync.angejia__page_information_statistics a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) > ${dealDate} and broker_uid>0) b
    on a.inventory_id = b.inventory_id
left join dw_db.dw_broker_summary_basis_info_daily c
    on a.broker_uid = c.user_id and c.p_dt = ${dealDate}
where to_date(a.created_at) = ${dealDate}
--and type = 1     --  1-PC, 2-TW 
and a.action_type = 9
and c.agent_name is not null
and c.agent_name not like '%测试%'
and c.broker_status = '在职'
and c.user_id is not null
group by a.type,case when b.inventory_id is not null then '1' else '0' end
;


-- 自营销微聊对数
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt;
create table dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt as
select 
    case when wl_type <> '3' and device_from = '1' then '1'   -- android 房源单页
        when wl_type <> '3' and device_from = '2' then '2'    -- ios 房源单页
        when wl_type = '3' and device_from = '1' then '3'     -- android 经纪人单页
        else '4' end as type                                  -- ios 经纪人单页
    ,a.broker_id
    ,a.user_id
    ,to_date(a.created_at) as p_dt
    ,count(distinct a.inventory_id) as wechat_cnt
    ,min(a.created_at) as send_click_time
from db_sync.angejia__wl_statistics a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) > ${dealDate} and broker_uid>0) b
    on a.inventory_id = b.inventory_id
left join dw_db.dw_broker_summary_basis_info_daily c
    on a.broker_id = c.user_id and c.p_dt = ${dealDate}
where to_date(a.created_at) = ${dealDate}
and b.inventory_id is not null
and c.agent_name is not null
and c.agent_name not like '%测试%'
and c.broker_status = '在职'
and c.user_id is not null
group by case when wl_type <> '3' and device_from = '1' then '1'
        when wl_type <> '3' and device_from = '2' then '2'
        when wl_type = '3' and device_from = '1' then '3'
        else '4' end, a.broker_id, a.user_id, to_date(a.created_at)
;


-- 非自营销微聊对数
drop table if exists dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt;
create table dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt as
select 
    case when wl_type <> '3' and device_from = '1' then '1'   -- android 房源单页
        when wl_type <> '3' and device_from = '2' then '2'    -- ios 房源单页
        when wl_type = '3' and device_from = '1' then '3'     -- android 经纪人单页
        else '4' end as type                                  -- ios 经纪人单页
    ,a.broker_id
    ,a.user_id
    ,to_date(a.created_at) as p_dt
    ,count(distinct a.inventory_id) as wechat_cnt
    ,min(a.created_at) as send_click_time
from db_sync.angejia__wl_statistics a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) > ${dealDate} and broker_uid>0) b
    on a.inventory_id = b.inventory_id
left join dw_db.dw_broker_summary_basis_info_daily c
    on a.broker_id = c.user_id and c.p_dt = ${dealDate}
where to_date(a.created_at) = ${dealDate}
and b.inventory_id is null
and c.agent_name is not null
and c.agent_name not like '%测试%'
and c.user_id is not null
group by case when wl_type <> '3' and device_from = '1' then '1'
        when wl_type <> '3' and device_from = '2' then '2'
        when wl_type = '3' and device_from = '1' then '3'
        else '4' end, a.broker_id, a.user_id, to_date(a.created_at)
;


-- 微聊对,设[用户id]-[经纪人id]-[日期]为唯一键，用户发送，设from_uid-to_uid-p_dt为微聊key, 否则设to_uid-from_uid-p_dt为微聊key
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_wechat_today;
create table dw_temp_angejia.zhiwen_promotion2_effect_wechat_today as
select case when account_type = 1 then concat(from_uid,'-',to_uid,'-',p_dt) else concat(to_uid,'-',from_uid,'-',p_dt) end as msg_key
    ,from_uid
    ,to_uid
    ,account_type
    ,content
    ,created_at
from dw_db.dw_wechat_detail_info_daily
where p_dt = ${dealDate}
and account_type in (1,2)
;


-- 自营销微聊内容
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt_back_1;   -- 剔除系统记录发送时间之前的微聊记录
create table dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt_back_1 as
select 
a.msg_key
,max(b.wechat_cnt) as back_user_cnt
,count(case when a.account_type=1 then a.msg_key end) as back_msg_cnt   -- 用户回复内容条数
,min(case when a.account_type=1 then a.created_at end) as min_user_send_time    -- 用户最小发送时间
from dw_temp_angejia.zhiwen_promotion2_effect_wechat_today a
inner join
(select 
  concat(user_id,'-',broker_id,'-',p_dt) as msg_key
  ,wechat_cnt
  ,send_click_time
  from dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt
) b on a.msg_key = b.msg_key
where a.created_at >= b.send_click_time
group by a.msg_key
;

drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt_back_2;    -- 经纪人回复时间要小于用户第一次发送的时间
create table dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt_back_2 as
select t.msg_key
    ,case when unix_timestamp(min_broker_send_time) - unix_timestamp(min_user_send_time) <= 300 then 1 else 0 end as 5_min_cnt
from
(select 
  a.msg_key
  ,min(case when a.account_type=2 then a.created_at end) as min_broker_send_time
  ,min(b.min_user_send_time) as min_user_send_time
  from dw_temp_angejia.zhiwen_promotion2_effect_wechat_today a
  left join dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt_back_1 b
    on a.msg_key=b.msg_key
  where b.msg_key is not null and a.created_at >= b.min_user_send_time
  group by a.msg_key
) t
;

drop table if exists dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back;   -- 微聊数据汇总
create table dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back as
select type
    ,count(t.msg_key) as wechat_cnt    --微聊对数，统计有瑕疵，在后面代码补充计算
    ,sum(t.back_user_cnt) as back_user_cnt    --有回复的微聊数
    ,count(t.msg_key) - sum(t.back_user_cnt) as no_back_user_cnt   --无回复经纪人数量，统计有瑕疵，在后面代码补充计算
    ,avg(t.back_msg_cnt) as back_msg_cnt    --回复深度
    ,sum(5_min_cnt)/count(t.msg_key) as 5_min_percent    -- 5分钟回复率
from
(select 
    a.msg_key
    ,a.type
    ,nvl(b.back_user_cnt, 0) as back_user_cnt
    ,nvl(b.back_msg_cnt, 0) as back_msg_cnt
    ,nvl(c.5_min_cnt, 0) as 5_min_cnt
    from (select 
        concat(user_id,'-',broker_id,'-',p_dt) as msg_key, type, send_click_time
        from dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt
    ) a
    left join dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt_back_1 b
        on a.msg_key = b.msg_key
    left join dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt_back_2 c
        on a.msg_key = c.msg_key
) t group by type
;


-- 非自营销微聊内容
drop table if exists dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt_back_1;   -- 剔除系统记录发送时间之前的微聊记录
create table dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt_back_1 as
select 
a.msg_key
,max(b.wechat_cnt) as back_user_cnt
,count(case when a.account_type=1 then a.msg_key end) as back_msg_cnt
,min(case when a.account_type=1 then a.created_at end) as min_user_send_time
from dw_temp_angejia.zhiwen_promotion2_effect_wechat_today a
left join
(select 
  concat(user_id,'-',broker_id,'-',p_dt) as msg_key
  ,wechat_cnt
  ,send_click_time
  from dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt
) b on a.msg_key = b.msg_key
where b.msg_key is not null and a.created_at >= b.send_click_time
group by a.msg_key
;

drop table if exists dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt_back_2;    -- 经纪人回复时间要小于用户第一次发送的时间
create table dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt_back_2 as
select t.msg_key
    ,case when unix_timestamp(min_broker_send_time) - unix_timestamp(min_user_send_time) <= 300 then 1 else 0 end as 5_min_cnt
from
(select 
  a.msg_key
  ,min(case when a.account_type=2 then a.created_at end) as min_broker_send_time
  ,min(b.min_user_send_time) as min_user_send_time
  from dw_temp_angejia.zhiwen_promotion2_effect_wechat_today a
  left join dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt_back_1 b
    on a.msg_key=b.msg_key
  where b.msg_key is not null and a.created_at >= b.min_user_send_time
  group by a.msg_key
) t
;

drop table if exists dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back;   -- 微聊数据汇总
create table dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back as
select type
    ,count(t.msg_key) as wechat_cnt    --微聊对数，统计有瑕疵，在后面代码补充计算
    ,sum(back_user_cnt) as back_user_cnt    --有回复的微聊数
    ,count(t.msg_key) - sum(t.back_user_cnt) as no_back_user_cnt   --无回复经纪人数量，统计有瑕疵，在后面代码补充计算
    ,avg(t.back_msg_cnt) as back_msg_cnt    --回复深度
    ,sum(5_min_cnt)/count(t.msg_key) as 5_min_percent    -- 5分钟回复率
from
(select 
    a.msg_key
    ,a.type
    ,nvl(b.back_user_cnt, 0) as back_user_cnt
    ,nvl(b.back_msg_cnt, 0) as back_msg_cnt
    ,nvl(c.5_min_cnt, 0) as 5_min_cnt
    from (select 
        concat(user_id,'-',broker_id,'-',p_dt) as msg_key, type, send_click_time
        from dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt
    ) a
    left join dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt_back_1 b
        on a.msg_key = b.msg_key
    left join dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt_back_2 c
        on a.msg_key = c.msg_key
) t group by t.type
;


-- 电话click[当前无法区分具体的用户来电来源，使用click做区分]
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_call_click;
create table dw_temp_angejia.zhiwen_promotion2_effect_call_click as
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

drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_call_click_2;
create table dw_temp_angejia.zhiwen_promotion2_effect_call_click_2 as
select a.type
    ,a.inventory_id
from dw_temp_angejia.zhiwen_promotion2_effect_call_click a
left join (select inventory_id,max(click_time) as max_click_time from dw_temp_angejia.zhiwen_promotion2_effect_call_click group by inventory_id) b
    on a.inventory_id = b.inventory_id and a.click_time = b.max_click_time
where b.inventory_id is not null
group by a.type,a.inventory_id
;


-- 自营销和非自营销电话连接数
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_call_cnt;
create table dw_temp_angejia.zhiwen_promotion2_effect_call_cnt as
select 
    case when c.inventory_id is not null then '1' else '0' end as promotion_check
    ,case when e.type is not null then type else 'pc' end as type
    ,count(distinct b.called_uid, b.caller, a.inventory_id) as call_cnt
from db_sync.angejia__call_relation_with_inventory a
left join db_sync.angejia__call_log b
    on a.call_log_id = b.id
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) > ${dealDate} and broker_uid>0) c
    on a.inventory_id = c.inventory_id
left join dw_db.dw_broker_summary_basis_info_daily d
    on b.called_uid = d.user_id and d.p_dt = ${dealDate}
left join dw_temp_angejia.zhiwen_promotion2_effect_call_click_2 e 
    on a.inventory_id = e.inventory_id
where to_date(a.created_at) = ${dealDate}
and to_date(b.start_at) = ${dealDate}
and b.call_type = 2
and d.agent_name is not null
and d.agent_name not like '%测试%'
and d.broker_status = '在职'
and d.user_id is not null
group by case when c.inventory_id is not null then '1' else '0' end
    ,case when e.type is not null then type else 'pc' end
;


-- 自营销房源量
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_id;
create table dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_id as
select distinct a.inventory_id
from db_sync.angejia__article a
left join dw_db.dw_broker_summary_basis_info_daily b
    on a.broker_uid = b.user_id and b.p_dt = ${dealDate}
left join dw_db.dw_property_summary_inventory_detail_daily c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where a.status = 1 
and to_date(a.expire_at) > ${dealDate}
and a.broker_uid>0
and b.agent_name is not null
and b.agent_name not like '%测试%'
and b.broker_status = '在职'
and b.user_id is not null
and c.inventory_id is not null
;


-- 自营销和非自营销房源量
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_cnt;
create table dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_cnt as
select count(case when b.inventory_id is not null then a.inventory_id end) as promotion_inventory_cnt
,count(case when b.inventory_id is null then a.inventory_id end) as not_promotion_inventory_cnt
,count(*) as inventory_cnt
from dw_db.dw_property_summary_inventory_detail_daily a
left join dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_id b 
    on a.inventory_id = b.inventory_id
where a.p_dt = ${dealDate}
;


-- VPPV
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_vppv;
create table dw_temp_angejia.zhiwen_promotion2_effect_vppv as
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
from dw_db.dw_property_summary_inventory_detail_daily a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) >= ${dealDate} and broker_uid>0) b
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
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_vpud;
create table dw_temp_angejia.zhiwen_promotion2_effect_vpud as
select 
    case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,a.type
    ,count(distinct guid) as vpud
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) >= ${dealDate} and broker_uid>0) b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_summary_inventory_detail_daily c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, case when b.inventory_id is not null then '1' else '0' end
;


-- 计算fvpuv
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_fvpuv;
create table dw_temp_angejia.zhiwen_promotion2_effect_fvpuv as
select a.type
    ,case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) >= ${dealDate} and broker_uid>0) b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_summary_inventory_detail_daily c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid, case when b.inventory_id is not null then '1' else '0' end
having vppv >= 5
;


-- 计算evpuv
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_evpuv;
create table dw_temp_angejia.zhiwen_promotion2_effect_evpuv as
select a.type
    ,case when b.inventory_id is not null then '1' else '0' end as promotion_check
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) >= ${dealDate} and broker_uid>0) b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_summary_inventory_detail_daily c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid, case when b.inventory_id is not null then '1' else '0' end
having vppv >= 3
;


-- 全站访问数据vpuv
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_vpud_total;
create table dw_temp_angejia.zhiwen_promotion2_effect_vpud_total as
select 
    a.type
    ,count(distinct guid) as vpud
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) >= ${dealDate} and broker_uid>0) b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_summary_inventory_detail_daily c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type
;

-- 全站访问fvpuv
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_fvpuv_total;
create table dw_temp_angejia.zhiwen_promotion2_effect_fvpuv_total as
select a.type
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) >= ${dealDate} and broker_uid>0) b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_summary_inventory_detail_daily c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid
having vppv >= 5
;

-- 全站访问evpuv
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_evpuv_total;
create table dw_temp_angejia.zhiwen_promotion2_effect_evpuv_total as
select a.type
    ,a.guid
    ,count(*) as vppv
from dw_temp_angejia.zhiwen_guid_visit_inventory a
left join (select distinct inventory_id from db_sync.angejia__article where status = 1 and to_date(expire_at) >= ${dealDate} and broker_uid>0) b 
    on a.inventory_id = b.inventory_id
left join dw_db.dw_property_summary_inventory_detail_daily c
    on a.inventory_id = c.inventory_id and c.p_dt = ${dealDate}
where c.inventory_id is not null
group by a.type, a.guid
having vppv >= 3
;

---------------------------------自营销内容------------------------------------------
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_end_1;
create table dw_temp_angejia.zhiwen_promotion2_effect_end_1 as 
select 
0 as rank
,'房源量' as type 
,promotion_inventory_cnt as total
,promotion_inventory_cnt as android
,promotion_inventory_cnt as ios
,promotion_inventory_cnt as pc
,promotion_inventory_cnt as tw
from dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_cnt
union all

select 
1.1 as rank
,'VPPV' as type
,sum(android_vppv + ios_vppv + pc_vppv + tw_vppv) as total
,sum(android_vppv) as android
,sum(ios_vppv) as ios
,sum(pc_vppv) as pc
,sum(tw_vppv) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vppv
where promotion_check = 1
union all

select 
2 as rank
,'VPUV' as type
,sum(vpud) as total
,sum(case when type = 'android' then vpud end) as android
,sum(case when type = 'ios' then vpud end) as ios
,sum(case when type = 'pc' then vpud end) as pc
,sum(case when type = 'tw' then vpud end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vpud
where promotion_check = 1
union all

select 
3 as rank
,'EVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_evpuv
where promotion_check = 1
union all

select 
3.1 as rank
,'FVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_fvpuv
where promotion_check = 1
union all

select 
4 as rank
,'有vppv房源量' as type
,sum(total_inventory_cnt) as total
,sum(android_inventory_cnt) as android
,sum(ios_inventory_cnt) as ios
,sum(pc_inventory_cnt) as pc
,sum(tw_inventory_cnt) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vppv
where promotion_check = 1

union all
select
7 as rank
,'来电量' as type
,nvl(android,0)+nvl(ios,0)+nvl(pc,0)+nvl(tw,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,nvl(tw,0) as tw
from
(select sum(case when type = 'android' then call_cnt end) as android
  ,sum(case when type = 'ios' then call_cnt end) as ios
  ,sum(case when type = 'pc' then call_cnt end) as pc
  ,sum(case when type = 'tw' then call_cnt end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_call_cnt
where promotion_check = 1) t
union all

select 
8 as rank
,'联系顾问量' as type
,nvl(connect_broker_cnt,0) as total
,'' as android
,'' as ios
,nvl(connect_broker_cnt,0) as pc
,'' as tw
from (
select sum(connect_broker_cnt) as connect_broker_cnt 
from dw_temp_angejia.zhiwen_promotion2_effect_connect_broker_cnt
where promotion_check = 1
) t
union all

select 
14 as rank
,'房源单页微聊连接对话数' as type
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select sum(case when type = 1 then wechat_cnt end) as android
  ,sum(case when type = 2 then wechat_cnt end) as ios
  from dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt
) t
union all

select 
15 as rank
,'有回复的对话数' as type
,nvl(total,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select sum(back_user_cnt) as total
,sum(case when type=1 then back_user_cnt end) as android
,sum(case when type=2 then back_user_cnt end) as ios
from dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back
where type in ('1','2')
) t
union all

select 
17 as rank
,'对话深度' as type
,nvl((nvl(android,0)*nvl(android_cnt,0)+nvl(ios,0)*nvl(ios_cnt,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=1 then back_user_cnt end) as android_cnt
,sum(case when type=2 then back_user_cnt end) as ios_cnt
,sum(case when type=1 then back_msg_cnt end) as android
,sum(case when type=2 then back_msg_cnt end) as ios
from dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back
) t
union all

select 
18 as rank
,'5分钟回复率' as type
,nvl((android*android_cnt+ios*ios_cnt)/(android_cnt+ios_cnt),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=1 then back_user_cnt end) as android_cnt
,sum(case when type=2 then back_user_cnt end) as ios_cnt
,sum(case when type=1 then 5_min_percent end) as android
,sum(case when type=2 then 5_min_percent end) as ios
from dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back
) t
union all

select 
19 as rank
,'顾问单页微聊连接对话数' as type
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select sum(case when type = 3 then wechat_cnt end) as android
  ,sum(case when type = 4 then wechat_cnt end) as ios
  from dw_temp_angejia.zhiwen_promotion2_effect_wechat_cnt
) t
union all

select 
20 as rank
,'有回复的对话数' as type
,nvl(total,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type in ('3','4') then back_user_cnt end) as total
,sum(case when type=3 then back_user_cnt end) as android
,sum(case when type=4 then back_user_cnt end) as ios
from dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back
) t
union all

select 
22 as rank
,'对话深度' as type
,nvl((nvl(android,0)*nvl(android_cnt,0)+nvl(ios,0)*nvl(ios_cnt,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=3 then back_user_cnt end) as android_cnt
,sum(case when type=4 then back_user_cnt end) as ios_cnt
,sum(case when type=3 then back_msg_cnt end) as android
,sum(case when type=4 then back_msg_cnt end) as ios
from dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back
) t
union all

select 
23 as rank
,'5分钟回复率' as type
,nvl((android*android_cnt+ios*ios_cnt)/(android_cnt+ios_cnt),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=3 then back_user_cnt end) as android_cnt
,sum(case when type=4 then back_user_cnt end) as ios_cnt
,sum(case when type=3 then 5_min_percent end) as android
,sum(case when type=4 then 5_min_percent end) as ios
from dw_temp_angejia.zhiwen_promotion_wechat_cnt_android_back
) t
union all

select
24 as rank
,'来电量' as type
,nvl(android,0)+nvl(ios,0)+nvl(pc,0)+nvl(tw,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,nvl(tw,0) as tw
from
(select sum(case when type = 'android' then call_cnt end) as android
  ,sum(case when type = 'ios' then call_cnt end) as ios
  ,sum(case when type = 'pc' then call_cnt end) as pc
  ,sum(case when type = 'tw' then call_cnt end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_call_cnt
where promotion_check = 1) t
union all

select
25 as rank
,'房源单页来电量' as type
,nvl(android,0)+nvl(ios,0)+nvl(pc,0)+nvl(tw,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,nvl(tw,0) as tw
from
(select sum(case when type = 'android' then call_cnt end) as android
  ,sum(case when type = 'ios' then call_cnt end) as ios
  ,sum(case when type = 'pc' then call_cnt end) as pc
  ,sum(case when type = 'tw' then call_cnt end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_call_cnt
where promotion_check = 1) t
union all

select 
26 as rank
,'顾问单页来电量' as type
,'' as total   -- 后续补充
,'' as android
,'' as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion_effect
limit 1
;


drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_end_2;
create table dw_temp_angejia.zhiwen_promotion2_effect_end_2 as 
select 
16 as rank
,'无回复对话数' as type
,sum(case when rank = 14 then total end) - sum(case when rank = 15 then total end) as total
,sum(case when rank = 14 then android end) - sum(case when rank = 15 then android end) as android
,sum(case when rank = 14 then ios end) - sum(case when rank = 15 then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_1
where rank in (14,15)
union all

select 
21 as rank
,'无回复对话数' as type
,sum(case when rank = 19 then total end) - sum(case when rank = 20 then total end) as total
,sum(case when rank = 19 then android end) - sum(case when rank = 20 then android end) as android
,sum(case when rank = 19 then ios end) - sum(case when rank = 20 then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_1
where rank in (19,20)
union all

select
6 as rank
,'微聊连接对话数' as type
,sum(total) as total
,sum(android) as android
,sum(ios) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_1
where rank in (14,19)
union all

select
9 as rank
,'微聊连接对话数' as type
,sum(total) as total
,sum(android) as android
,sum(ios) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_1
where rank in (14,19)
union all

select 
10 as rank
,'有回复对话数' as type
,sum(android+ios) as total
,sum(android) as android
,sum(ios) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_1
where rank in (15,20)
union all

select 
12 as rank
,'对话深度' as type
,(sum(case when rank=15 then total end)*sum(case when rank=17 then total end)
  +sum(case when rank=20 then total end)*sum(case when rank=22 then total end))
  /sum(case when rank in (15,20) then total end) as total
,(sum(case when rank=15 then android end)*sum(case when rank=17 then android end)
  +sum(case when rank=20 then android end)*sum(case when rank=22 then android end))
  /sum(case when rank in (15,20) then android end) as android
,(sum(case when rank=15 then ios end)*sum(case when rank=17 then ios end)
  +sum(case when rank=20 then ios end)*sum(case when rank=22 then ios end))
  /sum(case when rank in (15,20) then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_1
where rank in (15,17,20,22)
union all

select 
13 as rank
,'5分钟回复率' as type
,(sum(case when rank=15 then total end)*sum(case when rank=18 then total end)
  +sum(case when rank=20 then total end)*sum(case when rank=23 then total end))
  /sum(case when rank in (15,20) then total end) as total
,(sum(case when rank=15 then android end)*sum(case when rank=18 then android end)
  +sum(case when rank=20 then android end)*sum(case when rank=23 then android end))
  /sum(case when rank in (15,20) then android end) as android
,(sum(case when rank=15 then ios end)*sum(case when rank=18 then ios end)
  +sum(case when rank=20 then ios end)*sum(case when rank=23 then ios end))
  /sum(case when rank in (15,20) then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_1
where rank in (15,18,20,23)
union all

select * from dw_temp_angejia.zhiwen_promotion2_effect_end_1
;


drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_end;
create table dw_temp_angejia.zhiwen_promotion2_effect_end as 
select 
5 as rank
,'连数接' as type
,sum(nvl(total,0)) as total
,sum(nvl(android,0)) as android
,sum(nvl(ios,0)) as ios
,sum(nvl(pc,0)) as pc
,sum(nvl(tw,0)) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_end_2
where rank in (6,7,8)
union all

select 
11 as rank
,'无回复对话数' as type
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select sum(nvl(android,0)) as android
,sum(nvl(ios,0)) as ios
from dw_temp_angejia.zhiwen_promotion2_effect_end_2
where rank in (16,21)
) t
union all

select * from dw_temp_angejia.zhiwen_promotion2_effect_end_2
;

export hive dw_temp_angejia.zhiwen_promotion2_effect_end
to mysql dw_temp_angejia.zhiwen_promotion2_effect_end;


---------------------------------非自营销内容------------------------------------------
drop table if exists dw_temp_angejia.zhiwen_not_promotion2_effect_end_1;
create table dw_temp_angejia.zhiwen_not_promotion2_effect_end_1 as 
select 
0 as rank
,'房源量' as type 
,not_promotion_inventory_cnt as total
,not_promotion_inventory_cnt as android
,not_promotion_inventory_cnt as ios
,not_promotion_inventory_cnt as pc
,not_promotion_inventory_cnt as tw
from dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_cnt
union all

select 
1.1 as rank
,'VPPV' as type
,sum(android_vppv + ios_vppv + pc_vppv + tw_vppv) as total
,sum(android_vppv) as android
,sum(ios_vppv) as ios
,sum(pc_vppv) as pc
,sum(tw_vppv) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vppv
where promotion_check = 0
union all

select 
2 as rank
,'VPUV' as type
,sum(vpud) as total
,sum(case when type = 'android' then vpud end) as android
,sum(case when type = 'ios' then vpud end) as ios
,sum(case when type = 'pc' then vpud end) as pc
,sum(case when type = 'tw' then vpud end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vpud
where promotion_check = 0
union all

select 
3 as rank
,'EVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_evpuv
where promotion_check = 0
union all

select 
3.1 as rank
,'FVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_fvpuv
where promotion_check = 0
union all

select 
4 as rank
,'有vppv房源量' as type
,sum(total_inventory_cnt) as total
,sum(android_inventory_cnt) as android
,sum(ios_inventory_cnt) as ios
,sum(pc_inventory_cnt) as pc
,sum(tw_inventory_cnt) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vppv
where promotion_check = 0

union all
select
7 as rank
,'来电量' as type
,nvl(android,0)+nvl(ios,0)+nvl(pc,0)+nvl(tw,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,nvl(tw,0) as tw
from
(select sum(case when type = 'android' then call_cnt end) as android
  ,sum(case when type = 'ios' then call_cnt end) as ios
  ,sum(case when type = 'pc' then call_cnt end) as pc
  ,sum(case when type = 'tw' then call_cnt end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_call_cnt
where promotion_check = 0 ) t
union all

select 
8 as rank
,'联系顾问量' as type
,nvl(connect_broker_cnt,0) as total
,'' as android
,'' as ios
,nvl(connect_broker_cnt,0) as pc
,'' as tw
from (
select sum(connect_broker_cnt) as connect_broker_cnt 
from dw_temp_angejia.zhiwen_promotion2_effect_connect_broker_cnt
where promotion_check = 0
) t
union all

select 
14 as rank
,'房源单页微聊连接对话数' as type
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select sum(case when type = 1 then wechat_cnt end) as android
  ,sum(case when type = 2 then wechat_cnt end) as ios
  from dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt
) t
union all

select 
15 as rank
,'有回复的对话数' as type
,nvl(total,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select sum(back_user_cnt) as total
,sum(case when type=1 then back_user_cnt end) as android
,sum(case when type=2 then back_user_cnt end) as ios
from dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back
where type in ('1','2')
) t
union all

select 
17 as rank
,'对话深度' as type
,nvl((nvl(android,0)*nvl(android_cnt,0)+nvl(ios,0)*nvl(ios_cnt,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=1 then back_user_cnt end) as android_cnt
,sum(case when type=2 then back_user_cnt end) as ios_cnt
,sum(case when type=1 then back_msg_cnt end) as android
,sum(case when type=2 then back_msg_cnt end) as ios
from dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back
) t
union all

select 
18 as rank
,'5分钟回复率' as type
,nvl((android*android_cnt+ios*ios_cnt)/(android_cnt+ios_cnt),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=1 then back_user_cnt end) as android_cnt
,sum(case when type=2 then back_user_cnt end) as ios_cnt
,sum(case when type=1 then 5_min_percent end) as android
,sum(case when type=2 then 5_min_percent end) as ios
from dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back
) t
union all

select 
19 as rank
,'顾问单页微聊连接对话数' as type
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select sum(case when type = 3 then wechat_cnt end) as android
  ,sum(case when type = 4 then wechat_cnt end) as ios
  from dw_temp_angejia.zhiwen_not_promotion2_effect_wechat_cnt
) t
union all

select 
20 as rank
,'有回复的对话数' as type
,nvl(total,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type in ('3','4') then back_user_cnt end) as total
,sum(case when type=3 then back_user_cnt end) as android
,sum(case when type=4 then back_user_cnt end) as ios
from dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back
) t
union all

select 
22 as rank
,'对话深度' as type
,nvl((nvl(android,0)*nvl(android_cnt,0)+nvl(ios,0)*nvl(ios_cnt,0))/(nvl(android_cnt,0)+nvl(ios_cnt,0)),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=3 then back_user_cnt end) as android_cnt
,sum(case when type=4 then back_user_cnt end) as ios_cnt
,sum(case when type=3 then back_msg_cnt end) as android
,sum(case when type=4 then back_msg_cnt end) as ios
from dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back
) t
union all

select 
23 as rank
,'5分钟回复率' as type
,nvl((android*android_cnt+ios*ios_cnt)/(android_cnt+ios_cnt),0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from
(select 
sum(case when type=3 then back_user_cnt end) as android_cnt
,sum(case when type=4 then back_user_cnt end) as ios_cnt
,sum(case when type=3 then 5_min_percent end) as android
,sum(case when type=4 then 5_min_percent end) as ios
from dw_temp_angejia.zhiwen_not_promotion_wechat_cnt_android_back
) t
union all

select
24 as rank
,'来电量' as type
,nvl(android,0)+nvl(ios,0)+nvl(pc,0)+nvl(tw,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,nvl(tw,0) as tw
from
(select sum(case when type = 'android' then call_cnt end) as android
  ,sum(case when type = 'ios' then call_cnt end) as ios
  ,sum(case when type = 'pc' then call_cnt end) as pc
  ,sum(case when type = 'tw' then call_cnt end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_call_cnt
where promotion_check = 0 ) t
union all

select
25 as rank
,'房源单页来电量' as type
,nvl(android,0)+nvl(ios,0)+nvl(pc,0)+nvl(tw,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,nvl(pc,0) as pc
,nvl(tw,0) as tw
from
(select sum(case when type = 'android' then call_cnt end) as android
  ,sum(case when type = 'ios' then call_cnt end) as ios
  ,sum(case when type = 'pc' then call_cnt end) as pc
  ,sum(case when type = 'tw' then call_cnt end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_call_cnt
where promotion_check = 0 ) t
union all

select 
26 as rank
,'顾问单页来电量' as type
,'' as total   -- 后续补充
,'' as android
,'' as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_promotion2_effect_call_cnt
limit 1
;


drop table if exists dw_temp_angejia.zhiwen_not_promotion2_effect_end_2;
create table dw_temp_angejia.zhiwen_not_promotion2_effect_end_2 as 
select 
16 as rank
,'无回复对话数' as type
,sum(case when rank = 14 then total end) - sum(case when rank = 15 then total end) as total
,sum(case when rank = 14 then android end) - sum(case when rank = 15 then android end) as android
,sum(case when rank = 14 then ios end) - sum(case when rank = 15 then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
where rank in (14,15)
union all

select 
21 as rank
,'无回复对话数' as type
,sum(case when rank = 19 then total end) - sum(case when rank = 20 then total end) as total
,sum(case when rank = 19 then android end) - sum(case when rank = 20 then android end) as android
,sum(case when rank = 19 then ios end) - sum(case when rank = 20 then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
where rank in (19,20)
union all

select
6 as rank
,'微聊连接对话数' as type
,sum(total) as total
,sum(android) as android
,sum(ios) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
where rank in (14,19)
union all

select
9 as rank
,'微聊连接对话数' as type
,sum(total) as total
,sum(android) as android
,sum(ios) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
where rank in (14,19)
union all

select 
10 as rank
,'有回复对话数' as type
,sum(android+ios) as total
,sum(android) as android
,sum(ios) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
where rank in (15,20)
union all

select 
12 as rank
,'对话深度' as type
,(sum(case when rank=15 then total end)*sum(case when rank=17 then total end)
  +sum(case when rank=20 then total end)*sum(case when rank=22 then total end))
  /sum(case when rank in (15,20) then total end) as total
,(sum(case when rank=15 then android end)*sum(case when rank=17 then android end)
  +sum(case when rank=20 then android end)*sum(case when rank=22 then android end))
  /sum(case when rank in (15,20) then android end) as android
,(sum(case when rank=15 then ios end)*sum(case when rank=17 then ios end)
  +sum(case when rank=20 then ios end)*sum(case when rank=22 then ios end))
  /sum(case when rank in (15,20) then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
where rank in (15,17,20,22)
union all

select 
13 as rank
,'5分钟回复率' as type
,(sum(case when rank=15 then total end)*sum(case when rank=18 then total end)
  +sum(case when rank=20 then total end)*sum(case when rank=23 then total end))
  /sum(case when rank in (15,20) then total end) as total
,(sum(case when rank=15 then android end)*sum(case when rank=18 then android end)
  +sum(case when rank=20 then android end)*sum(case when rank=23 then android end))
  /sum(case when rank in (15,20) then android end) as android
,(sum(case when rank=15 then ios end)*sum(case when rank=18 then ios end)
  +sum(case when rank=20 then ios end)*sum(case when rank=23 then ios end))
  /sum(case when rank in (15,20) then ios end) as ios
,'' as pc
,'' as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
where rank in (15,18,20,23)
union all

select * from dw_temp_angejia.zhiwen_not_promotion2_effect_end_1
;

drop table if exists dw_temp_angejia.zhiwen_not_promotion2_effect_end;
create table dw_temp_angejia.zhiwen_not_promotion2_effect_end as 
select 
5 as rank
,'连数接' as type
,sum(nvl(total,0)) as total
,sum(nvl(android,0)) as android
,sum(nvl(ios,0)) as ios
,sum(nvl(pc,0)) as pc
,sum(nvl(tw,0)) as tw
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_2
where rank in (6,7,8)

union all
select 
11 as rank
,'无回复对话数' as type
,nvl(android,0)+nvl(ios,0) as total
,nvl(android,0) as android
,nvl(ios,0) as ios
,'' as pc
,'' as tw
from 
(select 
sum(nvl(android,0)) as android
,sum(nvl(ios,0)) as ios
from dw_temp_angejia.zhiwen_not_promotion2_effect_end_2
where rank in (16,21)
) t
union all
select * from dw_temp_angejia.zhiwen_not_promotion2_effect_end_2
;

export hive dw_temp_angejia.zhiwen_not_promotion2_effect_end
to mysql dw_temp_angejia.zhiwen_not_promotion2_effect_end;


-------------------------------全站访问效果日报---------------------------------
drop table if exists dw_temp_angejia.zhiwen_promotion2_effect_end_total;
create table dw_temp_angejia.zhiwen_promotion2_effect_end_total as 
select 
0 as rank
,'房源量' as type 
,inventory_cnt as total
,inventory_cnt as android
,inventory_cnt as ios
,inventory_cnt as pc
,inventory_cnt as tw
from dw_temp_angejia.zhiwen_promotion2_effect_promotion_inventory_cnt
union all

select 
1.1 as rank
,'VPPV' as type
,sum(android_vppv + ios_vppv + pc_vppv + tw_vppv) as total
,sum(android_vppv) as android
,sum(ios_vppv) as ios
,sum(pc_vppv) as pc
,sum(tw_vppv) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vppv
union all

select 
2 as rank
,'VPUV' as type
,sum(vpud) as total
,sum(case when type = 'android' then vpud end) as android
,sum(case when type = 'ios' then vpud end) as ios
,sum(case when type = 'pc' then vpud end) as pc
,sum(case when type = 'tw' then vpud end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vpud_total
union all

select 
3 as rank
,'EVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_evpuv_total
union all


select 
3.1 as rank
,'FVPUV' as type
,count(*) as total
,count(case when type = 'android' then guid end) as android
,count(case when type = 'ios' then guid end) as ios
,count(case when type = 'pc' then guid end) as pc
,count(case when type = 'tw' then guid end) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_fvpuv_total
union all

select 
4 as rank
,'有vppv房源量' as type
,sum(total_inventory_cnt) as total
,sum(android_inventory_cnt) as android
,sum(ios_inventory_cnt) as ios
,sum(pc_inventory_cnt) as pc
,sum(tw_inventory_cnt) as tw
from dw_temp_angejia.zhiwen_promotion2_effect_vppv
;

export hive dw_temp_angejia.zhiwen_promotion2_effect_end_total
to mysql dw_temp_angejia.zhiwen_promotion2_effect_end_total;