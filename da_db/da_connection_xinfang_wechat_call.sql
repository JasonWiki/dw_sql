--新房电话明细
drop table if exists dw_db_temp.da_xinfang_call_stats_detail;
create table dw_db_temp.da_xinfang_call_stats_detail as
select
a.account_id as broker_account_id,
a.originating_call as caller,
a.terminating_call as called,
b.phone as buyer_phone,
a.keep_time
from db_sync.xinfang__broker_call_records a
left join db_sync.secret__phones b
on a.originating_call=b.id
where to_date(a.start_time)=${dealDate};

--新房电话统计
drop table if exists dw_db_temp.da_xinfang_call_stats;
create table dw_db_temp.da_xinfang_call_stats as
select broker_account_id as account_id,
count(distinct concat(caller,called)) as come_call_num,
count(distinct case when keep_time>0 then concat(caller,called) end) as hold_call_num,
count(distinct case when keep_time>=50 then concat(caller,called) end) as come_call_time_morethan50s_num,
count(distinct case when keep_time>0 and keep_time<50 then concat(caller,called) end) as come_call_time_lessthan50s_num
from dw_db_temp.da_xinfang_call_stats_detail
group by broker_account_id
;

--微聊数据
drop table if exists dw_db_temp.da_xinfang_weichat_stats;
create table dw_db_temp.da_xinfang_weichat_stats as
select broker_uid,
count(user_id) as all_weichat_num,
count(case when is_new_wechat='1' then user_id end) as first_weichat_num,
count(case when source_type='需求找房' then 1 end) as dispatch_weichat_num,
count(case when source_type not in ('需求找房') then 1 end) as detail_weichat_num,
count(case when (hour(user_first_msg_at) between 9 and 22) and (unix_timestamp(broker_first_reply_at)-unix_timestamp(user_first_msg_at))<=300 then 1 end) as five_minute_reply_num,
count(case when (hour(user_first_msg_at) between 9 and 22) and (unix_timestamp(broker_first_reply_at)-unix_timestamp(user_first_msg_at))<=1800 then 1 end) as thirty_minute_reply_num,
count(case when (hour(user_first_msg_at) between 9 and 22) then 1 end) as all_weichat_work_time_num
from dw_db.dw_connection_wechat_sd
where p_dt=${dealDate}
group by broker_uid
;

--录入私客
drop table if exists dw_db_temp.da_xinfang_buyer_stats;
create table dw_db_temp.da_xinfang_buyer_stats as
select broker_id as broker_id,
count(distinct case when divide_source=1 and origin_source in (13,22) then customer_id end) as weichat_buyer_num,
count(distinct case when divide_source=1 and origin_source=12 then customer_id end) as come_call_buyer_num
from db_sync.xinfang__buyer
where to_date(created_at)=${dealDate}
group by broker_id;


insert overwrite table da_db.da_connection_xinfang_wechat_call partition (p_dt=${dealDate})
select
brk.city_id,
brk.city_name,
brk.agent_id as center_id,
brk.agent_name as center_name,
brk.team_id as department_id,
brk.team_name as department_name,
brk.account_id,
brk.broker_uid as broker_id,
brk.broker_name,
nvl(wechat.all_weichat_num,0) as all_weichat_num,
nvl(wechat.dispatch_weichat_num,0) as dispatch_weichat_num,
nvl(wechat.detail_weichat_num,0) as detail_weichat_num,
nvl(wechat.five_minute_reply_num,0) as five_minute_reply_num,
nvl(wechat.thirty_minute_reply_num,0) as thirty_minute_reply_num,
nvl(wechat.all_weichat_work_time_num,0) as all_weichat_work_time_num,
nvl(nvl(wechat.five_minute_reply_num,0)/nvl(wechat.all_weichat_work_time_num,0),0) as five_minute_reply_percentage,
nvl(nvl(wechat.thirty_minute_reply_num,0)/nvl(wechat.all_weichat_work_time_num,0),0) as thirty_minute_reply_percentage,
nvl(call.come_call_num,0) as come_call_num,
nvl(call.hold_call_num,0) as hold_call_num,
case when nvl(call.come_call_num,0)<>0 then nvl(call.hold_call_num,0)/nvl(call.come_call_num,0) else 0 end as call_connect_num,
nvl(wechat.first_weichat_num,0) as first_weichat_num,
nvl(buyer.weichat_buyer_num,0) as weichat_buyer_num,
nvl(buyer.come_call_buyer_num,0) as come_call_buyer_num,
nvl(call.come_call_time_morethan50s_num,0) as come_call_time_morethan50s_num,
nvl(call.come_call_time_lessthan50s_num,0) as come_call_time_lessthan50s_num
from (
  select * from dw_db.dim_broker
  where p_dt=${dealDate}
  and category='新房'
) brk
left join dw_db_temp.da_xinfang_weichat_stats as wechat
on brk.broker_uid=wechat.broker_uid
left join dw_db_temp.da_xinfang_buyer_stats buyer
on brk.account_id=buyer.broker_id
left join dw_db_temp.da_xinfang_call_stats call
on brk.account_id=call.account_id
where brk.leaving_date>=${dealDate} --顾问离职时间大于分区日期
;
