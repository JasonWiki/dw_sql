drop table if exists dw_db_temp.dw_newland_demand_process_ud_hzq_temp;
create table dw_db_temp.dw_newland_demand_process_ud_hzq_temp as
select platform,
version,
'ud' as type,
nvl(sum(case when action_id = '1-1000001' then ud end),0) as wechat_onview_ud,
nvl(sum(case when action_id = '1-1000002' then ud end),0) as wechat_clicklogin_ud,
nvl(sum(case when action_id = '1-1000003' then ud end),0) as wechat_phonelogin_ud,
nvl(sum(case when action_id = '1-1400001' then ud end),0) as demand_budget_onview_ud,
nvl(sum(case when action_id = '1-1500001' then ud end),0) as demand_type_onview_ud,
nvl(sum(case when action_id = '1-1600001' then ud end),0) as demand_position_onview_ud,
nvl(sum(case when action_id = '1-960001' then ud end),0) as firstpage_onview_ud,
nvl(sum(case when action_id = '1-960050' then ud end),0) as firstpage_clickall_ud,
nvl(sum(case when action_id = '1-960051' then ud end),0) as firstpage_no_demand_ud,
nvl(sum(case when action_id = '1-960052' then ud end),0) as firstpage_send_demand_ud
from
(
  select case when a.app_name='a-angejia' then 'android' else 'ios' end as platform,
  a.version as version,
  a.action_id,
  a.action_name,
  a.ud,
  a.fud,
  a.pv,
  a.fpv,
  a.p_dt
  from dw_db.dw_app_action_version_daily_summary a
  left join dw_db.dw_basis_dimen_action_id_name_lkp b
  on a.action_id = b.action_id
  where a.action_id in ('1-1000001', -- 登录页面
    '1-1000002', -- 微信登录
    '1-1000003', -- 手机登录
    '1-1400001', -- 打开预算
    '1-1500001', -- 打开户型
    '1-1600001', -- 打开位置
    '1-960001', -- 打开首页
    '1-960050', -- 首页-全部二手房
    '1-960051', -- 首页-全部二手房未发需求
    '1-960052' -- 首页-全部二手房已发需求
  )
  and a.p_dt=${dealDate}
  and a.app_name in ('a-angejia', 'i-angejia')
  union all
  select case when a.app_name='a-angejia' then 'android' else 'ios' end as platform,
  'total' as version,
  a.action_id,
  a.action_name,
  a.ud,
  a.fud,
  a.pv,
  a.fpv,
  a.p_dt
  from dw_db.dw_app_action_daily_summary a
  left join dw_db.dw_basis_dimen_action_id_name_lkp b
  on a.action_id = b.action_id
  where a.action_id in ('1-1000001', -- 登录页面
    '1-1000002', -- 微信登录
    '1-1000003', -- 手机登录
    '1-1400001', -- 打开预算
    '1-1500001', -- 打开户型
    '1-1600001', -- 打开位置
    '1-960001', -- 打开首页
    '1-960050', -- 首页-全部二手房
    '1-960051', -- 首页-全部二手房未发需求
    '1-960052' -- 首页-全部二手房已发需求
  )
  and a.p_dt=${dealDate}
  and a.app_name in ('a-angejia', 'i-angejia')
) t
group by platform,version

union all

select platform,
version,
'fud' as type,
nvl(sum(case when action_id = '1-1000001' then fud end),0) as wechat_onview_ud,
nvl(sum(case when action_id = '1-1000002' then fud end),0) as wechat_clicklogin_ud,
nvl(sum(case when action_id = '1-1000003' then fud end),0) as wechat_phonelogin_ud,
nvl(sum(case when action_id = '1-1400001' then fud end),0) as demand_budget_onview_ud,
nvl(sum(case when action_id = '1-1500001' then fud end),0) as demand_type_onview_ud,
nvl(sum(case when action_id = '1-1600001' then fud end),0) as demand_position_onview_ud,
nvl(sum(case when action_id = '1-960001' then fud end),0) as firstpage_onview_ud,
nvl(sum(case when action_id = '1-960050' then fud end),0) as firstpage_clickall_ud,
nvl(sum(case when action_id = '1-960051' then fud end),0) as firstpage_no_demand_ud,
nvl(sum(case when action_id = '1-960052' then fud end),0) as firstpage_send_demand_ud
from (
  select case when a.app_name='a-angejia' then 'android' else 'ios' end as platform,
  a.version as version,
  a.action_id,
  a.action_name,
  a.ud,
  a.fud,
  a.pv,
  a.fpv,
  a.p_dt
  from dw_db.dw_app_action_version_daily_summary a
  left join dw_db.dw_basis_dimen_action_id_name_lkp b
  on a.action_id = b.action_id
  where a.action_id in ('1-1000001', -- 登录页面
    '1-1000002', -- 微信登录
    '1-1000003', -- 手机登录
    '1-1400001', -- 打开预算
    '1-1500001', -- 打开户型
    '1-1600001', -- 打开位置
    '1-960001', -- 打开首页
    '1-960050', -- 首页-全部二手房
    '1-960051', -- 首页-全部二手房未发需求
    '1-960052' -- 首页-全部二手房已发需求
  )
  and a.p_dt=${dealDate}
  and a.app_name in ('a-angejia', 'i-angejia')
  union all
  select case when a.app_name='a-angejia' then 'android' else 'ios' end as platform,
  'total' as version,
  a.action_id,
  a.action_name,
  a.ud,
  a.fud,
  a.pv,
  a.fpv,
  a.p_dt
  from dw_db.dw_app_action_daily_summary a
  left join dw_db.dw_basis_dimen_action_id_name_lkp b
  on a.action_id = b.action_id
  where a.action_id in ('1-1000001', -- 登录页面
    '1-1000002', -- 微信登录
    '1-1000003', -- 手机登录
    '1-1400001', -- 打开预算
    '1-1500001', -- 打开户型
    '1-1600001', -- 打开位置
    '1-960001', -- 打开首页
    '1-960050', -- 首页-全部二手房
    '1-960051', -- 首页-全部二手房未发需求
    '1-960052' -- 首页-全部二手房已发需求
  )
  and a.p_dt=${dealDate}
  and a.app_name in ('a-angejia', 'i-angejia')
) t
group by platform,version
;


drop table if exists dw_db_temp.dw_newland_demand_process_fud_temp;
create table dw_db_temp.dw_newland_demand_process_fud_temp as
select
name as app_name,
dvid,
sum(case when rn=1 then 1 else 0 end) as is_fud
from (
  select name,
  action_id,
  action_name,
  version,
  dvid,
  uid,
  p_dt,
  row_number() over (distribute by dvid sort by server_time asc) as rn
  from dw_db.dw_app_action_detail_log
  where p_dt>=date_sub(${dealDate},89) and p_dt<=${dealDate}
  and name in ('i-angejia', 'a-angejia')
) t
where p_dt=${dealDate}
group by name,dvid
;


drop table if exists dw_db_temp.dw_newland_demand_process_demand_hzq_temp;
create table dw_db_temp.dw_newland_demand_process_demand_hzq_temp as
select a.platform,
case when grouping__id=1 then 'total' else a.version end as version,
'ud' as type,
count(distinct b.user_id) as demand_ud,
count(distinct c.user_id) as connection_ud
from (
  select case when name='a-angejia' then 'android' else 'ios' end as platform,
  version,uid,action_id
  from dw_db.dw_app_action_detail_log
  where name in ('i-angejia', 'a-angejia')
  and p_dt=${dealDate}
) a
left join dw_db.dw_user_demand_log b
on a.uid=b.user_id
and to_date(b.created_at)=${dealDate}
and a.action_id='1-1600001'
left join dw_db.dw_connection_wechat_sd c
on b.user_id=c.user_id
and c.p_dt=${dealDate}
and c.is_new_wechat=1
and c.source_type='需求找房'
group by a.platform,a.version with rollup

union all

select a.platform,
case when grouping__id=1 then 'total' else a.version end as version,
'fud' as type,
count(distinct b.user_id) as demand_ud,
count(distinct c.user_id) as connection_ud
from (
  select case when a.name='a-angejia' then 'android' else 'ios' end as platform,
  a.version,a.uid,a.action_id
  from dw_db.dw_app_action_detail_log a
  inner join dw_db_temp.dw_newland_demand_process_fud_temp b
  on a.dvid=b.dvid and b.is_fud<>'0'
  and a.name in ('i-angejia', 'a-angejia')
  and a.p_dt=${dealDate}
) a
left join dw_db.dw_user_demand_log b
on a.uid=b.user_id
and to_date(b.created_at)=${dealDate}
and a.action_id='1-1600001'
left join dw_db.dw_connection_wechat_sd c
on b.user_id=c.user_id
and c.p_dt=${dealDate}
and c.is_new_wechat=1
and c.source_type='需求找房'
group by a.platform,a.version with rollup
;

insert overwrite table dm_db.dm_newland_demand_process partition (p_dt=${dealDate})
select a.platform,
a.version,
a.type,
a.wechat_onview_ud,
a.wechat_clicklogin_ud,
a.wechat_phonelogin_ud,
a.demand_budget_onview_ud,
a.demand_type_onview_ud,
a.demand_position_onview_ud,
a.firstpage_onview_ud,
a.firstpage_clickall_ud,
a.firstpage_no_demand_ud,
a.firstpage_send_demand_ud,
nvl(b.demand_ud,0) as demand_ud,
nvl(b.connection_ud,0) as connection_ud
from dw_db_temp.dw_newland_demand_process_ud_hzq_temp a
left join dw_db_temp.dw_newland_demand_process_demand_hzq_temp b
on a.platform=b.platform
and a.version=b.version
and a.type=b.type
;
