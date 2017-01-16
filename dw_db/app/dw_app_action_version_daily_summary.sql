drop table if exists dw_db_temp.dw_app_action_version_daily_summary_hzq_tmp;
create table dw_db_temp.dw_app_action_version_daily_summary_hzq_tmp as
select
name as app_name,
version,
dvid,
sum(case when rn=1 then 1 else 0 end) as is_fud,
sum(case when uid>0 then 1 else 0 end) as is_log_ud,
sum(case when rn=1 and max_user_id>0 then 1 else 0 end) as is_log_fud
from (
  select name,
  action_id,
  action_name,
  version,
  dvid,
  uid,
  p_dt,
  row_number() over (distribute by dvid sort by server_time asc) as rn,
  max(uid) over (distribute by p_dt,dvid) as max_user_id
  from dw_db.dw_app_action_detail_log
  where p_dt>=date_sub(${dealDate},89) and p_dt<=${dealDate}
  --and name in ('i-angejia', 'a-angejia')
) t
where p_dt=${dealDate}
group by name,version,dvid
;

insert overwrite table dw_db.dw_app_action_version_daily_summary partition (p_dt=${dealDate})
select *
from (
  select a.name as app_name,
  a.version,
  a.action_id,
  a.action_name,
  count(distinct a.dvid) as ud,
  count(distinct case when b.is_fud<>'0' then a.dvid end) as fud,
  count(distinct case when b.is_log_ud<>'0' then a.dvid end) as log_ud,
  count(distinct case when b.is_log_fud<>'0' then a.dvid end) as log_fud,
  count(a.dvid) as pv,
  count(case when b.is_fud<>'0' then a.dvid end) as fpv,
  count(case when b.is_log_ud<>'0' then a.dvid end) as log_pv,
  count(case when b.is_log_fud<>'0' then a.dvid end) as log_fpv
  from dw_db.dw_app_action_detail_log a
  inner join dw_db_temp.dw_app_action_version_daily_summary_hzq_tmp b
  on a.name=b.app_name
  and a.dvid=b.dvid
  and a.p_dt=${dealDate}
  group by a.name,a.version,a.action_id,a.action_name

  union all

  select a.name as app_name,
  a.version,
  'total' as action_id,
  'total' as action_name,
  count(distinct a.dvid) as ud,
  count(distinct case when b.is_fud<>'0' then a.dvid end) as fud,
  count(distinct case when b.is_log_ud<>'0' then a.dvid end) as log_ud,
  count(distinct case when b.is_log_fud<>'0' then a.dvid end) as log_fud,
  count(a.dvid) as pv,
  count(case when b.is_fud<>'0' then a.dvid end) as fpv,
  count(case when b.is_log_ud<>'0' then a.dvid end) as log_pv,
  count(case when b.is_log_fud<>'0' then a.dvid end) as log_fpv
  from dw_db.dw_app_action_detail_log a
  inner join dw_db_temp.dw_app_action_version_daily_summary_hzq_tmp b
  on a.name=b.app_name
  and a.dvid=b.dvid
  and a.p_dt=${dealDate}
  group by a.name,a.version
) t
where ud>0 or fud>0 or log_ud>0 or log_fud>0
;
