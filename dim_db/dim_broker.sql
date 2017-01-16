insert overwrite table dw_db.dim_broker partition (p_dt=${dealDate})
select broker.user_id as broker_uid,
usr.account_id as account_id,
broker.name as broker_name,
broker.on_duty_date as on_duty_date,
to_date(case when emp.dismission_time ='0000-00-00 00:00:00' then '2100-01-01 00:00:00' else emp.dismission_time end) as leaving_date,
broker.status as status_id,
case when broker.status=1 then '待入职'
when broker.status=2 then '在职'
when broker.status=3 then '取消入职'
when broker.status=4 then '离职' end as status,
broker.type as type_id,
case when broker.type=1 then '直营经纪人'
when broker.type=2 then '合伙经纪人'
when broker.type=3 then '部经理'
when broker.type=9 then '其他'
when broker.type=10 then '合伙团队经理' end as type,
'二手房' as category,
broker.city_id as city_id,
city.name as city_name,
org.team_id as team_id,
org.team_name as team_name,
org.agent_id as agent_id,
org.agent_name as agent_name,
org.company_id as company_id,
org.company_name as company_name,
substr(emp.phone,1,11) as phone,
broker.work_number as work_number,
broker.mail as email_address,
broker.identity_card_number as id_number,
broker.company_type as company_type
from db_sync.angejia__broker broker
inner join db_sync.angejia__user usr
on broker.user_id=usr.user_id
inner join db_sync.angejia__city city
on broker.city_id=city.id
inner join db_sync.account__employee emp
on usr.account_id=emp.id
left join (
  select m.user_id,
  a.id as team_id,
  a.name as team_name,
  b.id as agent_id,
  b.name as agent_name,
  c.id as company_id,
  c.name as company_name
  from db_sync.property__agent_team_broker m
  left join db_sync.property__agent_team a on m.team_id=a.id and a.level='5' --and a.deleted_at is null
  left join db_sync.property__agent_team b on a.parent_team_id=b.id and b.level='3' --and b.deleted_at is null
  left join db_sync.property__agent_team c on b.parent_team_id=c.id and c.level='1' --and c.deleted_at is null
) org
on broker.user_id=org.user_id
where broker.user_id not in (3,4)
--and org.agent_id not in ('','46','140') --排除二手房测试中心
and org.agent_name not like '%测试%'
and org.agent_id not in ('','140')
and to_date(broker.created_at)<=${dealDate}

union all

select distinct
usr.user_id as broker_uid,
xb.id as account_id,
xb.name as broker_name,
to_date(xb.entry_time) as on_duty_date,
case when xb.status=4 then to_date(xb.not_entry_time)
else to_date(case when xb.dismission_time='0000-00-00 00:00:00' then '2100-01-01 00:00:00' else xb.dismission_time end) end as leaving_date,
case when xb.status=3 then 4 when xb.status=4 then 3 else xb.status end as status_id,
case when xb.status=1 then '待入职'
when xb.status=2 then '在职'
when xb.status=3 then '离职'
when xb.status=4 then '取消入职' end as status,
null as type_id,
case when xb.job_id=152 then '新房顾问'
when xb.job_id=153 then '新房部经理' end as type,
'新房' as category,
xb.city_id as city_id,
city.name as city_name,
team.id as team_id,
team.name as team_name,
agent.id as agent_id,
agent.name as agent_name,
company.id as company_id,
company.name as company_name,
substr(xb.phone,1,11) as phone,
xb.work_number as work_number,
xb.email as email_address,
substr(xb.id_card,1,18) as id_number,
1 as company_type
from (
  select *
  from db_sync.account__employee
  where job_id in (152,153)
) xb
left join db_sync.angejia__user usr
on xb.id=usr.account_id and usr.user_type=9
left join db_sync.angejia__city city
on xb.city_id=city.id
left join db_sync.account__department team
on xb.department=team.id and team.status=1
left join db_sync.account__department agent
on team.parent_id=agent.id and agent.status=1
left join db_sync.account__department company
on agent.parent_id=company.id and company.status=1
where agent.id<>290 --排除新房测试中心
and to_date(xb.created_at)<=${dealDate}
;
