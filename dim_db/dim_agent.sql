insert overwrite table dw_db.dim_agent
--二手房中心
select agent.id as agent_id,
agent.name as agent_name,
company.id as company_id,
company.name as company_name,
agent.city_id as city_id,
city.name as city_name,
dep.status as status,
usr.user_id as leader_uid,
agent.leader_account_id as leader_aid,
emp.name as leader_name,
emp.phone as leader_phone,
emp.work_number as leader_work_number,
emp.email as leader_email_address,
emp.id_card as leader_id_number,
emp.status as leader_status,
emp.entry_time as leader_entry_time,
emp.dismission_time as leader_dismission_time,
agent.created_at,
agent.updated_at,
agent.deleted_at
from db_sync.property__agent_team agent
inner join db_sync.account__department dep
on agent.id=dep.id
and agent.level=3
inner join db_sync.account__employee emp
on agent.leader_account_id=emp.id
inner join db_sync.angejia__city city
on agent.city_id=city.id
left join db_sync.property__agent_team company
on agent.parent_team_id=company.id
left join (select user_id,account_id from db_sync.angejia__user where user_type=7) usr
on emp.id=usr.account_id
where agent.city_id in (1,2)
union all
--新房中心
select agent.id as agent_id,
agent.name as agent_name,
company.id as company_id,
company.name as company_name,
agent.city_id as city_id,
city.name as city_name,
agent.status as status,
usr.user_id as leader_uid,
agent.leader as leader_aid,
emp.name as leader_name,
emp.phone as leader_phone,
emp.work_number as leader_work_number,
emp.email as leader_email_address,
emp.id_card as leader_id_number,
emp.status as leader_status,
emp.entry_time as leader_entry_time,
emp.dismission_time as leader_dismission_time,
agent.created_at,
agent.updated_at,
null as deleted_at
from db_sync.account__department agent
inner join db_sync.account__employee emp
on agent.leader=emp.id
and agent.parent_id=240
inner join db_sync.angejia__city city
on agent.city_id=city.id
left join db_sync.account__department company
on agent.parent_id=company.id
left join (select user_id,account_id from db_sync.angejia__user where user_type=7) usr
on emp.id=usr.account_id
where agent.city_id in (1,2);
