insert overwrite table dim_db.dim_community_classify
select
x.community_id,
x.community_name,
x.block_id,
x.block_name,
x.district_id,
x.district_name,
x.agent_id,
x.agent_name,
x.com_type,
o.is_active,
o.lj_community_id
from(
select
 m.community_id
,a.name as community_name
,b.id as block_id
,b.name as block_name
,c.id as district_id
,c.name as district_name
,m.agent_id
,d.name as agent_name
,m.com_type
from(
select community_id,agent_id,'1' as com_type from db_sync.angejia__community_team  where group_id<>'0'
union all
select community_id,null as agent_id,'2' as com_type from db_sync.angejia_dw__dw_community_main_community_daily where status='1' and p_dt=regexp_replace(${dealDate},'-','')
union all
select community_id,agent_id,'3' as com_type from db_sync.angejia__community_team  where group_id='0' and broker_id='0' and agent_id is not null
) m inner join db_sync.angejia__community a on m.community_id=a.id
inner join db_sync.angejia__block b on a.block_id=b.id
inner join db_sync.angejia__district c on a.district_id=c.id
left join db_sync.property__agent_team d on m.agent_id=d.id and d.level='3'
group by
 m.community_id
,a.name
,b.id
,b.name
,c.id
,c.name
,m.com_type
,m.agent_id
,d.name
) x inner join db_sync.angejia__community o on x.community_id=o.id;
