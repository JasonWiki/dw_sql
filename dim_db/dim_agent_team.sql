insert overwrite table dim_db.dim_agent_team
select
a.id as team_id,
a.name as team_name,
b.id as agent_id,
b.name as agent_name,
c.id as company_id,
c.name as company_name,
a.city_id,
a.created_at,
a.updated_at,
a.acommunity_count as team_community_cnt,
a.bambooplate_count as team_bambooplate_cnt,
a.lng as team_lng,
a.lat as team_lat,
b.acommunity_count as agent_community_cnt,
b.bambooplate_count as agent_bambooplate_cnt,
b.lng as agent_lng,
b.lat as agent_lat
from
db_sync.property__agent_team a
inner join db_sync.property__agent_team b on a.parent_team_id=b.id and b.level='3' and b.deleted_at is null
inner join db_sync.property__agent_team c on b.parent_team_id=c.id and c.level='1' and c.deleted_at is null
where a.level='5' and a.deleted_at is null and a.name not like '%测试%'
;
