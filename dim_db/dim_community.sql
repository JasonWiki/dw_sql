insert overwrite table dim_db.dim_community
select
a.id as community_id,
a.name as community_name,
a.block_id,
b.name as block_name,
a.district_id,
c.name as district_name,
a.city_id,
a.alias,
a.address,
a.lng as community_lng,
a.lat as community_lat,
a.zoom,
a.is_active as communtiy_is_active,
a.created_at,
a.updated_at,
a.quanpin,
a.jianpin,
a.review_status,
a.lj_community_id,
b.is_active as block_is_active,
b.lng as block_lng,
b.lat as block_lat,
c.is_active as district_is_active,
c.lng as district_lng,
c.lat as district_lat,
d.builder_id,
d.manage_company_id,
d.label_id,
d.use_type,
d.area,
d.manage_pay,
d.house_total,
d.contain_pert,
d.carbarn_state,
d.green_pert,
d.intro,
d.build_date
from db_sync.angejia__community a
inner join db_sync.angejia__block b on a.block_id=b.id
inner join db_sync.angejia__district c on a.district_id=c.id
inner join db_sync.angejia__community_extend d on a.id=d.community_id
;
