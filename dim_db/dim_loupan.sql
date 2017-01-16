insert overwrite table dw_db.dim_loupan partition (p_dt=${dealDate})
select
a.id as loupan_id,
a.loupan_name as loupan_name,
a.loupan_alias as loupan_alias,
a.address as address,
a.city_id as city_id,
b.name as city_name,
a.district_id as district_id,
c.name as district_name,
a.block_id as block_id,
d.name as block_name,
a.loop_line as loop_line,
case when a.sale_status=1 then '工地楼盘'
when a.sale_status=2 then '即将开盘'
when a.sale_status=3 then '期房'
when a.sale_status=4 then '现房'
when a.sale_status=5 then '已售罄'
when a.sale_status=6 then '尾盘'
when a.sale_status=7 then '待售' end as sale_status,
case when a.display_status=0 then '下线'
when a.display_status=1 then '待上线'
when a.display_status=2 then '上线' end as display_status,
case when a.partner_status=0 then '不合作'
when a.partner_status=1 then '合作'
when a.partner_status=2 then '竞品盘' end as partner_status,
a.unit_price as unit_price,
a.selling_date as selling_date,
a.handover_date as handover_date,
a.rank_level as rank_level,
a.rank_score as rank_score,
a.content_score,
a.take_time
from db_sync.xinfang__loupan_basic a
inner join db_sync.angejia__city b
on a.city_id=b.id
left join db_sync.angejia__district c
on a.district_id=c.id
left join db_sync.angejia__block d
on a.block_id=d.id
where a.city_id in (1,2);
