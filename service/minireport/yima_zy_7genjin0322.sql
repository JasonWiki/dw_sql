drop table if exists dw_temp_angejia.yima_zy_7genjin0322;
create table dw_temp_angejia.yima_zy_7genjin0322 as
--小区7天跟进房源
select k1.p_dt,k1.community_id
,count(distinct k1.inventory_id) as 7genjin_cnt
from dw_db.dw_property_inventory_sd k1
inner join(
  select a1.p_dt,a1.inventory_id
  from db_gather.angejia__inventory_followup a1
  left join db_sync.angejia__target_call_detail b1
    on a1.id=b1.target_followup_id
  ----and a1.content is not null
  where a1.type='0' and a1.source='1'
    and a1.p_dt<=${dealDate}
    and a1.p_dt>=date_sub(${dealDate},7)
    and to_date(a1.create_at)<=a1.p_dt and to_date(a1.create_at)>=date_sub(a1.p_dt,6)
    -----电话接通或有微聊截图
    and ((b1.type='1' and b1.is_active='1') or a1.images <>'')
  group by a1.p_dt,a1.inventory_id
) k2 --7天跟进房源
  on k1.inventory_id=k2.inventory_id and k1.p_dt=k2.p_dt
where k1.p_dt<=${dealDate}
  and k1.p_dt>=date_sub(${dealDate},7)
group by k1.p_dt,k1.community_id
;
