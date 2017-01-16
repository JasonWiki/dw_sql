-- 房源 - 房源局精耕小区日报表
insert overwrite table dw_temp_angejia.xiaohei_followup_temp partition (p_dt=${dealDate})
select
 case when b.agent_name like '%合伙%' then substring(b.agent_name,1,instr(b.agent_name,'-')-1) else b.agent_name end as team_name
,count(m.inventory_id) as inventory_cnt
,count(case when nvl(to_date(a.max_time),to_date(m.i_created_at)) between date_sub(${dealDate},7) and ${dealDate} then m.inventory_id end) as day_followup
,count(case when nvl(to_date(a.max_time),to_date(m.i_created_at)) between date_sub(${dealDate},14) and date_sub(${dealDate},8) then m.inventory_id end) as 7day_followup
,count(case when nvl(to_date(a.max_time),to_date(m.i_created_at))<date_sub(${dealDate},14) then m.inventory_id end) as 14day_followup
,avg(datediff(${dealDate},nvl(to_date(a.max_time),to_date(m.i_created_at)))) as followup_time
,count(case when a.source='1' then a.inventory_id end) as followup_broker
,count(case when a.source='2' then a.inventory_id end) as followup_service
from
dw_db.dw_property_inventory m
left join (
select x.inventory_id,x.source,b.max_time from db_sync.angejia__inventory_followup x
inner join (select inventory_id,max(create_at) max_time from db_sync.angejia__inventory_followup where to_date(create_at)<=${dealDate} and  type='0' group by inventory_id) b
on x.inventory_id=b.inventory_id and x.create_at=b.max_time
where x.type='0' and to_date(x.create_at)<=${dealDate}
group by x.inventory_id,x.source,b.max_time
) a on m.inventory_id=a.inventory_id
inner join dim_db.dim_community_classify b on m.community_id=b.community_id and b.com_type='1'
where m.i_status='2' and m.city_id<>'3'
and to_date(m.i_created_at)<=${dealDate}
group by
 case when b.agent_name like '%合伙%' then substring(b.agent_name,1,instr(b.agent_name,'-')-1) else b.agent_name end
;



insert overwrite table dw_temp_angejia.xiaohei_community_qualityup partition (p_dt=${dealDate})
select
 m.inventory_id
,b.community_name
,case when b.agent_name like '%合伙%' then substring(b.agent_name,1,instr(b.agent_name,'-')-1) else b.agent_name end as team_name
,to_date(m.i_created_at) stat_date
,m.verify_status
,1 as fy_cnt
,case when m.source in('4','5','6') then 1 else 0 end as fyj_cnt
,case when m.source in('1','3','7') then 1 else 0 end as fd_cnt
,case when m.source='2' then 1 else 0 end as jjr_cnt
from
dw_db.dw_property_inventory m inner join dim_db.dim_community_classify b on m.community_id=b.community_id and b.com_type='1'
where m.i_status='2' and m.city_id<>'3' and to_date(m.i_created_at)=${dealDate}
;





drop table if exists dw_db_temp.dw_inventory_verification_stat_rpt;
create table dw_db_temp.dw_inventory_verification_stat_rpt
as
select
 c.agent_name
,count(verif_cnt) as verif_cnt
,count(fyj_verif_cnt_month) as fyj_verif_cnt_month
,count(fd_verif_cnt_month) as fd_verif_cnt_month
,count(jjr_verif_cnt_month) as jjr_verif_cnt_month
,count(audit_success_cnt) as audit_success_cnt
,count(audit_cnt) as audit_cnt
,count(distinct followup_cnt) as followup_cnt
,count(distinct fyj_followup_cnt_month) as fyj_followup_cnt_month
,count(distinct fd_followup_cnt_month) as fd_followup_cnt_month
,count(distinct jjr_followup_cnt_month) as jjr_followup_cnt_month
from(
select
 a.broker_id as user_id
,case when to_date(a.audit_time)=${dealDate} then a.inventory_id end as verif_cnt
,case when c.source in('4','5','6') then a.inventory_id end as fyj_verif_cnt_month
,case when c.source in('1','3','7') then a.inventory_id end as fd_verif_cnt_month
,case when c.source='2' then a.inventory_id end as jjr_verif_cnt_month
,case when a.status='1' then a.inventory_id end as audit_success_cnt
,a.inventory_id as audit_cnt
,null as followup_cnt
,null as fyj_followup_cnt_month
,null as fd_followup_cnt_month
,null as jjr_followup_cnt_month
from db_sync.angejia__inventory_correct a
inner join(
select
 inventory_id
,to_date(audit_time) as audit_time
,max(updated_at) max_time
from db_sync.angejia__inventory_correct
where to_date(audit_time) between date_add(last_day(add_months(${dealDate},-1)),1) and ${dealDate}
group by inventory_id,to_date(audit_time)
) b on a.inventory_id=b.inventory_id and a.updated_at=b.max_time and to_date(a.audit_time)=b.audit_time
inner join db_sync.property__inventory c on a.inventory_id=c.id
where to_date(a.audit_time) between date_add(last_day(add_months(${dealDate},-1)),1) and ${dealDate}
union all

select
 a.broker_uid as user_id
,null as verif_cnt
,null as fyj_verif_cnt_month
,null as fd_verif_cnt_month
,null as jjr_verif_cnt_month
,null as audit_success_cnt
,null as audit_cnt
,case when to_date(a.create_at)=${dealDate} then a.inventory_id end as followup_cnt
,case when b.source in('4','5','6') then a.inventory_id end as fyj_followup_cnt_month
,case when b.source in('1','3','7') then a.inventory_id end as fd_followup_cnt_month
,case when b.source='2' then a.inventory_id end as jjr_followup_cnt_month
from db_sync.angejia__inventory_followup a inner join db_sync.property__inventory b on a.inventory_id=b.id
where to_date(create_at) between date_add(last_day(add_months(${dealDate},-1)),1) and ${dealDate} and a.type='0' and a.source='1'
) m left join dw_db.dw_broker_sd c on m.user_id=c.user_id and c.p_dt=${dealDate}
group by c.agent_name
;



drop table if exists dw_db_temp.a_community_qualityup_daily_temp;
create table dw_db_temp.a_community_qualityup_daily_temp
as
select
 b.community_id
,m.stat_date
,m.verify_status
,sum(m.fy_cnt) as fy_cnt
,sum(m.fyj_cnt) as fyj_cnt
,sum(m.fd_cnt) as fd_cnt
,sum(m.jjr_cnt) as jjr_cnt
,count(distinct m.gj_cnt) as gj_cnt
from(
select
 m.property_id
,to_date(m.created_at) stat_date
,m.verify_status
,1 as fy_cnt
,case when m.source in('4','5','6') then 1 else 0 end as fyj_cnt
,case when m.source in('1','3','7') then 1 else 0 end as fd_cnt
,case when m.source='2' then 1 else 0 end as jjr_cnt
,null as gj_cnt
from
db_sync.property__inventory m
where m.status='2' and m.city_id<>'3'
union all

select
 a.property_id
,to_date(m.create_at) stat_date
,a.verify_status
,0 as fy_cnt
,0 as fyj_cnt
,0 as fd_cnt
,0 as jjr_cnt
,m.inventory_id as gj_cnt
from db_sync.angejia__inventory_followup m
inner join db_sync.property__inventory a on m.inventory_id=a.id
where a.status='2' and a.city_id<>'3'
) m
inner join db_sync.property__property a on m.property_id=a.id
inner join db_sync.property__house b on a.house_id = b.id
group by b.community_id,m.stat_date,m.verify_status
;


drop table if exists dw_db_temp.a_community_qualityup_daily_agent_temp;
create table dw_db_temp.a_community_qualityup_daily_agent_temp
as
select  d.id as agent_id,d.name as agent_name,count(*) agent_cnt
from db_sync.angejia__broker x
left join db_sync.property__agent_team_broker b on b.user_id=x.user_id
left join db_sync.property__agent_team c on b.team_id=c.id and c.level=5
left join db_sync.property__agent_team d on c.parent_team_id=d.id and d.level=3
where d.id<>'46' and x.status='2' and x.type ='1' and x.city_id<>'3'
group by  d.id,d.name
;

drop table if exists dw_db_temp.a_community_qualityup_daily;
create table dw_db_temp.a_community_qualityup_daily
as
select
 trim(case when a.agent_name like '%合伙%' then substring(a.agent_name,1,instr(a.agent_name,'-')-1) else a.agent_name end) as agent_name
,a.community_id
,b.stat_date
,b.verify_status
,nvl(b.fy_cnt,0) as fy_cnt
,nvl(b.fyj_cnt,0) as fyj_cnt
,nvl(b.fd_cnt,0) as fd_cnt
,nvl(b.jjr_cnt,0) as jjr_cnt
,nvl(d.lj_community_inventory,0) as yg_cnt
,nvl(b.gj_cnt,0) as gj_cnt
,nvl(c.agent_cnt,0) as agent_cnt
from dim_db.dim_community_classify a
inner join dw_db_temp.a_community_qualityup_daily_temp b on a.community_id=b.community_id
left join dw_db_temp.a_community_qualityup_daily_agent_temp c on trim(case when a.agent_name like '%合伙%' then substring(a.agent_name,1,instr(a.agent_name,'-')-1) else a.agent_name end)=trim(c.agent_name)
left join db_sync.angejia__community_associate d on a.community_id=d.agj_community_id
where a.com_type='1'
;


-- 导入到 mysql , 记住，不执行的语句不要使用分号
-- export hive dw_temp_angejia.xiaohei_followup_temp to mysql dw_temp_angejia.xiaohei_followup_info
-- export hive dw_temp_angejia.xiaohei_community_qualityup to mysql dw_temp_angejia.xiaohei_community_qualityup
-- export hive dw_db_temp.dw_inventory_verification_stat_rpt to mysql dw_temp_angejia.dw_inventory_verification_stat_rpt
-- export hive dw_db_temp.a_community_qualityup_daily to mysql dw_temp_angejia.a_community_qualityup_daily
