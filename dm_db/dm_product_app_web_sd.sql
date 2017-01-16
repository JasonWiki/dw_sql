--app action
drop table if exists dw_db_temp.dw_product_app_action_page;
create table dw_db_temp.dw_product_app_action_page as
select
p_dt,
case when model like '%Android%' then 'android' else 'ios' end as platform,--平台
substr(version,1,3) as version,--版本
case when name in ('a-angejia','i-angejia') then 'user'
     when name in ('a-broker','i-broker') then 'broker'
     when name = 'fy360' then 'circle'
end product_type,--产品类型
ccid as city_id,--城市id
GROUPING__ID,
'action_page' as type,
current_page_id as id,
current_page_name as name,
count(1) as pv,
count(distinct dvid) as ud
from dw_db.dw_app_action_detail_log
where p_dt = ${dealDate}
group by
p_dt,
case when model like '%Android%' then 'android' else 'ios' end,
substr(version,1,3),
case when name in ('a-angejia','i-angejia') then 'user'
     when name in ('a-broker','i-broker') then 'broker'
     when name = 'fy360' then 'circle'
end,
ccid,
current_page_id,
current_page_name
--两个分组，1包括city_id，2不包括city_id
grouping sets ((p_dt,
case when model like '%Android%' then 'android' else 'ios' end,
substr(version,1,3),
case when name in ('a-angejia','i-angejia') then 'user'
     when name in ('a-broker','i-broker') then 'broker'
     when name = 'fy360' then 'circle'
end,
ccid,
current_page_id,
current_page_name),(p_dt,
case when model like '%Android%' then 'android' else 'ios' end,
substr(version,1,3),
case when name in ('a-angejia','i-angejia') then 'user'
     when name in ('a-broker','i-broker') then 'broker'
     when name = 'fy360' then 'circle'
end,
current_page_id,
current_page_name))
;

--app page
drop table if exists dw_db_temp.dw_product_app_page;
create table dw_db_temp.dw_product_app_page as
select
p_dt,
case when platform in ('Android','android') then 'android'
     when platform in ('IOS','iOS') then 'ios'
end as platform,--平台
substr(app_version,1,3) as version,--版本
case when app_name in ('a-angejia','i-angejia') then 'user'
     when app_name in ('a-broker','i-broker') then 'broker'
     else 'circle'
end as product_type,--产品类型
selection_city_id as city_id,--城市id
GROUPING__ID,
'page' as type,
request_page_id as id,
request_page_name as name,
count(1) as pv,
count(distinct device_id) as ud
from dw_db.dw_app_access_log
where p_dt = ${dealDate}
group by
p_dt,
case when platform in ('Android','android') then 'android'
     when platform in ('IOS','iOS') then 'ios' end,
substr(app_version,1,3),
case when app_name in ('a-angejia','i-angejia') then 'user'
     when app_name in ('a-broker','i-broker') then 'broker'
else 'circle' end,
selection_city_id,
request_page_id,
request_page_name
--两个分组，1包括city_id，2不包括city_id
grouping sets((p_dt,
case when platform in ('Android','android') then 'android'
     when platform in ('IOS','iOS') then 'ios' end,
substr(app_version,1,3),
case when app_name in ('a-angejia','i-angejia') then 'user'
     when app_name in ('a-broker','i-broker') then 'broker'
else 'circle' end,
selection_city_id,
request_page_id,
request_page_name),(p_dt,
case when platform in ('Android','android') then 'android'
     when platform in ('IOS','iOS') then 'ios' end,
substr(app_version,1,3),
case when app_name in ('a-angejia','i-angejia') then 'user'
     when app_name in ('a-broker','i-broker') then 'broker'
else 'circle' end,
request_page_id,
request_page_name))
;

--pc or tw action
drop table if exists dw_db_temp.dw_product_pctw_action_page;
create table dw_db_temp.dw_product_pctw_action_page as
select
p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw'
end platform,--平台
'' as version,--版本
'' AS product_type,--产品类型
ccid as city_id,--城市id
GROUPING__ID,
'action_page' as type,
current_page_id as id,
current_page_name as name,
count(1) as pv,
count(distinct user_id) as ud
from dw_db.dw_web_action_detail_log
where p_dt = ${dealDate} and (current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com')
or current_host like 'm.%')
group by
p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw' end,
ccid,
current_page_id,
current_page_name
--两个分组，1包括city_id，2不包括city_id
grouping sets ((p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw' end,
ccid,
current_page_id,
current_page_name),(p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw' end,
current_page_id,
current_page_name))
;

--pc or tw page
drop table if exists dw_db_temp.dw_product_pctw_page;
create table dw_db_temp.dw_product_pctw_page as
select
p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw'
end platform,--平台
'' as version,--版本
'' AS product_type,--产品类型
selection_city_id as city_id,--城市id
GROUPING__ID,
'page' as type,
current_page_id as id,
current_page_name as name,
count(1) as pv,
count(distinct user_id) as ud
from dw_db.dw_web_visit_traffic_log
where p_dt = ${dealDate} and (current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com')
or current_host like 'm.%')
group by
p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw' end,
selection_city_id,
current_page_id,
current_page_name
--两个分组，1包括city_id，2不包括city_id
grouping sets ((p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw' end,
selection_city_id,
current_page_id,
current_page_name),(p_dt,
case when current_host in ('bj.angejia.com','sh.angejia.com','xg.angejia.com','hz.angejia.com') then 'pc'
     when current_host like 'm.%' then 'tw' end,
current_page_id,
current_page_name))
;


insert overwrite table dm_db.dm_product_app_web_sd partition (p_dt = ${dealDate})
select
p_dt as cal_dt,
platform,
version,
product_type,
case when t.city_id_new='all' then 0 else city_id end as city_id,
type,
t.id,
t.name,
pv,
ud,
case when t.city_id_new='all' then '全国' else city.name end city_name
from (
  select *,
  case when GROUPING__ID=111 then 'all' else city_id end as city_id_new
  from dw_db_temp.dw_product_app_action_page
  union all
  select *,
  case when GROUPING__ID=111 then 'all' else city_id end as city_id_new
  from dw_db_temp.dw_product_app_page
  union all
  select *,
  case when GROUPING__ID=27 then 'all' else city_id end as city_id_new
  from dw_db_temp.dw_product_pctw_action_page
  union all
  select *,
  case when GROUPING__ID=27 then 'all' else city_id end as city_id_new
  from dw_db_temp.dw_product_pctw_page
) t
left join dw_db.dim_city city
on t.city_id_new=city.id
;
