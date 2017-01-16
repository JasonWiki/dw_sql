-----BOSS近两个月使用准备数据/BOSS每日经纪人、房源、客户UV/PV
drop table if exists dw_temp_angejia.yuan_boss_uv_pv_m;
create table dw_temp_angejia.yuan_boss_uv_pv_m as
select to_date(created_at) as p_dt,'活跃用户数' as action_desc,
count(distinct user_id) as uv,
count(user_id) as pv
from db_sync.property__boss_action_log
where to_date(created_at)>=date_sub(${dealDate},30)
and to_date(created_at)<=${dealDate}
group by to_date(created_at)
union all
select to_date(created_at) as p_dt,
case when action_name in ('acommunity_show','acommunity_followup','acommunity_setnice','acommunity_delACommunity',
                    'inventory_followup_record','inventory_search','inventory_exclusive','inventory_maintainer','inventory_key_holder'
                   ,'bambooplate_reject','bambooplate_show','bambooplate_review','bambooplate_pass') then '房源'
     when action_name in ('acommunity_show','acommunity_followup','acommunity_setnice','acommunity_delACommunity') then 'A类小区'
     when action_name in ('inventory_followup_record','inventory_search') then '房源管理'
     when action_name in ('inventory_exclusive','inventory_maintainer','inventory_key_holder') then '房源详情页'
     when action_name in ('bambooplate_reject','bambooplate_show','bambooplate_review','bambooplate_pass') then '笋盘管理'
     when action_name in ('performance_show','performance_download','report_show','report_download',
'broker_generalize_show','broker_dimission','broker_join_cancel','broker_join','broker_image_upload','broker_reset_password') then '经纪人'
     when action_name in ('performance_show','performance_download') then '绩效考核'
     when action_name in ('report_show','report_download') then '业务报表'
     when action_name in ('broker_generalize_show') then '自营销'
     when action_name in ('customer_inventory_log','customer_log','customer_show','customer_detail') then '客户管理'
     when action_name in ('customer_show') then '客户管理列表'
     when action_name in ('customer_inventory_log','customer_log','customer_detail') then '客户详情单页' end as action_desc,
count(distinct user_id) as uv,
count(user_id) as pv
from db_sync.property__boss_action_log
where to_date(created_at)>=date_sub(${dealDate},30)
and to_date(created_at)<=${dealDate}
group by to_date(created_at),case when action_name in ('acommunity_show','acommunity_followup','acommunity_setnice','acommunity_delACommunity',
                    'inventory_followup_record','inventory_search','inventory_exclusive','inventory_maintainer','inventory_key_holder'
                   ,'bambooplate_reject','bambooplate_show','bambooplate_review','bambooplate_pass') then '房源'
     when action_name in ('acommunity_show','acommunity_followup','acommunity_setnice','acommunity_delACommunity') then 'A类小区'
     when action_name in ('inventory_followup_record','inventory_search') then '房源管理'
     when action_name in ('inventory_exclusive','inventory_maintainer','inventory_key_holder') then '房源详情页'
     when action_name in ('bambooplate_reject','bambooplate_show','bambooplate_review','bambooplate_pass') then '笋盘管理'
     when action_name in ('performance_show','performance_download','report_show','report_download',
'broker_generalize_show','broker_dimission','broker_join_cancel','broker_join','broker_image_upload','broker_reset_password') then '经纪人'
     when action_name in ('performance_show','performance_download') then '绩效考核'
     when action_name in ('report_show','report_download') then '业务报表'
     when action_name in ('broker_generalize_show') then '自营销'
     when action_name in ('customer_inventory_log','customer_log','customer_show','customer_detail') then '客户管理'
     when action_name in ('customer_show') then '客户管理列表'
     when action_name in ('customer_inventory_log','customer_log','customer_detail') then '客户详情单页' end;                
                    
                    
------------每日公司经纪人数--------------------------
drop table if exists dw_temp_angejia.yuan_boss_broker_m;
create table dw_temp_angejia.yuan_boss_broker_m as
select '1' as num  
,'部经理人数' as zb
,count(case when p_dt=${dealDate} then user_id end) as today_1
,count(case when p_dt=date_sub(${dealDate},1) then user_id end) as today_2
,count(case when p_dt=date_sub(${dealDate},2) then user_id end) as today_3
,count(case when p_dt=date_sub(${dealDate},3) then user_id end) as today_4
,count(case when p_dt=date_sub(${dealDate},4) then user_id end) as today_5

,count(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then user_id end)/7 as week_1
,count(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then user_id end)/7 as week_2
,count(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then user_id end)/7 as week_3
,count(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then user_id end)/7 as week_4
from dw_db.dw_broker_summary_basis_info_daily
where p_dt>=date_sub(${dealDate},59)
and broker_duty_status_id='2'
and broker_type_id='3'
union all
select '2' as num  
,'经纪人数' as zb
,count(case when p_dt=${dealDate} then user_id end) as today_1
,count(case when p_dt=date_sub(${dealDate},1) then user_id end) as today_2
,count(case when p_dt=date_sub(${dealDate},2) then user_id end) as today_3
,count(case when p_dt=date_sub(${dealDate},3) then user_id end) as today_4
,count(case when p_dt=date_sub(${dealDate},4) then user_id end) as today_5

,count(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then user_id end)/7 as week_1
,count(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then user_id end)/7 as week_2
,count(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then user_id end)/7 as week_3
,count(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then user_id end)/7 as week_4
from dw_db.dw_broker_summary_basis_info_daily
where p_dt>=date_sub(${dealDate},59)
and broker_duty_status_id='2'
and broker_type_id in ('1','2')
union all
select '3' as num  
,'-直营经纪人数' as zb
,count(case when p_dt=${dealDate} then user_id end) as today_1
,count(case when p_dt=date_sub(${dealDate},1) then user_id end) as today_2
,count(case when p_dt=date_sub(${dealDate},2) then user_id end) as today_3
,count(case when p_dt=date_sub(${dealDate},3) then user_id end) as today_4
,count(case when p_dt=date_sub(${dealDate},4) then user_id end) as today_5

,count(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then user_id end)/7 as week_1
,count(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then user_id end)/7 as week_2
,count(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then user_id end)/7 as week_3
,count(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then user_id end)/7 as week_4
from dw_db.dw_broker_summary_basis_info_daily
where p_dt<=${dealDate}
and p_dt>=date_sub(${dealDate},59)
and broker_duty_status_id='2'
and broker_type_id in ('1')
union all
select '4' as num  
,'-合伙经纪人数' as zb
,count(case when p_dt=${dealDate} then user_id end) as today_1
,count(case when p_dt=date_sub(${dealDate},1) then user_id end) as today_2
,count(case when p_dt=date_sub(${dealDate},2) then user_id end) as today_3
,count(case when p_dt=date_sub(${dealDate},3) then user_id end) as today_4
,count(case when p_dt=date_sub(${dealDate},4) then user_id end) as today_5

,count(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then user_id end)/7 as week_1
,count(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then user_id end)/7 as week_2
,count(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then user_id end)/7 as week_3
,count(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then user_id end)/7 as week_4
from dw_db.dw_broker_summary_basis_info_daily
where p_dt<=${dealDate}
and p_dt>=date_sub(${dealDate},59)
and broker_duty_status_id='2'
and broker_type_id in ('2')
;


----每日活跃用户数/UV/PV-----
drop table if exists dw_temp_angejia.yuan_boss_uv_pv_type;
create table dw_temp_angejia.yuan_boss_uv_pv_type as
select k.num,k.zb
,sum(case when k.p_dt=${dealDate} and k.zb like '%UV' then k.uv 
    when k.p_dt=${dealDate} and k.zb like '%PV' then k.pv end) as today_1
,sum(case when k.p_dt=date_sub(${dealDate},1) and k.zb like '%UV' then k.uv 
    when k.p_dt=date_sub(${dealDate},1) and k.zb like '%PV' then k.pv end) as today_2
,sum(case when k.p_dt=date_sub(${dealDate},2) and k.zb like '%UV' then k.uv 
    when k.p_dt=date_sub(${dealDate},2) and k.zb like '%PV' then k.pv end) as today_3
,sum(case when k.p_dt=date_sub(${dealDate},3) and k.zb like '%UV' then k.uv 
    when k.p_dt=date_sub(${dealDate},3) and k.zb like '%PV' then k.pv end) as today_4
,sum(case when k.p_dt=date_sub(${dealDate},4) and k.zb like '%UV' then k.uv 
    when k.p_dt=date_sub(${dealDate},4) and k.zb like '%PV' then k.pv end) as today_5

,avg(case when k.p_dt<=${dealDate} and k.p_dt>=date_sub(${dealDate},6) and k.zb like '%UV' then k.uv 
    when k.p_dt<=${dealDate} and k.p_dt>=date_sub(${dealDate},6) and k.zb like '%PV' then k.pv end) as week_1
,avg(case when k.p_dt<=date_sub(${dealDate},7) and k.p_dt>=date_sub(${dealDate},13) and k.zb like '%UV' then k.uv 
    when k.p_dt<=date_sub(${dealDate},7) and k.p_dt>=date_sub(${dealDate},13) and k.zb like '%PV' then k.pv end) as week_2
,avg(case when k.p_dt<=date_sub(${dealDate},14) and k.p_dt>=date_sub(${dealDate},20) and k.zb like '%UV' then k.uv 
    when k.p_dt<=date_sub(${dealDate},14) and k.p_dt>=date_sub(${dealDate},20) and k.zb like '%PV' then k.pv end) as week_3
,avg(case when k.p_dt<=date_sub(${dealDate},21) and k.p_dt>=date_sub(${dealDate},27) and k.zb like '%UV' then k.uv 
    when k.p_dt<=date_sub(${dealDate},21) and k.p_dt>=date_sub(${dealDate},27) and k.zb like '%PV' then k.pv end) as week_4
from
(
select '5' as num
,'全站UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='活跃用户数'

union all
select '6' as num
,'全站PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='活跃用户数'

union all
select '7' as num
,'经纪人UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='经纪人'

union all
select '8' as num
,'经纪人PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='经纪人'

union all
select '9' as num
,'-自营销UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='自营销'

union all
select '10' as num
,'-自营销PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='自营销'

union all
select '11' as num
,'-绩效考核UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='绩效考核'

union all
select '12' as num
,'-绩效考核PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='绩效考核'

union all
select '13' as num
,'-业务报表UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='业务报表'

union all
select '14' as num
,'-业务报表PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='业务报表'

union all
select '15' as num
,'房源UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='房源'

union all
select '16' as num
,'房源PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='房源'

union all
select '17' as num
,'-房源管理UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='房源管理'

union all
select '18' as num
,'-房源管理PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='房源管理'

union all
select '19' as num
,'-笋盘管理UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='笋盘管理'

union all
select '20' as num
,'-笋盘管理PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='笋盘管理'

union all
select '23' as num
,'-房源详情页UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='房源详情页'

union all
select '24' as num
,'-房源详情页PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='房源详情页'

union all
select '25' as num
,'客户管理UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='客户管理'

union all
select '26' as num
,'客户管理PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='客户管理'

union all
select '27' as num
,'-客户管理列表UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='客户管理列表'

union all
select '28' as num
,'-客户管理列表PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='客户管理列表'

union all
select '29' as num
,'-客户详情单页UV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='客户详情单页'

union all
select '30' as num
,'-客户详情单页PV' as zb
,uv,pv,p_dt
from dw_temp_angejia.yuan_boss_uv_pv_m
where p_dt>=date_sub(${dealDate},30)
and p_dt<=${dealDate}
and action_desc='客户详情单页'
  ) k
group by k.num,k.zb
;


-----汇总
drop table if exists dw_temp_angejia.yuan_boss_uv_pv_all;
create table dw_temp_angejia.yuan_boss_uv_pv_all as
select a.num,a.zb,a.today_5,a.today_4,a.today_3,a.today_2,a.today_1,
a.today_1/a.today_2-1 as DOD,
a.week_4,a.week_3,a.week_2,a.week_1,a.week_1/a.week_2-1 as WOW
from 
(
  select * from dw_temp_angejia.yuan_boss_broker_m
  union all
  select * from dw_temp_angejia.yuan_boss_uv_pv_type
  ) a
;


export hive dw_temp_angejia.yuan_boss_uv_pv_all
to mysql dw_temp_angejia.yuan_boss_uv_pv_all;