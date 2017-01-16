drop table if exists dw_temp_angejia.jenny_report_broker_20150521;
create table dw_temp_angejia.jenny_report_broker_20150521 as
select '2' as num,'经纪人数' as zb
       ,count(case when p_dt = ${dealDate} then user_id end) as today
       ,count(case when p_dt = date_sub(${dealDate},1) then user_id end) as today_1
       ,count(case when p_dt = date_sub(${dealDate},2) then user_id end) as today_2
       ,count(case when p_dt = date_sub(${dealDate},3) then user_id end) as today_3
       ,count(case when p_dt = date_sub(${dealDate},4) then user_id end) as today_4
       ,count(case when p_dt = date_sub(${dealDate},7) then user_id end) as today_7
       
       ,count(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then user_id end)/7 as week
       ,count(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then user_id end)/7 as week_1
       ,count(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then user_id end)/7 as week_2
       ,count(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then user_id end)/7 as week_3

       ,count(case when p_dt >= date_sub(${dealDate},29) and p_dt <= ${dealDate} then user_id end)/30 as month
       ,count(case when p_dt >= date_sub(${dealDate},59) and p_dt <= date_sub(${dealDate},30) then user_id end)/30 as month_1
      ,count(case when p_dt >= date_sub(${dealDate},89) and p_dt <= date_sub(${dealDate},60) then user_id end)/30 as month_2

from db_gather.angejia__broker
where status = 2
and user_id not in (3,4)
and city_id <> 3 
and p_dt >= date_sub(${dealDate},89)
;


drop table if exists dw_temp_angejia.jenny_report_inventory_20150529;
create table dw_temp_angejia.jenny_report_inventory_20150529 as
select p_dt,count(distinct property_id) as property_num
from db_gather.property__inventory
where status = 2
and city_id <>3
and p_dt >= date_sub(${dealDate},89)
group by p_dt
;

drop table if exists dw_temp_angejia.jenny_report_inventory_20150521;
create table dw_temp_angejia.jenny_report_inventory_20150521 as
select '1' as num,'房源数' as zb
       
       ,sum(case when p_dt = ${dealDate} then property_num end) as today
       ,sum(case when p_dt = date_sub(${dealDate},1) then property_num end) as today_1
       ,sum(case when p_dt = date_sub(${dealDate},2) then property_num end) as today_2
       ,sum(case when p_dt = date_sub(${dealDate},3) then property_num end) as today_3
       ,sum(case when p_dt = date_sub(${dealDate},4) then property_num end) as today_4
       ,sum(case when p_dt = date_sub(${dealDate},7) then property_num end) as today_7

       ,avg(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then property_num end) as week
       ,avg(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then property_num end) as week_1
       ,avg(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then property_num end) as week_2
       ,avg(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then property_num end) as week_3

       ,avg(case when p_dt >= date_sub(${dealDate},29) and p_dt <= ${dealDate} then property_num end) as month
       ,avg(case when p_dt >= date_sub(${dealDate},59) and p_dt <= date_sub(${dealDate},30) then property_num end) as month_1
       ,avg(case when p_dt >= date_sub(${dealDate},89) and p_dt <= date_sub(${dealDate},60) then property_num end) as month_2
  
  
from dw_temp_angejia.jenny_report_inventory_20150529
;

/*uv--pc,gongzonghao,tw*/
drop table if exists dw_temp_angejia.jenny_report_ud_20150528;
create table dw_temp_angejia.jenny_report_ud_20150528 as
select p_dt
       ,count(distinct case when current_full_url like 'http://m.angejia.com%' and brower_type not in ('MicroMessenger') then guid end) as tw_ud
       ,count(distinct case when current_full_url like 'http://m.angejia.com%' and brower_type = 'MicroMessenger' then guid end) as gongzhonghao_ud
       ,count(distinct case when current_full_url like 'http://www.angejia.com%' or current_full_url like 'http://sale.sh.angejia.com/%' then guid end) as pc_ud
from dw_db.dw_web_visit_traffic_log
where server_time >= '2015-04-09 15:00:00'
and p_dt <= ${dealDate}
and p_dt >= date_sub(${dealDate},89)
group by p_dt
;


drop table if exists dw_temp_angejia.jenny_report_ud_pc_20150526;
create table dw_temp_angejia.jenny_report_ud_pc_20150526 as
select '4.1' as num,'-UD_PC' as zb
       ,sum(case when p_dt = ${dealDate} then pc_ud end) as today
       ,sum(case when p_dt = date_sub(${dealDate},1) then pc_ud end) as today_1
       ,sum(case when p_dt = date_sub(${dealDate},2) then pc_ud end) as today_2
       ,sum(case when p_dt = date_sub(${dealDate},3) then pc_ud end) as today_3
       ,sum(case when p_dt = date_sub(${dealDate},4) then pc_ud end) as today_4
       ,sum(case when p_dt = date_sub(${dealDate},7) then pc_ud end) as today_7
       
       ,avg(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then pc_ud end) as week
       ,avg(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then pc_ud end) as week_1
       ,avg(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then pc_ud end) as week_2
       ,avg(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then pc_ud end) as week_3

       ,avg(case when p_dt >= date_sub(${dealDate},29) and p_dt <= ${dealDate} then pc_ud end) as month
       ,avg(case when p_dt >= date_sub(${dealDate},59) and p_dt <= date_sub(${dealDate},30) then pc_ud end) as month_1
      ,avg(case when p_dt >= date_sub(${dealDate},89) and p_dt <= date_sub(${dealDate},60) then pc_ud end) as month_2

from dw_temp_angejia.jenny_report_ud_20150528
;

drop table if exists dw_temp_angejia.jenny_report_ud_wx_20150526;
create table dw_temp_angejia.jenny_report_ud_wx_20150526 as
select '4.3' as num,'-公众号_UV' as zb
       ,sum(case when p_dt = ${dealDate} then gongzhonghao_ud end) as today
       ,sum(case when p_dt = date_sub(${dealDate},1) then gongzhonghao_ud end) as today_1
       ,sum(case when p_dt = date_sub(${dealDate},2) then gongzhonghao_ud end) as today_2
       ,sum(case when p_dt = date_sub(${dealDate},3) then gongzhonghao_ud end) as today_3
       ,sum(case when p_dt = date_sub(${dealDate},4) then gongzhonghao_ud end) as today_4
       ,sum(case when p_dt = date_sub(${dealDate},7) then gongzhonghao_ud end) as today_7
       
       ,avg(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then gongzhonghao_ud end) as week
       ,avg(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then gongzhonghao_ud end) as week_1
       ,avg(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then gongzhonghao_ud end) as week_2
       ,avg(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then gongzhonghao_ud end) as week_3

       ,avg(case when p_dt >= date_sub(${dealDate},29) and p_dt <= ${dealDate} then gongzhonghao_ud end) as month
       ,avg(case when p_dt >= date_sub(${dealDate},59) and p_dt <= date_sub(${dealDate},30) then gongzhonghao_ud end) as month_1
        ,avg(case when p_dt >= date_sub(${dealDate},89) and p_dt <= date_sub(${dealDate},60) then gongzhonghao_ud end) as month_2

from dw_temp_angejia.jenny_report_ud_20150528
;

drop table if exists dw_temp_angejia.jenny_report_ud_tw_20150526;
create table dw_temp_angejia.jenny_report_ud_tw_20150526 as
select '4.2' as num,'-TW_UV' as zb
       ,sum(case when p_dt = ${dealDate} then tw_ud end) as today
       ,sum(case when p_dt = date_sub(${dealDate},1) then tw_ud end) as today_1
       ,sum(case when p_dt = date_sub(${dealDate},2) then tw_ud end) as today_2
       ,sum(case when p_dt = date_sub(${dealDate},3) then tw_ud end) as today_3
       ,sum(case when p_dt = date_sub(${dealDate},4) then tw_ud end) as today_4
       ,sum(case when p_dt = date_sub(${dealDate},7) then tw_ud end) as today_7
       
       ,avg(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then tw_ud end) as week
       ,avg(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then tw_ud end) as week_1
       ,avg(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then tw_ud end) as week_2
       ,avg(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then tw_ud end) as week_3

       ,avg(case when p_dt >= date_sub(${dealDate},29) and p_dt <= ${dealDate} then tw_ud end) as month
       ,avg(case when p_dt >= date_sub(${dealDate},59) and p_dt <= date_sub(${dealDate},30) then tw_ud end) as month_1
      ,avg(case when p_dt >= date_sub(${dealDate},89) and p_dt <= date_sub(${dealDate},60) then tw_ud end) as month_2
 
from dw_temp_angejia.jenny_report_ud_20150528
;

/*uv-app*/
drop table if exists dw_temp_angejia.jenny_report_ud_20150528_app;
create table dw_temp_angejia.jenny_report_ud_20150528_app as
select  p_dt
       ,count(distinct device_id) as app_ud
from dw_db.dw_app_access_log
where app_name in ('a-angejia','i-angejia')
and server_time >= '2015-04-09 15:00:00'
and p_dt<= ${dealDate}
and p_dt >= date_sub(${dealDate},89)
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
group by p_dt
;

drop table if exists dw_temp_angejia.jenny_report_ud_app_20150526;
create table dw_temp_angejia.jenny_report_ud_app_20150526 as
select '4.4' as num,'-APP_UD' as zb
       ,sum(case when p_dt = ${dealDate} then app_ud end) as today
       ,sum(case when p_dt = date_sub(${dealDate},1) then app_ud end) as today_1
       ,sum(case when p_dt = date_sub(${dealDate},2) then app_ud end) as today_2
       ,sum(case when p_dt = date_sub(${dealDate},3) then app_ud end) as today_3
       ,sum(case when p_dt = date_sub(${dealDate},4) then app_ud end) as today_4
       ,sum(case when p_dt = date_sub(${dealDate},7) then app_ud end) as today_7
       
       ,avg(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then app_ud end) as week
       ,avg(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then app_ud end) as week_1
       ,avg(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then app_ud end) as week_2
       ,avg(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then app_ud end) as week_3

       ,avg(case when p_dt >= date_sub(${dealDate},29) and p_dt <= ${dealDate} then app_ud end) as month
       ,avg(case when p_dt >= date_sub(${dealDate},59) and p_dt <= date_sub(${dealDate},30) then app_ud end) as month_1
     ,avg(case when p_dt >= date_sub(${dealDate},89) and p_dt <= date_sub(${dealDate},60) then app_ud end) as month_2

from dw_temp_angejia.jenny_report_ud_20150528_app
;


drop table if exists dw_temp_angejia.jenny_report_user_20150528;
create table dw_temp_angejia.jenny_report_user_20150528 as
select '4' as num,'UD_用户端' as zb
       ,sum(today) as today
       ,sum(today_1) as today_1
       ,sum(today_2) as today_2
       ,sum(today_3) as today_3
       ,sum(today_4) as today_4
       ,sum(today_7) as today_7
  
       ,sum(week) as week
       ,sum(week_1) as week_1
       ,sum(week_2) as week_2
       ,sum(week_3) as week_3

       ,sum(month) as month
       ,sum(month_1) as month_1
      ,sum(month_2) as month_2

from
(

  select *
  from dw_temp_angejia.jenny_report_ud_pc_20150526
  union all
  select *
  from dw_temp_angejia.jenny_report_ud_wx_20150526
  union all
  select *
  from dw_temp_angejia.jenny_report_ud_tw_20150526
  union all
  select *
  from dw_temp_angejia.jenny_report_ud_app_20150526
)t
;


/*经纪人日活*/
drop table if exists dw_temp_angejia.jenny_report_broker_20150528;
create table dw_temp_angejia.jenny_report_broker_20150528 as
select a.server_date as p_dt,count(distinct a.user_id) as broker_ud
from dw_db.dw_app_access_log a
left outer join db_sync.angejia__broker b on a.user_id = b.user_id 
where a.p_dt >= date_sub(${dealDate},89)
and server_date >= date_sub(${dealDate},89)
and a.app_name in ('i-broker','a-broker')
and b.city_id <>3
and a.user_id not in (3,4)
group by a.server_date
;

drop table if exists dw_temp_angejia.jenny_report_ud_broker_20150528;
create table dw_temp_angejia.jenny_report_ud_broker_20150528 as
select '5' as num,'UD_经纪人' as zb
       ,sum(case when p_dt = ${dealDate} then broker_ud end) as today
       ,sum(case when p_dt = date_sub(${dealDate},1) then broker_ud end) as today_1
       ,sum(case when p_dt = date_sub(${dealDate},2) then broker_ud end) as today_2
       ,sum(case when p_dt = date_sub(${dealDate},3) then broker_ud end) as today_3
       ,sum(case when p_dt = date_sub(${dealDate},4) then broker_ud end) as today_4
       ,sum(case when p_dt = date_sub(${dealDate},7) then broker_ud end) as today_7
       
       ,avg(case when p_dt >= date_sub(${dealDate},6) and p_dt <= ${dealDate} then broker_ud end) as week
       ,avg(case when p_dt >= date_sub(${dealDate},13) and p_dt <= date_sub(${dealDate},7) then broker_ud end) as week_1
       ,avg(case when p_dt >= date_sub(${dealDate},20) and p_dt <= date_sub(${dealDate},14) then broker_ud end) as week_2
       ,avg(case when p_dt >= date_sub(${dealDate},27) and p_dt <= date_sub(${dealDate},21) then broker_ud end) as week_3

       ,avg(case when p_dt >= date_sub(${dealDate},29) and p_dt <= ${dealDate} then broker_ud end) as month
       ,avg(case when p_dt >= date_sub(${dealDate},59) and p_dt <= date_sub(${dealDate},30) then broker_ud end) as month_1
      ,avg(case when p_dt >= date_sub(${dealDate},89) and p_dt <= date_sub(${dealDate},60) then broker_ud end) as month_2

from dw_temp_angejia.jenny_report_broker_20150528
;

/*客户委托量（经纪人录需求+客户录需求）*/
drop table if exists dw_temp_angejia.swan_broker_customer_demand_scorecard;
create table dw_temp_angejia.swan_broker_customer_demand_scorecard as
select  '6' as num        
       ,'客户委托量' as zb
       ,count(distinct case when cal_dt = ${dealDate} then buyer_uid end) as today
       ,count(distinct case when cal_dt = date_sub(${dealDate},1)then buyer_uid end) as today_1
       ,count(distinct case when cal_dt = date_sub(${dealDate},2)then buyer_uid end) as today_2
       ,count(distinct case when cal_dt = date_sub(${dealDate},3)then buyer_uid end) as today_3
       ,count(distinct case when cal_dt = date_sub(${dealDate},4)then buyer_uid end) as today_4
       ,count(distinct case when cal_dt = date_sub(${dealDate},7)then buyer_uid end) as today_7
 
       ,count(distinct case when cal_dt>= date_sub(${dealDate},6) and cal_dt<= ${dealDate} then buyer_uid end)/7 as week
       ,count(distinct case when cal_dt>= date_sub(${dealDate},13) and cal_dt<= date_sub(${dealDate},7) then buyer_uid end)/7 as week_1
       ,count(distinct case when cal_dt>= date_sub(${dealDate},20) and cal_dt<= date_sub(${dealDate},14) then buyer_uid end)/7 as week_2
       ,count(distinct case when cal_dt>= date_sub(${dealDate},27) and cal_dt<= date_sub(${dealDate},21) then buyer_uid end)/7 as week_3
       ,count(distinct case when cal_dt>= date_sub(${dealDate},29) and cal_dt<= ${dealDate} then buyer_uid end)/30 as month
       ,count(distinct case when cal_dt>= date_sub(${dealDate},59) and cal_dt<= date_sub(${dealDate},30) then buyer_uid end)/30 as month_1
       ,count(distinct case when cal_dt>= date_sub(${dealDate},89) and cal_dt<= date_sub(${dealDate},60) then buyer_uid end)/30 as month_2
          
from
(select a.buyer_uid as buyer_uid,to_date(a.created_at) as cal_dt 
from db_sync.angejia__demand a
join db_sync.angejia__broker b on a.creator_uid =b.user_id
where a.creator_uid not in (0,3,4)
and b.city_id<>3
union all
select user_id as buyer_uid,to_date(created_at) as cal_dt 
from db_sync.angejia__member_demand
)
t
;

/*带看量*/
drop table if exists dw_temp_angejia.swan_daikan_scorecard;
create table dw_temp_angejia.swan_daikan_scorecard as
select '7' as num
       ,'带看量' as zb
       ,count(case when to_date(a.visit_started_at)= ${dealDate} then a.buyer_uid end) as today
       ,count(case when to_date(a.visit_started_at)=date_sub(${dealDate},1) then a.buyer_uid end) as today_1
       ,count(case when to_date(a.visit_started_at)=date_sub(${dealDate},2) then a.buyer_uid end) as today_2
       ,count(case when to_date(a.visit_started_at)=date_sub(${dealDate},3) then a.buyer_uid end) as today_3
       ,count(case when to_date(a.visit_started_at)=date_sub(${dealDate},4) then a.buyer_uid end) as today_4
       ,count(case when to_date(a.visit_started_at)=date_sub(${dealDate},7) then a.buyer_uid end) as today_7
       
       ,count(case when to_date(a.visit_started_at)>=date_sub(${dealDate},6) and to_date(a.visit_started_at)<=${dealDate} then a.buyer_uid end)/7 as week
       ,count(case when to_date(a.visit_started_at)>= date_sub(${dealDate},13) and to_date(a.visit_started_at)<= date_sub(${dealDate},7) then a.buyer_uid end)/7 as week_1
       ,count(case when to_date(a.visit_started_at)>= date_sub(${dealDate},20) and to_date(a.visit_started_at)<= date_sub(${dealDate},14) then a.buyer_uid end)/7 as week_2
       ,count(case when to_date(a.visit_started_at)>= date_sub(${dealDate},27) and to_date(a.visit_started_at)<= date_sub(${dealDate},21) then a.buyer_uid end)/7 as week_3

       ,count(case when to_date(a.visit_started_at)>= date_sub(${dealDate},29) and to_date(a.visit_started_at)<= ${dealDate} then a.buyer_uid end)/30 as month
       ,count(case when to_date(a.visit_started_at)>= date_sub(${dealDate},59) and to_date(a.visit_started_at)<= date_sub(${dealDate},30) then a.buyer_uid end)/30 as month_1
       ,count(case when to_date(a.visit_started_at)>= date_sub(${dealDate},89) and to_date(a.visit_started_at)<= date_sub(${dealDate},60) then a.buyer_uid end)/30 as month_2
               
from db_sync.angejia__visit a
join db_sync.angejia__broker b on a.broker_uid=b.user_id
where a.is_valid=1              /*有效1，无效0*/
and   a.is_buyer_denied=0       /*买家否认1，买家未否认0*/
and   b.city_id<>3
and b.user_id not in (0,3,4)
;


/*电话量*/
drop table if exists dw_temp_angejia.swan_call_person_scorecard;
create table dw_temp_angejia.swan_call_person_scorecard as
select '3.1' as num
       ,'-安个家来电数' as zb     
       ,count(case when to_date(a.start_at) = ${dealDate} then a.caller end) as today
       ,count(case when to_date(a.start_at) = date_sub(${dealDate},1)then a.caller end) as today_1
       ,count(case when to_date(a.start_at) = date_sub(${dealDate},2)then a.caller end) as today_2
       ,count(case when to_date(a.start_at) = date_sub(${dealDate},3)then a.caller end) as today_3
       ,count(case when to_date(a.start_at) = date_sub(${dealDate},4)then a.caller end) as today_4
       ,count(case when to_date(a.start_at) = date_sub(${dealDate},7)then a.caller end) as today_7

       ,count(case when to_date(a.start_at)>= date_sub(${dealDate},6) and to_date(a.start_at)<= ${dealDate} then a.caller end)/7 as week
       ,count(case when to_date(a.start_at)>= date_sub(${dealDate},13) and to_date(a.start_at)<= date_sub(${dealDate},7) then a.caller end)/7 as week_1
       ,count(case when to_date(a.start_at)>= date_sub(${dealDate},20) and to_date(a.start_at)<= date_sub(${dealDate},14) then a.caller end)/7 as week_2
       ,count(case when to_date(a.start_at)>= date_sub(${dealDate},27) and to_date(a.start_at)<= date_sub(${dealDate},21) then a.caller end)/7 as week_3
       ,count(case when to_date(a.start_at)>= date_sub(${dealDate},29) and to_date(a.start_at)<= ${dealDate} then a.caller end)/30 as month
       ,count(case when to_date(a.start_at)>= date_sub(${dealDate},59) and to_date(a.start_at)<= date_sub(${dealDate},30) then a.caller end)/30 as month_1
       ,count(case when to_date(a.start_at)>= date_sub(${dealDate},89) and to_date(a.start_at)<= date_sub(${dealDate},60) then a.caller end)/30 as month_2
               
From db_sync.angejia__call_log a
join db_sync.angejia__broker b
on a.called_uid=b.user_id
left join db_sync.angejia__call_blacklist c on a.caller=c.phone
where c.phone is null
and a.call_type=2    /*1：经纪人 ->用户；2：用户->经纪人 */
and b.city_id<>3
and b.status=2
and a.orig_channel=0
and a.is_harass=0
and a.called_uid not in (0,3,4)
;


/*微聊人数*/
drop table if exists dw_temp_angejia.swan_wechat_scorecard;
create table dw_temp_angejia.swan_wechat_scorecard as
select '3.2' as num
       ,'-微聊人数' as zb   
       ,count(distinct case when to_date(a.created_at) = ${dealDate} then a.from_uid end) as today
        ,count(distinct case when to_date(a.created_at) = date_sub(${dealDate},1)then a.from_uid end) as today_1
        ,count(distinct case when to_date(a.created_at) = date_sub(${dealDate},2)then a.from_uid end) as today_2
        ,count(distinct case when to_date(a.created_at) = date_sub(${dealDate},3)then a.from_uid end) as today_3
        ,count(distinct case when to_date(a.created_at) = date_sub(${dealDate},4)then a.from_uid end) as today_4
        ,count(distinct case when to_date(a.created_at) = date_sub(${dealDate},7)then a.from_uid end) as today_7
        
       ,count(distinct case when to_date(a.created_at)>= date_sub(${dealDate},6) and to_date(a.created_at)<= ${dealDate} then a.from_uid end)/7 as week
       ,count(distinct case when to_date(a.created_at)>= date_sub(${dealDate},13) and to_date(a.created_at)<= date_sub(${dealDate},7) then a.from_uid end)/7 as week_1
       ,count(distinct case when to_date(a.created_at)>= date_sub(${dealDate},20) and to_date(a.created_at)<= date_sub(${dealDate},14) then a.from_uid end)/7 as week_2
       ,count(distinct case when to_date(a.created_at)>= date_sub(${dealDate},27) and to_date(a.created_at)<= date_sub(${dealDate},21) then a.from_uid end)/7 as week_3
       ,count(distinct case when to_date(a.created_at)>= date_sub(${dealDate},29) and to_date(a.created_at)<= ${dealDate} then a.from_uid end)/30 as month
       ,count(distinct case when to_date(a.created_at)>= date_sub(${dealDate},59) and to_date(a.created_at)<= date_sub(${dealDate},30) then a.from_uid end)/30 as month_1
       ,count(distinct case when to_date(a.created_at)>= date_sub(${dealDate},89) and to_date(a.created_at)<= date_sub(${dealDate},60) then a.from_uid end)/30 as month_2

from db_sync.angejia__user_msg a
join db_sync.angejia__user b on a.to_uid=b.user_id
join db_sync.angejia__broker c        on a.to_uid=c.user_id
where b.user_type=2   /*经纪人:2，用户:1*/
and a.from_uid not in (0,3,4)
and c.city_id<>3
and a.created_at>= '2015-04-09 15:00:00'
and content_type in ('1','2','3')
;

/*联系顾问-立即联系*/
drop table if exists dw_temp_angejia.swan_lianxi_person_scorecard;
create table dw_temp_angejia.swan_lianxi_person_scorecard as
select '3.3' as num
       ,'-联系顾问' as zb  
       ,count(distinct case when to_date(created_at) = ${dealDate} then user_phone end) as today
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},1)then user_phone end) as today_1
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},2)then user_phone end) as today_2
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},3)then user_phone end) as today_3
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},4)then user_phone end) as today_4
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},7)then user_phone end) as today_7
        
       ,count(distinct case when to_date(created_at) >= date_sub(${dealDate},6) and to_date(created_at) <= ${dealDate} then user_phone end)/7 as week
       ,count(distinct case when to_date(created_at) >= date_sub(${dealDate},13) and to_date(created_at) <= date_sub(${dealDate},7) then user_phone end)/7 as week_1
       ,count(distinct case when to_date(created_at) >= date_sub(${dealDate},20) and to_date(created_at) <= date_sub(${dealDate},14) then user_phone end)/7 as week_2
       ,count(distinct case when to_date(created_at) >= date_sub(${dealDate},27) and to_date(created_at) <= date_sub(${dealDate},21) then user_phone end)/7 as week_3
       ,count(distinct case when to_date(created_at) >= date_sub(${dealDate},29) and to_date(created_at) <= ${dealDate} then user_phone end)/30 as month
       ,count(distinct case when to_date(created_at) >= date_sub(${dealDate},59) and to_date(created_at) <= date_sub(${dealDate},30) then user_phone end)/30 as month_1
       ,count(distinct case when to_date(created_at) >= date_sub(${dealDate},89) and to_date(created_at) <= date_sub(${dealDate},60) then user_phone end)/30 as month_2

from db_sync.angejia__page_information_statistics
where to_date(created_at)<=${dealDate}
and to_date(created_at)>=date_sub(${dealDate},89)
and action_type=9
;


/*咨询量*/
drop table if exists dw_temp_angejia.swan_zixun_scorecard;
create table dw_temp_angejia.swan_zixun_scorecard as
select '3.4' as num
       ,'-咨询量' as zb  
       ,count(distinct case when to_date(created_at) = ${dealDate} then user_phone end) as today
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},1)then user_phone end) as today_1
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},2)then user_phone end) as today_2
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},3)then user_phone end) as today_3
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},4)then user_phone end) as today_4
        ,count(distinct case when to_date(created_at) = date_sub(${dealDate},7)then user_phone end) as today_7
        
       ,count(distinct case when to_date(created_at)>= date_sub(${dealDate},6) and to_date(created_at)<= ${dealDate} then user_phone end)/7 as week
       ,count(distinct case when to_date(created_at)>= date_sub(${dealDate},13) and to_date(created_at)<= date_sub(${dealDate},7) then user_phone end)/7 as week_1
       ,count(distinct case when to_date(created_at)>= date_sub(${dealDate},20) and to_date(created_at)<= date_sub(${dealDate},14) then user_phone end)/7 as week_2
       ,count(distinct case when to_date(created_at)>= date_sub(${dealDate},27) and to_date(created_at)<= date_sub(${dealDate},21) then user_phone end)/7 as week_3
       ,count(distinct case when to_date(created_at)>= date_sub(${dealDate},29) and to_date(created_at)<= ${dealDate} then user_phone end)/30 as month
       ,count(distinct case when to_date(created_at)>= date_sub(${dealDate},59) and to_date(created_at)<= date_sub(${dealDate},30) then user_phone end)/30 as month_1
       ,count(distinct case when to_date(created_at)>= date_sub(${dealDate},89) and to_date(created_at)<= date_sub(${dealDate},60) then user_phone end)/30 as month_2
from db_sync.angejia__page_information_statistics 
where action_type<>9
;




/*--------------------------------------------------------------------------连接量-----------------------------------------------------------------------------*/
/*微聊用户id，电话*/
/*drop table if exists dw_temp_angejia.swan_wechat_phone_20150527;
create table dw_temp_angejia.swan_wechat_phone_20150527 as
select  to_date(a.created_at) as cal_dt
       ,d.phone as customer_phone
from db_sync.angejia__user_msg a
left outer join db_sync.angejia__user b on a.from_uid = b.user_id
left outer join db_sync.angejia__broker c  on a.to_uid = c.user_id
left outer join db_sync.angejia__user_phone d on a.from_uid = d.user_id
where b.user_type=1   /*2.经纪人，1用户*/
/*and to_date(a.created_at) <= ${dealDate}
and to_date(a.created_at) > date_sub(${dealDate},89)
and a.created_at>= '2015-04-09 15:00:00'
and c.city_id <> 3
and content_type in ('1','2','3')
and a.to_uid not in (0,3,4)
;*/

/*电话用户电话*/
/*drop table if exists dw_temp_angejia.swan_call_phone_20150527;
create table dw_temp_angejia.swan_call_phone_20150527 as
select to_date(a.start_at) as cal_dt
      ,a.caller as customer_phone
From db_sync.angejia__call_log a
join db_sync.angejia__broker b     on a.called_uid=b.user_id
Where a.call_type=2    /*1：经纪人 ->用户；2：用户->经纪人 */
/*And to_date(a.start_at) <= ${dealDate}
and to_date(a.start_at) > date_sub(${dealDate},89)
and a.called_uid not in (0,3,4)
and b.city_id<>3
;*/

/*连接用户电话*/
/*drop table if exists dw_temp_angejia.swan_lianjie_phone;
create table dw_temp_angejia.swan_lianjie_phone as
select * from
(
select * from dw_temp_angejia.swan_wechat_phone_20150527
union all
select * from dw_temp_angejia.swan_call_phone_20150527
)t
;*/

/*客户需求量*/
drop table if exists dw_temp_angejia.swan_xuqiu_scorecard;
create table dw_temp_angejia.swan_xuqiu_scorecard as
select '3.4' as num
       ,'-客户需求量' as zb    
       ,count( case when to_date(created_at) = ${dealDate} then user_id end) as today
       ,count( case when to_date(created_at) = date_sub(${dealDate},1)then user_id end) as today_1
       ,count( case when to_date(created_at) = date_sub(${dealDate},2)then user_id end) as today_2
       ,count( case when to_date(created_at) = date_sub(${dealDate},3)then user_id end) as today_3
       ,count( case when to_date(created_at) = date_sub(${dealDate},4)then user_id end) as today_4
       ,count( case when to_date(created_at) = date_sub(${dealDate},7)then user_id end) as today_7
        
       ,count( case when to_date(created_at)>= date_sub(${dealDate},6) and to_date(created_at)<= ${dealDate} then user_id end)/7 as week
       ,count( case when to_date(created_at)>= date_sub(${dealDate},13) and to_date(created_at)<= date_sub(${dealDate},7) then user_id end)/7 as week_1
       ,count( case when to_date(created_at)>= date_sub(${dealDate},20) and to_date(created_at)<= date_sub(${dealDate},14) then user_id end)/7 as week_2
       ,count( case when to_date(created_at)>= date_sub(${dealDate},27) and to_date(created_at)<= date_sub(${dealDate},21) then user_id end)/7 as week_3
       ,count( case when to_date(created_at)>= date_sub(${dealDate},29) and to_date(created_at)<= ${dealDate} then user_id end)/30 as month
       ,count( case when to_date(created_at)>= date_sub(${dealDate},59) and to_date(created_at)<= date_sub(${dealDate},30) then user_id end)/30 as month_1
       ,count( case when to_date(created_at)>= date_sub(${dealDate},89) and to_date(created_at)<= date_sub(${dealDate},60) then user_id end)/30 as month_2
from db_sync.angejia__member_demand 
where to_date(created_at) >= date_sub(${dealDate},89)
and created_at >= '2015-04-09 15:00:00'
;


/*连接人数*/
drop table if exists dw_temp_angejia.swan_lianjie_scorecard;
create table dw_temp_angejia.swan_lianjie_scorecard as
select '3' as num
       ,'连接数' as zb     
       ,sum(today) as today
       ,sum(today_1) as today_1
       ,sum(today_2) as today_2
       ,sum(today_3) as today_3
       ,sum(today_4) as today_4
       ,sum(today_7) as today_7
       ,sum(week) as week
       ,sum(week_1) as week_1
       ,sum(week_2) as week_2
       ,sum(week_3) as week_3
       ,sum(month) as month
       ,sum(month_1) as month_1
       ,sum(month_2) as month_2
from
(
  select *
  from dw_temp_angejia.swan_call_person_scorecard
  union all
  select *
  from dw_temp_angejia.swan_wechat_scorecard
  union all
  select *
  from dw_temp_angejia.swan_yuyue_person_scorecard
  union all
  select *
  from dw_temp_angejia.swan_xuqiu_scorecard
  union all
  select *
  from dw_temp_angejia.swan_zixun_scorecard
)t
;

/*客户委托量*/
drop table if exists dw_temp_angejia.swan_broker_customer_demand_scorecard;
create table dw_temp_angejia.swan_broker_customer_demand_scorecard as
select  '6' as num        
       ,'客户委托量' as zb
       ,count(distinct case when cal_dt = ${dealDate} then buyer_uid end) as today
       ,count(distinct case when cal_dt = date_sub(${dealDate},1)then buyer_uid end) as today_1
       ,count(distinct case when cal_dt = date_sub(${dealDate},2)then buyer_uid end) as today_2
       ,count(distinct case when cal_dt = date_sub(${dealDate},3)then buyer_uid end) as today_3
       ,count(distinct case when cal_dt = date_sub(${dealDate},4)then buyer_uid end) as today_4
       ,count(distinct case when cal_dt = date_sub(${dealDate},7)then buyer_uid end) as today_7
 
       ,count(distinct case when cal_dt>= date_sub(${dealDate},6) and cal_dt<= ${dealDate} then buyer_uid end)/7 as week
       ,count(distinct case when cal_dt>= date_sub(${dealDate},13) and cal_dt<= date_sub(${dealDate},7) then buyer_uid end)/7 as week_1
       ,count(distinct case when cal_dt>= date_sub(${dealDate},20) and cal_dt<= date_sub(${dealDate},14) then buyer_uid end)/7 as week_2
       ,count(distinct case when cal_dt>= date_sub(${dealDate},27) and cal_dt<= date_sub(${dealDate},21) then buyer_uid end)/7 as week_3
       ,count(distinct case when cal_dt>= date_sub(${dealDate},29) and cal_dt<= ${dealDate} then buyer_uid end)/30 as month
       ,count(distinct case when cal_dt>= date_sub(${dealDate},59) and cal_dt<= date_sub(${dealDate},30) then buyer_uid end)/30 as month_1
       ,count(distinct case when cal_dt>= date_sub(${dealDate},89) and cal_dt<= date_sub(${dealDate},60) then buyer_uid end)/30 as month_2
          
from
(select a.buyer_uid as buyer_uid,to_date(a.created_at) as cal_dt 
from db_sync.angejia__demand a
join db_sync.angejia__broker b on a.creator_uid =b.user_id
where a.creator_uid not in (0,3,4)
and b.city_id<>3
union all
select user_id as buyer_uid,to_date(created_at) as cal_dt 
from db_sync.angejia__member_demand
)
t
;

/*日期*/
/*
drop table if exists dw_temp_angejia.jenny_date_20150602;
create table dw_temp_angejia.jenny_date_20150602 as
select '0' as num
       ,'指标' as zb
       ,cal_dt as today
       ,date_sub(cal_dt,1) as today_1
       ,date_sub(cal_dt,2) as today_2
       ,date_sub(cal_dt,3) as today_3
       ,date_sub(cal_dt,4) as today_4
       ,date_sub(cal_dt,7) as today_7
       ,concat('W',week_of_year_id) as week
       ,concat('W',week_of_year_id-1) as week_1
       ,concat('W',week_of_year_id-2) as week_2
       ,concat('W',week_of_year_id-3) as week_3
       ,concat('M',month_of_year_id) as month
       ,concat('M',month_of_year_id-1) as month_1
       ,concat('M',month_of_year_id-2) as month_2
from dw_db.dw_cal_dt
where cal_dt =${dealDate};  
*/

drop table if exists dw_temp_angejia.jenny_report_20150528;
create table dw_temp_angejia.jenny_report_20150528 as
select num
       ,zb
       ,today
       ,today_1
       ,today_2
       ,today_3
       ,today_4
       ,today_7
       ,round(week,0) as week
       ,round(week_1,0) as week_1
       ,round(week_2,0) as week_2
       ,round(week_3,0) as week_3
       ,round(month,0) as month
       ,round(month_1,0) as month_1
       ,round(month_2,0) as month_2
     
from
(
  select * 
  from dw_temp_angejia.jenny_report_ud_broker_20150528
  union all
  select *
  from dw_temp_angejia.jenny_report_ud_pc_20150526
  union all
  select *
  from dw_temp_angejia.jenny_report_ud_wx_20150526
  union all
  select *
  from dw_temp_angejia.jenny_report_ud_tw_20150526
  union all
  select *
  from dw_temp_angejia.jenny_report_ud_app_20150526
  union all
  select *
  from dw_temp_angejia.jenny_report_user_20150528
  union all
  select *
  from dw_temp_angejia.jenny_report_inventory_20150521
  union all
  select *
  from dw_temp_angejia.jenny_report_broker_20150521
  union all
  select *
from dw_temp_angejia.swan_daikan_scorecard
union all
select *
from dw_temp_angejia.swan_xuqiu_scorecard
union all
select *
from dw_temp_angejia.swan_call_person_scorecard
union all
select *
from dw_temp_angejia.swan_wechat_scorecard
union all
select *
from dw_temp_angejia.swan_lianxi_person_scorecard
union all
select *
from dw_temp_angejia.swan_zixun_scorecard
union all
select *
from dw_temp_angejia.swan_lianjie_scorecard
union all
select *
from dw_temp_angejia.swan_broker_customer_demand_scorecard

)t
;

export hive dw_temp_angejia.jenny_report_20150528
to mysql dw_temp_angejia.jenny_report_20150528;