--生态圈app整体漏斗
drop table if exists dw_db_temp.dw_fangyuan360_app_ud;
create table dw_db_temp.dw_fangyuan360_app_ud as
select
platform,
selection_city_id as city_id,
GROUPING__ID,
count(distinct device_id) as ud
from dw_db.dw_app_access_log
where p_dt=${dealDate}
and app_name ='FangYuan360'
and request_uri not like '/mobile/member/configs%'
and request_uri not like '/mobile/member/districts/show%'
and request_uri not like '/mobile/member/inventories/searchFilters%'
and request_uri not like '%/user/bind/push%'
and request_uri not like '%/common/push/acks%'
and hostname='api.fangyuan360.cn'
group by platform,selection_city_id with cube
;

drop table if exists dw_db_temp.dw_fangyuan360_app_stats;
create table dw_db_temp.dw_fangyuan360_app_stats as
select
case when a.model like 'Android%' then 'Android' else 'IOS' end as platform,
b.selection_city_id as city_id,
GROUPING__ID,
--房源360APP总体漏斗
count(distinct a.dvid) as log_ud,---登录ud
count(distinct case when a.action_id in ('101-100001') then a.dvid end) AS vpud, --房源单页—页面可见（即页面打开）
count(case when a.action_id in ('101-100001') then a.dvid end) AS vppv, --房源单页—页面可见（即页面打开）
count(distinct case when a.action_id in ('3-400011') then a.dvid end) as signin_ud,
count(distinct case when a.action_id in ('101-100017','3-100000-19') then a.dvid end) as success_call_ud,--拨打电话成功3-100000-19为1.0.6版本专用
count(case when a.action_id in ('101-100017','3-100000-19') then a.dvid end) as success_call_num,--拨打电话成功
count(distinct case when a.action_id in ('3-100000-43') then a.dvid end) as get_phone_ud,--获得房东电话ud
count(case when a.action_id in ('3-100000-43') then a.dvid end) as get_phone_num,--获得房东电话量
count(distinct case when a.action_id in ('101-300001','3-300000-19','101-320003') then a.dvid end) as fafang_ud,--发房单页—页面可见（即页面打开）3-300000-19为1.0.6版本专用
count(case when a.action_id in ('101-300001','3-300000-19','101-320003') then a.dvid end) as fafang_num,--发房单页—页面可见（即页面打开）
count(distinct case when a.action_id in ('101-320001') then a.dvid end) as fafang_success_ud,---发房成功ud
count(case when a.action_id in ('101-320001') then a.dvid end) as fafang_success_num,---发房成功量
--房源360APP发房漏斗
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-300018') then a.dvid end) as bigbutton_ud,--点击方法大按钮ud
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-300018') then a.dvid end) as bigbutton_num,---点击方法大按钮量
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-19','101-320003') then a.dvid end) as page1_ud,--发房单页—房源基本信息1—打开
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-19','101-320003') then a.dvid end) as page1_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-30') then a.dvid end) as nextstep1_ud,--发房单页—房源基本信息1—点击“下一步”
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-30') then a.dvid end) as nextstep1_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-31') then a.dvid end) as page2_ud,--发房单页—房源基本信息2—打开
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-31') then a.dvid end) as page2_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-40') then a.dvid end) as nextstep2_ud,--发房单页—房源基本信息2—点击“下一步”
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-40') then a.dvid end) as nextstep2_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-41') then a.dvid end) as page3_ud,--发房单页—房东信息—打开
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-41') then a.dvid end) as page3_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-49') then a.dvid end) as publish_ud,--发房单页—房东信息—点击“发布”
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-49') then a.dvid end) as publish_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-320001') then a.dvid end) as publish_success_ud,--发房单页—页面可见（即页面打开）
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-320001') then a.dvid end) as publish_success_num,
--房源360APP看房漏斗
count(distinct case when a.action_id in ('101-200001') then a.dvid end) as kanfang_ud,--首页—页面可见（即页面打开）
count(case when a.action_id in ('101-200001') then a.dvid end) as kanfang_num,--首页—页面可见（即页面打开）次数
count(distinct case when a.action_id in ('101-400001') then a.dvid end) as kf_inv_list_ud,--全部房源列表页—页面可见（即页面打开）ud
count(case when a.action_id in ('101-400001') then a.dvid end) as kf_inv_list_num,--全部房源列表页—页面可见（即页面打开）次数
count(distinct case when a.action_id in ('101-100004') then a.dvid end) as kf_contact_fd_ud,--房源单页—点击联系房东按钮ud
count(case when a.action_id in ('101-100004') then a.dvid end) as kf_contact_fd_num,--房源单页—点击联系房东按钮次数
count(distinct case when a.action_id in ('101-100006') then a.dvid end) as kf_call_ud,--房源单页—点击确认花费积分ud
count(case when a.action_id in ('101-100006') then a.dvid end) as kf_call_num,--房源单页点击关闭确认花费积
count(distinct case when a.action_id in ('101-100011','3-100000-20','3-100000-21') then a.dvid end) as kf_onsale_ud,--房源单页—点击“在卖”ud
count(case when a.action_id in ('101-100011','3-100000-20','3-100000-21') then a.dvid end) as kf_onsale_num,--房源单页—点击“在卖”
count(distinct case when a.action_id in ('101-100013','3-100000-25','3-100000-26','3-100000-27') then a.dvid end) as fake_ud,--房源单页—点击“虚假/不卖/已卖”ud
count(case when a.action_id in ('101-100013','3-100000-25','3-100000-26','3-100000-27') then a.dvid end) as fake_num,--房源单页—点击“虚假/不卖/已卖”
count(distinct case when a.action_id in ('101-100012','3-100000-24') then a.dvid end) as nocontact_ud,--房源单页—点击“联系不上”ud
count(case when a.action_id in ('101-100012','3-100000-24') then a.dvid end) as nocontact_num,--房源单页—点击“联系不上”
count(distinct case when a.action_id in ('3-100000-45') then a.dvid end) as rec_ud,--点击听录音ud
count(case when a.action_id in ('3-100000-45') then a.dvid end) as rec_pv,--点击听录音pv
count(distinct case when a.action_id='3-100000-47' then a.dvid end) as open_rec_ud,--录音弹出ud
count(case when a.action_id='3-100000-47' then a.dvid end) as open_rec_pv,--录音弹出pv
count(distinct case when a.action_id='3-100000-51' then a.dvid end) as cost_ud,--房源单页—点击［4积分看房东电话］确认按钮ud
count(case when a.action_id='3-100000-51' then a.dvid end) as cost_pv--房源单页—点击［4积分看房东电话］确认按钮pv
from dw_db.dw_app_action_detail_log a
left join (
  select user_id,
  collect_set(selection_city_id)[0] as selection_city_id
  from dw_db.dw_app_access_log
  where p_dt=${dealDate}
  and app_name ='FangYuan360'
  group by user_id
) b
on a.uid=b.user_id
where a.p_dt=${dealDate}
and a.name='fy360'
group by case when a.model like 'Android%' then 'Android' else 'IOS' end,
b.selection_city_id with cube
;

--新用户流程dvid(新)
drop table if exists dw_db_temp.dw_fangyuan360_new_dvid;
create table dw_db_temp.dw_fangyuan360_new_dvid as
select dvid,
min(p_dt) as first_come
from (
  select distinct dvid,p_dt from dw_db.dw_app_action_detail_log
  where p_dt<=${dealDate} and p_dt>=date_sub(${dealDate},90)
  and name='fy360'
) a
group by dvid
;

--新用户
drop table if exists dw_db_temp.dw_fangyuan360_app_new_stats;
create table dw_db_temp.dw_fangyuan360_app_new_stats as
select case when a.model like 'Android%' then 'Android' else 'IOS' end as platform,
c.selection_city_id as city_id,
GROUPING__ID,
--房源360APP总体漏斗
count(distinct a.dvid) AS fud,
count(distinct case when a.action_id in ('101-500001') then a.dvid end) AS f_open_ud, --页面可见（即页面打开)
count(distinct case when a.action_id in ('101-500002') then a.dvid end) AS f_mobile_ud, --点击手机号输入框
count(distinct case when a.action_id in ('101-500004') then a.dvid end) as f_code_ud,--点击验证码输入框
count(distinct case when a.action_id in ('101-500005') then a.dvid end) as f_log_ud,--登录注册页—点击登录按钮
count(distinct case when a.action_id in ('101-110002') then a.dvid end) as f_setblock_ud,--设置我的关注版块页—选择板块区域
count(distinct case when a.action_id in ('101-110004') then a.dvid end) as f_confirmblock_ud,--设置我的关注版块页—点击确认
count(distinct case when a.action_id in ('101-140001') then a.dvid end) as f_inv_ud,--全部房源列表页—页面可见（即页面打开）
count(distinct case when a.action_id in ('101-100001') then a.dvid end) AS f_vpud, --房源单页—页面可见（即页面打开）
count(case when a.action_id in ('101-100001') then a.dvid end) AS f_vppv, --房源单页—页面可见（即页面打开）
count(distinct case when a.action_id in ('101-100017','3-100000-19') then a.dvid end) as f_success_call_ud,--拨打电话成功3-100000-19为1.0.6版本专用
count(case when a.action_id in ('101-100017','3-100000-19') then a.dvid end) as f_success_call_num,--拨打电话成功
count(distinct case when a.action_id in ('3-100000-43') then a.dvid end) as f_get_phone_ud,--获得房东电话ud
count(case when a.action_id in ('3-100000-43') then a.dvid end) as f_get_phone_num,--获得房东电话量
count(distinct case when a.action_id in ('101-300001','3-300000-19','101-320003') then a.dvid end) as f_fafang_ud,--发房单页—页面可见（即页面打开）3-300000-19为1.0.6版本专用
count(case when a.action_id in ('101-300001','3-300000-19','101-320003') then a.dvid end) as f_fafang_num,--发房单页—页面可见（即页面打开）
count(distinct case when a.action_id in ('101-320001') then a.dvid end) as f_fafang_success_ud,---发房成功ud
count(case when a.action_id in ('101-320001') then a.dvid end) as f_fafang_success_num,---发房成功量
--房源360APP发房漏斗
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-300018') then a.dvid end) as f_bigbutton_ud,--点击方法大按钮ud
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-300018') then a.dvid end) as f_bigbutton_num,---点击方法大按钮量
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-19','101-320003') then a.dvid end) as f_page1_ud,--发房单页—房源基本信息1—打开
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-19','101-320003') then a.dvid end) as f_page1_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-30') then a.dvid end) as f_nextstep1_ud,--发房单页—房源基本信息1—点击“下一步”
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-30') then a.dvid end) as f_nextstep1_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-31') then a.dvid end) as f_page2_ud,--发房单页—房源基本信息2—打开
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-31') then a.dvid end) as f_page2_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-40') then a.dvid end) as f_nextstep2_ud,--发房单页—房源基本信息2—点击“下一步”
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-40') then a.dvid end) as f_nextstep2_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-41') then a.dvid end) as f_page3_ud,--发房单页—房东信息—打开
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-41') then a.dvid end) as f_page3_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-49') then a.dvid end) as f_publish_ud,--发房单页—房东信息—点击“发布”
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('3-300000-49') then a.dvid end) as f_publish_num,
count(distinct case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-320001') then a.dvid end) as f_publish_success_ud,--发房单页—页面可见（即页面打开）
count(case when a.version not in ('1.0.3','1.0.4','1.0.5') and a.action_id in ('101-320001') then a.dvid end) as f_publish_success_num,
--房源360APP看房漏斗
count(distinct case when a.action_id in ('101-200001') then a.dvid end) as f_kanfang_ud,--首页—页面可见（即页面打开）
count(case when a.action_id in ('101-200001') then a.dvid end) as f_kanfang_num,--首页—页面可见（即页面打开）次数
count(distinct case when a.action_id in ('101-400001') then a.dvid end) as f_kf_inv_list_ud,--全部房源列表页—页面可见（即页面打开）ud
count(case when a.action_id in ('101-400001') then a.dvid end) as f_kf_inv_list_num,--全部房源列表页—页面可见（即页面打开）次数
count(distinct case when a.action_id in ('101-100004') then a.dvid end) as f_kf_contact_fd_ud,--房源单页—点击联系房东按钮ud
count(case when a.action_id in ('101-100004') then a.dvid end) as f_kf_contact_fd_num,--房源单页—点击联系房东按钮次数
count(distinct case when a.action_id in ('101-100006') then a.dvid end) as f_kf_call_ud,--房源单页—点击确认花费积分ud
count(case when a.action_id in ('101-100006') then a.dvid end) as f_kf_call_num,--房源单页点击关闭确认花费积
count(distinct case when a.action_id in ('101-100011','3-100000-20','3-100000-21') then a.dvid end) as f_kf_onsale_ud,--房源单页—点击“在卖”ud
count(case when a.action_id in ('101-100011','3-100000-20','3-100000-21') then a.dvid end) as f_kf_onsale_num,--房源单页—点击“在卖”
count(distinct case when a.action_id in ('101-100013','3-100000-25','3-100000-26','3-100000-27') then a.dvid end) as f_fake_ud,--房源单页—点击“虚假/不卖/已卖”ud
count(case when a.action_id in ('101-100013','3-100000-25','3-100000-26','3-100000-27') then a.dvid end) as f_fake_num,--房源单页—点击“虚假/不卖/已卖”
count(distinct case when a.action_id in ('101-100012','3-100000-24') then a.dvid end) as f_nocontact_ud,--房源单页—点击“联系不上”ud
count(case when a.action_id in ('101-100012','3-100000-24') then a.dvid end) as f_nocontact_num,--房源单页—点击“联系不上”
count(distinct case when a.action_id in ('3-100000-45') then a.dvid end) as f_rec_ud,--点击听录音ud
count(case when a.action_id in ('3-100000-45') then a.dvid end) as f_rec_pv,--点击听录音pv
count(distinct case when a.action_id='3-100000-47' then a.dvid end) as f_open_rec_ud,--录音弹出ud
count(case when a.action_id='3-100000-47' then a.dvid end) as f_open_rec_pv,--录音弹出pv
count(distinct case when a.action_id='3-100000-51' then a.dvid end) as f_cost_ud,--房源单页—点击［4积分看房东电话］确认按钮ud
count(case when a.action_id='3-100000-51' then a.dvid end) as f_cost_pv--房源单页—点击［4积分看房东电话］确认按钮pv
from dw_db.dw_app_action_detail_log a
inner join dw_db_temp.dw_fangyuan360_new_dvid b
on a.p_dt=b.first_come and a.dvid=b.dvid
left join (
  select user_id,
  collect_set(selection_city_id)[0] as selection_city_id
  from dw_db.dw_app_access_log
  where p_dt=${dealDate}
  and app_name ='FangYuan360'
  group by user_id
) c
on a.uid=c.user_id
where a.p_dt=${dealDate}
and a.name='fy360'
group by case when a.model like 'Android%' then 'Android' else 'IOS' end,
c.selection_city_id with cube
;


--生态圈月累计积分,日积分
drop table if exists dw_db_temp.dw_fangyuan360_money_stats;
create table dw_db_temp.dw_fangyuan360_money_stats as
select
nvl(a.mtd_inventory,0)+nvl(a.mtd_house_looked_by_others,0) as mtd_fafang_add,
nvl(a.mtd_inventory,0) as mtd_inventory_add,---发房
nvl(a.mtd_house_looked_by_others,0) as mtd_house_looked_by_others_add,---别人看房返还积分
nvl(a.mtd_unlock_phone,0)-nvl(a.mtd_refund_unlock_phone,0) as mtd_bureau_phone_reduce,--看房
nvl(a.mtd_recharge,0) as mtd_recharge_add, --充值新增
nvl(a.mtd_withdrawals,0)-nvl(a.mtd_refund_withdrawals,0) as mtd_withdrawals,
nvl(a.td_inventory,0)+nvl(a.td_house_looked_by_others,0) as td_fafang_add,
nvl(a.td_inventory,0) as td_inventory_add,---发房
nvl(a.td_house_looked_by_others,0) as td_house_looked_by_others_add,---别人看房返还积分
nvl(a.td_unlock_phone,0)-nvl(a.td_refund_unlock_phone,0) as td_bureau_phone_reduce,--看房
nvl(a.td_recharge,0) as td_recharge_add, --充值新增
nvl(a.td_withdrawals,0)-nvl(a.td_refund_withdrawals,0) as td_withdrawals,
nvl(a.td_unlock_phone,0) as td_unlock_phone,
nvl(a.td_refund_unlock_phone,0) as td_refund_unlock_phone,
nvl(a.mtd_unlock_phone,0) as mtd_unlock_phone,
nvl(a.mtd_refund_unlock_phone,0) as mtd_refund_unlock_phone
from
(
  select sum(case when object_type='recharge' then `money` end) as mtd_recharge,---充值
  sum(case when object_type='house_looked_by_others' then `money` end) as mtd_house_looked_by_others,---别人看房返还积分
  sum(case when object_type='inventory' then `money` end) as mtd_inventory,---发房
  sum(case when object_type='withdrawals' then `money` end) as mtd_withdrawals,---提现
  sum(case when object_type='refund_withdrawals' then `money` end) as mtd_refund_withdrawals,---提现失败返还积分
  sum(case when object_type='unlock_phone' then `money` end) as mtd_unlock_phone,--联系房东消耗
  sum(case when object_type='refund_unlock_phone' then `money` end) as mtd_refund_unlock_phone,---联系房东返还
  sum(case when object_type='recharge' and to_date(created_at)=${dealDate} then `money` end) as td_recharge,
  sum(case when object_type='house_looked_by_others' and to_date(created_at)=${dealDate} then `money` end) as td_house_looked_by_others,
  sum(case when object_type='inventory' and to_date(created_at)=${dealDate} then `money` end) as td_inventory,
  sum(case when object_type='withdrawals' and to_date(created_at)=${dealDate} then `money` end) as td_withdrawals,
  sum(case when object_type='refund_withdrawals' and to_date(created_at)=${dealDate} then `money` end) as td_refund_withdrawals,
  sum(case when object_type='unlock_phone' and to_date(created_at)=${dealDate} then `money` end) as td_unlock_phone,
  sum(case when object_type='refund_unlock_phone' and to_date(created_at)=${dealDate} then `money` end) as td_refund_unlock_phone
  from db_sync.angejia__bureau_user_money_flow
  where to_date(created_at)>=date_add(last_day(add_months(${dealDate},-1)),1)
  and to_date(created_at)<=${dealDate}
  and is_active=1
  and user_id<>8
) a;

--生态圈房源量监控
drop table if exists dw_db_temp.dw_fangyuan360_inventory_stats;
create table dw_db_temp.dw_fangyuan360_inventory_stats as
select sum(online_inv_cnt) as online_inv_cnt,
sum(online_verify_inv_cnt) as online_verify_inv_cnt,
sum(td_add_cnt) as td_add_cnt,
sum(td_weixin_add_cnt) as td_weixin_add_cnt,
sum(td_app_add_cnt) as td_app_add_cnt,
sum(td_angejia_add) as td_angejia_add,
sum(td_checked_cnt) as td_checked_cnt,
sum(td_checked_onsale_cnt) as td_checked_onsale_cnt
from (
  select count(distinct case when status in ('2','9','0') then id end) as online_inv_cnt,
  count(distinct case when status in ('2','9','0') and is_verified='1' and has_voice=1 then id end) as online_verify_inv_cnt,
  count(distinct case when source=1 and to_date(created_at)=p_dt then id end) as td_add_cnt,
  count(distinct case when source=1 and to_date(created_at)=p_dt and platform=2 then id end) as td_weixin_add_cnt,
  count(distinct case when source=1 and to_date(created_at)=p_dt and platform=1 then id end) as td_app_add_cnt,
  count(distinct case when source=4 and to_date(created_at)=p_dt then id end) as td_angejia_add,
  0 as td_checked_cnt,
  0 as td_checked_onsale_cnt
  from dw_db.dw_property_bureau_inventory
  where p_dt=${dealDate}
  union all
  select 0 as online_inv_cnt,
  0 as online_verify_inv_cnt,
  0 as td_add_cnt,
  0 as td_weixin_add_cnt,
  0 as td_app_add_cnt,
  0 as td_angejia_add,
  count(distinct property_id) as td_checked_cnt,
  count(distinct case when result='在卖' then property_id end) as td_checked_onsale_cnt
  from db_sync.angejia__bureau_property_check_log
  where to_date(created_at)=${dealDate}
  and result<>'暂时跳过') t
;


--板块 城市cross join
drop table if exists dw_db_temp.dw_city_platform_cross;
create table dw_db_temp.dw_city_platform_cross as
select a.platform,
b.id as city_id,
b.name as city_name
from
(
  select 'All' as platform
  union all
  select 'Android' as platform
  union all
  select 'IOS' as platform
) a,
(
  select 'All' as id,'全国' as name
  union all
  select id,name from dw_db.dim_city
  where is_active=1
) b
;


--最终结果集
insert overwrite table dm_db.dm_fangyuan360_kpi_sd partition (p_dt = ${dealDate})
select
case when cp.city_id='All' then 0 else cp.city_id end as city_id,
cp.city_name,
cp.platform,
nvl(ud,0) as ud,--ud
nvl(log_ud,0) as log_ud,--登录ud
nvl(vpud,0) as vpud,--vpud
nvl(vppv,0) as vppv,--vppv
nvl(signin_ud,0) as signin_ud,--签到ud
nvl(success_call_ud,0) as success_call_ud,--联系房东成功ud
nvl(success_call_num,0) as success_call_num,--联系房东成功量
nvl(get_phone_ud,0) as get_phone_ud,--获取房东号码ud
nvl(get_phone_num,0) as get_phone_num,--获取房东号码量
nvl(fafang_ud,0) as fafang_ud,--发房首页ud
nvl(fafang_num,0) as fafang_num,--发房首页pv
nvl(fafang_success_ud,0) as fafang_success_ud,--发房成功ud
nvl(fafang_success_num,0) as fafang_success_num,--发房成功量
nvl(fud,0) as fud,--fud
nvl(f_open_ud,0) as f_open_ud,--新用户登录页面打开ud
nvl(f_mobile_ud,0) as f_mobile_ud,--新用户输入手机号ud
nvl(f_code_ud,0) as f_code_ud,--新用户输入验证码ud
nvl(f_log_ud,0) as f_log_ud,--新用户点击登录ud
nvl(f_setblock_ud,0) as f_setblock_ud,--新用户设置板块ud
nvl(f_confirmblock_ud,0) as f_confirmblock_ud,--新用户板块确认ud
nvl(f_inv_ud,0) as f_inv_ud,--新用户全部房源列表页ud
nvl(f_vpud,0) as f_vpud,--新用户vpud
nvl(f_vppv,0) as f_vppv,--新用户vppv
nvl(f_success_call_ud,0) as f_success_call_ud,--新用户联系房东成功ud
nvl(f_success_call_num,0) as f_success_call_num,--新用户联系房东成功量
nvl(f_get_phone_ud,0) as f_get_phone_ud,--新用户获取房东号码ud
nvl(f_get_phone_num,0) as f_get_phone_num,--新用户获取房东号码量
nvl(f_fafang_ud,0) as f_fafang_ud,--新用户发房首页ud
nvl(f_fafang_num,0) as f_fafang_num,--新用户发房首页pv
nvl(f_fafang_success_ud,0) as f_fafang_success_ud,--新用户发房成功ud
nvl(f_fafang_success_num,0) as f_fafang_success_num,--新用户发房成功量
--发房漏斗1.06版本以上
nvl(bigbutton_ud,0) as bigbutton_ud,--新用户发房大按钮点击ud
nvl(bigbutton_num,0) as bigbutton_num,--新用户发房大按钮点击量
nvl(page1_ud,0) as page1_ud,--新用户发房页面打开ud
nvl(page1_num,0) as page1_num,--新用户发房页面打开量
nvl(nextstep1_ud,0) as nextstep1_ud,--新用户发房第1页点击下一页ud
nvl(nextstep1_num,0) as nextstep1_num,--新用户发房第1页点击下一页量
nvl(page2_ud,0) as page2_ud,--新用户发房页面第2页打开ud
nvl(page2_num,0) as page2_num,--新用户发房页面第2页打开量
nvl(nextstep2_ud,0) as nextstep2_ud,--新用户发房第2页点击下一页ud
nvl(nextstep2_num,0) as nextstep2_num,--新用户发房第2页点击下一页量
nvl(page3_ud,0) as page3_ud,--新用户发房页面第3页打开ud
nvl(page3_num,0) as page3_num,--新用户发房页面第3页打开量
nvl(publish_ud,0) as publish_ud,--新用户点击发房ud
nvl(publish_num,0) as publish_num,--新用户点击发房量
nvl(publish_success_ud,0) as publish_success_ud,--新用户发房成功ud
nvl(publish_success_num,0) as publish_success_num,--新用户发房成功量
nvl(f_bigbutton_ud,0) as f_bigbutton_ud,--新用户发房大按钮点击ud
nvl(f_bigbutton_num,0) as f_bigbutton_num,--新用户发房大按钮点击量
nvl(f_page1_ud,0) as f_page1_ud,--新用户发房页面打开ud
nvl(f_page1_num,0) as f_page1_num,--新用户发房页面打开量
nvl(f_nextstep1_ud,0) as f_nextstep1_ud,--新用户发房第1页点击下一页ud
nvl(f_nextstep1_num,0) as f_nextstep1_num,--新用户发房第1页点击下一页量
nvl(f_page2_ud,0) as f_page2_ud,--新用户发房页面第2页打开ud
nvl(f_page2_num,0) as f_page2_num,--新用户发房页面第2页打开量
nvl(f_nextstep2_ud,0) as f_nextstep2_ud,--新用户发房第2页点击下一页ud
nvl(f_nextstep2_num,0) as f_nextstep2_num,--新用户发房第2页点击下一页量
nvl(f_page3_ud,0) as f_page3_ud,--新用户发房页面第3页打开ud
nvl(f_page3_num,0) as f_page3_num,--新用户发房页面第3页打开量
nvl(f_publish_ud,0) as f_publish_ud,--新用户点击发房ud
nvl(f_publish_num,0) as f_publish_num,--新用户点击发房量
nvl(f_publish_success_ud,0) as f_publish_success_ud,--新用户发房成功ud
nvl(f_publish_success_num,0) as f_publish_success_num,--新用户发房成功量
--看房漏斗
nvl(kanfang_ud,0) as kanfang_ud,--看房首页ud
nvl(kanfang_num,0) as kanfang_num,--看房首页浏览量
nvl(kf_inv_list_ud,0) as kf_inv_list_ud,--全部房源列表页ud
nvl(kf_inv_list_num,0) as kf_inv_list_num,--全部房源列表页浏览量
nvl(kf_contact_fd_ud,0) as kf_contact_fd_ud,--点击联系房东按钮ud
nvl(kf_contact_fd_num,0) as kf_contact_fd_num,--点击联系房东按钮量
nvl(kf_call_ud,0) as kf_call_ud,--点击确认花费积分ud
nvl(kf_call_num,0) as kf_call_num,--点击确认花费积分量
nvl(kf_onsale_ud,0) as kf_onsale_ud,--反馈在卖ud
nvl(kf_onsale_num,0) as kf_onsale_num,--反馈在卖量
nvl(fake_ud,0) as fake_ud,--反馈虚假/不卖/已卖ud
nvl(fake_num,0) as fake_num,--反馈虚假/不卖/已卖量
nvl(nocontact_ud,0) as nocontact_ud,--反馈无法联系ud
nvl(nocontact_num,0) as nocontact_num,--反馈无法联系量
nvl(rec_ud,0) as rec_ud,--点击听录音ud
nvl(rec_pv,0) as rec_pv,--点击听录音量
nvl(open_rec_ud,0) as open_rec_ud,--录音打开ud
nvl(open_rec_pv,0) as open_rec_pv,--录音打开量
nvl(cost_ud,0) as cost_ud,--确认4积分看房东电话ud
nvl(cost_pv,0) as cost_pv,--确认4积分看房东电话量
nvl(f_kanfang_ud,0) as f_kanfang_ud,--新用户看房首页ud
nvl(f_kanfang_num,0) as f_kanfang_num,--新用户看房首页浏览量
nvl(f_kf_inv_list_ud,0) as f_kf_inv_list_ud,--新用户全部房源列表页ud
nvl(f_kf_inv_list_num,0) as f_kf_inv_list_num,--新用户全部房源列表页浏览量
nvl(f_kf_contact_fd_ud,0) as f_kf_contact_fd_ud,--新用户点击联系房东按钮ud
nvl(f_kf_contact_fd_num,0) as f_kf_contact_fd_num,--新用户点击联系房东按钮量
nvl(f_kf_call_ud,0) as f_kf_call_ud,--新用户点击确认花费积分ud
nvl(f_kf_call_num,0) as f_kf_call_num,--新用户点击确认花费积分量
nvl(f_kf_onsale_ud,0) as f_kf_onsale_ud,--新用户反馈在卖ud
nvl(f_kf_onsale_num,0) as f_kf_onsale_num,--新用户反馈在卖量
nvl(f_fake_ud,0) as f_fake_ud,--新用户反馈虚假/不卖/已卖ud
nvl(f_fake_num,0) as f_fake_num,--新用户反馈虚假/不卖/已卖量
nvl(f_nocontact_ud,0) as f_nocontact_ud,--新用户反馈无法联系ud
nvl(f_nocontact_num,0) as f_nocontact_num,--新用户反馈无法联系量
nvl(f_rec_ud,0) as f_rec_ud,--新用户点击听录音ud
nvl(f_rec_pv,0) as f_rec_pv,--新用户点击听录音量
nvl(f_open_rec_ud,0) as f_open_rec_ud,--新用户录音打开ud
nvl(f_open_rec_pv,0) as f_open_rec_pv,--新用户录音打开量
nvl(f_cost_ud,0) as f_cost_ud,--新用户确认4积分看房东电话ud
nvl(f_cost_pv,0) as f_cost_pv,--新用户确认4积分看房东电话量
--生态圈月累计积分,日积分
nvl(mtd_fafang_add,0) as mtd_fafang_add,--月累计发房积分
nvl(mtd_inventory_add,0) as mtd_inventory_add,--月累计发房补贴积分
nvl(mtd_house_looked_by_others_add,0) as mtd_house_looked_by_others_add,--月累计别人看房补贴积分
nvl(mtd_bureau_phone_reduce,0) as ,--月累计看房积分
nvl(mtd_recharge_add,0) as mtd_recharge_add,--月累计充值积分
nvl(mtd_withdrawals,0) as mtd_withdrawals,--月累计提现积分
nvl(td_fafang_add,0) as td_fafang_add,--当日发房积分
nvl(td_inventory_add,0) as td_inventory_add,--当日发房补贴积分
nvl(td_house_looked_by_others_add,0) as td_house_looked_by_others_add,--当日别人看房补贴积分
nvl(td_bureau_phone_reduce,0) as td_bureau_phone_reduce,--当日看房积分
nvl(td_recharge_add,0) as td_recharge_add,--当日充值积分
nvl(td_withdrawals,0) as td_withdrawals,--当日提现积分
--生态圈房源量监控
nvl(online_inv_cnt,0) as online_inv_cnt,--在线房源量
nvl(online_verify_inv_cnt,0) as online_verify_inv_cnt,--在线认证房源量
nvl(td_add_cnt,0) as td_add_cnt,--当日新增用户发房量
nvl(td_weixin_add_cnt,0) as td_weixin_add_cnt,--当日微信端新增用户发房量
nvl(td_app_add_cnt,0) as td_app_add_cnt,--当日APP端新增用户发房量
nvl(td_angejia_add,0) as td_angejia_add,--当日安个家导入房源量
nvl(td_checked_cnt,0) as td_checked_cnt,--当日客服审核房源量
nvl(td_checked_onsale_cnt,0) as td_checked_onsale_cnt,--当日审核在卖房源量
nvl(td_unlock_phone,0) as td_unlock_phone,
nvl(td_refund_unlock_phone,0) as td_refund_unlock_phone,
nvl(mtd_unlock_phone,0) as mtd_unlock_phone,
nvl(mtd_refund_unlock_phone,0) as mtd_refund_unlock_phone
from dw_db_temp.dw_city_platform_cross cp
left join (
  select case when GROUPING__ID in (0,2) then 'All' else platform end as platform,
  case when GROUPING__ID in (0,1) then 'All' else city_id end as city_id,
  ud
  from dw_db_temp.dw_fangyuan360_app_ud
) ud
on cp.city_id=ud.city_id and cp.platform=ud.platform
left join (
  select case when GROUPING__ID in (0,2) then 'All' else platform end as platform_new,
  case when GROUPING__ID in (0,1) then 'All' else city_id end as city_id_new,
  *
from dw_db_temp.dw_fangyuan360_app_stats
) app_stats
on cp.city_id=app_stats.city_id_new and cp.platform=app_stats.platform_new
left join (
  select case when GROUPING__ID in (0,2) then 'All' else platform end as platform_new,
  case when GROUPING__ID in (0,1) then 'All' else city_id end as city_id_new,
  *
  from dw_db_temp.dw_fangyuan360_app_new_stats
) new_stats
on cp.city_id=new_stats.city_id_new and cp.platform=new_stats.platform_new
left join (
  select 'All' as platform,
  'All' as city_id,
  *
  from dw_db_temp.dw_fangyuan360_money_stats
) money_stats
on cp.city_id=money_stats.city_id and cp.platform=money_stats.platform
left join (
  select 'All' as platform,
  'All' as city_id,
  *
  from dw_db_temp.dw_fangyuan360_inventory_stats
) inv_stats
on cp.city_id=inv_stats.city_id and cp.platform=inv_stats.platform
;
