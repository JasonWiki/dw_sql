-- 依赖 Jar 包
add jar hdfs://Ucluster/user/jars/calculate_pageinfo.jar;
add jar hdfs://Ucluster/user/jars/parse_user_agent.jar;

-- UDF 
create temporary function parse_user_agent as 'com.angejia.hive.udf.useragent.ParseUserAgent';
create temporary function get_page_info as 'com.angejia.hive.udf.pageinfo.CalculatePageInfo';


-- 用户访问实时表
CREATE TABLE IF NOT EXISTS real_time.rt_web_visit_traffic_log (
  user_id   string,
  selection_city_id   string,
  client_time   string,
  user_based_city_id   string,
  referer_full_url   string,
  referer_page   string,
  referer_page_id   string,
  referer_page_name   string,
  current_full_url   string,
  current_page   string,
  current_page_id   string,
  current_page_name   string,
  channel_code   string,
  page_param   string,
  client_param   string,
  guid   string,
  client_ip   string,
  os_type   string,
  os_version   string,
  brower_type   string,
  brower_version   string,
  phone_type   string,
  server_time   string
);

INSERT OVERWRITE TABLE
    real_time.rt_web_visit_traffic_log
select
    if(length(a.uid)>0,uid,0) AS user_id,
    a.ccid as selection_city_id,
    a.client_time as client_time,
    '' as user_based_city_id,
    if(length(a.referer)>0,referer,'') as referer_full_url,
    coalesce(parse_url(a.referer,'PATH'),'') as referer_page,
    get_page_info(a.referer,'page_id') as referer_page_id,
    get_page_info(a.referer,'page_name') as referer_page_name,
    if(length(a.url)>0,url,'') as current_full_url,
    coalesce(parse_url(a.url,'PATH'),'') as current_page,
    get_page_info(a.url,'page_id') as current_page_id,
    get_page_info(a.url,'page_name') as current_page_name,
    get_page_info(a.url,'platform_id') as channel_code,
    a.page_param as page_param,
    a.client_param as client_param,
    a.guid as guid,
    a.ip as client_ip,
    parse_user_agent(a.agent,0) as os_type,
    parse_user_agent(a.agent,1) as os_version,
    parse_user_agent(a.agent,2) as brower_type,
    parse_user_agent(a.agent,3) as brower_version,
    parse_user_agent(a.agent,4) as phone_type,
    a.server_time as server_time
from
    real_time.uba_web_visit_log a
left outer join dw_db.dw_basis_dimension_filter_ip b
  on a.ip = b.client_ip
where b.client_ip is null
  and a.ip not like '61.135.190.%'
  and parse_user_agent(a.agent,2) != 'Robot/Spider'
  and a.agent not like '%spider%'
  and a.agent not like '%-broker%'
  and a.ip not like '10.%';