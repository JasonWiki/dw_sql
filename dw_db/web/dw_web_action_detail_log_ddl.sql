-- 创建 web action 表
CREATE TABLE IF NOT EXISTS dw_db.dw_web_action_detail_log (
  user_id      string,
  ccid         string,
  referer_full_url   string,
  referer_page_id    string,
  referer_page    string,
  referer_page_name  string,
  current_full_url   string,
  current_page    string,
  current_page_id    string,
  current_page_name  string,
  guid         string,
  client_time     string,
  page_param      string,
  action_id       string,
  action_name     string,
  action_cname    string,
  client_param    string,
  server_time     string,
  ip           string,
  os_type      string,
  os_version      string,
  brower_type     string,
  brower_version     string,
  phone_type      string,
  referer_host  String,
  referer_query String,
  referer_ref STRING,
  current_host STRING,
  current_query STRING,
  current_ref STRING,
  current_host_city_id STRING
) partitioned by (p_dt string);