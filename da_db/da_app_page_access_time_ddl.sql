CREATE TABLE IF NOT EXISTS da_db.da_app_page_access_time (
  name string,
  version string,
  model string,
  os string,
  dvid string,
  net string,
  ccid string,
  server_time string,
  view_id string,
  start_time string,
  finish_time string,
  spend string,
  api_list string
)
PARTITIONED BY (p_dt String)
stored AS ORC;
