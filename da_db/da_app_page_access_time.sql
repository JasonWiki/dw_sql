INSERT OVERWRITE TABLE da_db.da_app_page_access_time PARTITION(p_dt = ${dealDate})
SELECT
  name,
  version,
  model,
  os,
  dvid,
  net,
  ccid,
  server_time,
  get_json_object(extend,'$.viewId') AS view_id,
  get_json_object(extend,'$.startTime') AS start_time,
  get_json_object(extend,'$.finishTime') AS finish_time,
  get_json_object(extend,'$.spend') AS spend,
  get_json_object(extend,'$.api') AS api_list
FROM dw_db.dw_app_action_detail_log
WHERE p_dt = ${dealDate}
  AND action_id = '2-999000'
;
