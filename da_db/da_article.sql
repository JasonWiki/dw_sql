INSERT OVERWRITE TABLE da_db.da_article
SELECT
  a.article_id,
  a.inventory_id,
  nvl(a.pc_pv,0) + nvl(a.tw_pv,0) + nvl(a.app_user_ios_pv,0) + nvl(a.app_user_android_pv,0) AS article_pv,
  a.article_wechat,
  (nvl(a.pc_pv,0) + nvl(a.tw_pv,0) + nvl(a.app_user_ios_pv,0) + nvl(a.app_user_android_pv,0))/b.max_vppv as vppv_index,
  a.p_dt
FROM dw_db.dw_article_sd a
,(select max(nvl(pc_pv,0) + nvl(tw_pv,0) + nvl(app_user_ios_pv,0) + nvl(app_user_android_pv,0)) as max_vppv
  from dw_db.dw_article_sd where p_dt = ${dealDate}) b
WHERE a.p_dt = ${dealDate}
  AND (
    a.pc_pv <> 0
    OR a.tw_pv <> 0
    OR a.app_user_ios_pv <> 0
    OR a.app_user_android_pv <> 0
    OR a.article_wechat <> 0
  )
;
