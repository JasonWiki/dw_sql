drop table da_db.da_article;
create table da_db.da_article (
  article_id string comment '主键 id',
  inventory_id string comment '房源 id',
  article_pv String comment '页面 PV',
  article_wechat String comment '微聊',
  vppv_index String comment 'vppv系数',
  p_dt string
);
