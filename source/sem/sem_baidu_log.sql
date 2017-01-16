--- etl 脚本会拉去昨天的数据，并且放到对应的分区目录下，所以使用前要先创建一个新的分区(每天执行一次即可) START ---
ALTER TABLE sem_log.sem_baidu_log ADD PARTITION (p_dt = ${dealDate});
--- etl END ---
