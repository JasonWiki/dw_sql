-- 获取 Mysql 最大的表信息
SELECT
  `table_schema`,
  `table_name`,
  `table_rows`,
  `data_length`,
  `data_length` / 1024 / 1024 AS size,
FROM
  information_schema.tables
GROUP BY
 `table_schema`,`table_name`
ORDER BY
 `data_length` DESC
;

-- 查询表基本信息
SELECT
  `table_schema`,
  `table_name`,
  `table_rows`,
  `data_length`,
  `data_length` / 1024 / 1024 AS size,
FROM
  information_schema.tables
WHERE
 `table_schema`,
AND
  `table_name`
;
