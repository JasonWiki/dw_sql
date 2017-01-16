
-- 基础表 5724525
db_sync.angejia__msg_ext


-- 老数据 5600753
DROP TABLE dw_temp_angejia.jason_load_base;
CREATE TABLE dw_temp_angejia.jason_load_base AS
SELECT *
FROM db_sync.angejia__msg_ext
WHERE msg_id <= 5600831
;


-- 抽取到需要更新的数据 123776
DROP TABLE dw_temp_angejia.jason_load_update;
CREATE TABLE dw_temp_angejia.jason_load_update AS
-- 新增数据
SELECT *
FROM db_sync.angejia__msg_ext
WHERE msg_id > 5600831

-- 更新数据
UNION ALL
SELECT *
FROM db_sync.angejia__msg_ext
WHERE msg_id IN (5600800,5600801,5600802,5600803)
;








--- 处理逻辑 Start ---

-- 合并最终数据 5724525
DROP TABLE dw_temp_angejia.jason_load_result;
CREATE TABLE dw_temp_angejia.jason_load_result AS
SELECT
 *
FROM (
  -- 没有被影响的原始数据
  SELECT a.*
  FROM dw_temp_angejia.jason_load_base AS a
  LEFT JOIN dw_temp_angejia.jason_load_update AS b
    ON a.msg_id = b.msg_id
  WHERE b.msg_id IS NULL
) AS bs

UNION ALL
SELECT * FROM dw_temp_angejia.jason_load_update AS b
;


--- 处理逻辑 End ---



-- 测试验证: 插入覆盖数据表
DROP TABLE dw_temp_angejia.jason_load_base_load;
CREATE TABLE dw_temp_angejia.jason_load_base_load AS
SELECT * FROM dw_temp_angejia.jason_load_base;

INSERT OVERWRITE TABLE dw_temp_angejia.jason_load_base_load
SELECT
 *
FROM (
  -- 没有被影响的原始数据
  SELECT a.*
  FROM dw_temp_angejia.jason_load_base_load AS a
  LEFT JOIN dw_temp_angejia.jason_load_update AS b
    ON a.msg_id = b.msg_id
  WHERE b.msg_id IS NULL
) AS bs

UNION ALL
SELECT * FROM dw_temp_angejia.jason_load_update AS b

;
