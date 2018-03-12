CREATE VIEW sys.commented_function_signatures_6542 AS
SELECT f.id AS fid,
       s.name AS schema,
       f.name AS fname,
       f.type AS category,
       sf.function_id IS NOT NULL AS system,
       CASE RANK() OVER (PARTITION BY f.id ORDER BY p.number ASC) WHEN 1 THEN f.name ELSE NULL END AS name,
       CASE RANK() OVER (PARTITION BY f.id ORDER BY p.number DESC) WHEN 1 THEN c.remark ELSE NULL END AS remark,
       p.type, p.type_digits, p.type_scale,
       ROW_NUMBER() OVER (ORDER BY f.id, p.number) AS line
  FROM sys.functions f
  JOIN sys.comments c ON f.id = c.id
  JOIN sys.schemas s ON f.schema_id = s.id
  LEFT OUTER JOIN sys.systemfunctions sf ON f.id = sf.function_id
  LEFT OUTER JOIN sys.args p ON f.id = p.func_id AND p.inout = 1
 ORDER BY line;

select * from sys.commented_function_signatures_6542;
plan select count (*) from sys.commented_function_signatures_6542;
-- explain select count (*) from sys.commented_function_signatures;
select count (*) from sys.commented_function_signatures_6542;

DROP VIEW sys.commented_function_signatures_6542;

