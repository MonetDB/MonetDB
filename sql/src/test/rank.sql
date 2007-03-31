select RANK() over () as foo from tables;
select RANK() over (PARTITION BY schema_id) as foo, schema_id from tables;
select RANK() over (PARTITION BY schema_id ORDER BY schema_id) as foo, schema_id from tables;
select RANK() over (ORDER BY schema_id) as foo, schema_id from tables;
