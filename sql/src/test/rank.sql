select ROW_NUMBER() over () as foo from tables;
select ROW_NUMBER() over (PARTITION BY schema_id) as foo, schema_id from tables;
select ROW_NUMBER() over (PARTITION BY schema_id ORDER BY schema_id) as foo, schema_id from tables;
select ROW_NUMBER() over (ORDER BY schema_id) as foo, schema_id from tables;

select RANK() over () as foo from tables;
select RANK() over (PARTITION BY schema_id) as foo, schema_id from tables;
select RANK() over (PARTITION BY schema_id ORDER BY schema_id) as foo, schema_id from tables;
select RANK() over (ORDER BY schema_id) as foo, schema_id from tables;

select RANK() over () as foo, name, "type" from columns;
select RANK() over (PARTITION BY name) as foo, name, "type" from columns;
select RANK() over (PARTITION BY name ORDER BY name, "type") as foo, name, "type" from columns;
select RANK() over (ORDER BY name, "type") as foo, name, "type" from columns;

select DENSE_RANK() over () as foo, name, "type" from columns;
select DENSE_RANK() over (PARTITION BY name) as foo, name, "type" from columns;
select DENSE_RANK() over (PARTITION BY name ORDER BY name, "type") as foo, name, "type" from columns;
select DENSE_RANK() over (ORDER BY name, "type") as foo, name, "type" from columns;
