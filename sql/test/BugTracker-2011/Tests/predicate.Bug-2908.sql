select cast(null as varchar(1)) as table_cat, s."name" as table_schem, t."name"
as table_name, case when t."type" = 0 and t."system" = false and t."temporary"
= 0 then cast('TABLE' as varchar(20)) when t."type" = 0 and t."system" = true
and t."temporary" = 0 then cast('SYSTEM TABLE' as varchar(20)) when t."type" =
1 then cast('VIEW' as varchar(20)) when t."type" = 0 and t."system" = false and
t."temporary" = 1 then cast('LOCAL TEMPORARY' as varchar(20)) else
cast('INTERNAL TABLE TYPE' as varchar(20)) end as table_type, cast('' as
varchar(1)) as remarks from sys."schemas" s, sys."tables" t where s."id" =
t."schema_id" and t."name" like 'x%z' and (1 = 0 or (t."type" = 0 and t."system"
= false and t."temporary" = 0)) order by table_type, table_schem, table_name;
