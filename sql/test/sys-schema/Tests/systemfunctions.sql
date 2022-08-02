select s.name as "schema", f.name as "function", a.number as "argno", a.type as "argtype", case a.number when 0 then f.func else '' end as "definition"
  from sys.functions f, sys.schemas s, sys.args a
 where s.id = f.schema_id
   and f.id = a.func_id
   and f.system
 order by s.name, f.name, f.id, a.number;
