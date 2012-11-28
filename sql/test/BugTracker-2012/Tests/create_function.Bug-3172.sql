create function x(a int, b int) 
  returns table (c int,d int)
  external name sql.x;

select * from x((select id from _tables), (select schema_id from _tables));
