select CASE WHEN 1=1 THEN 1 ELSE NULL END;
select CASE WHEN 1=0 THEN 1 ELSE NULL END;
select CASE WHEN 2=2 THEN 2 else cast(null as char) end;
select CASE WHEN 2=0 THEN 2 else cast(null as char) end;
select CASE WHEN 3=3 THEN 3 else cast(null as int) end;
select CASE WHEN 3=0 THEN 3 else cast(null as int) end;
