create table ttt (a int, b int, c int);
select optimizer;
explain copy into ttt from '/tmp/xyz';
set optimizer = substring(optimizer,0,length(optimizer)-length('garbageCollector')) || 'sql_append,garbageCollector';
select optimizer;
explain copy into ttt from '/tmp/xyz';
drop table ttt;
