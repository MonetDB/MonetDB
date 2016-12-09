-- use sequential optimizer pipeline to ensure deterministic output also on multi-core systems
set optimizer='sequential_pipe';
create table tab_2826 (d double);
insert into tab_2826 values (1.0),(2.0),(3.0),(4.0),(5.0);
create function func_2826(f real) returns real begin return log10(f); end;
--explain select * from tab_2826 where func_2826(d) > 1;
select * from tab_2826 where func_2826(d) > 1;   
drop function func_2826;
drop table tab_2826;
