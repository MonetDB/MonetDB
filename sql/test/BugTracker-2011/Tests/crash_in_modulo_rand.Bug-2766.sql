create table test2766 (v int);
insert into test2766 values (23), (34), (12), (54);
update test2766 set v = mod(rand(), 32);
drop table test2766;
