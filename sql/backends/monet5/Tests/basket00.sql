create stream table s_tmp(i integer);
insert into s_tmp values(1),(2);
select * from s_tmp;
drop table s_tmp;
