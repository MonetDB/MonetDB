create table tmp(i int);
insert into tmp values(1);
insert into tmp values(null);
select * from tmp where i in (1);
select * from tmp where i in (2,1);
select i in (NULL,1) from tmp;
select i in (1,'a') from tmp;
