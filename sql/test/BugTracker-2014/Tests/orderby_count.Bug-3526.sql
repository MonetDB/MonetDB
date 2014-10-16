start transaction;
create table a (k varchar(64),v boolean);
insert into a values ('one',true),('two',false),('one',false);
create table b (k varchar(64));
insert into b values ('two'),('two'),('two');
select * from a;
select * from b;
select k, v from a union all select k,NULL from b;
select k,count(*),count(v) from (select k,v from a union all select k,null from b) as t(k,v) group by k order by count(*) desc;
