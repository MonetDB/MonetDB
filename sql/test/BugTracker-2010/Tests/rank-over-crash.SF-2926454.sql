-- preparation
create table score_table (
s_name varchar(20),
subject varchar(10),
score int
);

insert into score_table values('foo','english',70);
insert into score_table values('foo','history',50);
insert into score_table values('foo','math',60);
insert into score_table values('bar','english',45);
insert into score_table values('bar','history',75);

-- this causes an assertion failure:
select
s_name,
sum(score) as totalscore,
rank() over (order by sum(score) desc) -- cannot use alias 'totalscore' here
from score_table
group by s_name;

drop table score_table;
