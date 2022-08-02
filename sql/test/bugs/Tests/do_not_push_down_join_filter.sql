START TRANSACTION;

create table b (id int);
insert into b (id) values(1);


select a.* 
from (select 1 as id) a
inner join (select id from b where false) c
on a.id = c.id;

ROLLBACK;
