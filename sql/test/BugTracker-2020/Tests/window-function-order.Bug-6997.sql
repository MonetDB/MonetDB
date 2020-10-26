start transaction;

create table "ordertest"(str text, orderA int, orderB int, num int);
insert into "ordertest" values('a',1,1,1), ('a',2,1,10), ('a',3,1,20), ('a',4,1,30);

select *,
sum("num") over(order by orderA) as orderbyA,
sum("num") over(order by orderA,orderB) as orderbyAB,
sum("num") over(order by orderB,orderA) as orderbyBA,
sum("num") over(order by orderB) as orderbyB
from "ordertest";

rollback;
