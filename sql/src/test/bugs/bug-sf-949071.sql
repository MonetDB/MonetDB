select * from tables, (select count(*) from tables where "istable" = true) as t2 where "istable" = true;
select * from (select count(*) from tables where "istable" = true) as t2, tables where "istable" = true;
