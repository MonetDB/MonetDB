select * from tables, (select count(*) from tables where "type" = 1) as t2 where "type" = 1;
select * from (select count(*) from tables where "type" = 1) as t2, tables where "type" = 1;
