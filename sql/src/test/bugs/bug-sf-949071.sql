select * from tables, (select count(*) from tables) as t2;
select * from (select count(*) from tables) as t2, tables;
