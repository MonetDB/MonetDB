select name from tables, (select count(*) from tables where "istable" = true) as t2 where "istable" = true and "system" = true;
select name from (select count(*) from tables where "istable" = true) as t2, tables where "istable" = true and "system" = true;
