select x from (select count(*) as x from _tables where "istable" = true) as t;
select x from (select name as x from _tables where "istable" = true) as t;
