select x from (select count(*) as x from tables where "type" = 1) as t;
select x from (select name as x from tables where "type" = 1) as t;
