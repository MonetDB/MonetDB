select x from (select count(*) as x from ptables where "istable" = true) as t;
select x from (select name as x from ptables where "istable" = true) as t;
