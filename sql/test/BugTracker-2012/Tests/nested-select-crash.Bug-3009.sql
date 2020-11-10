select 1 as y, (select x from (select 1) as test where x = y);
