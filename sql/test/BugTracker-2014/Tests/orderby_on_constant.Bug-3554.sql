select a+b from (select 1 as a,1 as b) as q order by a limit 1;
select a+b from (select 1 as a,1 as b union select 1,1) as q order by a limit
1;

