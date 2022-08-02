select 1 in ('1', '2', '3');
select 1 in ((select 1 union select 2));
select 1 in (select * from (select 1 union select 2) as a);
