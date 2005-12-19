select * from env where name = ( select 'prefix' from env );
select name from env where 1 in ( select 1 from env );
