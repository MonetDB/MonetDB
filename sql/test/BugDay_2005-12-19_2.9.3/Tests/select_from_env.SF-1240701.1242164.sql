select * from env() as env where name = ( select 'prefix' from env() as env );
select name from env() as env where 1 in ( select 1 from env() as env ) order by name;
