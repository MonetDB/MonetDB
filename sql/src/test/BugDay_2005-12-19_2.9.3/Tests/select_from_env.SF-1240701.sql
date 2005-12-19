select * from env where name = ( select 'prefix' from env );
