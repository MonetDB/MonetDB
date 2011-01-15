create table systemfunctions (function_id)
	as (select id from functions) with data;
grant select on systemfunctions to public;
update _tables
	set system = true
	where name = 'systemfunctions'
		and schema_id = (select id from schemas where name = 'sys');
