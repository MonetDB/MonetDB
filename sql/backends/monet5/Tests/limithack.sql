-- how to deal with limit clause limitation in MonetDB

-- select * from tables where id in (select id from tables limit 1);
create function limited()
returns table (id integer)
begin
	return select id from tables order by id desc limit 1;
end;
select * from tables where id in (select id from limited());

