start transaction;
create table foo (id int);
insert into foo values (42);
select id-row_number() over (order by id) from (select id from foo union all select id from foo) as z;
