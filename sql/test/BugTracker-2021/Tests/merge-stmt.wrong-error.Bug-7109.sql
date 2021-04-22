
start transaction;

create table ce (id bigint);
insert into ce values (1), (2);
create table attr (id bigint, value text);
insert into attr values (1,'a'),(1,'b'),(2,'a'),(2,'a'),(3,'a'),(3,'b');

merge into attr using ce on ce.id = attr.id when matched then delete;

select * from attr;
	-- 3 a
	-- 3 b
rollback;
