start transaction;
create table fun(a int, b varchar(32));
create table fun2(c int, d varchar(32));

create or replace trigger mytrigger2
	after delete on fun referencing old row as "del"
	insert into fun2 values ("del"."a", "del"."b");

insert into fun values (1, 'a');
delete from fun;
select c, d from fun2;
	-- 1 'a'

rollback;
