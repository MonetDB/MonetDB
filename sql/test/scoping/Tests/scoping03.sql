create table fun(a int, b varchar(32));
create table fun2(c int, d varchar(32));

create or replace trigger mytrigger
	after insert on fun referencing new row as "ins"
	for each statement insert into fun2 values( a, b ); --possible "a" and "b" are from new row

create or replace trigger mytrigger2
	after update on fun referencing new row as "old"
	for each statement insert into fun2 values( 1, 'value' ); --error, new row and old row have same name

create or replace trigger mytrigger2
	after insert on fun referencing old row as "ups"
	for each statement insert into fun2 values( 1, 'value' ); --error, old row not allowed on insert events

create or replace trigger mytrigger2
	after update on fun referencing new row as "ins" old row as "ins"
	for each statement insert into fun2 values( 1, 'value' ); --error, new row and old row have same name

create or replace trigger mytrigger2
	after update on fun referencing new row as "ins" old row as "del"
	for each statement insert into fun2 values( a, 'value' ); --error, identifier "a" is ambiguous, it could be either for old row or new row

create or replace trigger mytrigger2
	after delete on fun referencing old row as "fun3"
	for each statement update fun2 fun3 set c = (select a from fun3); --error, identifier "a" is ambiguous, it could be either for old row or table to update

create or replace trigger mytrigger2
	after delete on fun referencing old row as "del"
	delete from fun2 where fun2.c = a; --possible, "a" refers to the old row

insert into fun values (1, 'a');
select c, d from fun2;
	-- 1 a
delete from fun;
	-- 1 row deleted
select c, d from fun2;
	--empty

create or replace trigger mytrigger3
	after insert on fun referencing new row as "fun2"
	for each statement insert into fun2 values( 1, 'values' ); --ok, no name conflict

truncate fun;
insert into fun;
select a, b from fun;
	-- NULL NULL

select c, d from fun2;
	-- NULL NULL
	-- 1 values

drop table fun cascade;
drop table fun2;
