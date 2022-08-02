start transaction;
create table foo (bar integer primary key);
create table baz (
	rof integer references foo (bar),
	car integer references foo (bar));
insert
	into foo (bar)
	select 1;
select 'the next query causes a "sql_mem.mx:50: sql_ref_dec: Assertion `r->refcnt > 0'' failed."';
delete from foo;
drop table baz;
drop table foo;
commit;
