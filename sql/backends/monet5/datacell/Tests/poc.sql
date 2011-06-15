-- A straw-man's datacell proof of concept implementation
-- This example relies on the standard SQL facilities
-- to demonstrate the DataCell functionality.
-- The specific runtime issues are handled by an optimizer

create schema datacell;
set optimizer='datacell_pipe';

create table datacell.sys_in(
    id integer,
    tag timestamp,
    payload integer
);

-- to be used by continous queries
-- could be generated from the table definition.
create function datacell.sys_in()
returns table (id integer, tag timestamp, payload integer)
begin
	return select * from datacell.sys_in;
end;

select * from datacell.sys_in();

call datacell.basket('datacell','sys_in');
call datacell.receptor('datacell','sys_in','localhost',50500,'passive');
call datacell.start('datacell','sys_in');

create table datacell.sysOut( etag timestamp, like datacell.sys_in);

-- the continues query is registered and started
create procedure datacell.query_sys_in_sysOut()
begin
	insert into datacell.sysOut select now(), *  from datacell.sys_in() ;
end;
call register_query('datacell.query_sys_in_sysOut');
call start_query('datacell.query_sys_in_sysOut');

create function datacell.sysOut()
returns table (id integer)
begin
	return select * from datacell.sysOut;
end;

-- sent it over the wire.
create function datacell.emitter_sysOut()
returns boolean;
begin
	select * from datacell.sysOut();
	return true;
end;
call datacell.basket('datacell','sysOut');
call datacell.emitter('datacell','sysOut','localhost:50101');
call datacell.start('datacell','sysOut');
