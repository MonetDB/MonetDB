-- A straw-man's datacell proof of concept implementation
-- This example relies on the standard SQL facilities
-- to demonstrate the DataCell functionality.
-- The specific runtime issues are handled by an optimizer

create schema datacell;
set optimizer='datacell_pipe';

create table datacell.sysIn(
    id integer auto_increment,
    tag timestamp default now(),
    payload integer
);

-- to be used by continous queries
-- could be generated from the table definition.
create function datacell.sysIn()
returns table (id integer, tag timestamp, payload integer)
begin
	return select * from datacell.sysIn;
end;

select * from datacell.sysIn();

-- to be used by receptor thread
create function datacell.receptor_sysIn()
returns boolean
begin
	insert into datacell.sysIn(payload) values ( 1);
	return true;
end;

call datacell.basket('datacell','sysIn');
call datacell.receptor('datacell','sysIn','localhost','50500');
call datacell.start('datacell','sysIn');

create table datacell.sysOut( etag timestamp, like datacell.sysIn);

-- the continues query is registered and started
create procedure datacell.query_sysIn_sysOut()
begin
	insert into datacell.sysOut select now(), *  from datacell.sysIn() ;
end;
call register_query('datacell.query_sysIn_sysOut');
call start_query('datacell.query_sysIn_sysOut');

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
