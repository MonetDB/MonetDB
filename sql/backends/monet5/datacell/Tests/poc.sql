-- A straw-man's datacell proof of concept implementation
-- This example relies on the standard SQL facilities
-- to demonstrate the DataCell functionality.
-- The specific runtime issues are handled by an optimizer

create schema datacell;

create table datacell.basket_X(
    id integer auto_increment,
    tag timestamp default now(),
    payload integer
);

-- to be used by continous queries
-- could be generated from the table definition.
create function datacell.basket_X()
returns table (id integer, tag timestamp, payload integer)
begin
	return select * from basket_X;
end;

-- to be used by receptor thread
create function datacell.receptor_X()
returns boolean
begin
	insert into basket_X(payload) values ( 1);
	return true;
end;

call register_basket('basket_x');
call register_receptor('receptor_x','localhost:50100');
call start_receptor('receptor_x');

create table basket_Y( etag timestamp, like basket_X);

-- the continues query is registered and started
create procedure query_X_Y()
begin
	insert into basket_Y select now(), *  from basket_X() ;
end;
call register_query('query_X_Y');
call start_query('query_X_Y');

create function basket_Y()
returns table (id integer)
begin
	return select * from basket_Y;
end;

-- sent it over the wire.
create function emitter_Y()
returns boolean;
begin
	select * from basket_Y();
	return true;
end;
call register_basket('basket_y');
call register_emitter('emitter_y','localhost:50101');
call start_emitter('emitter_y');
