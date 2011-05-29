-- A straw-man's datacell 
-- This example relies on the standard SQL facilities
-- to demonstrate the DataCell functionality.
-- The specific runtime issues are handled by an optimizer

create table basket_X(
    id integer auto_increment,
    tag timestamp default now(),
    payload integer
);

-- empty the basket
create function basket_X()
returns table (id integer, tag timestamp, payload integer)
begin
	return select * from basket_X;
end;

-- initialize and start the receptor for basket X
create procedure receptor_X()
begin
	call receptor('localhost:50100','start');
	insert into basket_X(tag,payload) values ( now(), receptor_int(0));
end;

create table basket_Y( id integer);

create function basket_Y()
returns table (id integer)
begin
	return select * from basket_Y;
end;


create procedure cquery_pass()
begin
	insert into basket_Y(id) select id from basket_X() ;
end;


-- sent it over the wire.
create procedure emitter_Y()
begin
	call emitter('localhost:50100','start');
	call emitter(select * from basket_Y());
end;
