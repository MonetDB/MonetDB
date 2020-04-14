-- MonetDB SQL has a number of predefined global variables, whose semantics should be respected.

select optimizer;

-- this one is defined in the sys schema
select current_schema;
select sys.optimizer;	-- can we find it there?

select tmp.optimizer; 	-- should not be found

-- the optimizer variable can be reassigned a value
declare aux string;
set aux = (select sys.optimizer);
set optimizer = 'minimal_pipe';
select optimizer;

set sys.optimizer = 'minimal_pipe';
select sys.optimizer;

-- the global variable may appear as a column in a table
create table mynewone( i integer, optimizer integer);
insert into mynewone(i, optimizer) values(1,2);		-- to marked as sheelding outer definition (ako Pythonic)
select i, optimizer from mynewone;					-- ambiguous
select i, sys.optimizer from mynewone;				-- should be recognized

-- entering the world of functions
create function foo()
returns integer
begin
	return optimizer;
end;

create function foo2()
returns integer
begin
	return sys.optimizer;
end;

create procedure poo()
begin
	set optimizer='volcano_pipe';
end;

create procedure poo2()
begin
	set sys.optimizer='volcano_pipe';	
end;

create procedure poo4()
begin
	set optimizer='deep-pipe';
	select optimizer; --error, regular select statements not allowed inside procedures (disallowed by the parser)
end;

set optimizer = (select sys.aux);
