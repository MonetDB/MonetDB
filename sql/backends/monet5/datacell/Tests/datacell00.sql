#set up the minimal test environment for datacell

create schema datacell;
create table datacell.X( id int, tag timestamp, payload int);
create table datacell.Y( id int, tag timestamp, payload int, msdelay int);

-- the continoues query
create procedure datacell.tranport()
begin
	-- insert into datacell.Y select *, cast(now() as milliseconds) -  cast(tag as milliseconds) from datacell.X;
	 insert into datacell.Y select *, 1 from datacell.X;
end;
