#set up the minimal test environment for datacell

create table datacell.X( id string, tag timestamp, payload int);
create table datacell.Y( id string, tag timestamp, payload int, msdelay int);

-- the continoues query
create procedure datacell.transport()
begin
	-- insert into datacell.Y select *, cast(now() as milliseconds) -  cast(tag as milliseconds) from datacell.X;
	 insert into datacell.Y select *, 1 from datacell.X;
end;
