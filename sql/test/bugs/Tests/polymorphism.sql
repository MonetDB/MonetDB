start transaction;

create function bwdecompose(col decimal(7,2), bits integer)
	returns varchar(4096) 
begin
	return 1 || cast ( col as varchar(20));
end;

create function bwdecompose(col date, bits integer)
returns varchar(4096) 
begin
	return 2 || cast ( col as varchar(20));
end;

create function bwdecompose(col integer, bits integer)
returns varchar(4096) 
begin
	return 3 || cast ( col as varchar(20));
end;

create table bwd (
	l_int integer,
	l_dat date,
	l_dec decimal(7,2)
);

select bwdecompose(l_int,24), bwdecompose(l_dat,24), bwdecompose(l_dec,24) from bwd;

rollback;
