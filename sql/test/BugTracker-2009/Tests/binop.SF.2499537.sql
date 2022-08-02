-- example skeleton to study opportunities 
-- for algebraic GIS accellerators.
create function in_p_l(p int, l int)
returns boolean
begin
	return true;
end;

create table points(p int);
create table lines(l int);

select * from points x,lines y where in_p_l(x.p,y.l);

drop table lines;
drop table points;
drop function in_p_l;
