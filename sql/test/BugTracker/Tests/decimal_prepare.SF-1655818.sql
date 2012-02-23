-- origin: Anders Blaagaard

START TRANSACTION;

create table P1655818 (
	time timestamp not null,
	x numeric(5,2) not null,
	y numeric(5,2) not null
);

PREPARE insert into P1655818 (time,x,y) values (?,?,?);

exec **(timestamp '2003-01-30 18:03:35.0', 71.91, 71.98);

ROLLBACK;
