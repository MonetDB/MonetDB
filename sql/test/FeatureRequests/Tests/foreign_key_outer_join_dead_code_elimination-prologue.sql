create table pk1 (
	pk1	int generated always as identity not null primary key,
	v1	int
);
create table pk2 (
	pk2	int generated always as identity not null primary key,
	v2	int
);

create table fk (
	id	int generated always as identity not null primary key,
	fk1	int references pk1(pk1),
	fk2	int references pk2(pk2)
);

create view v0 as (
	select * from fk
);
create view v1 as (
	select * from fk
	left outer join pk1 on fk.fk1 = pk1.pk1
);
create view v2 as (
	select * from fk
	left outer join pk1 on fk.fk1 = pk1.pk1
	left outer join pk2 on fk.fk2 = pk2.pk2
);

insert into pk1 (v1) values (11),(12),(13),(14);
insert into pk2 (v2) values (21),(22),(23),(24);

insert into fk (fk1,fk2) values (1,1),(1,2),(2,1),(null,3),(4,null),(null,null);

select * from pk1 order by pk1;
select * from pk2 order by pk2;

select * from fk order by id;

select * from v0 order by id;
select * from v1 order by id;
select * from v2 order by id;

select count(*) from fk;
select count(*) from v0;
select count(*) from fk left outer join pk1 on fk.fk1 = pk1.pk1;
select count(*) from v1;
select count(*) from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2;
select count(*) from v2;

select id       from fk order by id;
select id       from v0 order by id;
select id       from fk left outer join pk1 on fk.fk1 = pk1.pk1 order by id;
select id       from v1 order by id;
select id       from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;
select id       from v2 order by id;

select id , v1  from v1 order by id;
select id , v1  from fk left outer join pk1 on fk.fk1 = pk1.pk1 order by id;
select id , v2  from fk left outer join pk2 on fk.fk2 = pk2.pk2 order by id;
select id , v1  from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;
select id , v2  from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;
select id , v2  from v2 order by id;
