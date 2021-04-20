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

-- On the following queries, the NULL values must be removed from the result.
select count(*) from pk1 join fk on fk.fk1 = pk1.pk1;
	-- 4
select count(*) from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2;
	-- 3
select id from pk1 join fk on fk.fk1 = pk1.pk1 order by id;
	-- 1
	-- 2
	-- 3
	-- 5
select id from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;
	-- 1
	-- 2
	-- 3
select id, v1 from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;
	-- 1, 11
	-- 2, 11
	-- 3, 12
select id, v2 from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;
	-- 1, 21
	-- 2, 22
	-- 3, 21

start transaction;
CREATE TABLE "myt" ("myid" int NOT NULL,"great" varchar(32),CONSTRAINT "mypkey" PRIMARY KEY ("myid"));
insert into myt values (1, 'a'),(2, 'b'),(3, 'c'),(4, 'd');

CREATE TABLE "testme" ("good" int,"myid" int NOT NULL,"hello" varchar(32),CONSTRAINT "givemeapkey" PRIMARY KEY ("myid"),CONSTRAINT "myfkey" FOREIGN KEY ("good") REFERENCES "myt" ("myid"));
insert into testme values (1, 1, 'a'),(2, 2, 'b'),(3, 3, 'c'),(4, 4, 'd');

-- the optimization cannot kick in while ordering or grouping on a primary key side column
select testme.myid from testme inner join "myt" on testme.good = "myt".myid where testme.hello = 'd' order by "myt".great limit 10;
	-- 4
select count(*) from testme inner join "myt" on testme.good = "myt".myid where testme.hello = 'd' group by "myt".great;
	-- 1
rollback;
