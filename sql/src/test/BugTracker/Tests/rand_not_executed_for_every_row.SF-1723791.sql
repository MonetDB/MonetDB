create table nr (nr int);
insert into nr values (1);
insert into nr values (2);
insert into nr values (3);
insert into nr values (4);
insert into nr values (5);
insert into nr values (6);
insert into nr values (7);
insert into nr values (8);
insert into nr values (9);
insert into nr values (10);

select count (*) from 
	(select rand() as r, nr from nr) a,
	(select rand() as r, nr from nr) b
	where a.r = b.r;

drop table nr;
