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
select count(distinct nr) from
(SELECT nr, RAND() FROM nr) as x;
select count(distinct rnd) from
(SELECT nr, RAND() AS rnd FROM nr) as x;
drop table nr;
