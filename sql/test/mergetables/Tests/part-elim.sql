create table mt2(id bigint, posX real); 
create table mt1(id bigint, posX real);

create merge table test(id bigint, posX real); 
alter table test add table mt1; 
alter table test add table mt2;

insert into mt1 values (1021, 12.4);
insert into mt1 values (1022, 13.4);
insert into mt1 values (1023, 14.4);
insert into mt1 values (1024, 15.4);

insert into mt2 values (1, 1.4);
insert into mt2 values (2, 1.4);
insert into mt2 values (3, 1.4);
insert into mt2 values (4, 1.4);

alter table mt1 set read only;
alter table mt2 set read only;

analyze sys.mt1 (id,posX) minmax;
analyze sys.mt2 (id,posX) minmax;

plan select * from test where id between 1 and 10000;
plan select * from test where id between 1 and 1000;
declare l integer;
set l = 1;
declare h integer;
set h = 10000;
plan select * from test where id between l and h;
set h = 1000;
plan select * from test where id between l and h;

plan select * from test where id between 1 and 1000*10;
plan select * from test where id between 1 and 100*10;

plan select * from test where id in (1, 1022);
plan select * from test where id in (1, 1000);

drop table test;
drop table mt1;
drop table mt2;

