START TRANSACTION;

create table data(i integer);
insert into data values(0),(1),(2);

create table multiples(i integer);
insert into multiples VALUES((select count(*) from data)), ((select count(distinct i) from data));
select * from multiples;

insert into multiples VALUES((select count(*) from data));
insert into multiples VALUES((select count(distinct i) from data));
select * from multiples;
ROLLBACK;
