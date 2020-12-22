create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;

-- drop view example_view;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;
select * from example where val1 >= 5 order by val1, val2;
select * from example_view where val1 > 5 order by val1, val2;
drop view example_view;
drop table example;
create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;

-- drop view example_view;
select * from example order by val1, val2;
delete from example where val1 >= 5;
select * from example order by val1, val2;
drop view example_view;
drop table example;
create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;

-- drop view example_view;
select * from example order by val1, val2;
delete from example where val1 = 5;
select * from example order by val1, val2;
delete from example where val1 = 9;
select * from example order by val1, val2;
delete from example where val1 = 8;
select * from example order by val1, val2;
drop view example_view;
drop table example;
create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;

-- drop view example_view;
select * from example order by val1, val2;
delete from example where val1 > 5;
select * from example order by val1, val2;
drop view example_view;
drop table example;
create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;

-- drop view example_view;
select * from example order by val1, val2;
update example set val3=-1.0 where val1 >= 5;
select * from example order by val1, val2;
drop view example_view;
drop table example;
create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;

-- drop view example_view;
select * from example order by val1, val2;
update example set val1 = -5 where val1 = 5;
select * from example order by val1, val2;
update example set val1 = -5 where val1 = 9;
select * from example order by val1, val2;
update example set val1 = -5 where val1 = 8;
select * from example order by val1, val2;
drop view example_view;
drop table example;
create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

select * from example order by val1, val2;

select * from example_view order by val1, val2;

-- drop view example_view;
select * from example order by val1, val2;
update example set val2 = 'updated' where val1 > 5;
select * from example order by val1, val2;
drop view example_view;
drop table example;
START TRANSACTION;
create table example ( val1 integer, val2 varchar(10), val3 float );
create view example_view as select * from example;
insert into example values( 1, 'test', 0.1 );
insert into example values( 5, 'xtest', 0.9 );
insert into example values( 9, 'testx', 9.1 );
insert into example values( 0, 'texst', 99999.1 );
insert into example values( 8, 'texxst', 0.0001 );

COMMIT;

select * from example order by val1, val2;

select * from example_view order by val1, val2;
drop view example_view;
drop table example;
