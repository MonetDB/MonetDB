select 6e-1;
select cast(6e-1 as integer);
select cast(0.6 as integer);


select 1.7777777;
select cast(1.7777777 as integer);
select cast(cast(1.7777777 as decimal(10,7)) as integer);

create table testdec(testdec decimal(5,4));
insert into testdec values (-0.12341);
insert into testdec values (-0.12347);
prepare insert into testdec (testdec) values (?);
exec ** (-0.12341);
exec ** (-0.12347);
select * from testdec;

select cast(13.8 as int);
select cast(cast(13.8 as decimal(10,7)) as int);
select cast(cast(13.8 as double) as int);
