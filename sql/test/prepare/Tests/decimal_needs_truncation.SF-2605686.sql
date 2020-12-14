
create table testdec(testdec decimal(4,4));
drop table testdec;

create table testdec(testdec decimal(5,4));
insert into testdec values (0.12345);
select * from testdec;

PREPARE INSERT INTO testdec (testdec) values (?);
exec **(0.12345);

select * from testdec;

drop table testdec;
