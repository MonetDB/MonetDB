-- Example of a stream splitter
-- SET WINDOW 2 STRIDE 1
-- call cquery."window"('sys','stmp2',1); -- consume 1 tuple and tumble 1 from this stream
create stream table stmp2 (t timestamp, sensor integer, val decimal(8,2)) set window 2;
--select * from cquery.streams();

insert into stmp2 values('2005-09-23 12:34:26.000',1,11.0);
insert into stmp2 values('2005-09-23 12:34:27.000',1,11.0);
insert into stmp2 values('2005-09-23 12:34:28.000',1,13.0);
insert into stmp2 values('2005-09-23 12:34:28.000',1,13.0);

create table result1(like stmp2);
create table result2(like stmp2);

-- CREATE PROCEDURE cq_splitter
create procedure cq_splitter()
begin
    insert into result1 select * from stmp2 where val <12;
    insert into result2 select * from stmp2 where val >12;
end;

start continuous sys.cq_splitter();

-- wait for a few seconds for scheduler to do work
call cquery.wait(1000);

-- STOP cq_splitter;
pause continuous sys.cq_splitter();

--select * from cquery.status();
--select * from cquery.status();
--select * from cquery.log();

select 'RESULT';
select * from stmp2;
select val from result1;
select val from result2;

-- ideally auto remove upon dropping the procedure
stop continuous sys.cq_splitter();

drop procedure cq_splitter;
drop table stmp2;
drop table result1;
drop table result2;
