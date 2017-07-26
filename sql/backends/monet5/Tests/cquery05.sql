-- A simple continuous procedure over non-stream relations
-- The procedure will not be executed before a heartbeat is set.
create table cqresult05(i integer);

create procedure cq_basic()
begin
	insert into cqresult05 (select count(*) from cqresult05);
end;

start continuous sys.cq_basic();

call cquery."heartbeat"('sys','cq_basic',1000);

call cquery.wait(2100);

pause continuous sys.cq_basic();

--select * from cquery.status();
--select * from cquery.summary();
--select * from cquery.log();

stop continuous sys.cq_basic();

select 'RESULT';
select * from cqresult05;


drop procedure cq_basic;
drop table cqresult05;
