-- A simple continuous procedure over non-stream relations
-- The procedure will not be executed before a heartbeat is set.
create table cqresult05(i integer);

create procedure cq_basic()
begin
	insert into cqresult05 (select count(*) from cqresult05);
end;

start continuous sys.cq_basic() with heartbeat 1000 cycles 3;

call cquery.wait(3500);

--select * from cquery.status();
--select * from cquery.summary();
--select * from cquery.log();

select 'RESULT';
select * from cqresult05;

drop procedure cq_basic;
drop table cqresult05;
