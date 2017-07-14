-- A simple continuous query over non-stream relations
-- controlled by a heartbeat.
create table cqresult05(i integer);

create procedure cq_basic()
begin
	insert into cqresult05 (select count(*) from cqresult05);
end;

-- register the CQ
call cquery.register('sys','cq_basic');

-- The scheduler executes this CQ every 1000 milliseconds
call cquery.heartbeat('sys','cq_basic',1000);

-- reactivate this continuous query
call cquery.resume('sys','cq_basic');
call cquery.wait(2100);
call cquery.pause('sys','cq_basic');

select 'RESULT';
select * from cqresult05;

--select * from cquery.summary();
--select * from cquery.log();

-- ideally auto remove upon dropping the procedure
call cquery.deregister('sys','cq_basic');

drop procedure cq_basic;
drop table cqresult05;
