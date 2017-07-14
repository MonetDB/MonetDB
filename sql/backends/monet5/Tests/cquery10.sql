-- A simple continuous query over non-stream relations
-- controlled by a cycle count
create table result(i integer);

create procedure cq_cycles()
begin
	insert into sys.result (select count(*) from sys.result);
end;

-- register the CQ
call cquery.register('sys','cq_cycles');

-- The scheduler interval is 1 sec 
call cquery.heartbeat('sys','cq_cycles',1000);

-- The scheduler executes all CQ at most 5 rounds
call cquery.cycles('sys','cq_cycles',3);

-- reactivate all continuous queries
call cquery.resume();
call cquery.wait(4000);
call cquery.pause();

select 'RESULT';
select * from result;

--select * from cquery.summary();
--select * from cquery.log();

-- ideally auto remove upon dropping the procedure
call cquery.deregister('sys','cq_cycles');

drop procedure cq_cycles;
drop table result;
