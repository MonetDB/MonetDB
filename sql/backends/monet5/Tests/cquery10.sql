-- A simple continuous query over non-stream relations
-- controlled by a cycle count
create stream table result(i integer);

create procedure cq_cycles()
begin
	insert into sys.result (select count(*) from sys.result);
end;

-- register the CQ
-- The scheduler executes all CQ at most 3 rounds
start continuous sys.cq_cycles() with cycles 3;

-- The scheduler interval is 1 sec 
--call cquery."heartbeat"('sys','cq_cycles',1000);

-- reactivate all continuous queries

call cquery.wait(4000);
pause continuous sys.cq_cycles();

select 'RESULT';
select * from result;

--select * from cquery.summary();
--select * from cquery.log();

-- ideally auto remove upon dropping the procedure
stop continuous sys.cq_cycles();

drop procedure cq_cycles;
drop table result;
