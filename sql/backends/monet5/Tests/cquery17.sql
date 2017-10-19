-- test multiple inserts from multiple SQL types
create table testing17(aaa integer, bbb real, ccc text, ddd date, eee time, fff timestamp);

create procedure cq_query17(aaa integer, bbb real, ccc text, ddd date, eee time, fff timestamp)
begin
	insert into testing17 values (aaa, bbb, ccc, ddd, eee, fff);
end;

start continuous cq_query17(null, null, null, null, null, null) with heartbeat 1000 cycles 1;
call cquery.wait(1800);

start continuous cq_query17('-2', 132, 'abc', date '2017-01-01', time '12:34:56', timestamp '2007-03-07 15:28:16.577') with heartbeat 1000 cycles 1;
call cquery.wait(1800);

start continuous cq_query17(0, '-10.51', 'just another string', date '1980-12-13', time '17:35:58', timestamp '2009-02-09 15:00:00') with heartbeat 1000 cycles 1;
call cquery.wait(1800);

select * from testing17;

drop procedure cq_query17;
drop table testing17;
