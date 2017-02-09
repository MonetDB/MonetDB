select * from sys.querylog_history;
select * from sys.querylog_catalog;
select * from sys.querylog_calls;

call querylog_enable();

select 1;
select owner, query, pipe, plan from sys.querylog_catalog;
select arguments, tuples from sys.querylog_calls;
select owner, query, pipe, plan, arguments, tuples from sys.querylog_history;

call querylog_disable();
call sys.querylog_empty();

select * from sys.querylog_history;
select * from sys.querylog_calls;
select * from sys.querylog_catalog;

