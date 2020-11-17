select count(*) from sys.prepared_statements;
prepare select 1;
prepare select 2;
deallocate **;
select count(*) from sys.prepared_statements;
deallocate all;
select count(*) from sys.prepared_statements;
