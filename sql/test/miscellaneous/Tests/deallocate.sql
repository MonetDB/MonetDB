deallocate all; --deallocate all the prepared statements from the current directory run

prepare select "system" or ? from _tables WHERE false;
select "statement" from prepared_statements; --only 1
EXEC **(false);
select "statement" from prepared_statements; --only 1
deallocate **;
select "statement" from prepared_statements; --empty
exec **(false); --error, the last prepared statement, no longer exists;
DEALLOCATE **; --error, last prepared statement already closed

prepare select "system" or ? from _tables WHERE false;
execute **(false);
DEALLOCATE prepare all;
EXECUTE **(false); --error, the last prepared statement, no longer exists;

select "statement" from prepared_statements; --empty

DEALLOCATE 100000; --error, it doesn't exist
DEALLOCATE ALL;

select "statement" from prepared_statements; --empty
