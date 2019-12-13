prepare select "system" or ? from _tables WHERE false;
EXEC **(false);
dealloc **;
exec **(false); --error, the last prepared statement, no longer exists;
DEALLOCATE **; --error, last prepared statement already closed

prepare select "system" or ? from _tables WHERE false;
execute **(false);
DEALLOCATE prepare all;
EXECUTE **(false); --error, the last prepared statement, no longer exists;

DEALLOCATE 100000; --error, it doesn't exist
DEALLOCATE ALL;
