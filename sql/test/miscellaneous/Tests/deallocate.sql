prepare select "system" or ? from _tables WHERE false;

EXEC **(false);
dealloc **;
exec **(false); --error, the last prepared statement, no longer exists;

prepare select "system" or ? from _tables WHERE false;
execute **(false);
DEALLOCATE prepare all;
EXECUTE **(false); --error, the last prepared statement, no longer exists;
