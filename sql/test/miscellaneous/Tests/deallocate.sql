deallocate all; --deallocate all the prepared statements from the current directory run (but does the client reconnect after each test?)

prepare select "system" or ? from sys._tables WHERE false;
select "statement" from prepared_statements; --only 1
select "inout", "number", "type", "type_digits", "type_scale", "schema", "table", "column" from prepared_statements_args; --only 1
select "prep"."statement", "inout", "number", "type", "type_digits", "type_scale", "schema", "table", "column" 
from prepared_statements prep
inner join prepared_statements_args psa on prep."statementid" = psa."statementid"; --two rows

EXEC **(false);
select "statement" from prepared_statements; --only 1
select "inout", "number", "type", "type_digits", "type_scale", "schema", "table", "column" from prepared_statements_args; --only 1
deallocate **;
select "statement" from prepared_statements; --empty
select "inout", "number", "type", "type_digits", "type_scale", "schema", "table", "column" from prepared_statements_args; --empty
exec **(false); --error, the last prepared statement, no longer exists;
DEALLOCATE **; --error, last prepared statement already closed

prepare select "system" or ? from sys._tables WHERE false;
execute **(false);
DEALLOCATE prepare all;
EXECUTE **(false); --error, the last prepared statement, no longer exists;

select "statement" from prepared_statements; --empty
select "inout", "number", "type", "type_digits", "type_scale", "schema", "table", "column" from prepared_statements_args; --empty

DEALLOCATE 100000; --error, it doesn't exist
DEALLOCATE ALL;

select "statement" from prepared_statements; --empty
select "inout", "number", "type", "type_digits", "type_scale", "schema", "table", "column" from prepared_statements_args; --empty
