# Test error reporting on exceptions, this is different from parse errors because we get the line number by checking the stack trace
# We do want to tell the user exactly which line broke
START TRANSACTION;

CREATE TABLE rval(i integer);
INSERT INTO rval VALUES (1), (2), (3), (4), (5);

CREATE FUNCTION pyapi20(i integer) returns integer
language P
{
    a = 3
    b = 0
    c = a / b
    return i
};

SELECT pyapi20(i) FROM rval;
DROP FUNCTION pyapi20;
DROP TABLE rval;

ROLLBACK;

