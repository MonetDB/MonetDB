CREATE FUNCTION fibonacci(i integer)
RETURNS integer
BEGIN
	if ( i = 0) THEN RETURN 0; END IF;
	if ( i = 1) THEN RETURN 1; END IF;
	RETURN fibonacci(CAST(i-1 AS INTEGER))+fibonacci(CAST(i-2 AS INTEGER));
END;

SELECT fibonacci(0);
SELECT fibonacci(1);
SELECT fibonacci(2);
SELECT fibonacci(3);
SELECT fibonacci(4);
SELECT fibonacci(5);
SELECT fibonacci(6);
SELECT fibonacci(7);
SELECT fibonacci(8);
DROP FUNCTION fibonacci(integer);
