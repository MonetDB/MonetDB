--The most basic coroutine operation test. Show that the MAL stack is correctly stopped and returned at the point
--after each call
CREATE FUNCTION factory00() RETURNS INT BEGIN
    YIELD 1;
    YIELD 2;
    YIELD 3;
END;

SELECT factory00();
SELECT factory00();
SELECT factory00();
SELECT factory00(); --error

DROP FUNCTION factory00;
