START TRANSACTION;
CREATE OR REPLACE FUNCTION test(i integer) 
RETURNS TABLE (value integer)
BEGIN
  return values (1), (2), (2), (3), (i);
END;

select distinct value from test(3);
ROLLBACK;
