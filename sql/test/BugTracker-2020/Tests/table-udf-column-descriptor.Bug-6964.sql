START TRANSACTION;
CREATE OR REPLACE FUNCTION test(i integer) 
RETURNS TABLE (value integer)
BEGIN
  return select value from generate_series(1,i);
END;

select value from test(3);
ROLLBACK;
