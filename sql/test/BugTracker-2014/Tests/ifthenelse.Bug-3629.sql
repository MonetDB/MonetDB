START TRANSACTION;
CREATE FUNCTION test(b1 BOOLEAN)
RETURNS INTEGER
BEGIN
  IF b1 THEN RETURN 1;
  ELSEIF NOT(b1) THEN RETURN 0;
  ELSE RETURN NULL;
  END IF;
END;
SELECT test(true), test(false), test(null);
select test(true);
select test(false);
select test(null);
