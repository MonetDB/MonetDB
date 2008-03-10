CREATE FUNCTION fEq (ra float, decim float, r float)
RETURNS int
BEGIN
        return 0;
END;

SELECT  fEq(212.82496,1.27536,0.167);
SELECT  fEq(134.44708,-0.2,0.167);
SELECT  fEq(261,-90,0.167);


drop function fEq;
