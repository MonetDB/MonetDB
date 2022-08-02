CREATE FUNCTION f2 ()
RETURNS TABLE (
fieldID bigint,
distance bigint
)
BEGIN
DECLARE TABLE cover(
htmidStart bigint, htmidEnd bigint
);
INSERT into cover
SELECT 1, 2;
RETURN TABLE (
SELECT htmidStart, 1 as distance
FROM cover H);
END;

CREATE FUNCTION f3 ()
RETURNS TABLE (
fieldID bigint
)
BEGIN
RETURN TABLE (
SELECT fieldID
FROM f2 () n
ORDER BY fieldID ASC LIMIT 1);
END;


drop function f3();
drop function f2();
