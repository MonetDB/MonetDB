
CREATE FUNCTION fHtm(x float, y float, z float, radius float)
RETURNS TABLE (
HtmIDStart bigint ,
HtmIDEnd bigint
)
BEGIN
RETURN TABLE (SELECT 1,2);
END;
create table t31 (id int, age int);

CREATE FUNCTION f31(ra float, deci float,
radius float, zoo int)
RETURNS TABLE (
fieldID bigint ,
distance float -- distance in arc minutes
)
BEGIN
--
DECLARE nx float,ny float,nz float;
SET nx = ra;
SET ny = ra;
SET nz = deci;
DECLARE TABLE cover(
htmidStart bigint, htmidEnd bigint
);
INSERT into cover
SELECT htmidStart, htmidEnd
FROM fHtm(nx,ny,nz,radius);
RETURN TABLE(SELECT age,
(2*DEGREES(ASIN(sqrt(power(nx-id,2)+power(ny-id,2)+power(nz-id,2))/2))*60)
as val
FROM cover H, t31 F );
END;

select * from f31(1,2,3,4) n;

drop function f31;
drop table t31;
drop function fHtm;
