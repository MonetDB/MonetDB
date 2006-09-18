START TRANSACTION;

CREATE FUNCTION fWedgeV3(x1 float,y1 float, z1 float, x2 float, y2
float, z2 float)
RETURNS TABLE (x float, y float, z float)
     RETURN TABLE(SELECT
    (y1*z2 - y2*z1) as x,
        (x2*z1 - x1*z2) as y,
    (x1*y2 - x2*y1) as z);

CREATE FUNCTION fRotateV3(inpmode varchar(16), pcx float, pcy float, pcz
float)
RETURNS TABLE (
    x float,
    y float,
    z float)
begin
    DECLARE px float,py float, pz float;
    SELECT x*pcx+y*pcy+z*pcz into px FROM Rmatrix WHERE mode=inpmode
and row=1;
    SELECT x*pcx+y*pcy+z*pcz into py FROM Rmatrix WHERE mode=inpmode
and row=2;
    SELECT x*pcx+y*pcy+z*pcz into pz FROM Rmatrix WHERE mode=inpmode
and row=3;
    RETURN TABLE (SELECT px,py,pz);
END; 

CREATE FUNCTION fStripeOfRun(prun int)
RETURNS int
BEGIN
    declare x int;
      SELECT stripe into x from Segment where run = prun and camcol=1;
    return x;
END;

CREATE FUNCTION fPhotoStatus(pname varchar(40))
RETURNS int
BEGIN
    declare x int;
    SELECT cast(value as int) into x
    FROM PhotoStatus
    WHERE name = UPPER(pname);
    RETURN x;
END;

CREATE FUNCTION fGetDiagChecksum()
RETURNS BIGINT
BEGIN
    RETURN (select sum(count)= count(*) from "Diagnostics");
END;

COMMIT;
