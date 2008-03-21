
CREATE FUNCTION fGetNearbyObjEq (ra float, decim float, r float)
RETURNS TABLE (
objID bigint,
run int ,
camcol int ,
field int ,
rerun int ,
type int ,
cx float ,
cy float ,
cz float ,
htmID bigint,
distance float -- distance in arc minutes
)
BEGIN
DECLARE TABLE proxtab (
objID bigint,
run int ,
camcol int ,
field int ,
rerun int ,
type int ,
cx float ,
cy float ,
cz float ,
htmID bigint,
distance float -- distance in arc minutes
);
RETURN proxtab;
END;

create table t1913572 (objID bigint NULL);

SELECT * FROM t1913572 s, fGetNearbyObjEq(20.925,1.67,29.4) n WHERE
s.objID=n.objID;

drop function fGetNearbyObjEq;
drop table t1913572;
