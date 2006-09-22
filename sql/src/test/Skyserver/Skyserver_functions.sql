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

CREATE FUNCTION fGetDiagChecksum()
RETURNS BIGINT
BEGIN
    RETURN (select sum(count)= count(*) from "Diagnostics");
END;

CREATE FUNCTION fPhotoStatusN(val int)
RETURNS varchar(1000)
BEGIN
    	DECLARE bit int, mask bigint, out varchar(2000);
    	SET bit=32;
	SET out ='';
	WHILE (bit > 0) DO
		SET bit = bit-1;
	    	SET mask = left_shift(cast(1 as bigint),bit);
		CASE 
			WHEN (bit_and(mask,val)=0) THEN SET out = out || ''; 
			ELSE SET out = out || (COALESCE((select name from PhotoStatus where value=mask),'')||' ');
	    	END CASE;
	END WHILE;
    	RETURN out;
END;

CREATE FUNCTION fPhotoStatus(pname varchar(40)) 
RETURNS int 
BEGIN 
	declare x int;
	SELECT cast(value as int) into x FROM PhotoStatus WHERE name = UPPER(pname); 
	RETURN x;
END;

CREATE FUNCTION fPrimTargetN(val int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from PrimTarget where value=mask),'')||' ');
	    END CASE;
    END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPrimTarget(nme varchar(40))
RETURNS int
BEGIN
	RETURN ( SELECT cast(value as int)
		FROM PrimTarget
		WHERE name = UPPER(nme)
		);
END;

CREATE FUNCTION fSecTarget(nme varchar(40))
RETURNS int
BEGIN
RETURN ( SELECT cast(value as int)
	FROM SecTarget
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fSecTargetN(val int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from SecTarget where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fInsideMask(nme varchar(40))
RETURNS smallint
BEGIN
RETURN ( SELECT cast(value as tinyInt)
	FROM InsideMask
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fInsideMaskN(val smallint)
RETURNS varchar(1000)
BEGIN
    DECLARE bit smallint, mask smallint, out varchar(2000);
    SET bit=7;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from InsideMask where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fSpecZWarning(nme varchar(40))
RETURNS INT 
BEGIN
    RETURN ( SELECT cast(value as int)
	FROM SpecZWarning
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fSpecZWarningN(val int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || '';
		ELSE SET out = out || (coalesce((select name from SpecZWarning where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fImageMask(nme varchar(40))
RETURNS int
BEGIN
    RETURN ( SELECT cast(value as int)
	FROM ImageMask
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fImageMaskN(val int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from ImageMask where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fTiMask(nme varchar(40))
RETURNS int
BEGIN
RETURN ( SELECT cast(value as int)
	FROM TiMask
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fTiMaskN(val int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || '';
		ELSE SET out = out || (coalesce((select name from TiMask where value=mask),'')||' ');
	    END CASE;
    END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPhotoModeN(val int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM PhotoMode
	WHERE value = val
	);
END;

CREATE FUNCTION fPhotoMode(nme varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM PhotoMode
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fPhotoTypeN(val int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM PhotoType
	WHERE value = val
	);
END;

CREATE FUNCTION fPhotoType(nme varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM PhotoType
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fMaskTypeN(val int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM MaskType
	WHERE value = val
	);
END;

CREATE FUNCTION fMaskType(nme varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM MaskType
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fFieldQualityN(val int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM FieldQuality
	WHERE value = val
	);
END;

CREATE FUNCTION fFieldQuality(nme varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM FieldQuality
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fPspStatus(nme varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM PspStatus
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fPspStatusN(val int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM PspStatus
	WHERE value = val
	);
END;

CREATE FUNCTION fFramesStatus(nme varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM FramesStatus
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fFramesStatusN(val int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM FramesStatus
	WHERE value = val
	);
END;

CREATE FUNCTION fSpecClass(nme varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value 
	FROM SpecClass
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fSpecClassN(val int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM SpecClass
	WHERE value = val
	);
END;

CREATE FUNCTION fSpecLineNames(nme varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM SpecLineNames
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fSpecLineNamesN(val int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM SpecLineNames
	WHERE value = val
	);
END;

CREATE FUNCTION fSpecZStatusN(val int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM SpecZStatus
	WHERE value = val
	);
END;

CREATE FUNCTION fSpecZStatus(nme varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM SpecZStatus
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fHoleType(nme varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM HoleType
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fHoleTypeN(val int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM HoleType
	WHERE value = val
	);
END;

CREATE FUNCTION fObjType(nme varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM ObjType
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fObjTypeN(val int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM ObjType
	WHERE value = val
	);
END;

CREATE FUNCTION fProgramType(nme varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM ProgramType
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fProgramTypeN(val int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM ProgramType
	WHERE value = val
	);
END;

CREATE FUNCTION fCoordType(nme varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM CoordType
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fCoordTypeN(val int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM CoordType
	WHERE value = val
	);
END;


CREATE FUNCTION fFieldMask(nme varchar(40))
RETURNS int
BEGIN
RETURN ( SELECT cast(value as int)
	FROM FieldMask
	WHERE name = UPPER(nme)
	);
END;

CREATE FUNCTION fFieldMaskN(val int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from FieldMask where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPhotoFlagsN(val bigint)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=63;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,val)=0) THEN SET out = out || '';
		ELSE SET out = out || (coalesce((select name from PhotoFlags where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPhotoFlags(nme varchar(40))
RETURNS bigint
BEGIN
RETURN ( SELECT cast(value as bigint)
	FROM PhotoFlags
	WHERE name = UPPER(nme)
	);
END;

COMMIT;
