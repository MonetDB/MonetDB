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

CREATE FUNCTION fPhotoStatusN(value int)
RETURNS varchar(1000)
BEGIN
    	DECLARE bit int, mask bigint, out varchar(2000);
    	SET bit=32;
	SET out ='';
	WHILE (bit > 0) DO
		SET bit = bit-1;
	    	SET mask = left_shift(cast(1 as bigint),bit);
		CASE 
			WHEN (bit_and(mask,value)=0) THEN SET out = out || ''; 
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

CREATE FUNCTION fPrimTargetN(value int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from PrimTarget where value=mask),'')||' ');
	    END CASE;
    END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPrimTarget(name varchar(40))
RETURNS int
BEGIN
	RETURN ( SELECT cast(value as int)
		FROM PrimTarget
		WHERE name = UPPER(name)
		);
END;

CREATE FUNCTION fSecTarget(name varchar(40))
RETURNS int
BEGIN
RETURN ( SELECT cast(value as int)
	FROM SecTarget
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fSecTargetN(value int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from SecTarget where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fInsideMask(name varchar(40))
RETURNS smallint
BEGIN
RETURN ( SELECT cast(value as tinyInt)
	FROM InsideMask
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fInsideMaskN(value smallint)
RETURNS varchar(1000)
BEGIN
    DECLARE bit smallint, mask smallint, out varchar(2000);
    SET bit=7;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from InsideMask where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fSpecZWarning(name varchar(40))
RETURNS INT 
BEGIN
    RETURN ( SELECT cast(value as int)
	FROM SpecZWarning
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fSpecZWarningN(value int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || '';
		ELSE SET out = out || (coalesce((select name from SpecZWarning where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fImageMask(name varchar(40))
RETURNS int
BEGIN
    RETURN ( SELECT cast(value as int)
	FROM ImageMask
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fImageMaskN(value int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from ImageMask where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fTiMask(name varchar(40))
RETURNS int
BEGIN
RETURN ( SELECT cast(value as int)
	FROM TiMask
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fTiMaskN(value int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || '';
		ELSE SET out = out || (coalesce((select name from TiMask where value=mask),'')||' ');
	    END CASE;
    END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPhotoModeN(value int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM PhotoMode
	WHERE value = value
	);
END;

CREATE FUNCTION fPhotoMode(name varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM PhotoMode
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fPhotoTypeN(value int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM PhotoType
	WHERE value = value
	);
END;

CREATE FUNCTION fPhotoType(name varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM PhotoType
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fMaskTypeN(value int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM MaskType
	WHERE value = value
	);
END;

CREATE FUNCTION fMaskType(name varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM MaskType
	WHERE name = UPPER(name)
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

CREATE FUNCTION fPspStatus(name varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM PspStatus
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fPspStatusN(value int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM PspStatus
	WHERE value = value
	);
END;

CREATE FUNCTION fFramesStatus(name varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value
	FROM FramesStatus
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fFramesStatusN(value int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM FramesStatus
	WHERE value = value
	);
END;

CREATE FUNCTION fSpecClass(name varchar(40))
RETURNS INTEGER
BEGIN
RETURN ( SELECT value 
	FROM SpecClass
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fSpecClassN(value int)
RETURNS varchar(40)
BEGIN
RETURN ( SELECT name
	FROM SpecClass
	WHERE value = value
	);
END;

CREATE FUNCTION fSpecLineNames(name varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM SpecLineNames
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fSpecLineNamesN(value int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM SpecLineNames
	WHERE value = value
	);
END;

CREATE FUNCTION fSpecZStatusN(value int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM SpecZStatus
	WHERE value = value
	);
END;

CREATE FUNCTION fSpecZStatus(name varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM SpecZStatus
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fHoleType(name varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM HoleType
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fHoleTypeN(value int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM HoleType
	WHERE value = value
	);
END;

CREATE FUNCTION fObjType(name varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM ObjType
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fObjTypeN(value int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM ObjType
	WHERE value = value
	);
END;

CREATE FUNCTION fProgramType(name varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM ProgramType
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fProgramTypeN(value int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM ProgramType
	WHERE value = value
	);
END;

CREATE FUNCTION fCoordType(name varchar(40))
RETURNS INTEGER
BEGIN
    RETURN ( SELECT value
	FROM CoordType
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fCoordTypeN(value int)
RETURNS varchar(40)
BEGIN
    RETURN ( SELECT name
	FROM CoordType
	WHERE value = value
	);
END;


CREATE FUNCTION fFieldMask(name varchar(40))
RETURNS int
BEGIN
RETURN ( SELECT cast(value as int)
	FROM FieldMask
	WHERE name = UPPER(name)
	);
END;

CREATE FUNCTION fFieldMaskN(value int)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=32;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || ''; 
		ELSE SET out = out || (coalesce((select name from FieldMask where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPhotoFlagsN(value bigint)
RETURNS varchar(1000)
BEGIN
    DECLARE bit int, mask bigint, out varchar(2000);
    SET bit=63;
    SET out ='';
    WHILE (bit>0) DO
	    SET bit = bit-1;
	    SET mask = left_shift(cast(1 as bigint),bit);
	    CASE 
		WHEN (bit_and(mask,value)=0) THEN SET out = out || '';
		ELSE SET out = out || (coalesce((select name from PhotoFlags where value=mask),'')||' ');
	    END CASE;
	END WHILE;
    RETURN out;
END;

CREATE FUNCTION fPhotoFlags(name varchar(40))
RETURNS bigint
BEGIN
RETURN ( SELECT cast(value as bigint)
	FROM PhotoFlags
	WHERE name = UPPER(name)
	);
END;

COMMIT;
