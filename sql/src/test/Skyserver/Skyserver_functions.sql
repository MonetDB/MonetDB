START TRANSACTION;
-----------------------IDENTIFIERS------------------------------

------
CREATE FUNCTION fSkyVersion(ObjID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((ObjID / left_shift(cast(2 as bigint),59)), 0x0000000F) AS INT));
END;

CREATE FUNCTION fRerun(ObjID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((ObjID / left_shift(cast(2 as bigint),48)), 0x000007FF) AS INT));
END;

CREATE FUNCTION fRun(ObjID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((ObjID / left_shift(cast(2 as bigint),32)), 0x0000FFFF) AS INT));
END;

CREATE FUNCTION fCamcol(ObjID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((ObjID / left_shift(cast(2 as bigint),29)), 0x00000007) AS INT));
END;

CREATE FUNCTION  fField(ObjID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((ObjID / left_shift(cast(2 as bigint),16)), 0x00000FFF) AS INT));
END;

CREATE FUNCTION fObj(ObjID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((ObjID / left_shift(cast(2 as bigint),0)), 0x0000FFFF) AS INT));
END;

CREATE FUNCTION fSDSS(objid bigint)
RETURNS varchar(64)
BEGIN
    RETURN (
	cast(fSkyVersion(objid) as varchar(6))||'-'||
	cast(fRun(objid) as varchar(6))||'-'||
	cast(fRerun(objid) as varchar(6))||'-'||
	cast(fCamcol(objid) as varchar(6))||'-'||
	cast(fField(objid) as varchar(6))||'-'||
	cast(fObj(objid) as varchar(6))
	);
END;

CREATE FUNCTION fObjidFromSDSS(skyversion int, run int, rerun int, camcol int, field int, obj int)
RETURNS BIGINT
BEGIN
    DECLARE two bigint, sky int;
    SET two = 2;
    SET sky = skyversion;
    IF skyversion=-1 
	THEN SET sky=15;
    END IF;
    RETURN ( cast(sky*left_shift(two,59) + rerun*left_shift(two,48) + 
	run*left_shift(two,32) + camcol*left_shift(two,29) + 
	field*left_shift(two,16)+obj as bigint));
END;

CREATE FUNCTION fObjidFromSDSSWithFF(skyversion int, run int, rerun int, 
				     camcol int, field int, obj int, 
				     firstfield int)
RETURNS BIGINT
BEGIN
    DECLARE two bigint, sky int;
    SET two = 2;
    SET sky = skyversion;
    IF skyversion=-1 
	THEN SET sky=15;
    END IF;
    RETURN ( cast(sky*left_shift(two,59) + rerun*left_shift(two,48) + 
	run*left_shift(two,32) + camcol*left_shift(two,29) + 
	field*left_shift(two,16)+firstfield*left_shift(two,28)+obj as bigint));
END;

-------

CREATE FUNCTION fSpecidFromSDSS(plate int, mjd int, fiber int)
RETURNS BIGINT
BEGIN
    DECLARE two bigint;
    SET two = 2;
    RETURN ( cast(
	  plate*cast(0x0001000000000000 as bigint)
	+   mjd*cast(0x0000000100000000 as bigint)
	+ fiber*cast(0x0000000000400000 as bigint)
	as bigint));
END;

CREATE FUNCTION fPlate(SpecID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((SpecID / cast(0x0001000000000000 as bigint)), 0x0000EFFF ) AS INT));
END;

CREATE FUNCTION fMJD(SpecID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((SpecID / cast(0x0000000100000000 as bigint)), 0x0000FFFF ) AS INT));
END;


CREATE FUNCTION fFiber(SpecID bigint)
RETURNS INT
BEGIN
    RETURN ( cast( bit_and((SpecID / cast(0x0000000000400000 as bigint)), 0x000003FF ) AS INT));
END;


-----------------------FLAGS ACCESS-----------------------------

CREATE FUNCTION fPhotoStatusN(val int)
RETURNS varchar(1000)
BEGIN
    	DECLARE bit int, mask bigint, out varchar(2000);
    	SET bit=32;
	SET out ='';
	WHILE bit > 0 DO
		SET bit = bit-1;
		SET mask = left_shift(cast(2 as bigint),bit);
		CASE 
			WHEN (bit_and(mask,val)=0) THEN SET out = out || ''; 
			ELSE SET out = out || (coalesce((select name from PhotoStatus where value=mask),'')||' ');
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
	    SET mask = left_shift(cast(2 as bigint),bit);
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
	    SET mask = left_shift(cast(2 as bigint),bit);
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
	    SET mask = left_shift(2,bit);
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
	    SET mask = left_shift(cast(2 as bigint),bit);
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
	    SET mask = left_shift(cast(2 as bigint),bit);
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
	    SET mask = left_shift(cast(2 as bigint),bit);
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
	    SET mask = left_shift(cast(2 as bigint),bit);
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
	    SET mask = left_shift(cast(2 as bigint),bit);
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

------------------------VECTOR OPERATIONS----------------------------

CREATE FUNCTION fWedgeV3(x1 float,y1 float, z1 float, x2 float, y2 float, z2 float)
RETURNS TABLE(x float, y float, z float)
BEGIN
    RETURN TABLE(SELECT 
	(y1*z2 - y2*z1) as x,
    	(x2*z1 - x1*z2) as y,
	(x1*y2 - x2*y1) as z);
END;


CREATE FUNCTION fRotateV3(mod varchar(16),cx float,cy float,cz float)
RETURNS TABLE (
	x float, 
	y float, 
	z float)
BEGIN
    -- 
    DECLARE xx float, yy float, zz float;
    --
    SELECT x*cx+y*cy+z*cz INTO xx FROM Rmatrix WHERE mode=mod and row=1;
    SELECT x*cx+y*cy+z*cz INTO yy FROM Rmatrix WHERE mode=mod and row=2;
    SELECT x*cx+y*cy+z*cz INTO zz FROM Rmatrix WHERE mode=mod and row=3;
    --
    RETURN TABLE(SELECT xx as x, yy as y, zz as z);
END;


--------------------------TRANSFORMATIONS AND COMPUTATIONS----------------------------


CREATE FUNCTION fMJDToGMT(mjd float)
RETURNS varchar(32)
BEGIN 
    DECLARE jd int, l int, n int,i int, j int,
	    rem real, days bigint, d int ,m int,
	    y int, hr int, min int, sec float; 
    SET jd = mjd + 2400000.5 + 0.5;   -- convert from MDJ to JD  (the .5 fudge makes it work).
    SET l = jd + 68569; 
    SET n = ( 4 * l ) / 146097;
    SET l = l - ( 146097 * n + 3 ) / 4;
    SET i = ( 4000 * ( l + 1 ) ) / 1461001 ;
    SET l = l - ( 1461 * i ) / 4 + 31 ;
    SET j = ( 80 * l ) / 2447;
    SET d = (l - ( 2447 * j ) / 80);  
    SET l = j / 11;
    SET m = j + 2 - ( 12 * l );
    SET y = 100 * ( n - 49 ) + i + l;
    SET rem =  mjd - floor(mjd); -- extract hh:mm:ss.sssssss  
    SET hr = 24*rem; 
    SET min = 60*(24*rem -hr);
    SET sec = 60*(60*(24*rem -hr)-min);
    RETURN (cast(y as varchar(4)) || '-' || 
                cast(m as varchar(2)) || '-' || 
                cast(d as varchar(2)) || ':' || 
                cast(hr as varchar(2))|| ':' || 
                cast(min as varchar(2))|| ':' || 
                cast(sec as varchar(9))); 
END;


CREATE FUNCTION fDistanceArcMinXYZ(nx1 float, ny1 float, nz1 float, 
					nx2 float, ny2 float, nz2 float)
RETURNS float
BEGIN
    DECLARE d2r float; 
    RETURN ( 2*DEGREES(ASIN(sqrt(left_shift(nx1-nx2,2)+left_shift(ny1-ny2,2)+left_shift(nz1-nz2,2))/2))*60);
END;

CREATE FUNCTION fDistanceArcMinEq(ra1 float, dec1 float, 
                                  ra2 float, dec2 float)
RETURNS float
BEGIN
	DECLARE d2r float,nx1 float,ny1 float,nz1 float, nx2 float,ny2 float,nz2 float;
	SET d2r = PI()/180.0;
	SET nx1  = COS(dec1*d2r)*COS(ra1*d2r);
	SET ny1  = COS(dec1*d2r)*SIN(ra1*d2r);
	SET nz1  = SIN(dec1*d2r);
	SET nx2  = COS(dec2*d2r)*COS(ra2*d2r);
	SET ny2  = COS(dec2*d2r)*SIN(ra2*d2r);
	SET nz2  = SIN(dec2*d2r);

  RETURN ( 2*DEGREES(ASIN(sqrt(left_shift(nx1-nx2,2)+left_shift(ny1-ny2,2)+left_shift(nz1-nz2,2))/2))*60);
END;

CREATE  FUNCTION fDMSbase(deg float, truncat int, precision int)
RETURNS varchar(32)
BEGIN
    DECLARE	
	s char(1), 
	d float, 
	nd int, 
	np int, 
	q varchar(32),
	t varchar(32);
	--
	SET s = '+';
 	IF  deg<0 
		THEN SET s = '-';
	END IF;
	--
	SET t = '00:00:00.0';
	IF (precision < 1) 
		THEN SET precision = 1;
	END IF;
	IF (precision > 10) 
		THEN SET precision = 10;
	END IF;
	SET np = 0;
	WHILE (np < precision-1) DO
		SET t = t||'0';
		SET np = np + 1;
	END WHILE;
	SET d = ABS(deg);
	-- degrees
	SET nd = FLOOR(d);
	SET q  = LTRIM(CAST(nd as varchar(2)));
	SET t  = MS_STUFF(t,3-LENGTH(q),LENGTH(q), q);
	-- minutes
	SET d  = 60.0 * (d-nd);
	SET nd = FLOOR(d);
	SET q  = LTRIM(CAST(nd as varchar(4)));
	SET t  = MS_STUFF(t,6-LENGTH(q),LENGTH(q), q);
	-- seconds
	SET d  = MS_ROUND( 60.0 * (d-nd),precision,truncat );
--	SET d  = 60.0 * (d-nd);
	SET q  = LTRIM(STR(d,precision));
	SET t = MS_STUFF(t,10+precision-LENGTH(q),LENGTH(q), q);
	--
	RETURN(s||t);
END;

CREATE FUNCTION fDMS(deg float)
RETURNS varchar(32)
BEGIN
	Declare default_truncat int, default_precision int;
	SET default_truncat = 0;
	SET default_precision = 2;
    	RETURN fDMSbase(deg,default_truncat,default_precision);
END;

CREATE FUNCTION fHMS(deg float)
RETURNS varchar(32)
BEGIN
	Declare default_truncat int, default_precision int;
	SET default_truncat = 0;
	SET default_precision = 2;
    	RETURN fDMSbase(deg,default_truncat,default_precision);
END;

CREATE  FUNCTION fHMSbase(deg float, truncat int , precision int)
RETURNS varchar(32)
BEGIN
    DECLARE
	d float,
	nd int, 
	np int, 
	q varchar(10),
	t varchar(16);
	--
	SET t = '00:00:00.0';
	IF (precision < 1) 
		THEN SET precision = 1;
	END IF;
	IF (precision > 10) 
		THEN SET precision = 10;
	END IF;
	SET np = 0;
	WHILE (np < precision-1) DO
		SET t = t||'0';
		SET np = np + 1;
	END WHILE;
	SET d = ABS(deg/15.0);
	-- degrees
	SET nd = FLOOR(d);
	SET q  = LTRIM(CAST(nd as varchar(2)));
	SET t  = MS_STUFF(t,3-LENGTH(q),LENGTH(q), q);
	-- minutes
	SET d  = 60.0 * (d-nd);
	SET nd = FLOOR(d);
	SET q  = LTRIM(CAST(nd as varchar(4)));
	SET t  = MS_STUFF(t,6-LENGTH(q),LENGTH(q), q);
	-- seconds
	SET d  = MS_ROUND( 60.0 * (d-nd),precision,truncat );
	SET q  = LTRIM(STR(d,precision));
	SET t = MS_STUFF(t,10+precision-LENGTH(q),LENGTH(q), q);
--	SET d  = 60.0 * (d-nd);
--	SET q = LTRIM(STR(d,3));
--	SET t = MS_STUFF(t,13-LENGTH(q),LENGTH(q), q);
	--
	RETURN(t);
END;

CREATE FUNCTION fIAUFromEq(ra float, dec1 float)
RETURNS varchar(64)
BEGIN
	RETURN('SDSS J'||REPLACE(fHMSbase(ra,1,2)||fDMSbase(dec1,1,1),':',''));
END;

CREATE FUNCTION fMagToFlux(mag real, band int)
RETURNS real
BEGIN
    DECLARE counts1 float, counts2 float, bparm float;
    CASE band
	WHEN 0 THEN SET bparm = 1.4E-10;
	WHEN 1 THEN SET bparm = 0.9E-10;
	WHEN 2 THEN SET bparm = 1.2E-10;
	WHEN 3 THEN SET bparm = 1.8E-10;
	WHEN 4 THEN SET bparm = 7.4E-10;
    END CASE;
    IF (mag < -99.0 ) 
	THEN SET mag = 1.0;
    END IF;
    SET counts1 = (mag/ -1.0857362048) - LOG(bparm);
    SET counts2 = bparm * 3630.78 * (EXP(counts1) - EXP(-counts1));  -- implement SINH()
    RETURN 1.0E9* counts2;
END;

CREATE FUNCTION fMagToFluxErr(mag real, err real, band int)
RETURNS real
BEGIN
    DECLARE flux real, bparm float;
    CASE band
	WHEN 0 THEN SET bparm = 1.4E-10;
	WHEN 1 THEN SET bparm = 0.9E-10;
	WHEN 2 THEN SET bparm = 1.2E-10;
	WHEN 3 THEN SET bparm = 1.8E-10;
	WHEN 4 THEN SET bparm = 7.4E-10;
    END CASE;
    IF (mag < -99.0 )
	THEN SET err = 1.0;
    END IF;
    SET flux = (SELECT fMagToFlux(mag,band));
    RETURN err*SQRT(left_shift(flux,2)+ 4.0E18*left_shift(3630.78*bparm,2))/1.0857362048;
END;

CREATE FUNCTION fEtaToNormal(eta float)
RETURNS TABLE (x float, y float, z float)
BEGIN
    --
    DECLARE x float, y float, z float;
    SET x = SIN(RADIANS(eta));
    SET y = COS(RADIANS(eta));
    SET z = 0.0;
    --
    RETURN TABLE(SELECT v2.x, v2.y, v2.z 
	FROM fRotateV3('S2J', x, y, z) v2);
END;

CREATE FUNCTION fStripeToNormal(stripe int)
RETURNS TABLE (x float, y float, z float)
BEGIN
    --
    DECLARE TABLE t(x float, y float, z float);
    DECLARE x float, y float, z float, eta float;
    --
    IF (stripe < 0 or stripe>86) 
--	THEN return TABLE(SELECT * from t);
	THEN return t;
    END IF;
    IF (stripe is null) 
	THEN SET stripe = 10;	-- default is the equator
    END IF;
    --
    CASE 
	WHEN (stripe<50) THEN SET eta = (stripe-10)*2.5 -32.5;
	ELSE SET eta = (stripe-82)*2.5 -32.5;
    END CASE;
    --
    SET x = SIN(RADIANS(eta));
    SET y = COS(RADIANS(eta));
    SET z = 0.0;
    --
    RETURN TABLE(SELECT v2.x, v2.y, v2.z 
	FROM fRotateV3('S2J',x, y, z) v2);
END;

CREATE FUNCTION fGetLat(mode varchar(8),cx float,cy float,cz float)
RETURNS float
BEGIN
    DECLARE lat float;
    SELECT DEGREES(ASIN(v3.z)) INTO lat FROM fRotateV3(mode,cx,cy,cz) v3;
    RETURN lat;
END;

CREATE FUNCTION fGetLon(mode varchar(8),cx float,cy float,cz float)
RETURNS float
BEGIN
    DECLARE lon float;
    SELECT DEGREES(ATAN(v3.y,v3.x)) INTO lon FROM fRotateV3(mode,cx,cy,cz) v3;
    IF lon<0 
	THEN SET lon=lon+360;
    END IF;
    RETURN lon;
END;

CREATE FUNCTION fGetLonLat(mode varchar(8),cx float,cy float,cz float)
RETURNS TABLE (lon float, lat float)
BEGIN
    --
    DECLARE lon float, lat float;
    --
    SELECT DEGREES(ATAN(v3.y,v3.x)), DEGREES(ASIN(v3.z)) INTO lon, lat
	FROM fRotateV3(mode,cx,cy,cz) v3;
    --
    IF lon<0 
	THEN SET lon=lon+360;
    END IF;
    RETURN TABLE (SELECT lon as lon, lat as lat);
END;

CREATE FUNCTION fEqFromMuNu(
	mu float,
	nu float,
	node float,
	incl float
)
RETURNS TABLE (ra float, decim float, cx float, cy float, cz float)
BEGIN
    DECLARE
	rmu float, rnu float, rin float,
	ra float, deci float, 
	cx float, cy float, cz float,
	gx float, gy float, gz float;
	--
    -- convert to radians
    SET rmu = RADIANS(mu-node);
    SET rnu = RADIANS(nu);
    SET rin = RADIANS(incl);
    --
    SET gx = cos(rmu)*cos(rnu);
    SET gy = sin(rmu)*cos(rnu);
    SET gz = sin(rnu);
    --
    SET cx = gx;
    SET cy = gy*cos(rin)-gz*sin(rin);
    SET cz = gy*sin(rin)+gz*cos(rin);
    --
    SET deci = DEGREES(asin(cz));
    SET ra  = DEGREES(ATAN(cy,cx)) + node;
    IF  ra<0 
	THEN SET ra = ra+360 ;
    END IF;
    --
    SET cx = cos(RADIANS(ra))*cos(RADIANS(deci));
    SET cy = sin(RADIANS(ra))*cos(RADIANS(deci));
    --
    RETURN TABLE (SELECT ra, deci, cx, cy, cz);
END;

CREATE FUNCTION fCoordsFromEq(ra float,deci float)
RETURNS TABLE (
    ra	float,
    decim float,
    stripe int,
    incl float,
    lambda float,
    eta float,
    mu float,
    nu float
)
BEGIN
    DECLARE 
	cx float, cy float, cz float,
	qx float, qy float, qz float,
	lambda float, eta float, 
	stripe int, incl float,
	mu float, nu float,
        stripeEta float;
    --
    SET cx = cos(radians(deci))* cos(radians(ra-95.0));
    SET cy = cos(radians(deci))* sin(radians(ra-95.0));
    SET cz = sin(radians(deci));
    --
    SET lambda = -degrees(asin(cx));
    IF (cy = 0.0 and cz = 0.0)
        THEN SET cy = 1e-16;
    END IF;
    SET eta    =  degrees(ATAN(cz,cy))-32.5;
    SET stripeEta = eta;
    --
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF ABS(lambda) > 90.0
       THEN 
           SET lambda = 180.0-lambda;
           SET eta = eta+180.0;
    END IF;
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF eta < 0.0 
	THEN SET eta = eta+360.0;
    END IF;
    IF eta >= 360.0 
	THEN SET eta = eta-360.0;
    END IF;
    IF ABS(lambda) = 90.0 
	THEN SET eta = 0.0;
    END IF;
    IF eta < -180.0 
	THEN SET eta = eta+360.0;
    END IF;
    IF eta >= 180.0 
	THEN SET eta = eta-360.0;
    END IF;
    IF eta > 90.0-32.5
       THEN
           SET eta = eta-180.0;
           SET lambda = 180.0-lambda;
    END IF;
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    --
    IF  stripeEta<-180 
	THEN SET stripeEta = stripeEta+360;
    END IF;
    SET stripe = 23 + floor(stripeEta/2.5+0.5);
    --
    SET incl = (stripe-10)*2.5;
    IF  stripe>50 
	THEN SET incl = (stripe-82)*2.5;
    END IF;
    --
    SET qx = cx;
    SET qy = cy*cos(radians(incl))+cz*sin(radians(incl));
    SET qz =-cy*sin(radians(incl))+cz*cos(radians(incl));
    --
    SET mu = degrees(ATAN(qy,qx))+95.0;
    SET nu = degrees(asin(qz));
    IF  stripe>50 
	THEN SET mu = mu+360;
    END IF;
    --
    RETURN TABLE (SELECT
	ra, deci, stripe, incl, lambda, eta, mu, nu);
END;

CREATE FUNCTION fMuFromEq(ra float,deci float)
RETURNS float
BEGIN
    DECLARE 
	cx float, cy float, cz float,
	qx float, qy float, qz float,
	eta float, 
	stripe int, incl float,
	mu float;
    --
    SET cx = cos(radians(deci))* cos(radians(ra-95.0));
    SET cy = cos(radians(deci))* sin(radians(ra-95.0));
    SET cz = sin(radians(deci));
    --
    SET eta = degrees(ATAN(cz,cy));
    SET eta = eta -32.5;
    IF  eta<-180 
	THEN SET eta = eta+360;
    END IF;
    SET stripe = 23 + floor(eta/2.5+0.5);
    -- 
    SET incl = (stripe-10)*2.5;
    IF  stripe>50 
	THEN SET incl = (stripe-82)*2.5;
    END IF;
    --
    SET qx = cx;
    SET qy = cy*cos(radians(incl))+cz*sin(radians(incl));
    SET qz =-cy*sin(radians(incl))+cz*cos(radians(incl));
    --
    SET mu = degrees(ATAN(qy,qx))+95.0;
    IF  stripe>50 
	THEN SET mu = mu+360;
    END IF;
    --
    RETURN mu;
END;

CREATE FUNCTION fNuFromEq(ra float,deci float)
RETURNS float
BEGIN
    DECLARE 
	cy float, cz float,
	qz float,
	eta float, 
	stripe int, incl float,
	nu float;
    --
    SET cy = cos(radians(deci))* sin(radians(ra-95.0));
    SET cz = sin(radians(deci));
    --
    SET eta    =  degrees(ATAN(cz,cy));
    SET eta	= eta -32.5;
    IF  eta<-180 
	THEN SET eta = eta+360;
    END IF;
    --
    SET stripe = 23 + floor(eta/2.5+0.5);
    -- 
    SET incl = (stripe-10)*2.5;
    IF  stripe>50 
	THEN SET incl = (stripe-82)*2.5;
    END IF;
    --
    SET qz =-cy*sin(radians(incl))+cz*cos(radians(incl));
    --
    SET nu = degrees(asin(qz));
    --
    RETURN nu;
END;

CREATE FUNCTION fEtaFromEq(ra float,deci float)
RETURNS float
BEGIN
    DECLARE 
	cx float, cy float, cz float,
	lambda float, eta float, 
        stripeEta float;
    --
    SET cx = cos(radians(deci))* cos(radians(ra-95.0));
    SET cy = cos(radians(deci))* sin(radians(ra-95.0));
    SET cz = sin(radians(deci));
    --
    SET lambda = -degrees(asin(cx));
    IF (cy = 0.0 and cz = 0.0)
        THEN SET cy = 1e-16;
    END IF;
    SET eta    =  degrees(ATAN(cz,cy))-32.5;
    SET stripeEta = eta;
    --
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF ABS(lambda) > 90.0
       THEN 
           SET lambda = 180.0-lambda;
           SET eta = eta+180.0;
    END IF;
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF eta < 0.0 
	THEN SET eta = eta+360.0;
    END IF;
    IF eta >= 360.0 
	THEN SET eta = eta-360.0;
    END IF;
    IF ABS(lambda) = 90.0 
	THEN SET eta = 0.0;
    END IF;
    IF eta < -180.0 
	THEN SET eta = eta+360.0;
    END IF;
    IF eta >= 180.0 
	THEN SET eta = eta-360.0;
    END IF;
    IF eta > 57.5	-- 90.0-32.5
       THEN SET eta = eta-180.0;
    END IF;
    --
    RETURN eta;
END;

CREATE FUNCTION fLambdaFromEq(ra float,deci float)
RETURNS float
BEGIN
    DECLARE 
	cx float, cy float, cz float,
	lambda float, eta float, 
        stripeEta float;
    --
    SET cx = cos(radians(deci))* cos(radians(ra-95.0));
    SET cy = cos(radians(deci))* sin(radians(ra-95.0));
    SET cz = sin(radians(deci));
    --
    SET lambda = -degrees(asin(cx));
    IF (cy = 0.0 and cz = 0.0)
        THEN SET cy = 1e-16;
    END IF;
    SET eta    =  degrees(ATAN(cz,cy))-32.5;
    SET stripeEta = eta;
    --
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF ABS(lambda) > 90.0
       THEN
           SET lambda = 180.0-lambda;
           SET eta = eta+180.0;
    END IF;
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF eta < 0.0 
	THEN SET eta = eta+360.0;
    END IF;
    IF eta >= 360.0 
	THEN SET eta = eta-360.0;
    END IF;
    IF ABS(lambda) = 90.0 
	THEN SET eta = 0.0;
    END IF;
    IF eta < -180.0 
	THEN SET eta = eta+360.0;
    END IF;
    IF eta >= 180.0 
	THEN SET eta = eta-360.0;
    END IF;
    IF eta > 90.0-32.5
       THEN
           SET eta = eta-180.0;
           SET lambda = 180.0-lambda;
    END IF;
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    --
    RETURN lambda;
END;

CREATE FUNCTION fMuNuFromEq(
	ra float,
	deci float,
	stripe int,
	node float,
	incl float
)
RETURNS TABLE (mu float, nu float)
BEGIN
    DECLARE
	rra float, rdec float, rin float,
	mu float, nu float,
	qx float, qy float, qz float,
	gx float, gy float, gz float;

    -- convert to radians
    SET rin  = RADIANS(incl);
    SET rra  = RADIANS(ra-node);
    SET rdec = RADIANS(deci);
    --
    SET qx   = cos(rra)*cos(rdec);
    SET qy   = sin(rra)*cos(rdec);
    SET qz   = sin(rdec);
    --
    SET gx =  qx;
    SET gy =  qy*cos(rin)+qz*sin(rin);
    SET gz = -qy*sin(rin)+qz*cos(rin);
    --
    SET nu = DEGREES(ASIN(gz));
    SET mu = DEGREES(ATAN(gy,gx)) + node;
    IF  mu<0 
	THEN SET mu = mu+360 ;
    END IF;
    IF  (stripe>50 AND mu<180) 
	THEN SET mu = mu+360 ;
    END IF;
    --
    RETURN TABLE (SELECT mu, nu);
END;


------------------------STRING OPERATIONS----------------------------

CREATE FUNCTION fTokenNext(s VARCHAR(8000), i int) 
RETURNS VARCHAR(8000)
BEGIN
	DECLARE j INT;
	-- eliminate multiple blanks
	SET j = LOCATE(' ',s,i);
	IF (j >0) 
		THEN RETURN ltrim(rtrim(substring(s,i,j-i)));
    	END IF;
	RETURN '';
END;

CREATE FUNCTION fTokenAdvance(s VARCHAR(8000), i int) 
RETURNS INT
BEGIN
	DECLARE j int;
	-----------------------------
	-- eliminate multiple blanks
	-----------------------------
	SET j = LOCATE(' ',s,i);
	IF (j >0) 
		THEN RETURN j+1;
    	END IF;
	RETURN 0;
END;

CREATE FUNCTION fNormalizeString(s VARCHAR(8000)) 
RETURNS VARCHAR(8000)
BEGIN
	DECLARE i int;
	-----------------------------------------------------
	-- discard leading and trailing blanks, and upshift
	-----------------------------------------------------
	SET s = ltrim(rtrim(upper(s))) || ' ';
	---------------------------
	-- eliminate trailing zeros
	---------------------------
	SET i = patindex('%00 %', s);
	----------------------
	-- trim trailing zeros
	----------------------
	WHILE(i >0) DO			
 		SET s = replace(s, '0 ', ' ');
 		SET i = patindex('%00 %', s);
 	END WHILE;
	----------------------------
	-- eliminate multiple blanks
	----------------------------
	SET i = patindex('%  %', s);
	---------------------
	-- trim double blanks
	---------------------
	WHILE(i >0) DO	
 		SET s = replace(s, '  ', ' ');
 		SET i = patindex('%  %', s);
 	END WHILE;
	------------------
	-- drop minus zero
	------------------
 	SET s = replace(s, '-0.0 ', '0.0 ');
	RETURN s; 
END;

CREATE FUNCTION fTokenStringToTable(types VARCHAR(8000)) 
RETURNS TABLE (token VARCHAR(16))
BEGIN  
	DECLARE tokenStart int;
	DECLARE TABLE tokens(token VARCHAR(16));
	SET tokenStart = 1;
	SET types = fNormalizeString(types);
	WHILE (ltrim(fTokenNext(types,tokenStart)) <> '') DO
		INSERT INTO tokens VALUES(fTokenNext(types,tokenStart));
    		SET tokenStart = fTokenAdvance(types,tokenStart);
	END WHILE;
	RETURN tokens;
END;

CREATE FUNCTION fReplace(OldString VARCHAR(8000), Pattern VARCHAR(1000), Replacement VARCHAR(1000))
RETURNS VARCHAR(8000) 
BEGIN 
	BEGIN
		DECLARE NewString VARCHAR(8000);
		SET NewString = '';
		IF (LTRIM(Pattern) = '') 
			THEN RETURN( NewString || OldString);
		END IF;
		DECLARE LowerOld VARCHAR(8000);
		SET LowerOld = LOWER(OldString);
		DECLARE LowerPattern VARCHAR(8000);
  		SET LowerPattern = LOWER(Pattern);
		DECLARE offset INT;
		SET offset = 0
		DECLARE PatLen INT;
       		SET PatLen = LENGTH(Pattern);
		 
		WHILE (LOCATE(LowerPattern, LowerOld, 1) <> 0 ) DO
			SET offset = LOCATE(LowerPattern, LowerOld, 1);
			SET NewString = NewString || SUBSTRING(OldString,1,offset-1) || Replacement;
			SET OldString = SUBSTRING(OldString,offset||PatLen,LENGTH(OldString)-offset||PatLen);
			SET LowerOld =  SUBSTRING(LowerOld,  offset||PatLen,LENGTH(LowerOld)-offset||PatLen);
		END WHILE;
	RETURN( NewString || OldString);
END;


------------------------HTML SPATIAL INDEX---------------------------

CREATE FUNCTION fIsNumbers (string varchar(8000), start int, stop int)
RETURNS INT
BEGIN 
	DECLARE offset int,		-- current offfset in string
		char	char,		-- current char in string
		dot	int,		-- flag says we saw a dot.
		num	int;		-- flag says we saw a digit
	SET dot = 0;			--
	SET num = 0;			--
	SET offset = start;		-- 
	IF (stop > len(string)) 
		THEN RETURN 0;   -- stop if past end
	END IF;
	SET char = substring(string,offset,1); -- handle sign
	IF(char ='+' or char='-') 
		THEN SET offset = offset + 1;
	END IF;
	-- process number
	WHILE (offset <= stop)	DO-- loop over digits
					-- get next char
		SET char = substring(string,offset,1);
		IF (char = '.') 	-- if a decmial point
			  		-- reject duplicate
			THEN 
				IF (dot = 1) 
					THEN RETURN 0;
				END IF;
				SET dot = 1;	-- set flag
				SET offset = offset + 1;  -- advance
	 		ELSEIF (char<'0' or '9' <char)  -- if not digit
				THEN RETURN 0;	-- reject
			ELSE 		-- its a digit
					-- advance
			     	SET offset = offset + 1;
				SET num= 1;	-- set digit flag

		END IF; 			-- end loop
	-- test for bigint overflow	
		IF (stop-start > 19) 
			THEN RETURN 0; -- reject giant numbers
		END IF;
	      	IF  (dot = 0 and  stop-start >= 19 )
					-- if its a bigint
			THEN
				IF ( ((stop-start)>19) or	-- reject if too big
				('9223372036854775807' > substring(string,start,stop)))
					THEN  RETURN 0;
				END IF;		--
		END IF; 			-- end bigint overflow test
		IF (num = 0) 
			THEN RETURN 0;		-- complain if no digits
		END IF;
		IF (dot = 0) 
			THEN RETURN 1; 	-- number ok, is it an int 
		END IF;
	END WHILE;
	RETURN -1 ;			-- or a float?
END; 				--- end of number syntax check


CREATE FUNCTION   fHtmToString (HTM BIGINT)
RETURNS VARCHAR(1000)
BEGIN
	 DECLARE HTMtemp BIGINT;  	-- eat away at HTM as you parse it.
	 DECLARE Answer  VARCHAR(1000); -- the answer string.
	 DECLARE Triangle  INT;  	-- the triangle id (0..3)
	 SET Answer = ' ';   		--
	 SET HTMtemp = HTM;   		--
	 ------------------------------------------
	 -- loop over the HTM pulling off a triangle till we have a faceid left (1...8)
	 WHILE (HTMtemp > 0) DO
	  	IF (HTMtemp <= 4)   		-- its a face  
	   					-- add face to string.
			THEN
				IF (HTMtemp=3) 
					THEN SET Answer='N'||Answer;
				END IF;
				IF (HTMtemp=2) 
					THEN SET Answer='S'||Answer;
				END IF;
				SET HTMtemp  = 0;
	   		   			-- end face case
	  		ELSE 
	   					-- its a triangle
				SET Triangle = HTMtemp % 4; 	-- get the id into answer
				SET Answer =  CAST(Triangle as VARCHAR(4)) || Answer;
	     			SET HTMTemp = HTMtemp / 4;  	-- move on to next triangle
	  	END IF;
	END WHILE;    			-- end loop
	RETURN(Answer);     			
END;

CREATE FUNCTION fHtmLookupXyz(x float, y float, z float) 
RETURNS bigint 
BEGIN 
	DECLARE cmd varchar(100); 
        SET cmd = 'CARTESIAN 20 ' 
             ||str(x,22,15)||' '||str(y,22,15)||' '||str(z,22,15);
	RETURN fHtmLookup(cmd);
END; 

CREATE FUNCTION fHtmXyz(x float, y float, z float) 
RETURNS bigint
BEGIN  
	RETURN fHtmLookupXyz(x, y, z);
END;

CREATE FUNCTION fHtmLookupEq(ra float, deci float)
RETURNS bigint
BEGIN
	DECLARE x float, y float, z float; 
	SET x  = COS(RADIANS(deci))*COS(RADIANS(ra));
	SET y  = COS(RADIANS(deci))*SIN(RADIANS(ra));
	SET z  = SIN(RADIANS(deci));
	RETURN fHtmLookupXyz(x, y, z);
END;

CREATE FUNCTION fHtmEq(ra float, deci float)
RETURNS bigint
BEGIN
	RETURN fHtmLookupEq(ra,deci);
END;

-----------------SPATIAL ACCESS BASED ON HTM--------------


CREATE FUNCTION fGetNearbyObjAllXYZ (nx float, ny float, nz float, r float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    mode int NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint,
    distance float		-- distance in arc minutes
)
BEGIN
	DECLARE TABLE proxtab(
	    objID bigint,
	    run int NOT NULL,
	    camcol int NOT NULL,
	    field int NOT NULL,
	    rerun int NOT NULL,
	    type int NOT NULL,
	    mode int NOT NULL,
	    cx float NOT NULL,
	    cy float NOT NULL,
	    cz float NOT NULL,
	    htmID bigint,
	    distance float		-- distance in arc minutes
	);
	DECLARE d2r float, cc float,cmd varchar(256);
	if (r<0) 
		THEN RETURN proxtab;
	END IF;
	set d2r = PI()/180.0;
	set cmd = 'CIRCLE CARTESIAN '|| 
			||str(nx,22,15)||' '||str(ny,22,15)||' '||str(nz,22,15)
			|| ' ' || str(r,10,2);

	INSERT INTO proxtab SELECT 
	    objID, 
	    run,
	    camcol,
	    field,
	    rerun,
	    type,
	    mode,
	    cx,
	    cy,
	    cz,
	    htmID,
 	    2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60 
	    --sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/d2r*60 
	    FROM fHtmCover(cmd) , PhotoTag
	    WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend )
	    AND ( (2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60)< r)
	ORDER BY (2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60) ASC
	OPTION(FORCE ORDER, LOOP JOIN);
	RETURN proxtab;
END;

CREATE FUNCTION fGetNearbyObjAllEq (ra float, deci float, r float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    mode tinyint NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint,
    distance float		-- distance in arc minutes
) 
BEGIN
	DECLARE prox TEMPORARY TABLE (
	    objID bigint,
	    run int NOT NULL,
	    camcol int NOT NULL,
	    field int NOT NULL,
	    rerun int NOT NULL,
	    type int NOT NULL,
	    mode tinyint NOT NULL,
	    cx float NOT NULL,
	    cy float NOT NULL,
	    cz float NOT NULL,
	    htmID bigint,
	    distance float		-- distance in arc minutes
	); 
	DECLARE d2r float, nx float,ny float,nz float;
	set d2r = PI()/180.0;
	if (r<0) 
		THEN RETURN proxtab;
	END IF;
	set nx  = COS(deci*d2r)*COS(ra*d2r);
	set ny  = COS(deci*d2r)*SIN(ra*d2r);
	set nz  = SIN(deci*d2r);
	INSERT INTO proxtab	
	SELECT * FROM fGetNearbyObjAllXYZ(nx,ny,nz,r);
	RETURN proxtab;
END;

CREATE FUNCTION fGetNearestObjAllEq (ra float, deci float, r float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    mode int NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint,
    distance float		-- distance in arc minutes
) 
BEGIN
	DECLARE d2r float,nx float,ny float,nz float;
	set d2r = PI()/180.0;
	set nx  = COS(deci*d2r)*COS(ra*d2r);
	set ny  = COS(deci*d2r)*SIN(ra*d2r);
	set nz  = SIN(deci*d2r);
	RETURN TABLE(	
	SELECT * 
	FROM fGetNearbyObjAllXYZ(nx,ny,nz,r)
	ORDER BY distance ASC LIMIT 1);   -- order by needed to get the closest one.
END;

CREATE FUNCTION fGetNearestObjIdEqMode (ra float, deci float, 
					r float, mode int)
RETURNS bigint
BEGIN
    DECLARE nx float, ny float, nz float;
    SET nx = cos(radians(deci))*cos(radians(ra));
    SET ny = cos(radians(deci))*sin(radians(ra));
    SET nz = sin(radians(deci));
    RETURN (
	select objID 
	from fGetNearbyObjAllXYZ(nx,ny,nz,r)
	where mode = mode
	order by distance asc LIMIT 1); 
END;

CREATE FUNCTION fGetNearbyObjXYZ (nx float, ny float, nz float, r float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint,
    distance float		-- distance in arc minutes
)
BEGIN
	DECLARE proxtab TEMPORAY TABLE (
	    objID bigint,
	    run int NOT NULL,
	    camcol int NOT NULL,
	    field int NOT NULL,
	    rerun int NOT NULL,
	    type int NOT NULL,
	    cx float NOT NULL,
	    cy float NOT NULL,
	    cz float NOT NULL,
	    htmID bigint,
	    distance float		-- distance in arc minutes
	);
	DECLARE d2r float, 
	    	cc float,
		cmd varchar(256);
	if (r<0) 
		THEN RETURN proxtab;
	END IF;
	set d2r = PI()/180.0				   
	set cmd = 'CIRCLE CARTESIAN '  
			|| str(nx,22,15)||' '||str(ny,22,15)||' '||str(nz,22,15)
			|| ' ' || str(r,10,2)
	INSERT INTO proxtab SELECT 
	    objID, 
	    run,
	    camcol,
	    field,
	    rerun,
	    type,
	    cx,
	    cy,
	    cz,
	    htmID,
 	    2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60 
	    --sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/d2r*60 
	    FROM fHtmCover(cmd), PhotoPrimary
	    WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend )
	    AND ( (2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60)< r) 
	ORDER BY (2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60) ASC
	OPTION(FORCE ORDER, LOOP JOIN);
	RETURN proxtab;
END;

CREATE FUNCTION fGetNearestObjXYZ (nx float, ny float, nz float, r float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint,
    distance float		-- distance in arc minutes
)
BEGIN
	RETURN TABLE(	
	SELECT * 
	FROM fGetNearbyObjXYZ(nx,ny,nz,r)
	ORDER BY distance ASC LIMIT 1);
END;

CREATE FUNCTION fGetNearbyObjEq (ra float, deci float, r float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint,
    distance float		-- distance in arc minutes
) 
BEGIN
	DECLARE proxtab TEMPORARY TABLE (
	    objID bigint,
	    run int NOT NULL,
	    camcol int NOT NULL,
	    field int NOT NULL,
	    rerun int NOT NULL,
	    type int NOT NULL,
	    cx float NOT NULL,
	    cy float NOT NULL,
	    cz float NOT NULL,
	    htmID bigint,
	    distance float		-- distance in arc minutes
	); 
	DECLARE d2r float, nx float,ny float,nz float;
	set d2r = PI()/180.0;
	if (r<0) 
		THEN RETURN proxtab;
	END IF;
	set nx  = COS(deci*d2r)*COS(ra*d2r);
	set ny  = COS(deci*d2r)*SIN(ra*d2r);
	set nz  = SIN(deci*d2r);
	INSERT INTO proxtab	
	SELECT * FROM fGetNearbyObjXYZ(nx,ny,nz,r);
	RETURN proxtab;
END;

CREATE FUNCTION fGetNearestObjEq (ra float, deci float, r float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint,
    distance float		-- distance in arc minutes
)
BEGIN
	DECLARE d2r float,nx float,ny float,nz float;
	set d2r = PI()/180.0;
	set nx  = COS(deci*d2r)*COS(ra*d2r);
	set ny  = COS(deci*d2r)*SIN(ra*d2r);
	set nz  = SIN(deci*d2r);
	RETURN TABLE(	
	SELECT * 
	FROM fGetNearbyObjXYZ(nx,ny,nz,r)
	ORDER BY distance ASC LIMIT 1);   -- order by needed to get the closest one.
END;

CREATE FUNCTION fGetNearestObjIdEq(ra float, deci float, r float)
RETURNS bigint
BEGIN
    RETURN (select objID from fGetNearestObjEq(ra,deci,r)); 
END;

CREATE FUNCTION fGetNearestObjIdAllEq(ra float, deci float, r float)
RETURNS bigint
BEGIN
    RETURN (select objID from fGetNearestObjAllEq(ra,deci,r));
END;

CREATE FUNCTION fGetNearestObjIdEqType (ra float, deci float, r float, t int)
RETURNS bigint
BEGIN
    RETURN (	select objID 
		from fGetNearbyObjEq(ra,deci,r)
		where type=t 
		order by distance asc LIMIT 1); 
END;

CREATE FUNCTION fGetObjFromRect (ra1 float, ra2 float, 
				 dec1 float, dec2 float)
RETURNS TABLE (
    objID bigint,
    run int NOT NULL,
    camcol int NOT NULL,
    field int NOT NULL,
    rerun int NOT NULL,
    type int NOT NULL,
    cx float NOT NULL,
    cy float NOT NULL,
    cz float NOT NULL,
    htmID bigint
)
BEGIN
	declare d2r float, cmd varchar(1000), radius float, 
	    dot float, d1 float, d2 float,
	    level int, shift bigint, ra float, deci float,
	    nx1 float, ny1 float, nz1 float,
	    nx2 float, ny2 float, nz2 float,
	    nx float, ny float, nz float;
	-- calculate approximate center
	set ra  = (ra1+ra2)/2;
	set deci = (dec1+dec2)/2;
	--
	set d2r  = PI()/180.0;
	-- n1 is the normal vector to the plane of great circle 1
	set nx1  = SIN(ra1*d2r) * COS(dec1*d2r);
	set ny1  = COS(ra1*d2r) * COS(dec1*d2r);
	set nz1  = SIN(dec1*d2r);
	--
	set nx2  = SIN(ra2*d2r) * COS(dec2*d2r);
	set ny2  = COS(ra2*d2r) * COS(dec2*d2r);
	set nz2  = SIN(dec2*d2r);
	--
	set nx  = SIN(ra*d2r) * COS(deci*d2r);
	set ny  = COS(ra*d2r) * COS(deci*d2r);
	set nz  = SIN(deci*d2r);
	--
	set d1 = nx1*nx+ny1*ny+nz1*nz;
	set d2 = nx2*nx+ny2*ny+nz2*nz;
	if d1<d2 
		THEN SET dot=d1; 
		ELSE SET dot=d2;
	END IF;
	set radius = ACOS(dot)/d2r*60.0;
	RETURN TABLE(SELECT
	    objID, run, camcol, field, rerun, type,
	    cx, cy, cz, htmID
	from fGetNearbyObjEq(ra,deci,radius)
	    WHERE (cz>nz1) AND (cz<nz2) 
		AND (-cx*nx1+cy*ny1>0)
		AND (cx*nx2-cy*ny2)>0);
END;

CREATE FUNCTION fGetObjectsEq(flag int, ra float, 
				deci float, r float, zoom float)
RETURNS table(ra float, [dec] float, flag int, objid bigint)
BEGIN
	DECLARE table obj(ra float, [dec] float, flag int, objid bigint);
        DECLARE nx float,
                ny float,
                nz float,
                cmd varchar(1000),
                rad float,
                mag float;
	set rad = r;
        if (rad > 250) 
		THEN set rad = 250;      -- limit to 4.15 degrees == 250 arcminute radius
	END IF;
        set nx  = COS(RADIANS(deci))*COS(RADIANS(ra));
        set ny  = COS(RADIANS(deci))*SIN(RADIANS(ra));
        set nz  = SIN(RADIANS(deci));
        set mag =  25 - 1.5* zoom;  -- magnitude reduction.
        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)||' '||str(deci,22,15)||' '||str(rad,5,2);

        if ( bit_and(flag, 1) > 0 )  -- specObj
            THEN 
                INSERT INTO obj
                SELECT ra, dec,  1 as flag, specobjid as objid
                FROM fHtmCover(cmd) , SpecObj 
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
        END IF;

        if ( bit_and(flag, 2) > 0 )  -- photoObj
            THEN 
                INSERT INTO obj
                SELECT ra, dec, 2 as flag, objid
                FROM fHtmCover(cmd) , PhotoPrimary 
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad)
                and (r <= mag );
         END IF;

        if ( bit_and(flag, 4) > 0 )  -- target
            THEN 
                INSERT INTO obj
                SELECT ra, dec, 4 as flag, targetid as objid
                FROM fHtmCover(cmd) , Target 
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
	END IF;

        if ( bit_and(flag, 8) > 0 )  -- mask
            THEN
                INSERT INTO obj
                SELECT ra, dec, 8 as flag, maskid as objid
                FROM fHtmCover(cmd) , Mask 
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
	END IF;

        if ( bit_and(flag, 16) > 0 ) -- plate
            THEN
                SET rad = r + 89.4;   -- add the tile radius
	        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)
			||' '||str(deci,22,15)||' '||str(rad,5,2);
                INSERT INTO obj
                SELECT ra, dec, 16 as flag, plateid as objid
                FROM fHtmCover(cmd) , PlateX
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
	END IF;

        if ( bit_and(flag, 32) > 0 )  -- photoPrimary and secondary
            THEN
                INSERT INTO obj
                SELECT ra, dec, 2 as flag, objid
                FROM fHtmCover(cmd) , PhotoObjAll
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad)
		AND mode in (1,2);
	END IF;
        --
        RETURN obj;
END;

CREATE   FUNCTION fGetObjectsMaskEq(flag int, ra float, deci float, r float, zoom float)
RETURNS table (ra float, [dec] float, flag int, objid bigint)
BEGIN
	DECLARE table obj(ra float, [dec] float, flag int, objid bigint);
        DECLARE nx float,
                ny float,
                nz float,
                cmd varchar(1000),
                rad float,
                mag float;
	set rad = r;
        if (rad > 250) 
		THEN set rad = 250;
	END IF;      -- limit to 4.15 degrees == 250 arcminute radius
        set nx  = COS(RADIANS(deci))*COS(RADIANS(ra));
        set ny  = COS(RADIANS(deci))*SIN(RADIANS(ra));
        set nz  = SIN(RADIANS(deci));
        set mag =  25 - 1.5* zoom;  -- magnitude reduction.
        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)||' '||str(deci,22,15)||' '||str(rad,5,2);

        if ( bit_and(flag, 1) > 0 )  -- specObj
            THEN
                INSERT INTO obj
                SELECT ra, dec,  1 as flag, specobjid as objid
                FROM fHtmCover(cmd) , SpecObj
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
	END IF;
        if ( bit_and(flag, 2) > 0 )  -- photoObj
            THEN
                INSERT INTO obj
                SELECT ra, dec, 2 as flag, objid
                FROM fHtmCover(cmd) , PhotoPrimary
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad)
                and (r <= mag );
	END IF;
        if ( bit_and(flag, 4) > 0 )  -- target
            THEN
                INSERT INTO obj
                SELECT ra, dec, 4 as flag, targetid as objid
                FROM fHtmCover(cmd) , Target
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
	END IF;
        if ( bit_and(flag, 8) > 0 )  -- mask
            THEN
	        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)
			||' '||str(deci,22,15)||' '||str(rad+15,5,2);
                INSERT INTO obj
                SELECT ra, dec, 8 as flag, maskid as objid
                FROM fHtmCover(cmd) , Mask
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< (rad+radius));
	END IF;
        if ( bit_and(flag, 16) > 0 ) -- plate
            THEN
                SET rad = r + 89.4;   -- add the tile radius
	        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)
			||' '||str(deci,22,15)||' '||str(rad,5,2);
                INSERT INTO obj
                SELECT ra, dec, 16 as flag, plateid as objid
                FROM fHtmCover(cmd) , PlateX 
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
	END IF;
        --
        RETURN obj;
END;

CREATE FUNCTION fGetNearbyFrameEq (ra float, deci float, 
					radius float, zoom int)
RETURNS TABLE (
	fieldID 	bigint NOT NULL,
	a 		float NOT NULL ,
	b 		float NOT NULL ,
	c 		float NOT NULL ,
	d 		float NOT NULL ,
	e 		float NOT NULL ,
	f 		float NOT NULL ,
	node 		float NOT NULL ,
	incl 		float NOT NULL ,
        distance        float NOT NULL		-- distance in arc minutes 
)
BEGIN
	DECLARE d2r float,nx float,ny float,nz float,r float,
	    cc float,cmd varchar(80);
	set r = radius;
	set d2r = PI()/180.0;
	set nx  = COS(dec*d2r)*COS(ra*d2r);
	set ny  = COS(dec*d2r)*SIN(ra*d2r);
	set nz  = SIN(dec*d2r);
	set cc  = COS(r*d2r/60);     -- cos(r) (r converted to radians)	
	set cmd = 'CIRCLE J2000 ' ||str(ra,22,15)||' '||str(deci,22,15)||' '||str(r,5,2);
	RETURN TABLE(SELECT  
	    fieldID, 
	    a,b,c,d,e,f,node,incl,
            (2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60)
	    FROM fHtmCover(cmd) , Frame
	    WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
	    AND zoom = zoom
	    AND ( (2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60)< r) 
	    ORDER BY (2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)+power(nz-cz,2))/2))*60) ASC
	OPTION(FORCE ORDER, LOOP JOIN));
END;

CREATE FUNCTION fGetNearestFrameEq (ra float, deci float, zoom int)
RETURNS TABLE (
	fieldID 	bigint NOT NULL,
	a 		float NOT NULL ,
	b 		float NOT NULL ,
	c 		float NOT NULL ,
	d 		float NOT NULL ,
	e 		float NOT NULL ,
	f 		float NOT NULL ,
	node 		float NOT NULL ,
	incl 		float NOT NULL ,
        distance        float NOT NULL;		-- distance in arc minutes 
) 
BEGIN
	RETURN TABLE(	
	    SELECT fieldID, a, b, c, d, e, f, node, incl, distance  -- look up to 81
	    FROM fGetNearbyFrameEq (ra , deci, 81, zoom )	-- arcmin away from center.
            ORDER BY distance ASC LIMIT 1);   
END;

CREATE FUNCTION fGetNearestFrameidEq (ra float, deci float, zoom int)
RETURNS bigint
BEGIN
	RETURN (select fieldID from fGetNearestFrameEq(ra, deci, zoom) );
END;


----------------------REGION OPERATIONS-------------------------------------


-------------------------URL GENERATION----------------------------------------

CREATE FUNCTION fGetUrlFitsField(fieldId bigint)
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), run varchar(8), rerun varchar(8),
		run6 varchar(10), stripe varchar(8), camcol varchar(8), 
		field varchar(8), startMu varchar(10), skyVersion varchar(8);
	SET link = (select val from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT cast(fSkyVersion(fieldid) as varchar(8)) INTO skyVersion;
	IF (skyVersion = '0')
		THEN SET link = link || 'inchunk_target/';
	ELSEIF (skyVersion = '1')
		THEN SET link = link || 'inchunk_best/';
	ELSE
		SET link = link || 'inchunk_runs/';
	END IF;
	SELECT  cast(f.run as varchar(8)), 
		cast(f.rerun as varchar(8)), 
		cast(s.startMu as varchar(10)), 
		cast(s.stripe as varchar(8)),
		cast(f.camcol as varchar(8)), 
		cast(f.field as varchar(8))
	    INTO run, rerun, startMu, stripe, camcol, field
	    FROM Field f, Segment s
	    WHERE f.fieldId=fieldId and s.segmentID = f.segmentID; 
	SET run6   = substring('000000',1,6-len(run)) || run;
	SET field = substring('0000',1,4-len(field)) || field;
	RETURN 	 link || 'stripe' || stripe || '_mu' || startMu || '_' 
		|| skyVersion || '/'||camcol||'/tsField-'||run6||'-'
		||camcol||'-'||rerun||'-'||field||'.fit';
END;

CREATE FUNCTION fGetUrlFitsSpectrum(specObjId bigint)
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), plate varchar(8), mjd varchar(8), fiber varchar(8);
	SET link = (select val from SiteConstants where name='DataServerURL');
	SET link = link || 'spectro/1d_23/';
	SELECT cast(p.mjd as varchar(8)), cast(p.plate as varchar(8)), cast(s.fiberID as varchar(8))
	    INTO mjd, plate, fiber 
	    FROM PlateX p, specObjAll s 
	    WHERE p.plateId=s.plateId AND s.specObjID=specObjId;
	SET plate = substring('0000',1,4-len(plate)) || plate;
	SET fiber = substring( '000',1,3-len(fiber)) || fiber;
	RETURN 	 link || plate || '/1d/spSpec-'||mjd||'-'||plate||'-'||fiber||'.fit';
END;

CREATE FUNCTION fGetUrlSpecImg(specObjId bigint)
returns varchar(256)
begin
	declare WebServerURL varchar(500);
	set WebServerURL = 'http://localhost/';
	select cast(value as varchar(500))
		into WebServerURL
		from SiteConstants
		where name ='WebServerURL';
	return WebServerURL || 'get/specById.asp?id=' 
		|| cast(coalesce(specObjId,0) as varchar(32));
end;

CREATE FUNCTION fGetUrlFitsAtlas(fieldId bigint)
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), run varchar(8), rerun varchar(8),
		camcol varchar(8), field varchar(8), run6 varchar(10);
	SET link = (select val from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  cast(run as varchar(8)), cast(rerun as varchar(8)), 
		cast(camcol as varchar(8)), cast(field as varchar(8))
	    INTO run, rerun, camcol, field
	    FROM Field
	    WHERE fieldId=fieldId;
	SET run6   = substring('000000',1,6-len(run)) || run;
	SET field = substring('0000',1,4-len(field)) || field;
	RETURN 	 link || run || '/' || rerun || '/objcs/'||camcol||'/fpAtlas-'||run6||'-'||camcol||'-'||field||'.fit.gz';
END;

CREATE FUNCTION fGetUrlNavEq(ra float, deci float)
returns varchar(256)
begin
	declare WebServerURL varchar(500);
	set WebServerURL = 'http://localhost/';
	select cast(value as varchar(500))
		into WebServerURL
		from SiteConstants
		where name ='WebServerURL';
	return WebServerURL || 'tools/chart/navi.asp?zoom=1&ra=' 
		|| ltrim(str(ra,10,6)) || '&dec=' || ltrim(str(deci,10,6));
end;

CREATE FUNCTION fGetUrlFitsBin(fieldId bigint, filter varchar(4))
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), run varchar(8), rerun varchar(8),
		camcol varchar(8), field varchar(8), run6 varchar(10);
	SET link = (select val from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  cast(run as varchar(8)), cast(rerun as varchar(8)), 
		cast(camcol as varchar(8)), cast(field as varchar(8))
	    INTO run, rerun, camcol, field
	    FROM Field
	    WHERE fieldId=fieldId;
	SET run6   = substring('000000',1,6-len(run)) || run;
	SET field = substring('0000',1,4-len(field)) || field;
	RETURN 	 link || run || '/' || rerun || '/objcs/'||camcol||'/fpBIN-'||run6||'-'||filter||camcol||'-'||field||'.fit.gz';
END;

CREATE FUNCTION fGetUrlFitsMask(fieldId bigint, filter varchar(4))
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), run varchar(8), rerun varchar(8),
		camcol varchar(8), field varchar(8), run6 varchar(10);
	SET link = (select val from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  cast(run as varchar(8)), cast(rerun as varchar(8)), 
		cast(camcol as varchar(8)), cast(field as varchar(8))
	    INTO run, rerun, camcol, field
	    FROM Field
	    WHERE fieldId=fieldId;
	SET run6   = substring('000000',1,6-len(run)) || run;
	SET field = substring('0000',1,4-len(field)) || field;
	RETURN 	 link || run || '/' || rerun || '/objcs/'||camcol||'/fpM-'||run6||'-'||filter||camcol||'-'||field||'.fit.gz';
END;

CREATE FUNCTION fGetUrlFrameImg(frameId bigint, zoom int)
returns varchar(256)
begin
	declare WebServerURL varchar(500);
	set WebServerURL = 'http://localhost/';
	select cast(value as varchar(500))
		into WebServerURL
		from SiteConstants
		where name ='WebServerURL';
	return WebServerURL || 'get/frameById.asp?id=' 
		|| cast(frameId as varchar(32))
		|| '&zoom=' || cast(zoom as varchar(6));
end;

CREATE FUNCTION fGetUrlFitsCFrame(fieldId bigint, filter varchar(4))
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), run varchar(8), rerun varchar(8),
		camcol varchar(8), field varchar(8), run6 varchar(10);
	SET link = (select val from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  cast(run as varchar(8)), cast(rerun as varchar(8)), 
		cast(camcol as varchar(8)), cast(field as varchar(8))
	    INTO run, rerun, camcol, field
	    FROM Field
	    WHERE fieldId=fieldId;
	SET run6   = substring('000000',1,6-len(run)) || run;
	SET field = substring('0000',1,4-len(field)) || field;
	RETURN 	 link || run || '/' || rerun || '/corr/'||camcol||'/fpC-'||run6||'-'||filter||camcol||'-'||field||'.fit.gz';
END;

CREATE FUNCTION fGetUrlExpId(objId bigint)
returns varchar(256)
begin
	declare WebServerURL varchar(500);
	declare ra float;
	declare deci float;
	set ra = 0
	set deci = 0;
	set WebServerURL = 'http://localhost/';
	select cast(value as varchar(500))
		into WebServerURL
		from SiteConstants where name ='WebServerURL'; 
	select ra, dec
	into ra, dec
	from PhotoObjAll
	where objID = objId;
	return WebServerURL ||'tools/explore/obj.asp?id=' 
		|| cast(objId as varchar(32));
end;

CREATE FUNCTION fGetUrlExpEq(ra float, deci float)
returns varchar(256)
	begin
	declare WebServerURL varchar(500);
	set WebServerURL = 'http://localhost/';
	select cast(value as varchar(500))
		into WebServerURL
		from SiteConstants
		where name ='WebServerURL';
	return WebServerURL || 'tools/explore/obj.asp?ra=' 
		|| ltrim(str(ra,10,6)) || '&dec=' || ltrim(str(deci,10,6))
end;

CREATE FUNCTION fGetUrlNavId(objId bigint)
returns varchar(256)
begin
	declare WebServerURL varchar(500);
	declare ra float;
	declare deci float;
	set ra = 0
	set deci = 0;
	set WebServerURL = 'http://localhost/';
	select cast(value as varchar(500))
		into WebServerURL
		from SiteConstants where name ='WebServerURL'; 
	select ra, dec
	into ra, dec
	from PhotoObjAll
	where objID = objId;
	return WebServerURL ||'tools/chart/navi.asp?zoom=1&ra=' 
		|| ltrim(str(ra,10,10)) || '&dec=' || ltrim(str(deci,10,10));
	end


;

---------------------------NAME GENERATION-----------------------------

CREATE FUNCTION fIndexName(
	code char(1),
	tablename varchar(100),
	fieldList varchar(1000),
	foreignKey varchar(1000)
)
RETURNS varchar(32)
BEGIN
	DECLARE constraint varchar(2000), 
		head varchar(8),
		fk varchar(1000);
	--
	SET head = CASE code 
		WHEN 'K' THEN 'pk_';
		WHEN 'F' THEN 'fk_';
		WHEN 'I' THEN 'i_';
		END CASE;
	--
	SET fk = replace(replace(replace(foreignKey,',','_'),')',''),'(','_');
	SET constraint = head || tableName || '_'
		|| replace(replace(fieldList,' ',''),',','_');
	IF foreignkey <> '' 
		THEN SET constraint = constraint || '_' || fk;
	END IF;
	--
	SET constraint = substring(constraint,1,32);
	SET constraint = replace(replace(constraint,'',''),'','');
	RETURN constraint;
END;

CREATE FUNCTION fTileFileName (zoom int, 
	run int, rerun int,camcol int, field int)  
RETURNS varchar(512)
BEGIN
    DECLARE TheName VARCHAR(100), field4 char(4), 
	run6 char(6), c1 char(1), z2 char(2);
	-----------------------------------------
	SET field4 = cast( field as varchar(4));
	SET field4 = substring('0000',1,4-len(field4)) || field4;
	SET run6 = cast( run as varchar(6));
	SET run6 = substring('000000',1,6-len(run6)) || run6;
	SET z2 = cast( zoom as varchar(2));
	SET z2 = substring('00',1,2-len(z2)) || z2;
	SET c1   = cast(camcol as char(1));
	--
	SET TheName = c1 || '\\' || 'fpCi-' || run6 ||'-'|| c1||'-'||cast(rerun as varchar(4))||'-'
			|| field4 ||'-z'||z2|| '.jpeg';
	RETURN TheName;
END;

-------------------------DOCUMENTATION------------------------


CREATE FUNCTION fDocColumnsWithRank(TableName varchar(400))
RETURNS TABLE (
	enum	varchar(64),
	name		varchar(64),
	type 		varchar(32),
	length	int,
	unit		varchar(64),
	ucd		varchar(64),
	description	varchar(2048),
	rank		int
)
BEGIN
    RETURN TABLE(
    select  enum, name, type, length, unit, ucd, description, rank 
    from ( SELECT	distinct convert(c.name USING sysname) as name,
			t.name as type,
			coalesce(d.length, c.length) as length,
			colid as ColNumber,
			m.unit,
			m.enum,
			m.ucd,
			m.description,
			m.rank
		FROM 	sysobjects o,
			sysusers u,
   			master.spt_datatype_info d,
			systypes t,
			syscolumns c,
			DBColumns m
		WHERE o.name = TableName
		    AND o.uid = u.uid
		    AND c.id = o.id
		    AND t.xtype = c.xtype
		    AND d.ss_dtype = c.xtype
 		    AND coalesce(d.AUTO_INCREMENT,0) = 
		    	coalesce(ColumnProperty (c.id, c.name, 'IsIdentity'),0)
		    AND (m.tablename = TableName 
			OR (m.tablename IN 
				(select distinct b.parent from DBViewCols a, DBViewCols b 
				where a.viewname = TableName AND a.parent = b.viewname)
				) 
			OR (m.tablename IN 
				(select distinct parent 
				from DBViewCols where viewname = TableName )
			)
		    )
		    AND m.name  = c.name
		) as Column
	order by ColNumber);
END;

CREATE FUNCTION fDocColumns(TableName varchar(400))
RETURNS TABLE (
	enum	varchar(64),
	name		varchar(64),
	type 		varchar(32),
	length	int,
	unit		varchar(64),
	ucd		varchar(64),
	description	varchar(2048)
)
BEGIN
    RETURN TABLE(
    select  enum, name, type, length, unit, ucd, description
    from ( SELECT	distinct convert(c.name USING sysname) as name,
			t.name as type,
			coalesce(d.length, c.length) as length,
			colid as ColNumber,
			m.unit,
			m.enum,
			m.ucd,
			m.description
		FROM 	sysobjects o,
			sysusers u,
   			master.spt_datatype_info d,
			systypes t,
			syscolumns c,
			DBColumns m
		WHERE o.name = TableName
		    AND o.uid = u.uid
		    AND c.id = o.id
		    AND t.xtype = c.xtype
		    AND d.ss_dtype = c.xtype
 		    AND coalesce(d.AUTO_INCREMENT,0) = 
		    	coalesce(ColumnProperty (c.id, c.name, 'IsIdentity'),0)
		    AND (m.tablename = TableName 
			OR (m.tablename IN 
				(select distinct b.parent from DBViewCols a, DBViewCols b 
				where a.viewname = TableName AND a.parent = b.viewname)
				) 
			OR (m.tablename IN 
				(select distinct parent 
				from DBViewCols where viewname = TableName )
			)
		    )
		    AND m.name  = c.name
		) as Column
	order by ColNumber);
END;

CREATE FUNCTION fDocFunctionParams (FunctionName varchar(400))
RETURNS TABLE (
	name		varchar(64),
	type 		varchar(32),
	length	int,
	inout		varchar(8),
	pnum		int
)
BEGIN
    RETURN TABLE(
    SELECT  name, type, length, 
	(case output when 'yes' then 'output' else 'input' end) as inout,
	pnum
    FROM ( 
	SELECT	distinct convert(c.name USING sysname),
		t.name,
		coalesce(d.length, c.length), 
		case (substring(c.name,1,1)) 
			when '' then 'yes' else 'no' end,   
		case isoutparam  
			when 1 then 'yes' else 
			    case substring(c.name,1,1)  
				when '' then 'no' else 'yes' end
			end,
		colid  
		INTO name, type, length, input, output, pnum
		FROM 	sysobjects o,
			sysusers u,
   			master.spt_datatype_info d,
			systypes t,
			syscolumns c
		WHERE o.name =FunctionName
		  AND o.uid = u.uid
		  AND c.id = o.id
		  AND t.xtype = c.xtype
		  AND d.ss_dtype = c.xtype
 		  AND coalesce(d.AUTO_INCREMENT,0) = 
		    	coalesce(ColumnProperty (c.id, c.name, 'IsIdentity'),0)
		) as Param
	order by output, pnum asc);
END;


----------------------------------OTHERS------------------------------------


CREATE FUNCTION fSpecDescription(specObjID bigint)
RETURNS varchar(1000)
BEGIN
	DECLARE itClass bigint, itZStatus bigint, itZWarning bigint ;
	--
	SET itClass  = (SELECT specClass FROM SpecObjAll WHERE specObjId=specObjId);
	SET itZStatus = (SELECT zStatus   FROM SpecObjAll WHERE specObjId=specObjId);
	SET itZWarning  = (SELECT zWarning  FROM SpecObjAll WHERE specObjId=specObjId);
	RETURN 	(select fSpecClassN(itClass)) ||'| '
		||(select fSpecZStatusN(itZStatus))||'|'
		||(select fSpecZWarningN(itZWarning))||'|';
END;

CREATE FUNCTION fPhotoDescription(ObjectID bigint)
RETURNS varchar(1000)
BEGIN
	DECLARE itStatus bigint;
	DECLARE itFlags bigint ;
	--
	SET itStatus = (SELECT status FROM PhotoObjAll WHERE objID = ObjectID);
	SET itFlags  = (SELECT  flags FROM PhotoObjAll WHERE objID = ObjectID); 
	RETURN 	(select fPhotoStatusN(itSTatus)) ||'| '
		||(select fPhotoFlagsN(itFlags))||'|';
END;

CREATE FUNCTION fEnum(
	val binary(8), 
	type varchar(32), 
	length int)
RETURNS varchar(64)
BEGIN
    DECLARE vstr varchar(64),
	vbin4 binary(4),
	vint int;
    SET vbin4 = CAST(val as binary(4));
    SET vint  = cAST(val as int);
    --	
    IF (type<>'binary')
	THEN SET vstr = CAST(vint as varchar(64));
    ELSE( 
        IF (length = 8)
	   THEN EXEC master..xp_varbintohexstr val, vstr OUTPUT;	
        ELSE ( 
	    EXEC master..xp_varbintohexstr vbin4, vstr OUTPUT;
          -- also handle the tinyint and smallint cases
            IF (length = 2)
               THEN SET vstr = CAST(vstr as varchar(6));
            ELSE 
                IF (length=1)
		    THEN SET vstr = CAST(vstr as varchar(4));
	        END IF;
            END IF;
        END IF;
    END IF;
    RETURN vstr;
END;

CREATE FUNCTION fFirstFieldBit()
RETURNS BIGINT
BEGIN
    RETURN cast(0x0000000010000000 as bigint);
END;

CREATE FUNCTION fObjID(objID bigint)
RETURNS BIGINT
BEGIN
    DECLARE firstfieldbit bigint;
    SET firstfieldbit = 0x0000000010000000;
    SET objID = objID & ~firstfieldbit;
    IF (select count(*) from PhotoTag  where objID = objID) > 0
        THEN RETURN objID;
    	ELSE(
        	SET objID = objID + firstfieldbit;
        	IF (select count(*) from PhotoTag  where objID = objID) > 0
            		THEN RETURN objID;
		END IF;
	);
    END IF;
    RETURN cast(0 as bigint);
END;

CREATE FUNCTION fPrimaryObjID(objID bigint)
RETURNS BIGINT
BEGIN
    DECLARE firstfieldbit bigint;
    SET firstfieldbit = 0x0000000010000000;
    SET objID = objID & ~firstfieldbit;
    IF (select count(*) from PhotoPrimary  where objID = objID) > 0
        THEN RETURN objID;
    	ELSE (
	        SET objID = objID + firstfieldbit;
       		IF (select count(*) from PhotoPrimary  where objID = objID) > 0
            		THEN RETURN objID;
		END IF;
	);
    END IF;
    RETURN cast(0 as bigint);
END;

CREATE FUNCTION  fDatediffSec(start datetime, now datetime) 
RETURNS float
BEGIN
  RETURN(datediff(millisecond, start, now)/1000.0);
END;   			-- End fDatediffSec()

CREATE FUNCTION fRegionFuzz(d float, buffer float) 
RETURNS float
BEGIN 
	DECLARE fuzzR float;
	SET fuzzR = RADIANS(buffer/60.00000000);
	-----------------------------------------
	-- convert it to a normal form (blank separated trailing blank, upper case)
	-----------------------------------------
	IF d >  1 
		THEN SET d = 1;
	END IF;
	IF d < -1 
		THEN SET d = -1;
	END IF;
	CASE WHEN ACOS(d) + fuzzR <PI() 
		  	THEN SET d = COS(ACOS(d)+fuzzR);
		  	ELSE SET d = -1 ;
	END CASE;
	RETURN d;
END;

CREATE FUNCTION fStripeOfRun(run int)
RETURNS int as
BEGIN
  RETURN (SELECT stripe from Segment where run = run and camcol=1 LIMIT 1);
END;

CREATE FUNCTION fStripOfRun(run int)
RETURNS int as
BEGIN
  RETURN (SELECT strip from Segment where run = run and camcol=1 LIMIT 1);
END;

CREATE FUNCTION fGetDiagChecksum()
RETURNS BIGINT
BEGIN
	RETURN (select sum(count)+count(*) from Diagnostics);
END;

COMMIT;
