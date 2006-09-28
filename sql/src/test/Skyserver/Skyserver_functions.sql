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


CREATE FUNCTION fIAUFromEq(ra float, dec float)
RETURNS varchar(64)
BEGIN
	RETURN('SDSS J'||REPLACE(fHMSbase(ra,1,2)||fDMSbase(dec,1,1),':',''));
END;

CREATE FUNCTION fDMS(deg float)
RETURNS varchar(32)
BEGIN
    RETURN fDMSbase(deg,default,default);
END;

CREATE  FUNCTION fDMSbase(deg float, truncate int, precision int)
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
	SET t  = STUFF(t,3-LEN(q),LEN(q), q);
	-- minutes
	SET d  = 60.0 * (d-nd);
	SET nd = FLOOR(d);
	SET q  = LTRIM(CAST(nd as varchar(4)));
	SET t  = STUFF(t,6-LEN(q),LEN(q), q);
	-- seconds
	SET d  = ROUND( 60.0 * (d-nd),precision,truncate );
--	SET d  = 60.0 * (d-nd);
	SET q  = LTRIM(STR(d,6+precision,precision));
	SET t = STUFF(t,10+precision-LEN(q),LEN(q), q);
	--
	RETURN(s+t);
END;

CREATE FUNCTION fHMS(deg float)
RETURNS varchar(32)
BEGIN
    RETURN fHMSbase(deg,default,default);
END;

CREATE  FUNCTION fHMSbase(deg float, truncate int = 0, precision int)
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
	SET np = 0
	WHILE (np < precision-1) DO
		SET t = t||'0';
		SET np = np + 1;
	END WHILE;
	SET d = ABS(deg/15.0);
	-- degrees
	SET nd = FLOOR(d);
	SET q  = LTRIM(CAST(nd as varchar(2)));
	SET t  = STUFF(t,3-LEN(q),LEN(q), q);
	-- minutes
	SET d  = 60.0 * (d-nd);
	SET nd = FLOOR(d);
	SET q  = LTRIM(CAST(nd as varchar(4)));
	SET t  = STUFF(t,6-LEN(q),LEN(q), q);
	-- seconds
	SET d  = ROUND( 60.0 * (d-nd),precision,truncate );
	SET q  = LTRIM(STR(d,6+precision,precision));
	SET t = STUFF(t,10+precision-LEN(q),LEN(q), q);
--	SET d  = 60.0 * (d-nd);
--	SET q = LTRIM(STR(d,9,3));
--	SET t = STUFF(t,13-LEN(q),LEN(q), q);
	--
	RETURN(t);
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
    DECLARE v3 TEMPORARY TABLE (x float, y float, z float);
    DECLARE x float, y float, z float;
    SET x = SIN(RADIANS(eta));
    SET y = COS(RADIANS(eta));
    SET z = 0.0;
    --
    INSERT INTO v3 SELECT x, y, z 
	FROM fRotateV3('S2J',x, y, z);
    RETURN v3;
END;

CREATE FUNCTION fStripeToNormal(stripe int)
RETURNS TABLE (x float, y float, z float)
BEGIN
    --
    DECLARE v3 TEMPORARY TABLE (x float, y float, z float);
    DECLARE x float, y float, z float, eta float;
    --
    IF (stripe < 0 or stripe>86) 
	THEN return v3;
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
    INSERT INTO v3 SELECT x, y, z 
	FROM fRotateV3('S2J',x, y, z);
    RETURN v3;
END;

CREATE FUNCTION fGetLat(mode varchar(8),cx float,cy float,cz float)
RETURNS float
BEGIN
    DECLARE lat float;
    SELECT lat=DEGREES(ASIN(z)) FROM fRotateV3(mode,cx,cy,cz);
    RETURN lat;
END;

CREATE FUNCTION fGetLon(mode varchar(8),cx float,cy float,cz float)
RETURNS float
BEGIN
    DECLARE lon float;
    SELECT lon=DEGREES(ATN2(y,x)) FROM fRotateV3(mode,cx,cy,cz)
    IF lon<0 
	THEN SET lon=lon+360;
    END IF;
    RETURN lon;
END;

CREATE FUNCTION fGetLonLat(mode varchar(8),cx float,cy float,cz float)
RETURNS TABLE (lon float, lat float)
BEGIN
    --
    DECLARE coord TEMPORARY TABLE (lon float, lat float);
    DECLARE lon float, lat float;
    --
    SELECT lon=DEGREES(ATN2(y,x)),lat=DEGREES(ASIN(z))
	FROM fRotateV3(mode,cx,cy,cz);
    --
    IF lon<0 
	THEN SET lon=lon+360;
    END IF;
    INSERT INTO coord SELECT lon as lon, lat as lat;
    RETURN coord;
END;

CREATE FUNCTION fEqFromMuNu(
	mu float,
	nu float,
	node float,
	incl float
)
RETURNS TABLE (ra float, dec float, cx float, cy float, cz float)
BEGIN
    DECLARE coord TEMPORARY TABLE (ra float, dec float, cx float, cy float, cz float);
    DECLARE
	rmu float, rnu float, rin float,
	ra float, dec float, 
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
    SET dec = DEGREES(asin(cz));
    SET ra  = DEGREES(atn2(cy,cx)) + node;
    IF  ra<0 
	THEN SET ra = ra+360 ;
    END IF;
    --
    SET cx = cos(RADIANS(ra))*cos(RADIANS(dec));
    SET cy = sin(RADIANS(ra))*cos(RADIANS(dec));
    --
    INSERT INTO coord VALUES(ra, dec, cx, cy, cz);
    RETURN coord;
END;

CREATE FUNCTION fCoordsFromEq(ra float,dec float)
RETURNS TABLE (
    ra	float,
    dec float,
    stripe int,
    incl float,
    lambda float,
    eta float,
    mu float,
    nu float
)
BEGIN
    DECLARE coords TEMPORARY TABLE (
    ra	float,
    dec float,
    stripe int,
    incl float,
    lambda float,
    eta float,
    mu float,
    nu float
);
    DECLARE 
	cx float, cy float, cz float,
	qx float, qy float, qz float,
	lambda float, eta float, 
	stripe int, incl float,
	mu float, nu float,
        stripeEta float;
    --
    SET cx = cos(radians(dec))* cos(radians(ra-95.0));
    SET cy = cos(radians(dec))* sin(radians(ra-95.0));
    SET cz = sin(radians(dec));
    --
    SET lambda = -degrees(asin(cx));
    IF (cy = 0.0 and cz = 0.0)
        THEN SET cy = 1e-16;
    END IF;
    SET eta    =  degrees(atn2(cz,cy))-32.5;
    SET stripeEta = eta;
    --
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF ABS(lambda) > 90.0
       THEN (
           SET lambda = 180.0-lambda;
           SET eta = eta+180.0;
	)
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
       THEN (
           SET eta = eta-180.0;
           SET lambda = 180.0-lambda;
       )
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
    SET mu = degrees(atn2(qy,qx))+95.0;
    SET nu = degrees(asin(qz));
    IF  stripe>50 
	THEN SET mu = mu+360;
    END IF;
    --
    INSERT INTO coords SELECT
	ra, dec, stripe, incl, lambda, eta, mu, nu;
    RETURN coords;
END;

CREATE FUNCTION fMuFromEq(ra float,dec float)
RETURNS float
BEGIN
    DECLARE 
	cx float, cy float, cz float,
	qx float, qy float, qz float,
	eta float, 
	stripe int, incl float,
	mu float;
    --
    SET cx = cos(radians(dec))* cos(radians(ra-95.0));
    SET cy = cos(radians(dec))* sin(radians(ra-95.0));
    SET cz = sin(radians(dec));
    --
    SET eta = degrees(atn2(cz,cy));
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
    SET mu = degrees(atn2(qy,qx))+95.0;
    IF  stripe>50 
	THEN SET mu = mu+360;
    END IF;
    --
    RETURN mu;
END;

CREATE FUNCTION fNuFromEq(ra float,dec float)
RETURNS float
BEGIN
    DECLARE 
	cy float, cz float,
	qz float,
	eta float, 
	stripe int, incl float,
	nu float;
    --
    SET cy = cos(radians(dec))* sin(radians(ra-95.0));
    SET cz = sin(radians(dec));
    --
    SET eta    =  degrees(atn2(cz,cy));
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

CREATE FUNCTION fEtaFromEq(ra float,dec float)
RETURNS float
BEGIN
    DECLARE 
	cx float, cy float, cz float,
	lambda float, eta float, 
        stripeEta float;
    --
    SET cx = cos(radians(dec))* cos(radians(ra-95.0));
    SET cy = cos(radians(dec))* sin(radians(ra-95.0));
    SET cz = sin(radians(dec));
    --
    SET lambda = -degrees(asin(cx));
    IF (cy = 0.0 and cz = 0.0)
        THEN SET cy = 1e-16;
    END IF;
    SET eta    =  degrees(atn2(cz,cy))-32.5;
    SET stripeEta = eta;
    --
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF ABS(lambda) > 90.0
       THEN (
           SET lambda = 180.0-lambda;
           SET eta = eta+180.0;
	)
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

CREATE FUNCTION fLambdaFromEq(ra float,dec float)
RETURNS float
BEGIN
    DECLARE 
	cx float, cy float, cz float,
	lambda float, eta float, 
        stripeEta float;
    --
    SET cx = cos(radians(dec))* cos(radians(ra-95.0));
    SET cy = cos(radians(dec))* sin(radians(ra-95.0));
    SET cz = sin(radians(dec));
    --
    SET lambda = -degrees(asin(cx));
    IF (cy = 0.0 and cz = 0.0)
        THEN SET cy = 1e-16;
    END IF;
    SET eta    =  degrees(atn2(cz,cy))-32.5;
    SET stripeEta = eta;
    --
    IF lambda < -180.0 
	THEN SET lambda = lambda+360.0;
    END IF;
    IF lambda >= 180.0 
	THEN SET lambda = lambda-360.0;
    END IF;
    IF ABS(lambda) > 90.0
       THEN (
           SET lambda = 180.0-lambda;
           SET eta = eta+180.0;
	)
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
       THEN (
           SET eta = eta-180.0;
           SET lambda = 180.0-lambda;
       )
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
	dec float,
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
	gx float, gy float, gz float,
	coord TEMPORARY TABLE (mu float, nu float);

    -- convert to radians
    SET rin  = RADIANS(incl);
    SET rra  = RADIANS(ra-node);
    SET rdec = RADIANS(dec);
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
    SET mu = DEGREES(ATN2(gy,gx)) + node;
    IF  mu<0 
	THEN SET mu = mu+360 ;
    END IF;
    IF  (stripe>50 AND mu<180) 
	THEN SET mu = mu+360 ;
    END IF;
    --
    INSERT INTO coord VALUES(mu, nu);
    RETURN coord;
END;


------------------------VECTOR OPERATIONS----------------------------

CREATE FUNCTION fWedgeV3(x1 float,y1 float, z1 float, x2 float, y2 float, z2 float)
    RETURN TABLE(SELECT 
	(y1*z2 - y2*z1) as x,
    	(x2*z1 - x1*z2) as y,
	(x1*y2 - x2*y1) as z);
END;


CREATE FUNCTION fRotateV3(mode varchar(16),cx float,cy float,cz float)
RETURNS TABLE (
	x float NOT NULL, 
	y float NOT NULL, 
	z float NOT NULL)
BEGIN
    -- 
    DECLARE x float, y float, z float;
    --
    SELECT x=x*cx+y*cy+z*cz FROM Rmatrix WHERE mode=mode and row=1;
    SELECT y=x*cx+y*cy+z*cz FROM Rmatrix WHERE mode=mode and row=2;
    SELECT z=x*cx+y*cy+z*cz FROM Rmatrix WHERE mode=mode and row=3;
    --
    RETUNR TABLE(SELECT x as x, y as y, z as z);
END;


------------------------STRING OPERATIONS----------------------------

CREATE FUNCTION fTokenNext(s VARCHAR(8000), i int) 
RETURNS VARCHAR(8000)
BEGIN
	DECLARE j INT;
	-- eliminate multiple blanks
	SET j = charindex(' ',s,i);
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
	SET j = charindex(' ',s,i);
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
RETURNS TABLE (token VARCHAR(16) NOT NULL)
BEGIN  
	DECLARE tokenStart int;
	DECLARE tokens TEMPORARY TABLE (token VARCHAR(16) NOT NULL);
	SET tokenStart = 1;
	SET types = fNormalizeString(types);
	WHILE (ltrim(fTokenNext(types,tokenStart)) != '') DO
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
       		SET PatLen = LEN(Pattern);
		 
		WHILE (CHARINDEX(LowerPattern, LowerOld, 1) != 0 ) DO
			SET offset = CHARINDEX(LowerPattern, LowerOld, 1);
			SET NewString = NewString || SUBSTRING(OldString,1,offset-1) || Replacement;
			SET OldString = SUBSTRING(OldString,offset||PatLen,LEN(OldString)-offset||PatLen);
			SET LowerOld =  SUBSTRING(LowerOld,  offset||PatLen,LEN(LowerOld)-offset||PatLen);
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
			THEN (
				IF (dot = 1) 
					THEN RETURN 0;
				END IF;
				SET dot = 1;	-- set flag
				SET offset = offset + 1;  -- advance
			)		-- end dot case
	 		ELSEIF (char<'0' or '9' <char)  -- if not digit
				THEN RETURN 0;	-- reject
			ELSE (			-- its a digit
					-- advance
			     	SET offset = offset + 1;
				SET num= 1;	-- set digit flag
			) 		-- end digit case
		END IF; 			-- end loop
	-- test for bigint overflow	
		IF (stop-start > 19) 
			THEN RETURN 0; -- reject giant numbers
		END IF;
	      	IF  (dot = 0 and  stop-start >= 19 )
					-- if its a bigint
			THEN (
				IF ( ((stop-start)>19) or	-- reject if too big
				('9223372036854775807' > substring(string,start,stop)))
					THEN  RETURN 0;
				END IF;		--
			)
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
			THEN (
				IF (HTMtemp=3) 
					THEN SET Answer='N'||Answer;
				END IF;
				IF (HTMtemp=2) 
					THEN SET Answer='S'||Answer;
				END IF;
				SET HTMtemp  = 0;
			)
	   		   			-- end face case
	  		ELSE (
	   					-- its a triangle
				SET Triangle = HTMtemp % 4; 	-- get the id into answer
				SET Answer =  CAST(Triangle as VARCHAR(4)) || Answer;
	     			SET HTMTemp = HTMtemp / 4;  	-- move on to next triangle
	   		)   			-- end triangle case
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

CREATE FUNCTION fHtmLookupEq(ra float, dec float)
RETURNS bigint
BEGIN
	DECLARE x float, y float, z float; 
	SET x  = COS(RADIANS(dec))*COS(RADIANS(ra));
	SET y  = COS(RADIANS(dec))*SIN(RADIANS(ra));
	SET z  = SIN(RADIANS(dec));
	RETURN fHtmLookupXyz(x, y, z);
END;

CREATE FUNCTION fHtmEq(ra float, dec float)
RETURNS bigint
BEGIN
	RETURN fHtmLookupEq(ra,dec);
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
	DECLARE proxtab TEMPORARY TABLE (
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

CREATE FUNCTION fGetNearbyObjAllEq (ra float, dec float, r float)
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
	set nx  = COS(dec*d2r)*COS(ra*d2r);
	set ny  = COS(dec*d2r)*SIN(ra*d2r);
	set nz  = SIN(dec*d2r);
	INSERT INTO proxtab	
	SELECT * FROM fGetNearbyObjAllXYZ(nx,ny,nz,r);
	RETURN proxtab;
END;

CREATE FUNCTION fGetNearestObjAllEq (ra float, dec float, r float)
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
	set nx  = COS(dec*d2r)*COS(ra*d2r);
	set ny  = COS(dec*d2r)*SIN(ra*d2r);
	set nz  = SIN(dec*d2r);
	RETURN TABLE(	
	SELECT top 1 * 
	FROM fGetNearbyObjAllXYZ(nx,ny,nz,r)
	ORDER BY distance ASC);   -- order by needed to get the closest one.
END;

CREATE FUNCTION fGetNearestObjIdEqMode (ra float, dec float, 
					r float, mode int)
RETURNS bigint
BEGIN
    DECLARE nx float, ny float, nz float;
    SET nx = cos(radians(dec))*cos(radians(ra));
    SET ny = cos(radians(dec))*sin(radians(ra));
    SET nz = sin(radians(dec));
    RETURN (
	select top 1 objID 
	from fGetNearbyObjAllXYZ(nx,ny,nz,r)
	where mode = mode
	order by distance asc); 
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
	SELECT top 1 * 
	FROM fGetNearbyObjXYZ(nx,ny,nz,r)
	ORDER BY distance ASC);
END;

CREATE FUNCTION fGetNearbyObjEq (ra float, dec float, r float)
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
	set nx  = COS(dec*d2r)*COS(ra*d2r);
	set ny  = COS(dec*d2r)*SIN(ra*d2r);
	set nz  = SIN(dec*d2r);
	INSERT INTO proxtab	
	SELECT * FROM fGetNearbyObjXYZ(nx,ny,nz,r);
	RETURN proxtab;
END;

CREATE FUNCTION fGetNearestObjEq (ra float, dec float, r float)
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
	set nx  = COS(dec*d2r)*COS(ra*d2r);
	set ny  = COS(dec*d2r)*SIN(ra*d2r);
	set nz  = SIN(dec*d2r);
	RETURN TABLE(	
	SELECT top 1 * 
	FROM fGetNearbyObjXYZ(nx,ny,nz,r)
	ORDER BY distance ASC);   -- order by needed to get the closest one.
END;

CREATE FUNCTION fGetNearestObjIdEq(ra float, dec float, r float)
RETURNS bigint
BEGIN
    RETURN (select objID from fGetNearestObjEq(ra,dec,r)); 
END;

CREATE FUNCTION fGetNearestObjIdAllEq(ra float, dec float, r float)
RETURNS bigint
BEGIN
    RETURN (select objID from fGetNearestObjAllEq(ra,dec,r));
END;

CREATE FUNCTION fGetNearestObjIdEqType (ra float, dec float, r float, t int)
RETURNS bigint
BEGIN
    RETURN (	select top 1 objID 
		from fGetNearbyObjEq(ra,dec,r)
		where type=t 
		order by distance asc ); 
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
	    level int, shift bigint, ra float, dec float,
	    nx1 float, ny1 float, nz1 float,
	    nx2 float, ny2 float, nz2 float,
	    nx float, ny float, nz float;
	-- calculate approximate center
	set ra  = (ra1+ra2)/2;
	set dec = (dec1+dec2)/2;
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
	set nx  = SIN(ra*d2r) * COS(dec*d2r);
	set ny  = COS(ra*d2r) * COS(dec*d2r);
	set nz  = SIN(dec*d2r);
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
	from fGetNearbyObjEq(ra,dec,radius)
	    WHERE (cz>nz1) AND (cz<nz2) 
		AND (-cx*nx1+cy*ny1>0)
		AND (cx*nx2-cy*ny2)>0);
END;

CREATE FUNCTION fGetObjectsEq(flag int, ra float, 
				dec float, r float, zoom float)
RETURNS obj table (ra float, [dec] float, flag int, objid bigint)
BEGIN
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
        set nx  = COS(RADIANS(dec))*COS(RADIANS(ra));
        set ny  = COS(RADIANS(dec))*SIN(RADIANS(ra));
        set nz  = SIN(RADIANS(dec));
        set mag =  25 - 1.5* zoom;  -- magnitude reduction.
        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)||' '||str(dec,22,15)||' '||str(rad,5,2);

        if ( (flag & 1) > 0 )  -- specObj
            THEN (
                INSERT INTO obj
                SELECT ra, dec,  1 as flag, specobjid as objid
                FROM fHtmCover(cmd) , SpecObj WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
		)
        END IF;

        if ( (flag & 2) > 0 )  -- photoObj
            THEN (
                INSERT INTO obj
                SELECT ra, dec, 2 as flag, objid
                FROM fHtmCover(cmd) , PhotoPrimary WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad)
                and (r <= mag );
	    )
         END IF;

        if ( (flag & 4) > 0 )  -- target
            THEN (
                INSERT INTO obj
                SELECT ra, dec, 4 as flag, targetid as objid
                FROM fHtmCover(cmd) , Target WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
            )
	END IF;

        if ( (flag & 8) > 0 )  -- mask
            THEN(
                INSERT INTO obj
                SELECT ra, dec, 8 as flag, maskid as objid
                FROM fHtmCover(cmd) , Mask WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
            )
	END IF;

        if ( (flag & 16) > 0 ) -- plate
            THEN(
                SET rad = r + 89.4;   -- add the tile radius
	        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)
			||' '||str(dec,22,15)||' '||str(rad,5,2);
                INSERT INTO obj
                SELECT ra, dec, 16 as flag, plateid as objid
                FROM fHtmCover(cmd) , PlateX WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
            )
	END IF;

        if ( (flag & 32) > 0 )  -- photoPrimary and secondary
            THEN(
                INSERT INTO obj
                SELECT ra, dec, 2 as flag, objid
                FROM fHtmCover(cmd) , PhotoObjAll WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad)
		AND mode in (1,2);
            )
	END IF;
        --
        RETURN obj;
END;

CREATE   FUNCTION fGetObjectsMaskEq(flag int, ra float, dec float, r float, zoom float)
RETURNS obj table (ra float, [dec] float, flag int, objid bigint)
BEGIN
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
        set nx  = COS(RADIANS(dec))*COS(RADIANS(ra));
        set ny  = COS(RADIANS(dec))*SIN(RADIANS(ra));
        set nz  = SIN(RADIANS(dec));
        set mag =  25 - 1.5* zoom;  -- magnitude reduction.
        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)||' '||str(dec,22,15)||' '||str(rad,5,2);

        if ( (flag & 1) > 0 )  -- specObj
            THEN(
                INSERT INTO obj
                SELECT ra, dec,  1 as flag, specobjid as objid
                FROM fHtmCover(cmd) , SpecObj WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
            )
	END IF;
        if ( (flag & 2) > 0 )  -- photoObj
            THEN(
                INSERT INTO obj
                SELECT ra, dec, 2 as flag, objid
                FROM fHtmCover(cmd) , PhotoPrimary WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad)
                and (r <= mag );
            )
	END IF;
        if ( (flag & 4) > 0 )  -- target
            THEN(
                INSERT INTO obj
                SELECT ra, dec, 4 as flag, targetid as objid
                FROM fHtmCover(cmd) , Target WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
            )
	END IF;
        if ( (flag & 8) > 0 )  -- mask
            THEN (
	        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)
			||' '||str(dec,22,15)||' '||str(rad+15,5,2);
                INSERT INTO obj
                SELECT ra, dec, 8 as flag, maskid as objid
                FROM fHtmCover(cmd) , Mask WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< (rad+radius));
            )
	END IF;
        if ( (flag & 16) > 0 ) -- plate
            THEN(
                SET rad = r + 89.4;   -- add the tile radius
	        set cmd = 'CIRCLE J2000 '||' '||str(ra,22,15)
			||' '||str(dec,22,15)||' '||str(rad,5,2);
                INSERT INTO obj
                SELECT ra, dec, 16 as flag, plateid as objid
                FROM fHtmCover(cmd) , PlateX WITH (nolock)
                WHERE (HTMID BETWEEN  HTMIDstart AND HTMIDend)
                AND ((2*DEGREES(ASIN(sqrt(power(nx-cx,2)+power(ny-cy,2)
		    +power(nz-cz,2))/2))*60)< rad);
            )
	END IF;
        --
        RETURN obj;
END;

CREATE FUNCTION fGetNearbyFrameEq (ra float, dec float, 
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
	set cmd = 'CIRCLE J2000 ' ||str(ra,22,15)||' '||str(dec,22,15)||' '||str(r,5,2);
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

CREATE FUNCTION fGetNearestFrameEq (ra float, dec float, zoom int)
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
	    SELECT TOP 1 fieldID, a, b, c, d, e, f, node, incl, distance  -- look up to 81
	    FROM fGetNearbyFrameEq (ra , dec, 81, zoom )	-- arcmin away from center.
            ORDER BY distance ASC);   
END;

CREATE FUNCTION fGetNearestFrameidEq (ra float, dec float, zoom int)
RETURNS bigint
BEGIN
	RETURN (select fieldID from fGetNearestFrameEq(ra, dec, zoom) );
END;


----------------------REGION OPERATIONS-------------------------------------


CREATE FUNCTION fRegionFromString(regionString varchar(8000), buffer float)
RETURNS TABLE (
	id 	bigint identity(1,1)  NOT NULL,
	convexid bigint   NOT NULL,
	x	float  NOT NULL,
	y 	float  NOT NULL,
	z	float  NOT NULL,
	c	float  NOT NULL
	)
BEGIN
	DECLARE regionTable TEMPORARY TABLE (
	id 	bigint identity(1,1)  NOT NULL,
	convexid bigint   NOT NULL,
	x	float  NOT NULL,
	y 	float  NOT NULL,
	z	float  NOT NULL,
	c	float  NOT NULL
	);
	DECLARE cx varchar(32), 
		cy varchar(32), 
		cz varchar(32), 
		c varchar(32),
		convexid bigint,  
		tokenStart int,
		region varchar(8000),
		token varchar(8000);
	------------------------------------------------
 	-- convert it to a normal form (blank separated trailing blank, upper case)
	------------------------------------------------
	SET region = fRegionNormalizeString(regionString);
	------------------------------------------------
	-- return null of syntax is wrong
	------------------------------------------------
	IF substring(region,1,len('REGION CONVEX ')) != 'REGION CONVEX ' 
		THEN RETURN regionTable;
	END IF;
	------------------------------------------------
	-- discard the region token  
	------------------------------------------------
	SET tokenStart = len('REGION') + 2;
	SET convexid = 0;
	------------------------------------------------
	-- start the loop over the convexes
	------------------------------------------------
	WHILE(1=1) DO
		------------------------------------------------
		-- Get next convex 
		------------------------------------------------
		SET token = dbo.fTokenNext(region,tokenStart)
		IF token != 'CONVEX'
		    THEN  				-- if at end of string
			IF token = '' 
				THEN BREAK;		-- region table is complete.
				ELSE (
	     				DELETE FROM regionTable; 		-- empty  region table & return
	     				RETURN regionTable;			-- and return null table (in error case)
				)
			END IF;		-- else there is a syntax error	
		END IF;				
		------------------------------------------------
		-- we have a new convex.
		-- State machine looks for a sequence of 4 floats in a row.
		-- breaks at end or at "CONVEX"
		------------------------------------------------
		WHILE (1=1) DO 
			------------------------------------------------
			-- get cx,cy,cz,d
			------------------------------------------------
			SET tokenStart = dbo.fTokenAdvance(region,tokenStart);
			SET cx = dbo.fTokenNext(region,tokenStart) ;
			IF  cx = '' or cx = 'CONVEX' 
				THEN BREAK;
			END IF;
			SET tokenStart = dbo.fTokenAdvance(region,tokenStart);
			SET cy = dbo.fTokenNext(region,tokenStart);
			SET tokenStart = dbo.fTokenAdvance(region,tokenStart);
			SET cz = dbo.fTokenNext(region,tokenStart);
			SET tokenStart = dbo.fTokenAdvance(region,tokenStart);
			SET c  = dbo.fTokenNext(region,tokenStart);
			IF  cy = '' or cz = '' or c = '' 
				THEN (
	     				DELETE FROM regionTable; 		-- empty  region table & return
	     				RETURN regionTable;			-- and return null table (in error case)
				)
			END IF;
			INSERT INTO regionTable values( 	convexid,
						cast(cx as float), 
						cast(cy as float), 
						cast(cz as float), 
						cast(c  as float)
						);
		    END WHILE;		-- End of convex loop
		SET convexid = convexid+1;
	    END WHILE;		-- End of region loop
	    --------------------
	    -- apply the fuzz
	    --------------------
	    IF buffer > 0
		THEN UPDATE regionTable
		    SET c = dbo.fRegionFuzz(c,buffer);
	    END IF;
     	RETURN regionTable;
	------------------------------------------------
	-- common error exit, empty region table and return empty set if there is a parsing error.
	------------------------------------------------
END;

CREATE FUNCTION fRegionNormalizeString(regionString VARCHAR(8000) )
RETURNS VARCHAR(8000) 
BEGIN
    	DECLARE simpleRegionString varchar(8000);
	------------------------------------
	-- use HTM simplify code,
	-- catch overflow and underflow errors
	------------------------------------
 	SET simpleRegionString = fHtmToNormalForm(regionString);
	IF len(simpleRegionString) = 0	
		THEN SET simpleRegionString = 'REGION ERROR';
	END IF;
	--
	SET simpleRegionString = fNormalizeString(simpleRegionString);
	RETURN simpleRegionString;
END;

CREATE FUNCTION fRegionsContainingPointXYZ(
	x float, y float, z float, 
	types VARCHAR(1000), buffer float
)
RETURNS TABLE(	
		RegionID 	bigint PRIMARY KEY, 
		type 		varchar(16) NOT NULL
	)
BEGIN
	----------------------------------------------------
	DECLARE Objects TEMPORARY TABLE(	
		RegionID 	bigint PRIMARY KEY, 
		type 		varchar(16) NOT NULL
	);
	DECLARE typesTable TABLE (
		type varchar(16) 
		COLLATE SQL_Latin1_General_CP1_CI_AS 
		NOT NULL  PRIMARY KEY
	);
	----------------------------------------------------
	SET types = REPLACE(types,',',' ');
	INSERT INTO typesTable (type)
	    SELECT * FROM fTokenStringToTable(types) 
	IF (rowcount = 0) 
		THEN RETURN Objects;
	END IF;
	----------------------------------------
	-- this contains the prefetched convexes
	-- matching the type constraint
	----------------------------------------
	DECLARE region TABLE (
		regionid bigint PRIMARY KEY,
		type varchar(16)
	);
	--
	INSERT INTO region
	SELECT regionid, min(type)
	FROM (
	    select regionid, convexId, patch, type 
	    from RegionConvex with (nolock)
	    where type in (select type from typesTable)
--	      and fDistanceArcminXYZ(x,y,z,x,y,z) <radius+buffer
 	      and 2*DEGREES(ASIN(sqrt(power(x-x,2)+power(y-y,2)+power(z-z,2))/2)) <(radius+buffer)/60
	    )
	GROUP BY regionid;
	----------------------------------------------------
	IF (buffer = 0.0)
	    THEN 
	    -------------------------------------------------
		INSERT INTO objects
		    SELECT R.regionID, R.type
		    FROM  region as R 
		    WHERE EXISTS (
			select convexid from Halfspace
			where regionid=R.regionid
			and convexid not in (
			    select convexid from HalfSpace h
			    where regionid=R.regionid
			    and x*h.x+y*h.y+z*h.z<h.c
			    )
			);
		
	    -------------------------------------------------
	    ELSE 
	    -------------------------------------------------
		INSERT INTO objects
		    SELECT R.regionID, R.type
		    FROM  region as R 
		    WHERE EXISTS (
			select convexid from Halfspace
			where regionid=R.regionid
			and convexid not in (
			    select convexid from HalfSpace h
			    where regionid=R.regionid
			    and x*h.x+y*h.y+z*h.z<fRegionFuzz(h.c,buffer)
			    )
			);
	    -------------------------------------------------
	END IF;
	RETURN objects;
END;

CREATE FUNCTION fRegionsContainingPointEq(
	ra float, dec float, 
	types varchar(1000), buffer float)
RETURNS TABLE(	regionid bigint PRIMARY KEY, 
			type 	 varchar(16) NOT NULL)
BEGIN
	--------------------------------
	-- transform to xyz coordinates
	--------------------------------
	DECLARE x float, y float, z float;
	SET x  = COS(RADIANS(dec))*COS(RADIANS(ra));
	SET y  = COS(RADIANS(dec))*SIN(RADIANS(ra));
	SET z  = SIN(RADIANS(dec));
	-- call the xyz function
	RETURN TABLE(
	    SELECT * FROM fRegionsContainingPointXYZ(x,y,z,types,buffer));
END;

CREATE FUNCTION fRegionGetObjectsFromRegionId( regionID  bigint, flag int)
RETURNS  objects table (id bigint primary key NOT NULL, flag int NOT NULL )
BEGIN	
    DECLARE area varchar(8000);

    ------------------------------------
    -- fetch the regionString
    ------------------------------------
    SELECT area = regionString FROM Region with (nolock)
	WHERE regionid = regionid;

    ------------------------------------
    -- ready to execute the query
    ------------------------------------
        if ( (flag & 1) > 0 )  -- specObj
            THEN 
		INSERT INTO Objects
		SELECT	o.id, 1 as flag
		    FROM (SELECT q.specobjid as id, q.cx, q.cy, q.cz
  			FROM SpecObj q with (nolock) JOIN fHtmCover(area) AS c 
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from Halfspace
			where regionid=regionid
			and convexid not in (
				select convexid from HalfSpace h
				where regionid=regionid
				and o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;

        if ( (flag & 2) > 0 )  -- photoObj
            THEN
		INSERT INTO Objects
		SELECT	o.id, 2 as flag
		    FROM (SELECT q.objid as id, q.cx, q.cy, q.cz
  			FROM PhotoPrimary q JOIN fHtmCover(area) AS c
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from Halfspace
			where regionid=regionid
			and convexid not in (
				select convexid from HalfSpace h
				where regionid=regionid
				and o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;

        if ( (flag & 4) > 0 )  -- target
            THEN 
		INSERT INTO Objects
		SELECT	o.id, 4 as flag
		    FROM (SELECT q.targetid as id, q.cx, q.cy, q.cz
  			FROM Target q JOIN fHtmCover(area) AS c 
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from Halfspace
			where regionid=regionid
			and convexid not in (
				select convexid from HalfSpace h
				where regionid=regionid
				and o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;

        if ( (flag & 8) > 0 )  -- mask
            THEN
		INSERT INTO Objects
		SELECT	o.id, 8 as flag
		    FROM (SELECT q.maskid as id, q.cx, q.cy, q.cz
  			FROM Mask q JOIN fHtmCover(area) AS c 
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from Halfspace
			where regionid=regionid
			and convexid not in (
				select convexid from HalfSpace h
				where regionid=regionid
				and o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;

        if ( (flag & 32) > 0 )  -- photoPrimary and Secondary
            THEN
		INSERT INTO Objects
		SELECT	o.id, 2 as flag
		    FROM (SELECT q.objid as id, q.cx, q.cy, q.cz
  			FROM PhotoObjAll q JOIN fHtmCover(area) AS c
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			WHERE q.mode in (1,2)
			) AS o 
		    WHERE exists (
			select convexid from Halfspace
			where regionid=regionid
			and convexid not in (
				select convexid from HalfSpace h
				where regionid=regionid
				and o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;
    -----------------------------------
    RETURN  Objects;
END;

CREATE FUNCTION fRegionGetObjectsFromString(
	poly varchar(8000), 
	flag int, 
	buffer float
)
RETURNS TABLE(	objid  bigint NOT NULL, flag int NOT NULL)
BEGIN
    DECLARE Objects TEMPORARY TABLE(objid  bigint NOT NULL, flag int NOT NULL);
    DECLARE region TABLE (
	id 	int,
	convexid int,
	x 	float,
	y 	float,
	z	float,
	c 	float
	);
     DECLARE 
	area varchar(8000), 
	convexid int;

    -- convert the string to a region table
    INSERT INTO region
	SELECT * FROM fRegionFromString(poly, buffer);

    -- now convert back to string for the cover function
    SET area = 'REGION ';
    SET convexid=-1;
    SELECT area = area
	||CASE WHEN convexid>convexid 
		THEN CAST ('CONVEX ' as varchar(10))
		ELSE CAST ('' as varchar(10))
	END CASE
	|| STR(x,18,15)||' '
	|| STR(y,18,15)||' '
	|| STR(z,18,15)||' '
	|| STR(c,18,15)||' ',
	convexid=convexid
	FROM region
	ORDER BY convexid, id;
    --
    ------------------------------------
    -- ready to execute the query
    ------------------------------------
        if ( (flag & 1) > 0 )  -- specObj
            THEN 
		INSERT INTO Objects
		SELECT	o.id, 1 as flag
		    FROM (SELECT q.specobjid as id, q.cx, q.cy, q.cz
  			FROM SpecObj q with (nolock) JOIN fHtmCover(area) AS c 
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from region
			where convexid not in (
				select convexid from region h
				where o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
         END IF;

        if ( (flag & 2) > 0 )  -- photoObj
            THEN
		INSERT INTO Objects
		SELECT	o.id, 2 as flag
		    FROM (SELECT q.objid as id, q.cx, q.cy, q.cz
  			FROM PhotoPrimary q JOIN fHtmCover(area) AS c
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from region
			where convexid not in (
				select convexid from region h
				where o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;

        if ( (flag & 4) > 0 )  -- target
            THEN
		INSERT INTO Objects
		SELECT	o.id, 4 as flag
		    FROM (SELECT q.targetid as id, q.cx, q.cy, q.cz
  			FROM Target q JOIN fHtmCover(area) AS c 
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from region
			where convexid not in (
				select convexid from region h
				where o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;

        if ( (flag & 8) > 0 )  -- mask
            THEN
		INSERT INTO Objects
		SELECT	o.id, 8 as flag
		    FROM (SELECT q.maskid as id, q.cx, q.cy, q.cz
  			FROM Mask q JOIN fHtmCover(area) AS c 
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			) AS o 
		    WHERE exists (
			select convexid from region
			where convexid not in (
				select convexid from region h
				where o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;

        if ( (flag & 32) > 0 )  -- photoPrimary and Secondary
            THEN
		INSERT INTO Objects
		SELECT	o.id, 2 as flag
		    FROM (SELECT q.objid as id, q.cx, q.cy, q.cz
  			FROM PhotoObjAll q JOIN fHtmCover(area) AS c
			ON q.htmID between c.HtmIdStart and c.HtmIdEnd
			WHERE q.mode in (1,2)
			) AS o 
		    WHERE exists (
			select convexid from region
			where convexid not in (
				select convexid from region h
				where o.cx*h.x+o.cy*h.y+o.cz*h.z<h.c 
				)
			);
        END IF;
    -----------------------------------
    RETURN Objects;
END;

CREATE FUNCTION fRegionIdToString(regionID bigint )
RETURNS varchar(8000)
BEGIN  
	DECLARE	RegionString 	varchar(8000),
		convexCount	bigint;
	-----------------------------------------------------
	-- convert it to a normal form (blank separated trailing blank, uppper case)
	-----------------------------------------------------
	SET RegionString = 'REGION ';
	SET convexCount = 0;
	-------------------------------------
	-- start the looop over the convexes
	-------------------------------------
	SELECT RegionString = RegionString
		||(CASE WHEN len(fRegionConvexIdToString(regionID, convexID)) < 12 THEN '' 
	        ELSE 'CONVEX ' || fRegionConvexIdToString(regionID, convexID) END)
	    FROM (select distinct convexID from HalfSpace
			where regionID = regionID) as C	 	;
	IF (RegionString = 'REGION ')  
		THEN SET RegionString = 'REGION CONVEX ';
	END IF;
	-----------------------------------------------------
	RETURN RegionString;
END;			-- End fRegionIdToString

CREATE FUNCTION fRegionContainsPointXYZ(regionID bigint,cx float,cy float,cz float)
RETURNS bit 
BEGIN	
	IF EXISTS (
		select convexid from Halfspace
		where regionid=regionid
		  and convexid not in (
			select convexid from HalfSpace h
			where regionid=regionid
			and cx*h.x+cy*h.y+cz*h.z<h.c 
			)
	) THEN RETURN 1;
	END IF;
	--
	RETURN 0;
END;

CREATE FUNCTION fRegionContainsPointEq(regionID  bigint, ra float,dec float)
RETURNS bit 
BEGIN
	DECLARE cx float,cy float,cz float;
	SET cx  = COS(RADIANS(dec))*COS(RADIANS(ra));
	SET cy  = COS(RADIANS(dec))*SIN(RADIANS(ra));
	SET cz  = SIN(RADIANS(dec));
	IF EXISTS (
		select convexid from Halfspace
		where regionid=regionid
		  and convexid not in (
			select convexid from HalfSpace h
			where regionid=regionid
			and cx*h.x+cy*h.y+cz*h.z<h.c 
			)
	) THEN RETURN 1;
	END IF;
	--
	RETURN 0;
END;

CREATE FUNCTION fRegionConvexIdToString(regionID bigint, convexID bigint )
RETURNS VARCHAR(8000)
BEGIN  
	DECLARE ConvexString varchar(8000);
	------------------------------------------------
	-- convert it to a normal form (blank separated trailing blank, upper case)
	------------------------------------------------
	SET ConvexString = '';
	------------------------------------------------
	-- start the loop over the convexes
	------------------------------------------------
	SELECT ConvexString = ConvexString   
 			|| STR(x ,19, 16)||' '
 			|| STR(y ,19, 16)||' '
		 	|| STR(z ,19, 16)||' '
 			|| STR(c ,19, 16)||' '
 		FROM HalfSpace
	 	WHERE regionID =regionID
   		  AND convexID = convexID
		ORDER BY regionID, convexID, x,y,z,c;
	-- clean up the convex string.
	SET convexString = fRegionNormalizeString('REGION CONVEX ' || convexString);
	SET convexString = ltrim(replace(convexString, 'REGION CONVEX', '')); -- trim off the prefix
	SET convexString = ltrim(replace(convexString, 'REGION', ''));       -- trim off the prefix
	RETURN convexString;
END;      -- End fRegionConvexIdToString

CREATE FUNCTION fRegionNot(region bigint, convex bigint) 
returns us table(convexID bigint, constraintID bigint, 
                  x float, y float, z float, c float
		  , primary key (convexID, constraintID) )
BEGIN
	----------------------------------------------
	-- myConvex is the ID of this convex 
	-- we will take the Cartesian product of it 
	-- with the negative of the remaining convexes (if any)
	-- then we will add each 1/2 space to each such negative convex.
	----------------------------------------------

	declare myConvex bigint;
	select myConvex = min(convexID)
		from   HalfSpace
		where  regionID = region
		  and  convexID > convex;

	----------------------------------------------
	-- the ME table is a list of negations of all the halfPlanes of this convex.
	-- these halfPlanes have constraintIDs 0,1,2,.. 
	-- so they are dense and zero based.
	-- this is important for the math that follows.
	----------------------------------------------
	declare me table(constraintID bigint identity(0,1),  
		      x float, y float, z float, c float);
	insert INTO me 
		select -x,-y,-z,-c 
		from   HalfSpace
		where  regionID = region
		  and  convexID = myConvex;

	----------------------------------------------
	-- planes is the count of 1/2 spaces in me
	-- others is the number of convexes in the negation after me (or zero if none)
	----------------------------------------------
	declare planes bigint;
	declare others bigint;
	select planes = count(*) from me;
	select others = coalesce(count(distinct convexID),0)
		from   HalfSpace
		where  regionID = region
		 and   convexID > myConvex;

	----------------------------------------------
	-- if there are no others "after" me, the recursion is at an end.
	-- Each of my 1/2 spaces is negated and their union makes a N new convexs 
	-- in the answer
	-- Each half space ID becomes the convexID, and the constraintID is 0.
	----------------------------------------------
	if (others = 0)
	THEN
		insert INTO us     (convexID, constraintID, x, y, z, c)
			select  constraintID,        0, x, y, z, c 
			from   	me;
	else(
	----------------------------------------------
	-- if there are other convexes "after" me, then it is a three step process.
	-- (1) recursively compute the negative region of remaining convexes 
	--     Call that region ?him?; it is a union of convexes.
	-- (2) Cartesian product -me x him    
	-- (3) insert each pair into the answer creating |-me|x|him| edges
	--     that is, for each half-space in me, 
	--     create a new set of convexes that includes that 1/2 space.
	--
	--     the math here is:
	--     me has planes elements.
	--     he has other convexes.
	--     so, for each he, me make planes new convexes planes*other convexes
	--     These convexes need unique names.
	--     If he convexes are numbered 0,.., other-1 
	--     then meconstraintID*(max(maxMeconstraintID,maxHimConvexID)+1)+ HimConvexID
	--     is a convenient way to assign convexIDs
	----------------------------------------------
		----------------------------------------------
		-- this table stores the Cartesian product
		----------------------------------------------
		declare Product table ( meconstraintID bigint, 
			meX float,  meY float,  meZ float,  meL float,
			himConvexID bigint, himconstraintID bigint, 
			himX float, himY float, himZ float, himL float);

		----------------------------------------------
		-- compute the Cartesian product: use SQL join with no where clause.
		----------------------------------------------
		declare him table (himConvexID bigint, himconstraintID bigint, 
			himX float, himY float, himZ float, himL float);

		----------------------------------------------
		-- compute the Cartesian product: use SQL join with no where clause.
		----------------------------------------------
		insert  INTO Product
			select me.*, him.*
			from  me as me,
			      fRegionNot(region, myConvex) as him
		declare maxMeID bigint, maxHimID bigint, maxIdPlus1 bigint;
		select maxMeID = max(meconstraintID) from Product;
		select maxHimID = max(himConvexID) from product;
		set    maxIdPlus1 = case when maxMeID < maxHimID 
					then maxHimID + 1 
					else maxMeID + 1 
				end case;

		----------------------------------------------
		-- now insert each negative 1/2 space of my convex 
		-- as 1/2 space zero of the new convexes. 
		-- note the "distinct" causes us to contribute this 1/2 space only once 
		-- to each new convex.
		----------------------------------------------
	 	insert  INTO us
			select distinct meconstraintID*maxIdPlus1+himConvexID, 
				0, meX, meY, meZ, meL 
			from Product;

		----------------------------------------------
		-- now insert all negative 1/2 space of his convexes into each new convex 
		-- as 1/2 space 1,2,... of the new convexes 
		-- (notice the +1) because me is at 0. 
		----------------------------------------------
		insert  INTO us
			select meconstraintID*maxIdPlus1+himConvexID, 
				himconstraintID+1, himX, himY, himZ, himL 
			from Product;
		);
		----------------------------------------------
		-- end of recursion.
		----------------------------------------------
	end if;
	-- return the answer.
	RETURN us;
END;

CREATE  FUNCTION fRegionArcLength(
	ra1 float, dec1 float,
	ra2 float, dec2 float,
	x float, y float, z float, c float
)
RETURNS float
BEGIN
	DECLARE wx float, wy float, wz float,
		ux float, uy float, uz float,
		x1 float, y1 float, z1 float,
		x2 float, y2 float, z2 float,
		ra float, s  float, 
		a1 float, a2 float,
		w1 float, u1 float, 
		w2 float, u2 float,
		angle float;

	-------------------------------------
	-- Compute the Cartesian vectors
	-------------------------------------	
	SELECT 	x1 = COS(RADIANS(dec1))*COS(RADIANS(ra1)),
		y1 = COS(RADIANS(dec1))*SIN(RADIANS(ra1)),
		z1 = SIN(RADIANS(dec1)),
	 	x2 = COS(RADIANS(dec2))*COS(RADIANS(ra2)),
		y2 = COS(RADIANS(dec2))*SIN(RADIANS(ra2)),
		z2 = SIN(RADIANS(dec2));
	----------------------------------------
	-- Compute the parameters of the circle
	----------------------------------------
	SELECT	s  = CASE WHEN c<1 THEN sqrt(1-c*c) ELSE 0 END CASE,
		ra = (CASE WHEN abs(z)<1 THEN degrees(ATN2(y,x)) ELSE 0 END)
			  + (CASE WHEN y<0 THEN 360 ELSE 0 END);
	-------------------------------------------
	-- Compute the West pointing normal vector
	-------------------------------------------
	SELECT 	wx = -SIN(ACOS(c))* SIN(radians(ra))/s,
		wy = SIN(ACOS(c))* COS(radians(ra))/s,
		wz = 0;
	--------------------------------------------
	-- Compute the North pointing normal vector
	--------------------------------------------
	SELECT ux = x, uy = y, uz = z
	    FROM fWedgeV3(wx,wy,wz,x,y,z);
	--
	SELECT 	w1 = x1*wx+y1*wy+z1*wz,
		u1 = x1*ux+y1*uy+z1*uz,
		w2 = x2*wx+y2*wy+z2*wz,
		u2 = x2*ux+y2*uy+z2*uz;
	--
	SELECT 	a1 = DEGREES(ATN2(w1,u1))
		  +(CASE WHEN w1<0 THEN 360 ELSE 0 END),
		a2 = DEGREES(ATN2(w2,u2))
		  +(CASE WHEN w2<0 THEN 360 ELSE 0 END);
	--
	SET angle = a2-a1 + CASE WHEN a2<a1 THEN 360 ELSE 0 END;
	------------------------------------------
	-- multiply with the radius of the circle
	-- and convert length in arcmins
	------------------------------------------
	RETURN 60*angle*s;
END;

CREATE  FUNCTION fRegionAreaTriangle(
	ra1 float, dec1 float,
	ra2 float, dec2 float,
	x3  float, y3 float, z3 float
)
RETURNS float
BEGIN
	DECLARE	x1 float, y1 float, z1 float,
		x2 float, y2 float, z2 float,
		a1 float, a2 float, a3 float, 
		det float,area float,s float;
	---------------------------
	-- Cartesian delta vectors
	---------------------------	
	SELECT 	x1 = COS(RADIANS(dec1))*COS(RADIANS(ra1))-x3,
		y1 = COS(RADIANS(dec1))*SIN(RADIANS(ra1))-y3,
		z1 = SIN(RADIANS(dec1))-z3,
	 	x2 = COS(RADIANS(dec2))*COS(RADIANS(ra2))-x3,
		y2 = COS(RADIANS(dec2))*SIN(RADIANS(ra2))-y3,
		z2 = SIN(RADIANS(dec2))-z3;
	-------------------
	-- the determinant
	-------------------
	SELECT area=0, det = (x3*x+y3*y+z3*z) 
	    FROM fWedgeV3(x1,y1,z1,x2,y2,z2);
	--
	IF (abs(det)>1.0E-15) 
	THEN (
		---------------------------
		-- compute the three sides
		---------------------------
		SELECT 	a1 = 2.0*ASIN(0.5*SQRT(POWER(x2,2)+POWER(y2,2)+POWER(z2,2))),
			a2 = 2.0*ASIN(0.5*SQRT(POWER(x1,2)+POWER(y1,2)+POWER(z1,2))),
			a3 = 2.0*ASIN(0.5*SQRT(POWER(x1-x2,2)+POWER(y1-y2,2)+POWER(z1-z2,2)));
		--
		SELECT s  = 0.5*(a1+a2+a3);
		SELECT area = -PI() 
			+2*ASIN(SQRT(SIN(s-a2)*SIN(s-a3)/(SIN(a2)*SIN(a3))))
			+2*ASIN(SQRT(SIN(s-a1)*SIN(s-a3)/(SIN(a1)*SIN(a3))))
			+2*ASIN(SQRT(SIN(s-a2)*SIN(s-a1)/(SIN(a2)*SIN(a1))));
	);
	END IF;
	--
	IF det<0 
		THEN SET area = -area;
	END IF;
	RETURN area*POWER(180/PI(),2);
END;

CREATE  FUNCTION fRegionAreaSemiLune(
	ra1 float, dec1 float,
	ra2 float, dec2 float,
	x3  float, y3 float, z3 float, c float
)
RETURNS float
BEGIN
	DECLARE	x1 float, y1 float, z1 float,
		x2 float, y2 float, z2 float,
		asmall float, agreat float, area float,
		alpha float, beta float, 
		a float, b float, det float;

	IF c=0
	THEN (
	   SET area = 0;
	   RETURN POWER(180/PI(),2)*area;
	);
	END IF;
	-------------------------------------
	-- Compute the Cartesian vectors
	-------------------------------------	
	SELECT 	x1 = COS(RADIANS(dec1))*COS(RADIANS(ra1)),
		y1 = COS(RADIANS(dec1))*SIN(RADIANS(ra1)),
		z1 = SIN(RADIANS(dec1)),
	 	x2 = COS(RADIANS(dec2))*COS(RADIANS(ra2)),
		y2 = COS(RADIANS(dec2))*SIN(RADIANS(ra2)),
		z2 = SIN(RADIANS(dec2));
	--
	SELECT det = x1*y2*z3 + y1*z2*x3 + z1*x2*y3
		     -x1*z2*y3 - y1*x2*z3 - z1*y2*x3;
	--
	IF (c<0) THEN SELECT x3=-x3, y3=-y3, z3=-z3;
	END IF;
	-------------------------------------------
	-- compute relevant angles, using 
	-- approximations stable for small angles
	-------------------------------------------
	SELECT alpha = 2.0*ASIN(0.5*SQRT(POWER(x3-x1,2)+POWER(y3-y1,2)+POWER(z3-z1,2)));
	SELECT b     = 2.0*ASIN(0.5*SQRT(POWER(x2-x1,2)+POWER(y2-y1,2)+POWER(z2-z1,2))/SIN(alpha));
	SELECT beta  = 2.0*ASIN(0.5*SQRT(POWER(x2-x1,2)+POWER(y2-y1,2)+POWER(z2-z1,2)));
	SELECT a     = ASIN(TAN(0.5*beta)/TAN(alpha));
	------------------------
	-- now the precise area
	------------------------
	SET area = (2*a-b*COS(alpha));
	IF det<0 
		THEN SET area = 2*PI()*(1-cos(alpha))-area;
	END IF;
	IF c<0 
		THEN SET area = -area;
	END IF;
	RETURN POWER(180/PI(),2)*area;
END;

CREATE  FUNCTION fRegionAreaPatch(
	regionid bigint,
	convexid bigint,
	patch int
)
RETURNS float
BEGIN
	DECLARE area float, rows int, 
		x float, y float, z float, c float;
	--
	SELECT x=x, y=y, z=z 
	    FROM RegionConvex with (nolock)
	    WHERE regionid=regionid
	      and convexid=convexid
	      and patch=patch;

	--------------------------------------------
	-- collect the drawable arcs from the patch
	--------------------------------------------
	DECLARE arcs TABLE (
		arcid int identity(1,1) PRIMARY KEY,
		constraintid bigint,		-- constraintid 
		state int,			-- 3:BC, 2:ROOT, 1:HOLE
		ra1 float, dec1 float, 		-- beginning coordinate	
		ra2 float, dec2 float,		-- final coordinate
		x float, y float, z float, 	-- Normal vector of circle
		c float				-- Offset of circle
	);
	--
	INSERT INTO arcs(constraintid,state,ra1,dec1,ra2,dec2,x,y,z,c)
	SELECT constraintid,state,ra1,dec1,ra2,dec2,x,y,z,c
	  FROM RegionArcs
	  WHERE regionid=regionid
	    and convexid=convexid
	    and patch=patch
	    and draw=1;
	--
	SELECT area=0, rows=count(*) FROM arcs;
	---------------------------------
	-- test for null
	---------------------------------
	IF (rows=0) 
		THEN Return area;
	END IF;
	--------------------------------
	-- test for 1 constraint
	-- area is negative for a hole
	--------------------------------
	IF rows=1
	THEN (
	    SELECT c = c FROM arcs
	    SELECT area = 2*PI()*(1-c)*POWER(180/PI(),2);
	    IF c<0 
		THEN SET area=-area;
	    END IF;
	    Return area;
	);
	END IF;
	---------------------------------------------
	-- add area of the great circle triangles,
	-- and adjust area with the semilunes
	---------------------------------------------
	SELECT area = area
		+ fRegionAreaTriangle(ra1,dec1,ra2,dec2,x,y,z)
		+ fRegionAreaSemiLune(ra1,dec1,ra2,dec2,x,y,z,c)
	    FROM arcs;
	----------------------------------
	RETURN area;
END;

CREATE  FUNCTION fRegionToArcs(regionid bigint)
RETURNS TABLE (
		regionid bigint, 		-- regionid
		convexid bigint, 		-- convexid
		constraintid bigint,		-- constraintid 
		patch int,			-- patch number
		state int,			-- 3:BC, 2:ROOT, 1:HOLE
		draw int,			-- 1: draw, 0: hide
		ra1 float, dec1 float, 		-- beginning coordinate	
		ra2 float, dec2 float,		-- final coordinate
		x float, y float, z float, 	-- Normal vector of circle
		c float,			-- Offset of circle
		length float			-- length of the arc in arcmins
	    )
BEGIN
	RETURN TABLE(
	SELECT * FROM fRegionStringToArcs(regionid, '', 'AND'));
END;

CREATE FUNCTION fRegionBoundingCircle(regionid bigint)
RETURNS 
	rcvx TABLE (
		regionid bigint,
		convexid bigint,
		patch int,
		type varchar(16),
		radius float,
		ra float, dec float,
		x float, y float, z float, c float,
		htmid bigint,
		convexString varchar(7900)
	)
BEGIN
	DECLARE	convexString varchar(7500),
		rootid int,
		type varchar(16);

	-----------------
	-- get the type
	-----------------
	SELECT type=type FROM Region
	    WHERE regionid=regionid;

	---------------------
	-- table of the arcs
	---------------------
	DECLARE arcs TABLE (
		regionid bigint, 			-- regionid
		convexid bigint, 			-- convexid
		constraintid bigint,			-- constraintid 
		patch int,				-- patch number
		state int,				-- 3:BC, 2:ROOT, 1:HOLE
		draw int,				-- 1: draw, 0: hide
		ra1 float, dec1 float, 			-- beginning coordinate	
		ra2 float, dec2 float,			-- final coordinate
		x float, y float, z float, c float,	-- Equation of circle
		length float,				-- length of arc in arcmins
		arcid int identity(1,1),		-- arcid
		PRIMARY KEY(arcid)
	    );
	-----------------------------
	-- insert drawable arcs only
	-----------------------------
	INSERT INTO arcs
	SELECT * FROM fRegionStringToArcs(regionid,'','AND')
	WHERE draw=1;

	----------------------------
	-- store the halfspace rows
	----------------------------
	DECLARE halfspace TABLE (
		constraintid bigint,
		convexid bigint,
		patch int,
		x float, y float, z float, c float,
		PRIMARY KEY(convexid,patch,constraintid)
	);
	--
	INSERT INTO halfspace
	SELECT  distinct constraintid, 
		convexid, patch, x,y,z,c
	  FROM arcs;

	---------------------------------------------
	-- extract the best existing bounding circles
	---------------------------------------------
	DECLARE bcarcs TABLE (
		regionid bigint,
		convexid bigint,
		patch int,
		constraintid bigint,
		x float, y float, z float, c float,
		PRIMARY KEY (convexid, patch, constraintid)
	);
	--
	INSERT INTO bcarcs
	SELECT b.regionid, b.convexid, b.patch, 
		b.constraintid, b.x, b.y, b.z, b.c
	FROM arcs b, (
		select a.convexid, a.patch, 
		   (select top 1 arcid from arcs 
		    where convexid=a.convexid and patch=a.patch
		    order by c desc
		   ) as arcid
	  	from (select distinct convexid, patch from arcs) a
		group by a.convexid, a.patch
		) q
	WHERE b.arcid=q.arcid
	  and b.convexid=q.convexid
	  and b.patch=q.patch;

	-----------------------------
	-- extract all roots
	----------------------
	DECLARE roots TABLE (
		rootid int,
		arcid int,
		convexid bigint,
		constraintid bigint,
		patch int,
		x float, y float, z float,
		flag int,	-- 1: begin, 2:mid
		PRIMARY KEY (arcid, flag)
	) ;
	--
	INSERT INTO roots
	SELECT 0, arcid, convexid, constraintid, patch,
		COS(RADIANS(dec1))*COS(RADIANS(ra1)), 
		COS(RADIANS(dec1))*SIN(RADIANS(ra1)), 
		SIN(RADIANS(dec1)), 1
	  FROM arcs where draw=1 and state=2;

	IF rowcount!=0 
	THEN( 
		-----------------------------------
	-- insert the midpoint of c>0 arcs
	-----------------------------------
	DECLARE mid TABLE (
		arcid bigint,
		convexid bigint,
		constraintid bigint,
		patch int,
		sc float, c float,
		x1 float, y1 float, z1 float,
		x2 float, y2 float, z2 float,
		x3 float, y3 float, z3 float
	);
	INSERT INTO mid
	SELECT	arcid, convexid, constraintid, patch, 0, c,
		COS(RADIANS(dec1))*COS(RADIANS(ra1))-x*c,
		COS(RADIANS(dec1))*SIN(RADIANS(ra1))-y*c,
		SIN(RADIANS(dec1))-z*c,
		COS(RADIANS(dec2))*COS(RADIANS(ra2))-x*c,
		COS(RADIANS(dec2))*SIN(RADIANS(ra2))-y*c,
		SIN(RADIANS(dec2))-z*c,
		x,y,z
	  FROM arcs
	  WHERE draw=1 and state=2 and c>0 and c<1;
	--
	UPDATE mid
	    SET sc = SIGN(x1*y2*z3+y1*z2*x3 + z1*x2*y3 
		-z1*y2*x3 -y1*x2*z3 -x1*z2*y3)
		/SQRT(2*(1+(x1*x2+y1*y2+z1*z2)/(1-c*c)));
	---------------------------------------------------
	-- now (x3,y3,z3) is the 3-vector of the mid-point
	---------------------------------------------------
	INSERT INTO roots
	SELECT 0, arcid, convexid, constraintid, patch,
		sc*(x1+x2) + x3*c as x,
		sc*(y1+y2) + y3*c as y,
		sc*(z1+z2) + z3*c as z,
		2 as flag
	  FROM mid;

	------------------------------------------------
	-- update rootid, make sure it is in good order
	------------------------------------------------
	SET rootid = 0;
	UPDATE r
	  SET r.rootid=rootid,
		rootid=rootid+1
	  FROM roots r;

	-------------------------------------------
	-- get the pairwise distances within patch
	-- but only use the original vertices
	-------------------------------------------
	DECLARE pairs TABLE (
		convexid bigint,
		patch int,
		r1 int,
		r2 int,
		x float, y float, z float, c float
	);
	--
	INSERT INTO pairs
	SELECT c.convexid, c.patch,
		(select top 1 a.rootid
		  from roots a, roots b
		  where a.rootid<b.rootid 
		    and a.convexid=b.convexid and a.convexid=c.convexid 
		    and a.patch=b.patch  and a.patch=c.patch
		    and a.flag=1 and b.flag=1
		  order by a.x*b.x+a.y*b.y+a.z*b.z asc
		) as r1,
		(select top 1 b.rootid
		  from roots a, roots b
		  where a.rootid<b.rootid 
		    and a.convexid=b.convexid and a.convexid=c.convexid 
		    and a.patch=b.patch  and a.patch=c.patch
		    and a.flag=1 and b.flag=1
		  order by a.x*b.x+a.y*b.y+a.z*b.z asc
		) as r2,
		0,0,0,0
	  FROM (select distinct convexid, patch from arcs) c
	  GROUP BY c.convexid, c.patch;
	--
	UPDATE p
	    SET	x=(a.x+b.x)/SQRT(2.0*(1+a.x*b.x+a.y*b.y+a.z*b.z))
		  *(CASE WHEN c.c<0 THEN -1 ELSE 1 END),
		y=(a.y+b.y)/SQRT(2.0*(1+a.x*b.x+a.y*b.y+a.z*b.z)) 
		  *(CASE WHEN c.c<0 THEN -1 ELSE 1 END),
		z=(a.z+b.z)/SQRT(2.0*(1+a.x*b.x+a.y*b.y+a.z*b.z)) 
		  *(CASE WHEN c.c<0 THEN -1 ELSE 1 END),
		c=CASE WHEN a.x*b.x+a.y*b.y+a.z*b.z>1 THEN 1 
		  ELSE COS(0.5*ACOS(a.x*b.x+a.y*b.y+a.z*b.z)) END
	    FROM pairs p, roots a, roots b, bcarcs c
	    WHERE p.r1=a.rootid and p.r2=b.rootid
		and p.convexid=c.convexid and p.patch=c.patch;

	-----------------------------------------------
	-- replace circle if no roots in patch outside
	-----------------------------------------------
	UPDATE bcarcs
	  SET	constraintid=0,
		x = p.x, y=p.y, z=p.z, c=p.c
	  FROM  pairs p, bcarcs c
	  WHERE p.convexid=c.convexid
	    and p.patch=c.patch
	    and p.c>c.c
	    and not exists (
		select rootid from roots
		where convexid=p.convexid and patch=p.patch
		  and x*p.x+y*p.y+z*p.z-p.c<-1.0e-8
		);

	----------------------------------------------
	-- if there are any original bounding circles
	-- consider doing the 3point test. Only use
	-- patches with the original bounding circle.
	----------------------------------------------
	DECLARE triplets TABLE (
		id int IDENTITY(1,1),
		convexid bigint, patch int,
		r1 int,	r2 int,r3 int,
		x float, y float, z float, c float,
		r4 int,
		PRIMARY KEY (convexid, patch, r1, r2, r3)
	);
	--
	INSERT INTO triplets
	SELECT 	r1.convexid, r1.patch, 
		r1.rootid, r2.rootid, r3.rootid,
		0 as x, 0 as y, 0 as z, 0 as c, 0 as r4
	FROM roots r1, roots r2, roots r3,
		(select distinct convexid, patch from bcarcs
		 where constraintid>0) q
	  WHERE r1.convexid=r2.convexid and r1.patch=r2.patch
	    and r1.convexid=r3.convexid and r1.patch=r3.patch
	    and r1.rootid<r2.rootid and r2.rootid<r3.rootid
	    and r1.convexid= q.convexid and r1.patch=q.patch;
	----------------------------------------
	-- compute the bounding circle for each
	----------------------------------------
	UPDATE t
	    SET	x =(r3.y-r2.y)*(r1.z-r2.z)-(r3.z-r2.z)*(r1.y-r2.y),
		y =(r3.z-r2.z)*(r1.x-r2.x)-(r3.x-r2.x)*(r1.z-r2.z),
		z =(r3.x-r2.x)*(r1.y-r2.y)-(r3.y-r2.y)*(r1.x-r2.x)
	FROM triplets t, roots r1, roots r2, roots r3
	WHERE t.r1=r1.rootid and t.r2=r2.rootid and t.r3=r3.rootid;
	------------------------
	-- normalize the vector
	------------------------
	UPDATE triplets
	    SET	x = x/SQRT(x*x+y*y+z*z),
		y = y/SQRT(x*x+y*y+z*z),
		z = z/SQRT(x*x+y*y+z*z);
	-----------------------
	-- compute the offset
	-----------------------
	UPDATE t
	    SET	c = t.x*r.x+t.y*r.y+t.z*r.z
	  FROM triplets t, roots r
	  WHERE t.r1 = r.rootid;
	------------------------
	-- look for elimination
	------------------------
	UPDATE t
	    SET r4 = r.rootid
	  FROM triplets t, roots r
	  WHERE t.convexid=r.convexid and t.patch=r.patch
	    and t.r1!=r.rootid and t.r2!=r.rootid and t.r3!=r.rootid
	    and t.x*r.x+t.y*r.y+t.z*r.z<t.c;

	-----------------------------------
	-- from each patch select the best,
	-- and compare to original BC
	-----------------------------------
	UPDATE b
	    SET constraintid=0,
		x = t.x, 
		y = t.y, 
		z = t.z, 
		c = t.c
	FROM triplets t, bcarcs b, (
		select a.convexid, a.patch, 
		   (select top 1 id from triplets
		    where convexid=a.convexid and patch=a.patch and r4=0
--		    where convexid=a.convexid and patch=a.patch
		    order by c desc
		   ) as id
	  	from (select distinct convexid, patch from triplets) a
		group by a.convexid, a.patch
		) q
	WHERE t.id=q.id
	  and t.convexid=q.convexid and t.patch=q.patch
	  and t.convexid=b.convexid and t.patch=b.patch
	  and t.c>b.c;
	)
	END IF;
	
	------------------------------------
	-- compute max distance from center
	------------------------------------
	INSERT INTO rcvx
	SELECT	regionid,
		convexid, 
		--constraintid, 
		patch,
		type as type, 
		CASE WHEN c<1 THEN 60*DEGREES(ACOS(c)) ELSE 0.0 END as radius, 
		ra	= (CASE WHEN abs(z)<1 THEN degrees(ATN2(y,x)) ELSE 0 END)
	 	 	  +(CASE WHEN y<0 THEN 360 ELSE 0 END),
		dec	= (CASE WHEN abs(z)<1 THEN degrees(ASIN(z)) 
			ELSE 90*sign(z) END),
		x,y,z,c,
		fHtmLookupXYZ(x,y,z) as htmid, 
		'' as convexString
	FROM bcarcs;
	RETURN rcvx;
END;

CREATE FUNCTION fRegionOverlapString(
	regionid bigint,
	regionString varchar(8000), 
	buffer float
)
RETURNS varchar(7500) 
BEGIN
	DECLARE convexid 	int,
		area		varchar(8000),
		overlap	varchar(8000);
	----------------------------------------------------
	-- representation of the input region after the fuzz
	----------------------------------------------------
	DECLARE halfspace TABLE (
		id 	bigint PRIMARY KEY NOT NULL,
		convexID bigint NOT NULL,
		x	float NOT NULL,
		y 	float NOT NULL,
		z	float NOT NULL,
		c	float NOT NULL,
		radius float NOT NULL
	);
	----------------------------------------
	-- this contains the prefetched convexes
	-- matching the type constraint
	----------------------------------------
	DECLARE convex TABLE (
		regionid bigint,
		convexid bigint,
		patch int,
		type varchar(16),
		radius float,
		x float, y float, z float, c float,
		PRIMARY KEY (regionid, convexid, patch)
	);
	-------------------------------------------
	-- the arcs from the precise intersection
	-------------------------------------------
	DECLARE arcs TABLE (
		convexid bigint,
		constraintid bigint,
		x float,
		y float,
		z float,
		c float
	);
	----------------------------------------------------
	-- turn the regionString into a region table
	----------------------------------------------------
	INSERT INTO halfspace 
	    SELECT *, acos(c) as radius 
	    FROM fRegionFromString(regionString,buffer);
	----------------------------------------------------
	-- now convert back to string for later use
	-----------------------------------------------------
	SET area = 'REGION ';
	SET convexid=-99999;
	SELECT area = area
		||(CASE WHEN convexid>convexid THEN 'CONVEX ' ELSE '' END)
		|| STR(x,18,15)||' '
		|| STR(y,18,15)||' '
		|| STR(z,18,15)||' '
		|| STR(c,18,15)||' ',
		convexid=convexid
		FROM halfspace
		ORDER BY convexid, id;
	--
	INSERT INTO convex
	    SELECT regionid, convexid, patch, type, RADIANS(radius/60), x,y,z,c
	    FROM RegionConvex with (nolock)
	    WHERE regionid=regionid;
	------------------------------------------------------
	-- Do a trivial reject if any two constraints are exclusive.
	-- Use ASIN distance for stability. (see Word doc)
	------------------------------------------------------
	IF NOT EXISTS ( 	
		select 	h2.convexid, h2.patch, h1.convexid
		from halfspace as h1, convex h2 
		group by h2.convexid, h2.patch, h1.convexid
		having
		    max(case when (sqrt(power(h1.x-h2.x,2)
			+power(h1.y-h2.y,2)
			+power(h1.z-h2.z,2))/2)<1
		    then 2*asin(0.5*sqrt(power(h1.x-h2.x,2)
			+power(h1.y-h2.y,2)
			+power(h1.z-h2.z,2))) 
		    else PI() end - (h1.radius+h2.radius)) <0
	) THEN RETURN NULL;
	END IF;
	--
	INSERT INTO arcs
	    SELECT convexid, constraintid, x,y,z,c
	    FROM fRegionStringToArcs(regionID,area,'AND')
	    IF rowcount>0
		THEN (
		    SET overlap = 'REGION ';
		    SET convexid = -999999;
		    SELECT overlap = overlap
			||(CASE WHEN convexid>convexid 
			  THEN 'CONVEX ' ELSE '' END)
			|| STR(x,18,15)||' '
			|| STR(y,18,15)||' '
			|| STR(z,18,15)||' '
			|| STR(c,18,15)||' ',
			convexid=convexid
		    FROM (select distinct constraintid, 
			convexid, x,y,z,c 
			from arcs) o
		    ORDER BY convexid, constraintid;
		);
	   END IF;

	RETURN overlap;
END;

CREATE FUNCTION fRegionOverlapId(
	regionid bigint,
	otherid bigint,
	buffer float
)
RETURNS varchar(7500) 
BEGIN
	DECLARE convexid 	int,
		area		varchar(8000),
		overlap	varchar(8000),
		regionString varchar(8000);
	----------------------------------------------------
	-- representation of the input region after the fuzz
	----------------------------------------------------
	DECLARE halfspace TABLE (
		id 	bigint PRIMARY KEY NOT NULL,
		convexID bigint NOT NULL,
		x	float NOT NULL,
		y 	float NOT NULL,
		z	float NOT NULL,
		c	float NOT NULL,
		radius float NOT NULL
	);
	----------------------------------------
	-- this contains the prefetched convexes
	-- matching the type constraint
	----------------------------------------
	DECLARE convex TABLE (
		regionid bigint,
		convexid bigint,
		patch int,
		type varchar(16),
		radius float,
		x float, y float, z float, c float,
		PRIMARY KEY (regionid, convexid, patch)
	);
	-------------------------------------------
	-- the arcs from the precise intersection
	-------------------------------------------
	DECLARE arcs TABLE (
		convexid bigint,
		constraintid bigint,
		x float,
		y float,
		z float,
		c float
	);
	----------------------------------------------------
	-- turn the regionString into a region table
	----------------------------------------------------
	INSERT INTO halfspace 
	    SELECT constraintid, convexid, x,y,z,c,
		acos(c) as radius 
	    FROM Halfspace
	    WHERE regionid=otherid;
	-----------------------
	-- grow by the buffer
	-----------------------
	UPDATE halfspace
	    SET c = fRegionFuzz(c, buffer);
	UPDATE halfspace
	    SET radius = acos(c);

	----------------------------------------------------
	-- now convert back to string for later use
	-----------------------------------------------------
	SET area = 'REGION ';
	SET convexid=-99999;
	SELECT area = area
		||(CASE WHEN convexid>convexid THEN 'CONVEX ' ELSE '' END)
		|| STR(x,18,15)||' '
		|| STR(y,18,15)||' '
		|| STR(z,18,15)||' '
		|| STR(c,18,15)||' ',
		convexid=convexid
		FROM halfspace
		ORDER BY convexid, id;
	--
	INSERT INTO convex
	    SELECT regionid, convexid, patch, type, RADIANS(radius/60), x,y,z,c
	    FROM RegionConvex with (nolock)
	    WHERE regionid=regionid;
	------------------------------------------------------
	-- Do a trivial reject if any two constraints are exclusive.
	-- Use ASIN distance for stability. (see Word doc)
	------------------------------------------------------
	IF NOT EXISTS ( 	
		select 	h2.convexid, h2.patch, h1.convexid
		from halfspace as h1, convex h2 
		group by h2.convexid, h2.patch, h1.convexid
		having
		    max(case when (sqrt(power(h1.x-h2.x,2)
			+power(h1.y-h2.y,2)
			+power(h1.z-h2.z,2))/2)<1
		    then 2*asin(0.5*sqrt(power(h1.x-h2.x,2)
			+power(h1.y-h2.y,2)
			+power(h1.z-h2.z,2))) 
		    else PI() end - (h1.radius+h2.radius)) <0
	) THEN RETURN NULL;
	END IF;
	--
	INSERT INTO arcs
	    SELECT convexid, constraintid, x,y,z,c
	    FROM fRegionStringToArcs(regionID,area,'AND')
	    IF rowcount>0
		THEN (
		    SET overlap = 'REGION ';
		    SET convexid = -999999;
		    SELECT overlap = overlap
			||(CASE WHEN convexid>convexid 
			  THEN 'CONVEX ' ELSE '' END)
			|| STR(x,18,15)||' '
			|| STR(y,18,15)||' '
			|| STR(z,18,15)||' '
			|| STR(c,18,15)||' ',
			convexid=convexid
		    FROM (select distinct constraintid, 
			convexid, x,y,z,c 
			from arcs) o
		    ORDER BY convexid, constraintid;
		);
		END IF;

	RETURN overlap;
END;

CREATE FUNCTION fFootprintEq(ra float, dec float, radius float)
RETURNS TABLE (type varchar(16))
BEGIN
	RETURN TABLE(
	  SELECT distinct type
	  FROM fRegionsContainingPointEq(ra, dec,'CHUNK,PRIMARY,TILE,SECTOR',radius));
END;


-------------------------URL GENERATION----------------------------------------

CREATE FUNCTION fGetUrlFitsField(fieldId bigint)
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), run varchar(8), rerun varchar(8),
		run6 varchar(10), stripe varchar(8), camcol varchar(8), 
		field varchar(8), startMu varchar(10), skyVersion varchar(8);
	SET link = (select value from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT skyVersion=cast(fSkyVersion(fieldid) as varchar(8));
	IF (skyVersion = '0')
		THEN SET link = link || 'inchunk_target/';
	ELSEIF (skyVersion = '1')
		THEN SET link = link || 'inchunk_best/';
	ELSE
		SET link = link || 'inchunk_runs/';
	END IF;
	SELECT  run = cast(f.run as varchar(8)), 
		rerun=cast(f.rerun as varchar(8)), 
		startMu=cast(s.startMu as varchar(10)), 
		stripe=cast(s.stripe as varchar(8)),
		camcol=cast(f.camcol as varchar(8)), 
		field=cast(f.field as varchar(8))
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
	SET link = (select value from SiteConstants where name='DataServerURL');
	SET link = link || 'spectro/1d_23/';
	SELECT mjd = cast(p.mjd as varchar(8)), plate=cast(p.plate as varchar(8)), fiber=cast(s.fiberID as varchar(8)) 
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
	select WebServerURL = cast(value as varchar(500))
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
	SET link = (select value from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  run = cast(run as varchar(8)), rerun=cast(rerun as varchar(8)), 
		camcol=cast(camcol as varchar(8)), field=cast(field as varchar(8))
	    FROM Field
	    WHERE fieldId=fieldId;
	SET run6   = substring('000000',1,6-len(run)) || run;
	SET field = substring('0000',1,4-len(field)) || field;
	RETURN 	 link || run || '/' || rerun || '/objcs/'||camcol||'/fpAtlas-'||run6||'-'||camcol||'-'||field||'.fit.gz';
END;

CREATE FUNCTION fGetUrlNavEq(ra float, dec float)
returns varchar(256)
begin
	declare WebServerURL varchar(500);
	set WebServerURL = 'http://localhost/';
	select WebServerURL = cast(value as varchar(500))
		from SiteConstants
		where name ='WebServerURL';
	return WebServerURL || 'tools/chart/navi.asp?zoom=1&ra=' 
		|| ltrim(str(ra,10,6)) || '&dec=' || ltrim(str(dec,10,6));
end;

CREATE FUNCTION fGetUrlFitsBin(fieldId bigint, filter varchar(4))
RETURNS varchar(128)
BEGIN
	DECLARE link varchar(128), run varchar(8), rerun varchar(8),
		camcol varchar(8), field varchar(8), run6 varchar(10);
	SET link = (select value from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  run = cast(run as varchar(8)), rerun=cast(rerun as varchar(8)), 
		camcol=cast(camcol as varchar(8)), field=cast(field as varchar(8))
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
	SET link = (select value from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  run = cast(run as varchar(8)), rerun=cast(rerun as varchar(8)), 
		camcol=cast(camcol as varchar(8)), field=cast(field as varchar(8))
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
	select WebServerURL = cast(value as varchar(500))
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
	SET link = (select value from SiteConstants where name='DataServerURL');
	SET link = link || 'imaging/';
	SELECT  run = cast(run as varchar(8)), rerun=cast(rerun as varchar(8)), 
		camcol=cast(camcol as varchar(8)), field=cast(field as varchar(8))
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
	declare dec float;
	set ra = 0
	set dec = 0;
	set WebServerURL = 'http://localhost/';
	select WebServerURL = cast(value as varchar(500))
		from SiteConstants where name ='WebServerURL'; 
	select ra = ra, dec = dec
	from PhotoObjAll
	where objID = objId;
	return WebServerURL ||'tools/explore/obj.asp?id=' 
		|| cast(objId as varchar(32));
end;

CREATE FUNCTION fGetUrlExpEq(ra float, dec float)
returns varchar(256)
	begin
	declare WebServerURL varchar(500);
	set WebServerURL = 'http://localhost/';
	select WebServerURL = cast(value as varchar(500))
		from SiteConstants
		where name ='WebServerURL';
	return WebServerURL || 'tools/explore/obj.asp?ra=' 
		|| ltrim(str(ra,10,6)) || '&dec=' || ltrim(str(dec,10,6))
end;

CREATE FUNCTION fGetUrlNavId(objId bigint)
returns varchar(256)
begin
	declare WebServerURL varchar(500);
	declare ra float;
	declare dec float;
	set ra = 0
	set dec = 0;
	set WebServerURL = 'http://localhost/';
	select WebServerURL = cast(value as varchar(500))
		from SiteConstants where name ='WebServerURL'; 
	select ra = ra, dec = dec
	from PhotoObjAll
	where objID = objId;
	return WebServerURL ||'tools/chart/navi.asp?zoom=1&ra=' 
		|| ltrim(str(ra,10,10)) || '&dec=' || ltrim(str(dec,10,10));
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
	IF foreignkey != '' 
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
    from ( SELECT	distinct convert(sysname,c.name) as name,
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
    from ( SELECT	distinct convert(sysname,c.name) as name,
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
	SELECT	distinct name = convert(sysname,c.name),
		type =	t.name,
		length = coalesce(d.length, c.length), 
		input = case (substring(c.name,1,1)) 
			when '' then 'yes' else 'no' end,   
		output = case isoutparam  
			when 1 then 'yes' else 
			    case substring(c.name,1,1)  
				when '' then 'no' else 'yes' end
			end,
		pnum = colid  
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
	value binary(8), 
	type varchar(32), 
	length int)
RETURNS varchar(64)
BEGIN
    DECLARE vstr varchar(64),
	vbin4 binary(4),
	vint int;
    SET vbin4 = CAST(value as binary(4));
    SET vint  = cAST(value as int);
    --	
    IF (type<>'binary')
	THEN SET vstr = CAST(vint as varchar(64));
    ELSE( 
        IF (length = 8)
	   THEN EXEC master..xp_varbintohexstr value, vstr OUTPUT;	
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
    IF (select count_big(*) from PhotoTag WITH (nolock) where objID = objID) > 0
        THEN RETURN objID;
    	ELSE(
        	SET objID = objID + firstfieldbit;
        	IF (select count_big(*) from PhotoTag WITH (nolock) where objID = objID) > 0
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
    IF (select count_big(*) from PhotoPrimary WITH (nolock) where objID = objID) > 0
        THEN RETURN objID;
    	ELSE (
	        SET objID = objID + firstfieldbit;
       		IF (select count_big(*) from PhotoPrimary WITH (nolock) where objID = objID) > 0
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
	SET d = CASE WHEN ACOS(d) + fuzzR <PI() 
		  	THEN COS(ACOS(d)+fuzzR);
		  	ELSE -1 ;
		 END CASE;
	RETURN d;
END;

CREATE FUNCTION fStripeOfRun(run int)
RETURNS int as
BEGIN
  RETURN (SELECT TOP 1 stripe from Segment where run = run and camcol=1);
END;

CREATE FUNCTION fStripOfRun(run int)
RETURNS int as
BEGIN
  RETURN (SELECT TOP 1 strip from Segment where run = run and camcol=1);
END;

CREATE FUNCTION fGetDiagChecksum()
RETURNS BIGINT
BEGIN
	RETURN (select sum(count)+count_big(*) from Diagnostics);
END;

COMMIT;
