--CREATE VIEW Columns 
--AS
--SELECT * 
--    FROM DBColumns g
--;
CREATE VIEW CoordType
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='CoordType'
;
CREATE VIEW FieldMask
AS
SELECT 
	name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='FieldMask'
;
CREATE VIEW FieldQuality
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='FieldQuality'
;
CREATE VIEW FramesStatus
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='FramesStatus'
;
CREATE VIEW GalaxyTag
AS
SELECT * 
    FROM PhotoTag g
    WHERE type = 3 and mode=1
;
CREATE VIEW HoleType
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='HoleType'
;
CREATE VIEW ImageMask
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='ImageMask'
;
CREATE VIEW InsideMask
AS
SELECT name, 
	cast(value as smallint) as value, 
	description
    FROM DataConstants
    WHERE field='InsideMask'
;
CREATE VIEW MaskType
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='MaskType'
;
CREATE VIEW ObjType
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='ObjType'
;
CREATE VIEW PhotoAuxAll
AS
SELECT objid,mode,b,l,raErr,decErr,raDecCorr FROM PhotoObjAll g
;
CREATE VIEW PhotoAux
AS
SELECT * FROM PhotoAuxAll
	WHERE mode in (1,2)
;
CREATE VIEW PhotoFamily
AS
SELECT * FROM PhotoObjAll g
    WHERE mode=3
;
CREATE VIEW PhotoFlags
AS
SELECT 
	name, 
	value, 
	description
    FROM DataConstants
    WHERE field='PhotoFlags'
;
CREATE VIEW PhotoMode
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field = 'PhotoMode' 
;
CREATE VIEW PhotoObj
AS
SELECT * FROM PhotoObjAll g
	WHERE mode in (1,2)
;
CREATE VIEW PhotoPrimary 
AS
SELECT * FROM PhotoObjAll g
    WHERE mode=1
;
CREATE VIEW Galaxy
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type = 3
;
CREATE VIEW PhotoSecondary
AS
SELECT * FROM PhotoObjAll g
    WHERE mode=2
;
CREATE VIEW PhotoStatus
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PhotoStatus'
;
CREATE VIEW PhotoType
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PhotoType'
;
CREATE VIEW PrimTarget
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PrimTarget'
;
CREATE VIEW ProgramType
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='ProgramType'
;
CREATE VIEW PspStatus
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PspStatus'
;
CREATE VIEW QsoCatalog
AS
     SELECT * FROM QsoCatalogAll g 
     WHERE QsoPrimary = 1
;
CREATE VIEW QsoConcordance
AS
     SELECT * FROM QsoConcordanceAll g
     WHERE QsoPrimary = 1
;
CREATE VIEW RegionConvex
AS
	SELECT	regionid, convexid, patchid as patch, type, 
			radius, ra,"dec",x,y,z,c, htmid, convexString
	FROM RegionPatch
;
CREATE VIEW Run
AS
SELECT segmentID, run, rerun, field0, nFields
    FROM Segment g
    WHERE camcol=1
;
CREATE VIEW SecTarget
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SecTarget'
;
CREATE VIEW Sky
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type = 8
;
CREATE VIEW spbsParams
AS
SELECT	specobjid,
	plate,
	mjd,
	fiberid,
	fehspectype,
	ra,
	"dec",
	brun,
	brerun,
	bcamcol,
	bfield,
	bobj,
	zbsubclass,
	zbelodiesptype,
	zbclass,
	zbrchi2,
	zbdof,
	zbvdisp,
	zbvdisperr,
	zbzwarning,
	spec_cln,
	cast(round(sprv/3.0e5,5) as varchar(8)) as spz,
	cast(round(sprverr/3.0e5,5) as varchar(8)) as spzerr,
	vel_dis,
	vel_disperr,
	spz_conf,
	spz_status,
	spz_warning,
	eclass,
	ecoeff1,
	ecoeff2,
	ecoeff3,
	ecoeff4,
	ecoeff5 
FROM sppParams
WHERE spec_cln NOT IN (1,5,6)
;
CREATE VIEW SpecClass
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecClass'
;
CREATE VIEW SpecLine 
AS
SELECT * 
    FROM specLineAll g
    WHERE category=2
;
CREATE VIEW SpecLineNames
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecLineNames'
;
CREATE VIEW SpecObj 
AS
SELECT * 
    FROM specObjAll g
    WHERE sciencePrimary = 1 
;
CREATE VIEW SpecPhoto 
AS
SELECT * 
    FROM SpecPhotoAll g
    WHERE sciencePrimary = 1 and mode=1
;
CREATE VIEW SpecZStatus
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecZStatus'
;
CREATE VIEW SpecZWarning
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecZWarning'
;
CREATE VIEW Star
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type = 6
;
CREATE VIEW StarTag
AS
SELECT * 
    FROM PhotoTag g
    WHERE type = 6 and mode=1
;
CREATE VIEW TargPhotoObj
AS
SELECT * FROM TargPhotoObjAll g
	WHERE mode in (1,2)
;
CREATE VIEW TargPhotoPrimary 
AS
SELECT * FROM TargPhotoObjAll g
    WHERE mode=1
;
CREATE VIEW TargPhotoSecondary
AS
SELECT * FROM TargPhotoObjAll g
    WHERE mode=2
;
CREATE VIEW Tile
AS
SELECT * 
    FROM TileAll g
    WHERE untiled = 0
;
CREATE VIEW TiledTarget
AS
SELECT * 
    FROM TiledTargetAll g
    WHERE untiled = 0
;
CREATE VIEW TilingBoundary
AS
SELECT * 
    FROM TilingGeometry g
    WHERE isMask = 0
;
CREATE VIEW TilingMask
AS
SELECT * 
    FROM TilingGeometry g
    WHERE isMask = 1
;
CREATE VIEW TiMask
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='TiMask'
;
CREATE VIEW UberCalibStatus
AS
SELECT name, 
	cast(value as smallint) as value, 
	description
    FROM DataConstants
    WHERE field='UberCalibStatus'
;
CREATE VIEW Unknown
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type not in (3,6,8)
;
