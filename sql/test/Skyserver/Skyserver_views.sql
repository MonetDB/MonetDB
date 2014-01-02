CREATE VIEW SpecObj 
---------------------------------------------------------------
--/H A view of Spectro objects that just has the clean spectra.
--
--/T The view excludes QA and Sky and duplicates. Use this as the main
--/T way to access the spectro objects.
---------------------------------------------------------------
AS
SELECT * 
    FROM specObjAll 
    WHERE sciencePrimary = 1 
;
CREATE VIEW SpecLine 
---------------------------------------------------------------
--/H A view of SpecLines objects that have been measured
--
--/T The view excludes those SpecLine objects which have category=1,
--/T thus they have not been measured. This is the view you should
--/T use to access the SpecLine data.
---------------------------------------------------------------
AS
SELECT * 
    FROM specLineAll 
    WHERE category=2
;
CREATE VIEW SpecPhoto 
---------------------------------------------------------------
--/H A view of joined Spectro and Photo objects that have the clean spectra.
--
--/T The view includes only those pairs where the SpecObj is a
--/T sciencePrimary, and the BEST PhotoObj is a PRIMARY (mode=1).
---------------------------------------------------------------
AS
SELECT * 
    FROM SpecPhotoAll 
    WHERE sciencePrimary = 1 and mode=1
;
CREATE VIEW Tile
---------------------------------------------------------------
--/H A view of TileAll that have untiled=0
--
--/T The view excludes those Tiles that have been untiled.
---------------------------------------------------------------
AS
SELECT * 
    FROM TileAll
    WHERE untiled = 0
;
CREATE VIEW TilingBoundary
---------------------------------------------------------------
--/H A view of TilingGeometry objects that have isMask = 0
--
--/T The view excludes those TilingGeometry objects that have 
--/T isMask = 1.  See also TilingMask.
---------------------------------------------------------------
AS
SELECT * 
    FROM TilingGeometry
    WHERE isMask = 0
;
CREATE VIEW TilingMask
---------------------------------------------------------------
--/H A view of TilingGeometry objects that have isMask = 1
--
--/T The view excludes those TilingGeometry objects that have 
--/T isMask = 0.  See also TilingBoundary.
---------------------------------------------------------------
AS
SELECT * 
    FROM TilingGeometry
    WHERE isMask = 1
;
CREATE VIEW TiledTarget
---------------------------------------------------------------
--/H A view of TiledTargetAll objects that have untiled = 0
--
--/T The view excludes those TiledTarget objects that have 
--/T been untiled.
---------------------------------------------------------------
AS
SELECT * 
    FROM TiledTargetAll
    WHERE untiled = 0
;
CREATE VIEW Columns 
---------------------------------------------------------------
--/H Aliias the DBColumns table also as Columns, for legacy SkyQuery
---------------------------------------------------------------
AS
SELECT * 
    FROM DBColumns
;
CREATE VIEW Run
---------------------------------------------------------------------
--/H Distinct Run from Segment table, mainly for RUNS DB.
--------------------------------------------------------------------
AS
SELECT segmentID, run, rerun, field0, nFields
    FROM Segment
    WHERE camcol=1
;
CREATE VIEW PhotoSecondary
----------------------------------------------------------------------
--/H Secondary objects are reobservations of the same primary object.
----------------------------------------------------------------------
AS
SELECT * FROM PhotoObjAll
    WHERE mode=2
--status & 0x00001000 > 0 
;
CREATE VIEW PhotoFamily
----------------------------------------------------------------------
--/H These are in PhotoObj, but neither PhotoPrimary or Photosecondary.
--
--/T These objects are generated if they are neither primary nor 
--/T secondary survey objects but a composite object that has been 
--/T deblended or the part of an object that has been deblended 
--/T wrongfully (like the spiral arms of a galaxy). These objects 
--/T are kept to track how the deblender is working. It inherits 
--/T all members of the PhotoObj class. 
----------------------------------------------------------------------
AS
SELECT * FROM PhotoObjAll
    WHERE mode=3

--(status & 0x00001000 = 0)  -- not a secondary
--	and NOT ( (status & 0x00002000>0) and (status & 0x0010 >0)) -- not a primary either
;
CREATE VIEW PhotoObj
----------------------------------------------------------------------
--/H All primary and secondary objects in the PhotoObjAll table, which contains all the attributes of each photometric (image) object. 
--
--/T It selects PhotoObj with mode=1 or 2.
----------------------------------------------------------------------
AS
SELECT * FROM PhotoObjAll
	WHERE mode in (1,2)
;
CREATE VIEW PhotoPrimary 
----------------------------------------------------------------------
--/H These objects are the primary survey objects. 
--
--/T Each physical object 
--/T on the sky has only one primary object associated with it. Upon 
--/T subsequent observations secondary objects are generated. Since the 
--/T survey stripes overlap, there will be secondary objects for over 10% 
--/T of all primary objects, and in the southern stripes there will be a 
--/T multitude of secondary objects for each primary (i.e. reobservations). 
--/T <p>
--/T They are defined by the status flag: (PRIMARY &  OK_RUN) = 0x2010.
----------------------------------------------------------------------
AS
SELECT * FROM PhotoObjAll
    WHERE mode=1
;
CREATE VIEW PhotoAux
----------------------------------------------------------------------
--/H Extra parameters for the PhotoObj view.
--
--/T It selects PhotoAuxAll with mode=1 or 2.
----------------------------------------------------------------------
AS
SELECT * FROM PhotoAuxAll
	WHERE mode in (1,2)
;
CREATE VIEW PhotoStatus
------------------------------------------
--/H Contains the PhotoStatus flag values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PhotoStatus'
;
CREATE VIEW PrimTarget
------------------------------------------
--/H Contains the PrimTarget flag values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PrimTarget'
;
CREATE VIEW SecTarget
------------------------------------------
--/H Contains the SecTarget flag values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SecTarget'
;
CREATE VIEW InsideMask
------------------------------------------
--/H Contains the InsideMask flag values from DataConstants as smallint
------------------------------------------
AS
SELECT name, 
	cast(value as smallint) as value, 
	description
    FROM DataConstants
    WHERE field='InsideMask'
;
CREATE VIEW SpecZWarning
------------------------------------------
--/H Contains the SpecZWarning flag values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecZWarning'
;
CREATE VIEW ImageMask
------------------------------------------
--/H Contains the ImageMask flag values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='ImageMask'
;
CREATE VIEW TiMask
------------------------------------------
--/H Contains the TiMask flag values from DataConstants as int
------------------------------------------
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='TiMask'
;
CREATE VIEW PhotoMode
------------------------------------------
--/H Contains the PhotoMode enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field = 'PhotoMode' 
;
CREATE VIEW PhotoType
------------------------------------------
--/H Contains the PhotoType enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PhotoType'
;
CREATE VIEW MaskType
------------------------------------------
--/H Contains the MaskType enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='MaskType'
;
CREATE VIEW FieldQuality
------------------------------------------
--/H Contains the FieldQuality enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='FieldQuality'
;
CREATE VIEW PspStatus
------------------------------------------
--/H Contains the PspStatus enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='PspStatus'
;
CREATE VIEW FramesStatus
------------------------------------------
--/H Contains the FramesStatus enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='FramesStatus'
;
CREATE VIEW SpecClass
------------------------------------------
--/H Contains the SpecClass enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecClass'
;
CREATE VIEW SpecLineNames
------------------------------------------
--/H Contains the SpecLineNames enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecLineNames'
;
CREATE VIEW SpecZStatus
------------------------------------------
--/H Contains the SpecZStatus enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='SpecZStatus'
;
CREATE VIEW HoleType
------------------------------------------
--/H Contains the HoleType enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='HoleType'
;
CREATE VIEW ObjType
------------------------------------------
--/H Contains the ObjType enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='ObjType'
;
CREATE VIEW ProgramType
------------------------------------------
--/H Contains the ProgramType enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='ProgramType'
;
CREATE VIEW CoordType
------------------------------------------
--/H Contains the CoordType enumerated values from DataConstants as int
------------------------------------------
AS
SELECT name,
        cast(value as int) as value,
        description
    FROM DataConstants
    WHERE field='CoordType'
;
CREATE VIEW FieldMask
------------------------------------------
--/H Contains the FieldMask flag values from DataConstants as int
------------------------------------------
AS
SELECT 
	name, 
	cast(value as int) as value, 
	description
    FROM DataConstants
    WHERE field='FieldMask'
;
CREATE VIEW PhotoFlags
------------------------------------------
--/H Contains the PhotoFlags flag values from DataConstants as binary(8)
------------------------------------------
AS
SELECT 
	name, 
	value, 
	description
    FROM DataConstants
    WHERE field='PhotoFlags'
;
CREATE VIEW Star
--------------------------------------------------------------
--/H The objects classified as stars from PhotoPrimary
--
--/T The Star view essentially contains the photometric parameters
--/T (no redshifts or spectroscopic parameters) for all primary
--/T point-like objects, including quasars.
--------------------------------------------------------------
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type = 6
;
CREATE VIEW Galaxy
---------------------------------------------------------------
--/H The objects classified as galaxies from PhotoPrimary.
--
--/T The Galaxy view contains the photometric parameters (no
--/T redshifts or spectroscopic parameters) measured for
--/T resolved primary objects.
---------------------------------------------------------------
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type = 3
;
CREATE VIEW Sky
---------------------------------------------------------------
--/H The objects selected as sky samples in PhotoPrimary
---------------------------------------------------------------
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type = 8
;
CREATE VIEW Unknown
---------------------------------------------------------------------
--/H Everything in PhotoPrimary, which is not a galaxy, star or sky
--------------------------------------------------------------------
AS
SELECT * 
    FROM PhotoPrimary
    WHERE type not in (3,6,8);

