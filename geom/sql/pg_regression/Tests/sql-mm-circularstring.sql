SELECT 'ndims01', ST_ndims(ST_GeomFromText('CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2)'));
SELECT 'geometrytype01', geometrytype(ST_GeomFromText('CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2)'));
SELECT 'ndims02', ST_ndims(ST_GeomFromText('CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097 1)'));
SELECT 'geometrytype02', geometrytype(ST_GeomFromText('CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097 1)'));
SELECT 'ndims03', ST_ndims(ST_GeomFromText('CIRCULARSTRINGM(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097 2)'));
SELECT 'geometrytype03', geometrytype(ST_GeomFromText('CIRCULARSTRINGM(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097 2)'));
SELECT 'ndims04', ST_ndims(ST_GeomFromText('CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097)'));
SELECT 'geometrytype04', geometrytype(ST_GeomFromText('CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097)'));

SELECT 'isClosed01', ST_isClosed(ST_GeomFromText('CIRCULARSTRING(0 -2,-2 0,0 2,2 0,0 -2)'));
SELECT 'isSimple01', ST_isSimple(ST_GeomFromText('CIRCULARSTRING(0 -2,-2 0,0 2,2 0,0 -2)'));
SELECT 'isRing01', ST_isRing(ST_GeomFromText('CIRCULARSTRING(0 -2,-2 0,0 2,2 0,0 -2)'));
SELECT 'isClosed02', ST_isClosed(ST_GeomFromText('CIRCULARSTRING(0 -2,-2 0,0 2,-2 0,2 -2,-2 0,-2 -2,-2 0,0 -2)'));
SELECT 'isSimple02', ST_isSimple(ST_GeomFromText('CIRCULARSTRING(0 -2,-2 0,0 2,-2 0,2 -2,-2 0,-2 -2,-2 0,0 -2)'));
SELECT 'isRing02', ST_isRing(ST_GeomFromText('CIRCULARSTRING(0 -2,-2 0,0 2,-2 0,2 -2,-2 0,-2 -2,-2 0,0 -2)'));

CREATE TABLE circularstring (id INTEGER, description VARCHAR(128), the_geom_2d GEOMETRY(CIRCULARSTRING), the_geom_3dm GEOMETRY(CIRCULARSTRINGM), the_geom_3dz GEOMETRY(CIRCULARSTRINGZ), the_geom_4d GEOMETRY(CIRCULARSTRINGZM));

INSERT INTO circularstring (
        id, 
        description
      ) VALUES (
        1, 
        '180-135 degrees');
UPDATE circularstring
        SET the_geom_4d = ST_GeomFromText('CIRCULARSTRING(
                0 0 0 0, 
                0.26794919243112270647255365849413 1 3 -2, 
                0.5857864376269049511983112757903 1.4142135623730950488016887242097 1 2)')
        WHERE id = 1;
UPDATE circularstring
        SET the_geom_3dz = ST_GeomFromText('CIRCULARSTRING(
                0 0 0, 
                0.26794919243112270647255365849413 1 3, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097 1)')
        WHERE id = 1;
UPDATE circularstring
        SET the_geom_3dm = ST_GeomFromText('CIRCULARSTRINGM(
                0 0 0, 
                0.26794919243112270647255365849413 1 -2, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097 2)') 
        WHERE id = 1;
UPDATE circularstring       
        SET the_geom_2d = ST_GeomFromText('CIRCULARSTRING(
                0 0, 
                0.26794919243112270647255365849413 1, 
                0.5857864376269049511981127579 1.4142135623730950488016887242097)')
        WHERE id = 1;

INSERT INTO circularstring (
        id, description
      ) VALUES (2, '2-segment string');
UPDATE circularstring
        SET the_geom_4d = ST_GeomFromText('CIRCULARSTRING(-5 0 0 4, 0 5 1 3, 5 0 2 2, 10 -5 3 1, 15 0 4 0)')
        WHERE id = 2;
UPDATE circularstring
        SET the_geom_3dz = ST_GeomFromText('CIRCULARSTRING(-5 0 0, 0 5 1, 5 0 2, 10 -5 3, 15 0 4)')
        WHERE id = 2;
UPDATE circularstring
        SET the_geom_3dm = ST_GeomFromText('CIRCULARSTRINGM(-5 0 4, 0 5 3, 5 0 2, 10 -5 1, 15 0 0)')
        WHERE id = 2;
UPDATE circularstring
        SET the_geom_2d = ST_GeomFromText('CIRCULARSTRING(-5 0, 0 5, 5 0, 10 -5, 15 0)')
        WHERE id = 2;

SELECT 'astext01', ST_astext(the_geom_2d) FROM circularstring;        
SELECT 'astext02', ST_astext(the_geom_3dm) FROM circularstring;        
SELECT 'astext03', ST_astext(the_geom_3dz) FROM circularstring;        
SELECT 'astext04', ST_astext(the_geom_4d) FROM circularstring;        

SELECT 'asewkt01', ST_asewkt(the_geom_2d) FROM circularstring;        
SELECT 'asewkt02', ST_asewkt(the_geom_3dm) FROM circularstring;        
SELECT 'asewkt03', ST_asewkt(the_geom_3dz) FROM circularstring;        
SELECT 'asewkt04', ST_asewkt(the_geom_4d) FROM circularstring;        

-- These tests will fail on different architectures
-- We need a way to handle multiple byte orderings
--SELECT 'asbinary01', encode(asbinary(the_geom_2d), 'hex') FROM circularstring;
--SELECT 'asbinary02', encode(asbinary(the_geom_3dm), 'hex') FROM circularstring;
--SELECT 'asbinary03', encode(asbinary(the_geom_3dz), 'hex') FROM circularstring;
--SELECT 'asbinary04', encode(asbinary(the_geom_4d), 'hex') FROM circularstring;
--
--SELECT 'asewkb01', encode(asewkb(the_geom_2d), 'hex') FROM circularstring;
--SELECT 'asewkb02', encode(asewkb(the_geom_3dm), 'hex') FROM circularstring;
--SELECT 'asewkb03', encode(asewkb(the_geom_3dz), 'hex') FROM circularstring;
--SELECT 'asewkb04', encode(asewkb(the_geom_4d), 'hex') FROM circularstring;

SELECT 'ST_CurveToLine-201', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 2), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine-202', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 2), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine-203', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 2), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine-204', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 2), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;

SELECT 'ST_CurveToLine-401', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_2d, 4), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine-402', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_3dm, 4), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine-403', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_3dz, 4), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine-404', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_4d, 4), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;

SELECT 'ST_CurveToLine01', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_2d), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine02', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_3dm), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine03', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_3dz), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'ST_CurveToLine04', ST_asewkt( ST_SnapToGrid(ST_CurveToLine(the_geom_4d), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;

--Removed due to discrepencies between hardware
--SELECT 'box2d01', box2d(the_geom_2d) FROM circularstring;
--SELECT 'box2d02', box2d(the_geom_3dm) FROM circularstring;
--SELECT 'box2d03', box2d(the_geom_3dz) FROM circularstring;
--SELECT 'box2d04', box2d(the_geom_4d) FROM circularstring;

--SELECT 'box3d01', box3d(the_geom_2d) FROM circularstring;
--SELECT 'box3d02', box3d(the_geom_3dm) FROM circularstring;
--SELECT 'box3d03', box3d(the_geom_3dz) FROM circularstring;
--SELECT 'box3d04', box3d(the_geom_4d) FROM circularstring;
-- TODO: ST_SnapToGrid is required to remove platform dependent precision
-- issues.  Until ST_SnapToGrid is updated to work against curves, these
-- tests cannot be run.
--SELECT 'ST_LineToCurve01', ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_2d))) FROM circularstring;
--SELECT 'ST_LineToCurve02', ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dm))) FROM circularstring;
--SELECT 'ST_LineToCurve03', ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_3dz))) FROM circularstring;
--SELECT 'ST_LineToCurve04', ST_asewkt(ST_LineToCurve(ST_CurveToLine(the_geom_4d))) FROM circularstring;

-- Repeat tests with new function names.
SELECT 'astext01', ST_astext(the_geom_2d) FROM circularstring;        
SELECT 'astext02', ST_astext(the_geom_3dm) FROM circularstring;        
SELECT 'astext03', ST_astext(the_geom_3dz) FROM circularstring;        
SELECT 'astext04', ST_astext(the_geom_4d) FROM circularstring;        

SELECT 'asewkt01', ST_asewkt(the_geom_2d) FROM circularstring;        
SELECT 'asewkt02', ST_asewkt(the_geom_3dm) FROM circularstring;        
SELECT 'asewkt03', ST_asewkt(the_geom_3dz) FROM circularstring;        
SELECT 'asewkt04', ST_asewkt(the_geom_4d) FROM circularstring;        

-- These tests will fail on different architectures
-- We need a way to handle multiple byte orderings
--SELECT 'asbinary01', encode(ST_asbinary(the_geom_2d), 'hex') FROM circularstring;
--SELECT 'asbinary02', encode(ST_asbinary(the_geom_3dm), 'hex') FROM circularstring;
--SELECT 'asbinary03', encode(ST_asbinary(the_geom_3dz), 'hex') FROM circularstring;
--SELECT 'asbinary04', encode(ST_asbinary(the_geom_4d), 'hex') FROM circularstring;
--
--SELECT 'asewkb01', encode(ST_asewkb(the_geom_2d), 'hex') FROM circularstring;
--SELECT 'asewkb02', encode(ST_asewkb(the_geom_3dm), 'hex') FROM circularstring;
--SELECT 'asewkb03', encode(ST_asewkb(the_geom_3dz), 'hex') FROM circularstring;
--SELECT 'asewkb04', encode(ST_asewkb(the_geom_4d), 'hex') FROM circularstring;

--Removed due to discrepencies between hardware
--SELECT 'box2d01', box2d(the_geom_2d) FROM circularstring;
--SELECT 'box2d02', box2d(the_geom_3dm) FROM circularstring;
--SELECT 'box2d03', box2d(the_geom_3dz) FROM circularstring;
--SELECT 'box2d04', box2d(the_geom_4d) FROM circularstring;

--SELECT 'box3d01', box3d(the_geom_2d) FROM circularstring;
--SELECT 'box3d02', box3d(the_geom_3dm) FROM circularstring;
--SELECT 'box3d03', box3d(the_geom_3dz) FROM circularstring;
--SELECT 'box3d04', box3d(the_geom_4d) FROM circularstring;

SELECT 'isValid01', ST_isValid(the_geom_2d) FROM circularstring;
SELECT 'isValid02', ST_isValid(the_geom_3dm) FROM circularstring;
SELECT 'isValid03', ST_isValid(the_geom_3dz) FROM circularstring;
SELECT 'isValid04', ST_isValid(the_geom_4d) FROM circularstring;

SELECT 'dimension01', ST_dimension(the_geom_2d) FROM circularstring;
SELECT 'dimension02', ST_dimension(the_geom_3dm) FROM circularstring;
SELECT 'dimension03', ST_dimension(the_geom_3dz) FROM circularstring;
SELECT 'dimension04', ST_dimension(the_geom_4d) FROM circularstring;

SELECT 'SRID01', ST_SRID(the_geom_2d) FROM circularstring;
SELECT 'SRID02', ST_SRID(the_geom_3dm) FROM circularstring;
SELECT 'SRID03', ST_SRID(the_geom_3dz) FROM circularstring;
SELECT 'SRID04', ST_SRID(the_geom_4d) FROM circularstring;

SELECT 'accessors01', ST_IsEmpty(the_geom_2d), ST_isSimple(the_geom_2d), ST_isClosed(the_geom_2d), ST_isRing(the_geom_2d) FROM circularstring;
SELECT 'accessors02', ST_IsEmpty(the_geom_3dm), ST_isSimple(the_geom_3dm), ST_isClosed(the_geom_3dm), ST_isRing(the_geom_3dm) FROM circularstring;
SELECT 'accessors03', ST_IsEmpty(the_geom_3dz), ST_isSimple(the_geom_3dz), ST_isClosed(the_geom_3dz), ST_isRing(the_geom_3dz) FROM circularstring;
SELECT 'accessors04', ST_IsEmpty(the_geom_4d), ST_isSimple(the_geom_4d), ST_isClosed(the_geom_4d), ST_isRing(the_geom_4d) FROM circularstring;

SELECT 'envelope01', ST_AsText(ST_SnapToGrid(ST_Envelope(the_geom_2d), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'envelope02', ST_AsText(ST_SnapToGrid(ST_Envelope(the_geom_3dm), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'envelope03', ST_AsText(ST_SnapToGrid(ST_Envelope(the_geom_3dz), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;
SELECT 'envelope04', ST_AsText(ST_SnapToGrid(ST_Envelope(the_geom_4d), 'POINT(0 0 0 0)', 1e-8, 1e-8, 1e-8, 1e-8)) FROM circularstring;

DROP TABLE circularstring;
SELECT ST_AsText(st_snaptogrid(box2d('CIRCULARSTRING(220268.439465645 150415.359530563,220227.333322076 150505.561285879,220227.353105332 150406.434743975)'),0.0001));
SELECT 'npoints_is_five',ST_NumPoints(ST_GeomFromText('CIRCULARSTRING(0 0,2 0, 2 1, 2 3, 4 3)'));

SELECT 'straight_curve',ST_AsText(ST_CurveToLine(ST_GeomFromText('CIRCULARSTRING(0 0,1 0,2 0,3 0,4 0)')));

