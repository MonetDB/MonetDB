-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.

-------------------------------------------------------------------------
------------------------- Geography functions ---------------------------
-------------------------------------------------------------------------
CREATE FUNCTION ST_DistanceGeographic(geom1 Geometry, geom2 Geometry) RETURNS double EXTERNAL NAME geom."DistanceGeographic";
GRANT EXECUTE ON FUNCTION ST_DistanceGeographic(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_CoversGeographic(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."CoversGeographic";
GRANT EXECUTE ON FUNCTION ST_CoversGeographic(Geometry, Geometry) TO PUBLIC;

CREATE AGGREGATE ST_Collect(geom Geometry) RETURNS Geometry external name aggr."Collect";

CREATE FILTER FUNCTION ST_DWithinGeographic(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME geom."DWithinGeographic";
CREATE FILTER FUNCTION ST_IntersectsGeographic(geom1 Geometry, geom2 Geometry) EXTERNAL NAME geom."IntersectsGeographic";

-------------------------------------------------------------------------
------------------------- Geometry functions ---------------------------
-------------------------------------------------------------------------
--TODO why is this here??
CREATE FUNCTION Contains(a Geometry, x double, y double) RETURNS BOOLEAN external name geom."Contains";
GRANT EXECUTE ON FUNCTION Contains(Geometry, double, double) TO PUBLIC;

-------------------------------------------------------------------------
---------------------------- MBR functions ------------------------------
-------------------------------------------------------------------------
CREATE FUNCTION mbr(geom Geometry) RETURNS mbr external name geom."mbr";
GRANT EXECUTE ON FUNCTION mbr(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Overlaps(box1 mbr, box2 mbr) RETURNS boolean EXTERNAL NAME geom."mbrOverlaps";
GRANT EXECUTE ON FUNCTION ST_Overlaps(mbr, mbr) TO PUBLIC;
CREATE FUNCTION ST_Contains(box1 mbr, box2 mbr) RETURNS boolean EXTERNAL NAME geom."mbrContains";
GRANT EXECUTE ON FUNCTION ST_Contains(mbr, mbr) TO PUBLIC;
CREATE FUNCTION ST_Equals(box1 mbr, box2 mbr) RETURNS boolean EXTERNAL NAME geom."mbrEqual";
GRANT EXECUTE ON FUNCTION ST_Equals(mbr, mbr) TO PUBLIC;
CREATE FUNCTION ST_Distance(box1 mbr, box2 mbr) RETURNS double EXTERNAL NAME geom."mbrDistance";
GRANT EXECUTE ON FUNCTION ST_Distance(mbr, mbr) TO PUBLIC;
CREATE FUNCTION mbrOverlapOrLeft(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrLeft";
CREATE FUNCTION mbrOverlapOrBelow(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrBelow";
CREATE FUNCTION mbrOverlapOrRight(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrRight";
CREATE FUNCTION mbrLeft(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrLeft";
CREATE FUNCTION mbrBelow(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrBelow";
CREATE FUNCTION mbrEqual(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrEqual";
CREATE FUNCTION mbrRight(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrRight";
CREATE FUNCTION mbrContained(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrContained";
CREATE FUNCTION mbrOverlapOrAbove(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrAbove";
CREATE FUNCTION mbrAbove(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrAbove";


