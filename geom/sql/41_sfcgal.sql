-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

CREATE FUNCTION SFCGAL_VERSION() RETURNS string EXTERNAL NAME sfcgal."version";
CREATE FUNCTION ST_Extrude(geom geometry, x double, y double, z double) RETURNS geometry EXTERNAL NAME geom."extrude";
CREATE FUNCTION ST_StraightSkeleton(geom geometry) RETURNS geometry EXTERNAL NAME geom."straightSkeleton";
CREATE FUNCTION ST_Tesselate(geom geometry) RETURNS geometry EXTERNAL NAME geom."tesselate";
--CREATE FUNCTION ST_Triangulate2DZ(geometry geom) RETURNS geometry EXTERNAL NAME geom."Triangulate2DZ";;
