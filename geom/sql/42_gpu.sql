-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

CREATE FUNCTION G_Setup(flag integer) RETURNS BOOLEAN external name geom."gsetup";
CREATE FUNCTION G_Reset(flag integer) RETURNS BOOLEAN external name geom."greset";
CREATE FUNCTION G_Contains(a Geometry, x double, y double, z double, srid int) RETURNS BOOLEAN external name geom."gcontains";
