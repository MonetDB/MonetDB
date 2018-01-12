-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

create function angSep(ra1 double, dec1 double, ra2 double, dec2 double)
returns double external name lsst.angsep;

create function ptInSphBox(ra1 double, dec1 double, ra_min double,  dec_min double, ra_max double, dec_max double)
returns int external name lsst.ptinsphbox;

create function ptInSphEllipse(ra1 double, dec1 double , ra_cen double, dec_cen double, smaa double, smia double, ang double) 
returns int external name lsst.ptinsphellipse;

create function ptInSphCircle(ra1 double, dec1 double, ra_cen double, dec_cen double, radius double) 
returns int external name lsst.ptinsphcircle;

create function ptInSphPoly(ra1 double, dec1 double, list double)
returns int external name lsst.ptinsphpoly;

create filter function xmatch(a bigint, b bigint, opt int) external name lsst.xmatch;
