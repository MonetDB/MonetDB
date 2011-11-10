/*
The contents of this file are subject to the MonetDB Public License
Version 1.1 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://www.monetdb.org/Legal/MonetDBLicense

Software distributed under the License is distributed on an "AS IS"
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
License for the specific language governing rights and limitations
under the License.

The Original Code is the MonetDB Database System.

The Initial Developer of the Original Code is CWI.
Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
Copyright August 2008-2011 MonetDB B.V.
All Rights Reserved.
*/

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
