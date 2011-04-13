/*
The contents of this file are subject to the MonetDB Public License
Version 1.1 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html

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

create function qserv_angSep(ra1 dbl, dec1 dbl, ra2 dbl, dec2 dbl)
returns dbl external name 'mal.qserv_angSep';


create function qserv_ptInSphBox(ra, dbl dec dbl, ra_ dblmin, dec dbl_min, ra_ dblmax, dec dbl_max)
returns int external name 'mal.qserv_InSphBox;

create function qserv_ptInSphEllipse(ra, dbl dec dbl, ra_ dblcen, dec dbl_cen, sma dbla, smi dbla, ang dbl) 
returns int external name 'mal.qserv_InSphEllipse;

create function qserv_ptInSphCircle(ra, dbl dec dbl, ra_ dblcen, dec dbl_cen, rad dblius) 
returns int external name 'mal.qserv_InSphZ;

create function qserv_ptInSphPoly(ra dbl, dec dbl, list dbl)
returns int external name 'mal.qserv_InSphPoly;
