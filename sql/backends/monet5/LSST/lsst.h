/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
*/

#ifndef _SQL_UDF_H_
#define _SQL_UDF_H_
#include "sql.h"
#define _USE_MATH_DEFINES	/* needed for WIN32 to define M_PI */
#include <math.h>
#include <string.h>

#ifdef WIN32
#ifndef LIBLSST
#define lsst_export extern __declspec(dllimport)
#else
#define lsst_export extern __declspec(dllexport)
#endif
#else
#define lsst_export extern
#endif

lsst_export str qserv_angSep(dbl *sep, dbl *ra1, dbl *dec1, dbl *ra2, dbl *dec2);
lsst_export str qserv_ptInSphBox(int *ret, dbl *ra, dbl *dec, dbl *ra_min, dbl *dec_min, dbl *ra_max, dbl *dec_max);
lsst_export str qserv_ptInSphEllipse(int *ret, dbl *ra, dbl *dec, dbl *ra_cen, dbl *dec_cen, dbl *smaa, dbl *smia, dbl *ang);
lsst_export str qserv_ptInSphCircle(int *ret, dbl *ra, dbl *dec, dbl *ra_cen, dbl *dec_cen, dbl *radius);
lsst_export str qserv_ptInSphPoly(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

lsst_export str xmatch(int *ret, int *lid, int *rid, int *delta);
#endif /* _SQL_UDF_H_ */
