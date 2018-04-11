/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_LSST_H_
#define _SQL_LSST_H_
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
lsst_export str qserv_ptInSphPoly(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

lsst_export str LSSTxmatch(bit *res, lng *l, lng *r, int *delta);
lsst_export str LSSTxmatchselect(bat *res, bat *bid, bat *sid, lng *r, int *depth, bit *anti);
lsst_export str LSSTxmatchjoin(bat *l, bat *r, bat *lid, bat *rid, int *delta, bat *sl, bat *sr, bit *nil_matches, lng *estimate);

#endif /* _SQL_LSST_H_ */
