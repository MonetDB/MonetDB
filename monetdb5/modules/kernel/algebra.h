/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef ALGEBRA_H
#define ALGEBRA_H

#include "gdk.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

mal_export str ALGstdev(dbl *res, const bat *bid);
mal_export str ALGstdevp(dbl *res, const bat *bid);
mal_export str ALGvariance(dbl *res, const bat *bid);
mal_export str ALGvariancep(dbl *res, const bat *bid);

mal_export str ALGminany(ptr result, const bat *bid);
mal_export str ALGmaxany(ptr result, const bat *bid);
mal_export str ALGgroupby(bat *res, const bat *gids, const bat *cnts);
mal_export str ALGcard(lng *result, const bat *bid);
mal_export str ALGselect1(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
mal_export str ALGselect2(bat *result, const bat *bid, const bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
mal_export str ALGthetaselect1(bat *result, const bat *bid, const void *val, const char **op);
mal_export str ALGthetaselect2(bat *result, const bat *bid, const bat *sid, const void *val, const char **op);

mal_export str ALGjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGleftjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGouterjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGsemijoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGthetajoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const int *op, const bit *nil_matches, const lng *estimate);
mal_export str ALGbandjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const void *low, const void *high, const bit *li, const bit *hi, const lng *estimate);
mal_export str ALGrangejoin(bat *r1, bat *r2, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid, const bit *li, const bit *hi, const lng *estimate);
mal_export str ALGdifference(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);
mal_export str ALGintersect(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);

/* legacy join functions */
mal_export str ALGcrossproduct2(bat *l, bat *r, const bat *lid, const bat *rid);
/* end legacy join functions */

mal_export str ALGfirstn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

mal_export str ALGcopy(bat *result, const bat *bid);
mal_export str ALGunique2(bat *result, const bat *bid, const bat *sid);
mal_export str ALGunique1(bat *result, const bat *bid);
mal_export str ALGprojection(bat *result, const bat *lid, const bat *rid);

mal_export str ALGsort11(bat *result, const bat *bid, const bit *reverse, const bit *stable);
mal_export str ALGsort12(bat *result, bat *norder, const bat *bid, const bit *reverse, const bit *stable);
mal_export str ALGsort13(bat *result, bat *norder, bat *ngroup, const bat *bid, const bit *reverse, const bit *stable);
mal_export str ALGsort21(bat *result, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
mal_export str ALGsort22(bat *result, bat *norder, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
mal_export str ALGsort23(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
mal_export str ALGsort31(bat *result, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
mal_export str ALGsort32(bat *result, bat *norder, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
mal_export str ALGsort33(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
mal_export str ALGcount_bat(lng *result, const bat *bid);
mal_export str ALGcount_nil(lng *result, const bat *bid, const bit *ignore_nils);
mal_export str ALGcount_no_nil(lng *result, const bat *bid);
mal_export str ALGslice(bat *ret, const bat *bid, const lng *start, const lng *end);
mal_export str ALGslice_int(bat *ret, const bat *bid, const int *start, const int *end);
mal_export str ALGslice_lng(bat *ret, const bat *bid, const lng *start, const lng *end);
mal_export str ALGslice_oid(bat *ret, const bat *bid, const oid *start, const oid *end);
mal_export str ALGsubslice_lng(bat *ret, const bat *bid, const lng *start, const lng *end);
mal_export str ALGfetchoid(ptr ret, const bat *bid, const oid *pos);
mal_export str ALGexist(bit *ret, const bat *bid, const void *val);
mal_export str ALGfind(oid *ret, const bat *bid, ptr val);
mal_export str ALGselectNotNil(bat *result, const bat *bid);
mal_export str ALGprojecttail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str ALGreuse(bat *ret, const bat *bid);
#endif
