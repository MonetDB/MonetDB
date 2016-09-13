/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef ALGEBRA_H
#define ALGEBRA_H

#include <gdk.h>
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
mal_export str ALGsubselect1(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
mal_export str ALGsubselect2(bat *result, const bat *bid, const bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
mal_export str ALGthetasubselect1(bat *result, const bat *bid, const void *val, const char **op);
mal_export str ALGthetasubselect2(bat *result, const bat *bid, const bat *sid, const void *val, const char **op);

mal_export str ALGsubjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGsubleftjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGsubouterjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGsubsemijoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
mal_export str ALGsubthetajoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const int *op, const bit *nil_matches, const lng *estimate);
mal_export str ALGsubbandjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const void *low, const void *high, const bit *li, const bit *hi, const lng *estimate);
mal_export str ALGsubrangejoin(bat *r1, bat *r2, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid, const bit *li, const bit *hi, const lng *estimate);
mal_export str ALGsubdiff(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);
mal_export str ALGsubinter(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);

/* legacy join functions */
mal_export str ALGcrossproduct2(bat *l, bat *r, const bat *lid, const bat *rid);
/* end legacy join functions */

mal_export str ALGfirstn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

mal_export str ALGcopy(bat *result, const bat *bid);
mal_export str ALGsubunique2(bat *result, const bat *bid, const bat *sid);
mal_export str ALGsubunique1(bat *result, const bat *bid);
mal_export str ALGprojection(bat *result, const bat *lid, const bat *rid);
mal_export str ALGtinter(bat *result, const bat *lid, const bat *rid);
mal_export str ALGtdiff(bat *result, const bat *lid, const bat *rid);
mal_export str ALGsample(bat *result, const bat *bid, const int *param);

mal_export str ALGsubsort11(bat *result, const bat *bid, const bit *reverse, const bit *stable);
mal_export str ALGsubsort12(bat *result, bat *norder, const bat *bid, const bit *reverse, const bit *stable);
mal_export str ALGsubsort13(bat *result, bat *norder, bat *ngroup, const bat *bid, const bit *reverse, const bit *stable);
mal_export str ALGsubsort21(bat *result, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
mal_export str ALGsubsort22(bat *result, bat *norder, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
mal_export str ALGsubsort23(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
mal_export str ALGsubsort31(bat *result, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
mal_export str ALGsubsort32(bat *result, bat *norder, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
mal_export str ALGsubsort33(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
mal_export str ALGcount_bat(lng *result, const bat *bid);
mal_export str ALGcount_nil(lng *result, const bat *bid, const bit *ignore_nils);
mal_export str ALGcount_no_nil(lng *result, const bat *bid);
mal_export str ALGtmark(bat *result, const bat *bid, const oid *base);
mal_export str ALGtmark_default(bat *result, const bat *bid);
mal_export str ALGtmarkp(bat *result, const bat *bid, const int *nr_parts, const int *part_nr);
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
