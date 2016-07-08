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

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define algebra_export extern __declspec(dllimport)
#else
#define algebra_export extern __declspec(dllexport)
#endif
#else
#define algebra_export extern
#endif

algebra_export str ALGstdev(dbl *res, const bat *bid);
algebra_export str ALGstdevp(dbl *res, const bat *bid);
algebra_export str ALGvariance(dbl *res, const bat *bid);
algebra_export str ALGvariancep(dbl *res, const bat *bid);

algebra_export str ALGminany(ptr result, const bat *bid);
algebra_export str ALGmaxany(ptr result, const bat *bid);
algebra_export str ALGgroupby(bat *res, const bat *gids, const bat *cnts);
algebra_export str ALGcard(lng *result, const bat *bid);
algebra_export str ALGsubselect1(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGsubselect2(bat *result, const bat *bid, const bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGthetasubselect1(bat *result, const bat *bid, const void *val, const char **op);
algebra_export str ALGthetasubselect2(bat *result, const bat *bid, const bat *sid, const void *val, const char **op);

algebra_export str ALGsubjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubleftjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubouterjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubsemijoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubthetajoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const int *op, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubbandjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const void *low, const void *high, const bit *li, const bit *hi, const lng *estimate);
algebra_export str ALGsubrangejoin(bat *r1, bat *r2, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid, const bit *li, const bit *hi, const lng *estimate);
algebra_export str ALGsubdiff(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubinter(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);

/* legacy join functions */
algebra_export str ALGcrossproduct2(bat *l, bat *r, const bat *lid, const bat *rid);
/* end legacy join functions */

algebra_export str ALGfirstn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

algebra_export str ALGcopy(bat *result, const bat *bid);
algebra_export str ALGsubunique2(bat *result, const bat *bid, const bat *sid);
algebra_export str ALGsubunique1(bat *result, const bat *bid);
algebra_export str ALGprojection(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGtinter(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGtdiff(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGsample(bat *result, const bat *bid, const int *param);

algebra_export str ALGsubsort11(bat *result, const bat *bid, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort12(bat *result, bat *norder, const bat *bid, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort13(bat *result, bat *norder, bat *ngroup, const bat *bid, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort21(bat *result, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort22(bat *result, bat *norder, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort23(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort31(bat *result, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort32(bat *result, bat *norder, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort33(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
algebra_export str ALGcount_bat(lng *result, const bat *bid);
algebra_export str ALGcount_nil(lng *result, const bat *bid, const bit *ignore_nils);
algebra_export str ALGcount_no_nil(lng *result, const bat *bid);
algebra_export str ALGtmark(bat *result, const bat *bid, const oid *base);
algebra_export str ALGtmark_default(bat *result, const bat *bid);
algebra_export str ALGtmarkp(bat *result, const bat *bid, const int *nr_parts, const int *part_nr);
algebra_export str ALGslice(bat *ret, const bat *bid, const lng *start, const lng *end);
algebra_export str ALGslice_int(bat *ret, const bat *bid, const int *start, const int *end);
algebra_export str ALGslice_lng(bat *ret, const bat *bid, const lng *start, const lng *end);
algebra_export str ALGslice_oid(bat *ret, const bat *bid, const oid *start, const oid *end);
algebra_export str ALGsubslice_lng(bat *ret, const bat *bid, const lng *start, const lng *end);
algebra_export str ALGfetchoid(ptr ret, const bat *bid, const oid *pos);
algebra_export str ALGexist(bit *ret, const bat *bid, const void *val);
algebra_export str ALGfind(oid *ret, const bat *bid, ptr val);
algebra_export str ALGselectNotNil(bat *result, const bat *bid);
algebra_export str ALGprojecttail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
algebra_export str ALGreuse(bat *ret, const bat *bid);
#endif
