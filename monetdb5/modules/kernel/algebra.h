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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
algebra_export str ALGselect1(bat *result, const bat *bid, ptr value);
algebra_export str ALGuselect1(bat *result, const bat *bid, ptr value);
algebra_export str ALGselect(bat *result, const bat *bid, ptr low, ptr high);
algebra_export str ALGuselect(bat *result, const bat *bid, ptr low, ptr high);
algebra_export str ALGselectInclusive(bat *result, const bat *bid, ptr low, ptr high, const bit *lin, const bit *rin);
algebra_export str ALGuselectInclusive(bat *result, const bat *bid, ptr low, ptr high, const bit *lin, const bit *rin);

algebra_export str ALGsubjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubleftjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubouterjoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubthetajoin(bat *r1, bat *r2, const bat *l, const bat *r, const bat *sl, const bat *sr, const int *op, const bit *nil_matches, const lng *estimate);
algebra_export str ALGsubbandjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const void *low, const void *high, const bit *li, const bit *hi, const lng *estimate);
algebra_export str ALGsubrangejoin(bat *r1, bat *r2, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid, const bit *li, const bit *hi, const lng *estimate);

/* legacy join functions */
algebra_export str ALGantijoin2(bat *l, bat *r, const bat *lid, const bat *rid);
algebra_export str ALGjoin2(bat *l, bat *r, const bat *lid, const bat *rid);
algebra_export str ALGthetajoin2(bat *l, bat *r, const bat *lid, const bat *rid, const int *opc);
algebra_export str ALGcrossproduct2(bat *l, bat *r, const bat *lid, const bat *rid);
algebra_export str ALGbandjoin2(bat *l, bat *r, const bat *lid, const bat *rid, const void *minus, const void *plus, const bit *li, const bit *hi);
algebra_export str ALGrangejoin2(bat *l, bat *r, const bat *lid, const bat *rlid, const bat *rhid, const bit *li, const bit *hi);
algebra_export str ALGthetajoinEstimate(bat *result, const bat *lid, const bat *rid, const int *opc, const lng *estimate);
algebra_export str ALGthetajoin(bat *result, const bat *lid, const bat *rid, const int *opc);
algebra_export str ALGbandjoin_default(bat *result, const bat *lid, const bat *rid, const void *minus, const void *plus);
algebra_export str ALGbandjoin(bat *result, const bat *lid, const bat *rid, const void *minus, const void *plus, const bit *li, const bit *hi);
algebra_export str ALGrangejoin(bat *result, const bat *lid, const bat *rlid, const bat *rhid, const bit *li, const bit *hi);
/* end legacy join functions */

algebra_export str ALGfirstn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

algebra_export str ALGcopy(bat *result, const bat *bid);
algebra_export str ALGsubunique2(bat *result, const bat *bid, const bat *sid);
algebra_export str ALGsubunique1(bat *result, const bat *bid);
algebra_export str ALGantijoin(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGjoinestimate(bat *result, const bat *lid, const bat *rid, const lng *estimate);
algebra_export str ALGjoin(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGleftjoinestimate(bat *result, const bat *lid, const bat *rid, const lng *estimate);
algebra_export str ALGleftjoin(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGleftfetchjoin(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGouterjoinestimate(bat *result, const bat *lid, const bat *rid, const lng *estimate);
algebra_export str ALGouterjoin(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGsemijoin(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGkunion(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGtunion(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGtintersect(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGtinter(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGkdiff(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGtdifference(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGtdiff(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGsample(bat *result, const bat *bid, const int *param);

algebra_export str ALGtsort(bat *result, const bat *bid);
algebra_export str ALGtsort_rev(bat *result, const bat *bid);
algebra_export str ALGhsort(bat *result, const bat *bid);
algebra_export str ALGhsort_rev(bat *result, const bat *bid);
algebra_export str ALGhtsort(bat *result, const bat *lid);
algebra_export str ALGthsort(bat *result, const bat *lid);
algebra_export str ALGssort(bat *result, const bat *bid);
algebra_export str ALGssort_rev(bat *result, const bat *bid);
algebra_export str ALGsubsort11(bat *result, const bat *bid, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort12(bat *result, bat *norder, const bat *bid, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort13(bat *result, bat *norder, bat *ngroup, const bat *bid, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort21(bat *result, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort22(bat *result, bat *norder, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort23(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort31(bat *result, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort32(bat *result, bat *norder, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
algebra_export str ALGsubsort33(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable);
algebra_export str ALGrevert(bat *result, const bat *bid);
algebra_export str ALGcount_bat(wrd *result, const bat *bid);
algebra_export str ALGcount_nil(wrd *result, const bat *bid, const bit *ignore_nils);
algebra_export str ALGcount_no_nil(wrd *result, const bat *bid);
algebra_export str ALGtmark(bat *result, const bat *bid, const oid *base);
algebra_export str ALGtmark_default(bat *result, const bat *bid);
algebra_export str ALGtmarkp(bat *result, const bat *bid, const int *nr_parts, const int *part_nr);
algebra_export str ALGmark_grp_1(bat *result, const bat *bid, const bat *gid);
algebra_export str ALGmark_grp_2(bat *result, const bat *bid, const bat *gid, const oid *base);
algebra_export str ALGlike(bat *ret, const bat *bid, const str *k);
algebra_export str ALGslice(bat *ret, const bat *bid, const lng *start, const lng *end);
algebra_export str ALGslice_int(bat *ret, const bat *bid, const int *start, const int *end);
algebra_export str ALGslice_wrd(bat *ret, const bat *bid, const wrd *start, const wrd *end);
algebra_export str ALGslice_oid(bat *ret, const bat *bid, const oid *start, const oid *end);
algebra_export str ALGsubslice_wrd(bat *ret, const bat *bid, const wrd *start, const wrd *end);
algebra_export str ALGfetchoid(ptr ret, const bat *bid, const oid *pos);
algebra_export str ALGexist(bit *ret, const bat *bid, const void *val);
algebra_export str ALGfind(ptr ret, const bat *bid, ptr val);
algebra_export str ALGindexjoin(bat *result, const bat *lid, const bat *rid);
algebra_export str ALGprojectNIL(bat *ret, const bat *bid);
algebra_export str ALGselectNotNil(bat *result, const bat *bid);

algebra_export str ALGprojecthead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
algebra_export str ALGprojecttail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

algebra_export str ALGreuse(bat *ret, const bat *bid);
#endif
