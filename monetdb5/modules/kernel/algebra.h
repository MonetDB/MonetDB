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
 * Copyright August 2008-2013 MonetDB B.V.
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

algebra_export str ALGavg(dbl *res, int *bid);

algebra_export str ALGstdev(dbl *res, int *bid);
algebra_export str ALGstdevp(dbl *res, int *bid);
algebra_export str ALGvariance(dbl *res, int *bid);
algebra_export str ALGvariancep(dbl *res, int *bid);

algebra_export str ALGminany(ptr result, int *bid);
algebra_export str ALGmaxany(ptr result, int *bid);
algebra_export str ALGtopN(int *res, int *bid, lng *top);
algebra_export str ALGgroupby(int *res, int *gids, int *cnts);
algebra_export str ALGcard(lng *result, int *bid);
algebra_export str ALGsubselect1(bat *result, bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGsubselect2(bat *result, bat *bid, bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti);
algebra_export str ALGthetasubselect1(bat *result, bat *bid, const void *val, const char **op);
algebra_export str ALGthetasubselect2(bat *result, bat *bid, bat *sid, const void *val, const char **op);
algebra_export str ALGselect1(int *result, int *bid, ptr value);
algebra_export str ALGselect1Head(int *result, int *bid, ptr value);
algebra_export str ALGuselect1(int *result, int *bid, ptr value);
algebra_export str ALGthetauselect(int *result, int *bid, ptr value, str *op);
algebra_export str ALGantiuselect1(int *result, int *bid, ptr value);
algebra_export str ALGselect(int *result, int *bid, ptr low, ptr high);
algebra_export str ALGthetaselect(int *result, int *bid, ptr low, str *op);
algebra_export str ALGselectHead(int *result, int *bid, ptr low, ptr high);
algebra_export str ALGuselect(int *result, int *bid, ptr low, ptr high);
algebra_export str ALGselectInclusive(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin);
algebra_export str ALGselectInclusiveHead(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin);
algebra_export str ALGuselectInclusive(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin);
algebra_export str ALGantiuselectInclusive(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin);
algebra_export str ALGfragment(int *result, int *bid, ptr hlow, ptr hhigh, ptr tlow, ptr thigh);

algebra_export str ALGantijoin2(int *l, int *r, int *lid, int *rid);
algebra_export str ALGjoin2(int *l, int *r, int *lid, int *rid);
algebra_export str ALGthetajoin2(int *l, int *r, int *lid, int *rid, int *opc);
algebra_export str ALGcrossproduct2(int *l, int *r, int *lid, int *rid);
algebra_export str ALGbandjoin2(int *l, int *r, int *lid, int *rid, ptr *minus, ptr *plus, bit *li, bit *hi);
algebra_export str ALGrangejoin2(int *l, int *r, int *lid, int *rlid, int *rhid, bit *li, bit *hi);

algebra_export str ALGthetajoinEstimate(int *result, int *lid, int *rid, int *opc, lng *estimate);
algebra_export str ALGthetajoin(int *result, int *lid, int *rid, int *opc);
algebra_export str ALGbandjoin_default(int *result, int *lid, int *rid, ptr *minus, ptr *plus);
algebra_export str ALGbandjoin(int *result, int *lid, int *rid, ptr *minus, ptr *plus, bit *li, bit *hi);
algebra_export str ALGrangejoin(int *result, int *lid, int *rlid, int *rhid, bit *li, bit *hi);
algebra_export str ALGsubjoin(bat *r1, bat *r2, bat *l, bat *r, bat *sl, bat *sr, lng *estimate);
algebra_export str ALGsubleftjoin(bat *r1, bat *r2, bat *l, bat *r, bat *sl, bat *sr, lng *estimate);
algebra_export str ALGsubouterjoin(bat *r1, bat *r2, bat *l, bat *r, bat *sl, bat *sr, lng *estimate);
algebra_export str ALGsubthetajoin(bat *r1, bat *r2, bat *l, bat *r, bat *sl, bat *sr, str *op, lng *estimate);

algebra_export str ALGhistogram(int *result, int *bid);
algebra_export str ALGmerge(int *result, int *bid);
algebra_export str ALGsplit(int *result, int *bid);
algebra_export str ALGcopy(int *result, int *bid);
algebra_export str ALGkunique(int *result, int *bid);
algebra_export str ALGsunique(int *result, int *bid);
algebra_export str ALGtunique(int *result, int *bid);
algebra_export str ALGcross(int *result, int *lid, int *rid);
algebra_export str ALGantijoin(int *result, int *lid, int *rid);
algebra_export str ALGjoinestimate(int *result, int *lid, int *rid, lng *estimate);
algebra_export str ALGjoin(int *result, int* lid, int *rid);
algebra_export str ALGleftjoinestimate(int *result, int *lid, int *rid, lng *estimate);
algebra_export str ALGleftjoin(int *result, int* lid, int *rid);
algebra_export str ALGleftfetchjoinestimate(int *result, int *lid, int *rid, lng *estimate);
algebra_export str ALGleftfetchjoin(int *result, int* lid, int *rid);
algebra_export str ALGouterjoinestimate(int *result, int *lid, int *rid, lng *estimate);
algebra_export str ALGouterjoin(int *result, int* lid, int *rid);
algebra_export str ALGsemijoin(int *result, int *lid, int *rid);
algebra_export str ALGsunion(int *result, int *lid, int *rid);
algebra_export str ALGkunion(int *result, int *lid, int *rid);
algebra_export str ALGtunion(int *result, int *lid, int *rid);
algebra_export str ALGsintersect(int *result, int *lid, int *rid);
algebra_export str ALGtintersect(int *result, int *lid, int *rid);
algebra_export str ALGtinter(int *result, int *lid, int *rid);
algebra_export str ALGsdiff(int *result, int *lid, int *rid);
algebra_export str ALGkdiff(int *result, int *lid, int *rid);
algebra_export str ALGtdifference(int *result, int *lid, int *rid);
algebra_export str ALGtdiff(int *result, int *lid, int *rid);
algebra_export str ALGsample(int *result, int* bid, int *param);
algebra_export str ALGsubsample(int *result, int* bid, int *param);

algebra_export str ALGtunique(int *result, int *bid);
algebra_export str ALGtsort(int *result, int *bid);
algebra_export str ALGtsort_rev(int *result, int *bid);
algebra_export str ALGhsort(int *result, int *bid);
algebra_export str ALGhsort_rev(int *result, int *bid);
algebra_export str ALGhtsort(int *result, int *lid);
algebra_export str ALGthsort(int *result, int *lid);
algebra_export str ALGssort(int *result, int *bid);
algebra_export str ALGssort_rev(int *result, int *bid);
algebra_export str ALGsubsort11(bat *result, bat *bid, bit *reverse, bit *stable);
algebra_export str ALGsubsort12(bat *result, bat *norder, bat *bid, bit *reverse, bit *stable);
algebra_export str ALGsubsort13(bat *result, bat *norder, bat *ngroup, bat *bid, bit *reverse, bit *stable);
algebra_export str ALGsubsort21(bat *result, bat *bid, bat *order, bit *reverse, bit *stable);
algebra_export str ALGsubsort22(bat *result, bat *norder, bat *bid, bat *order, bit *reverse, bit *stable);
algebra_export str ALGsubsort23(bat *result, bat *norder, bat *ngroup, bat *bid, bat *order, bit *reverse, bit *stable);
algebra_export str ALGsubsort31(bat *result, bat *bid, bat *order, bat *group, bit *reverse, bit *stable);
algebra_export str ALGsubsort32(bat *result, bat *norder, bat *bid, bat *order, bat *group, bit *reverse, bit *stable);
algebra_export str ALGsubsort33(bat *result, bat *norder, bat *ngroup, bat *bid, bat *order, bat *group, bit *reverse, bit *stable);
algebra_export str ALGrevert(int *result, int *bid);
algebra_export str ALGcount_bat(wrd *result, int *bid);
algebra_export str ALGcount_nil(wrd *result, int *bid, bit *ignore_nils);
algebra_export str ALGcount_no_nil(wrd *result, int *bid);
algebra_export str ALGtmark(int *result, int *bid, oid *base);
algebra_export str ALGtmark_default(int *result, int *bid);
algebra_export str ALGtmarkp(int *result, int *bid, int *nr_parts, int *part_nr);
algebra_export str ALGmarkHead(int *result, int *bid, oid *base);
algebra_export str ALGmarkHead_default(int *result, int *bid);
algebra_export str ALGhmarkp(int *result, int *bid, int *nr_parts, int *part_nr);
algebra_export str ALGmark_grp_1(int *result, int *bid, int *gid);
algebra_export str ALGmark_grp_2(int *result, int *bid, int *gid, oid *base);
algebra_export str ALGhistogram_rev(int *result, int *bid);
algebra_export str ALGlike(int *ret, int *bid, str *k);
algebra_export str ALGslice(int *ret, bat *bid, lng *start, lng *end);
algebra_export str ALGslice_int(int *ret, bat *bid, int *start, int *end);
algebra_export str ALGslice_wrd(int *ret, bat *bid, wrd *start, wrd *end);
algebra_export str ALGslice_oid(int *ret, bat *bid, oid *start, oid *end);
algebra_export str ALGsubslice_wrd(int *ret, bat *bid, wrd *start, wrd *end);
algebra_export str ALGposition(wrd *retval, int *bid, ptr val);
algebra_export str ALGpositionBUN(wrd *retval, int *bid, ptr val, ptr tval);
algebra_export str ALGfetch(ptr ret, int *bid, lng *pos);
algebra_export str ALGfetchoid(int *ret, int *bid, oid *pos);
algebra_export str ALGfetchint(int *ret, int *bid, int *pos);
algebra_export str ALGexist(bit *ret, int *bid, ptr val);
algebra_export str ALGexistBUN(bit *ret, int *bid, ptr val, ptr tval);
algebra_export str ALGfind(ptr ret, int *bid, ptr val);
algebra_export str ALGindexjoin(int *result, int *lid, int *rid);
algebra_export str ALGprojectNIL(int *ret, int *bid);
algebra_export str ALGselectNotNil(int *result, int *bid);

algebra_export str ALGprojecthead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
algebra_export str ALGprojecttail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

algebra_export str ALGidentity(int *ret, int *bid);
algebra_export str ALGmaterialize(int *ret, int *bid);
algebra_export str ALGreuse(int *ret, int *bid);
#endif
