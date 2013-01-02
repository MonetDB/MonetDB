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

/* included from gdk.h */

gdk_export BAT *BATcalcnegate(BAT *b, BAT *s);
gdk_export BAT *BATcalcabsolute(BAT *b, BAT *s);
gdk_export BAT *BATcalcincr(BAT *b, BAT *s, int abort_on_error);
gdk_export BAT *BATcalcdecr(BAT *b, BAT *s, int abort_on_error);
gdk_export BAT *BATcalciszero(BAT *b, BAT *s);
gdk_export BAT *BATcalcsign(BAT *b, BAT *s);
gdk_export BAT *BATcalcisnil(BAT *b, BAT *s);
gdk_export BAT *BATcalcnot(BAT *b, BAT *s);
gdk_export BAT *BATcalcadd(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcaddcst(BAT *b, const ValRecord *v, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalccstadd(const ValRecord *v, BAT *b, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcsub(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcsubcst(BAT *b, const ValRecord *v, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalccstsub(const ValRecord *v, BAT *b, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcmul(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcmulcst(BAT *b, const ValRecord *v, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalccstmul(const ValRecord *v, BAT *b, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcdiv(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcdivcst(BAT *b, const ValRecord *v, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalccstdiv(const ValRecord *v, BAT *b, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcmod(BAT *b1, BAT *b2, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcmodcst(BAT *b, const ValRecord *v, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalccstmod(const ValRecord *v, BAT *b, BAT *s, int tp, int abort_on_error);
gdk_export BAT *BATcalcxor(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcxorcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstxor(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcor(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcorcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstor(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcand(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcandcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstand(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalclsh(BAT *b1, BAT *b2, BAT *s, int abort_on_error);
gdk_export BAT *BATcalclshcst(BAT *b, const ValRecord *v, BAT *s, int abort_on_error);
gdk_export BAT *BATcalccstlsh(const ValRecord *v, BAT *b, BAT *s, int abort_on_error);
gdk_export BAT *BATcalcrsh(BAT *b1, BAT *b2, BAT *s, int abort_on_error);
gdk_export BAT *BATcalcrshcst(BAT *b, const ValRecord *v, BAT *s, int abort_on_error);
gdk_export BAT *BATcalccstrsh(const ValRecord *v, BAT *b, BAT *s, int abort_on_error);
gdk_export BAT *BATcalclt(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcltcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstlt(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcle(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalclecst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstle(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcgt(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcgtcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstgt(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcge(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcgecst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstge(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalceq(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalceqcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccsteq(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcne(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcnecst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstne(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalccmp(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalccmpcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstcmp(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcbetween(BAT *b, BAT *lo, BAT *hi, BAT *s);
gdk_export BAT *BATcalcbetweencstcst(BAT *b, const ValRecord *lo, const ValRecord *hi, BAT *s);
gdk_export BAT *BATcalcbetweenbatcst(BAT *b, BAT *lo, const ValRecord *hi, BAT *s);
gdk_export BAT *BATcalcbetweencstbat(BAT *b, const ValRecord *lo, BAT *hi, BAT *s);
gdk_export int VARcalcbetween(ValPtr ret, const ValRecord *v, const ValRecord *lo, const ValRecord *hi);
gdk_export BAT *BATcalcifthenelse(BAT *b, BAT *b1, BAT *b2);
gdk_export BAT *BATcalcifthenelsecst(BAT *b, BAT *b1, const ValRecord *c2);
gdk_export BAT *BATcalcifthencstelse(BAT *b, const ValRecord *c1, BAT *b2);
gdk_export BAT *BATcalcifthencstelsecst(BAT *b, const ValRecord *c1, const ValRecord *c2);

gdk_export int VARcalcnot(ValPtr ret, const ValRecord *v);
gdk_export int VARcalcnegate(ValPtr ret, const ValRecord *v);
gdk_export int VARcalcabsolute(ValPtr ret, const ValRecord *v);
gdk_export int VARcalcincr(ValPtr ret, const ValRecord *v, int abort_on_error);
gdk_export int VARcalcdecr(ValPtr ret, const ValRecord *v, int abort_on_error);
gdk_export int VARcalciszero(ValPtr ret, const ValRecord *v);
gdk_export int VARcalcsign(ValPtr ret, const ValRecord *v);
gdk_export int VARcalcisnil(ValPtr ret, const ValRecord *v);
gdk_export int VARcalcisnotnil(ValPtr ret, const ValRecord *v);
gdk_export int VARcalcadd(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export int VARcalcsub(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export int VARcalcmul(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export int VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export int VARcalcmod(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export int VARcalcxor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalcor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalcand(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalclsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export int VARcalcrsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export int VARcalclt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalcgt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalcle(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalcge(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalceq(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalcne(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export int VARcalccmp(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export BAT *BATconvert(BAT *b, BAT *s, int tp, int abort_on_error);
gdk_export int VARconvert(ValPtr ret, const ValRecord *v, int abort_on_error);
gdk_export int BATcalcavg(BAT *b, BAT *s, dbl *avg, BUN *vals);

gdk_export BAT *BATgroupsum(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupprod(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupavg(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupcount(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupsize(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupmin(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupmedian(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
/* helper function for grouped aggregates */
gdk_export const char *BATgroupaggrinit(
	const BAT *b, const BAT *g, const BAT *e, const BAT *s,
	/* outputs: */
	oid *minp, oid *maxp, BUN *ngrpp, BUN *startp, BUN *endp, BUN *cntp,
	const oid **candp, const oid **candendp);

gdk_export gdk_return BATsum(void *res, int tp, BAT *b, BAT *s, int skip_nils, int abort_on_error, int nil_if_empty);
gdk_export gdk_return BATprod(void *res, int tp, BAT *b, BAT *s, int skip_nils, int abort_on_error, int nil_if_empty);

gdk_export dbl BATcalcstdev_population(dbl *avgp, BAT *b);
gdk_export dbl BATcalcstdev_sample(dbl *avgp, BAT *b);
gdk_export BAT *BATgroupstdev_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupstdev_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export dbl BATcalcvariance_population(dbl *avgp, BAT *b);
gdk_export dbl BATcalcvariance_sample(dbl *avgp, BAT *b);
gdk_export BAT *BATgroupvariance_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupvariance_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
