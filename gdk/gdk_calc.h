/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* included from gdk.h */

gdk_export BAT *BATcalcnegate(BAT *b, BAT *s);
gdk_export BAT *BATcalcabsolute(BAT *b, BAT *s);
gdk_export BAT *BATcalcincr(BAT *b, BAT *s, int abort_on_error);
gdk_export BAT *BATcalcdecr(BAT *b, BAT *s, int abort_on_error);
gdk_export BAT *BATcalciszero(BAT *b, BAT *s);
gdk_export BAT *BATcalcsign(BAT *b, BAT *s);
gdk_export BAT *BATcalcisnil(BAT *b, BAT *s);
gdk_export BAT *BATcalcisnotnil(BAT *b, BAT *s);
gdk_export BAT *BATcalcnot(BAT *b, BAT *s);
gdk_export BAT *BATcalcmin(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcmin_no_nil(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcmincst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalcmincst_no_nil(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstmin(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalccstmin_no_nil(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcmax(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcmax_no_nil(BAT *b1, BAT *b2, BAT *s);
gdk_export BAT *BATcalcmaxcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalcmaxcst_no_nil(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstmax(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalccstmax_no_nil(const ValRecord *v, BAT *b, BAT *s);
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
gdk_export BAT *BATcalcbetween(BAT *b, BAT *lo, BAT *hi, BAT *s, int sym);
gdk_export BAT *BATcalcbetweencstcst(BAT *b, const ValRecord *lo, const ValRecord *hi, BAT *s, int sym);
gdk_export BAT *BATcalcbetweenbatcst(BAT *b, BAT *lo, const ValRecord *hi, BAT *s, int sym);
gdk_export BAT *BATcalcbetweencstbat(BAT *b, const ValRecord *lo, BAT *hi, BAT *s, int sym);
gdk_export gdk_return VARcalcbetween(ValPtr ret, const ValRecord *v, const ValRecord *lo, const ValRecord *hi, int sym);
gdk_export BAT *BATcalcifthenelse(BAT *b, BAT *b1, BAT *b2);
gdk_export BAT *BATcalcifthenelsecst(BAT *b, BAT *b1, const ValRecord *c2);
gdk_export BAT *BATcalcifthencstelse(BAT *b, const ValRecord *c1, BAT *b2);
gdk_export BAT *BATcalcifthencstelsecst(BAT *b, const ValRecord *c1, const ValRecord *c2);

gdk_export gdk_return VARcalcnot(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcnegate(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcabsolute(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcincr(ValPtr ret, const ValRecord *v, int abort_on_error);
gdk_export gdk_return VARcalcdecr(ValPtr ret, const ValRecord *v, int abort_on_error);
gdk_export gdk_return VARcalciszero(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcsign(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcisnil(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcisnotnil(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcadd(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export gdk_return VARcalcsub(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export gdk_return VARcalcmul(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export gdk_return VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export gdk_return VARcalcmod(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export gdk_return VARcalcxor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcand(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalclsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export gdk_return VARcalcrsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, int abort_on_error);
gdk_export gdk_return VARcalclt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcgt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcle(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcge(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalceq(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcne(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalccmp(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export BAT *BATconvert(BAT *b, BAT *s, int tp, int abort_on_error);
gdk_export gdk_return VARconvert(ValPtr ret, const ValRecord *v, int abort_on_error);
gdk_export gdk_return BATcalcavg(BAT *b, BAT *s, dbl *avg, BUN *vals);

gdk_export BAT *BATgroupsum(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupprod(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export gdk_return BATgroupavg(BAT **bnp, BAT **cntsp, BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupcount(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupsize(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupmin(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupmedian(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupquantile(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile, int skip_nils, int abort_on_error);

/* helper function for grouped aggregates */
gdk_export const char *BATgroupaggrinit(
	BAT *b, BAT *g, BAT *e, BAT *s,
	/* outputs: */
	oid *minp, oid *maxp, BUN *ngrpp, BUN *startp, BUN *endp,
	const oid **candp, const oid **candendp);

gdk_export gdk_return BATsum(void *res, int tp, BAT *b, BAT *s, int skip_nils, int abort_on_error, int nil_if_empty);
gdk_export gdk_return BATprod(void *res, int tp, BAT *b, BAT *s, int skip_nils, int abort_on_error, int nil_if_empty);
gdk_export void *BATmax(BAT *b, void *aggr);
gdk_export void *BATmin(BAT *b, void *aggr);

gdk_export dbl BATcalcstdev_population(dbl *avgp, BAT *b);
gdk_export dbl BATcalcstdev_sample(dbl *avgp, BAT *b);
gdk_export BAT *BATgroupstdev_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupstdev_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export dbl BATcalcvariance_population(dbl *avgp, BAT *b);
gdk_export dbl BATcalcvariance_sample(dbl *avgp, BAT *b);
gdk_export BAT *BATgroupvariance_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
gdk_export BAT *BATgroupvariance_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp, int skip_nils, int abort_on_error);
