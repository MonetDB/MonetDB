/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <fenv.h>
#include "mmath_private.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

static str
CMDscienceUNARY(MalStkPtr stk, InstrPtr pci,
				float (*ffunc)(float), double (*dfunc)(double),
				const char *malfunc)
{
	bat bid;
	BAT *bn, *b, *s = NULL;
	struct canditer ci;
	oid x, off;
	BUN i, ncand;
	BUN nils = 0;
	int e = 0, ex = 0;
	BATiter bi;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (pci->argc == 3) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid)) {
			if ((s = BATdescriptor(bid)) == NULL) {
				BBPunfix(b->batCacheid);
				throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
		}
	}

	ncand = canditer_init(&ci, b, s);
	off = b->hseqbase;
	bn = COLnew(ci.hseq, b->ttype, ncand, TRANSIENT);
	if (bn == NULL || ncand == 0) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		if (bn == NULL)
			throw(MAL, malfunc, GDK_EXCEPTION);
		goto doreturn;
	}

	errno = 0;
	feclearexcept(FE_ALL_EXCEPT);
	bi = bat_iterator(b);
	switch (b->ttype) {
	case TYPE_flt: {
		const flt *restrict fsrc = (const flt *) bi.base;
		flt *restrict fdst = (flt *) Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			x = canditer_next(&ci) - off;
			if (is_flt_nil(fsrc[x])) {
				fdst[i] = flt_nil;
				nils++;
			} else {
				fdst[i] = ffunc(fsrc[x]);
			}
		}
		break;
	}
	case TYPE_dbl: {
		const dbl *restrict dsrc = (const dbl *) bi.base;
		dbl *restrict ddst = (dbl *) Tloc(bn, 0);
		for (i = 0; i < ncand; i++) {
			x = canditer_next(&ci) - off;
			if (is_dbl_nil(dsrc[x])) {
				ddst[i] = dbl_nil;
				nils++;
			} else {
				ddst[i] = dfunc(dsrc[x]);
			}
		}
		break;
	}
	default:
		assert(0);
	}
	bat_iterator_end(&bi);
	e = errno;
	ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (e != 0 || ex != 0) {
		const char *err;
		BBPunfix(bn->batCacheid);
		if (e)
			err = GDKstrerror(e, (char[128]){0}, 128);
		else if (ex & FE_DIVBYZERO)
			err = "Divide by zero";
		else if (ex & FE_OVERFLOW)
			err = "Overflow";
		else
			err = "Invalid result";
		throw(MAL, malfunc, "Math exception: %s", err);
	}

	BATsetcount(bn, ncand);
	bn->tsorted = false;
	bn->trevsorted = false;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	BATkey(bn, false);
  doreturn:
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDscienceBINARY(MalStkPtr stk, InstrPtr pci,
				 float (*ffunc)(float, float), double (*dfunc)(double, double),
				 const char *malfunc)
{
	bat bid;
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL;
	int tp1, tp2;
	struct canditer ci1 = (struct canditer){0}, ci2 = (struct canditer){0};
	oid x1, x2, off1, off2;
	BUN i, ncand, nils = 0;
	int e = 0, ex = 0;
	BATiter b1i, b2i;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;

	if (tp1 == TYPE_bat) {
		bid = *getArgReference_bat(stk, pci, 1);
		b1 = BATdescriptor(bid);
		if (b1 == NULL)
			goto bailout;
		tp1 = b1->ttype;
	}

	if (tp2 == TYPE_bat) {
		bid = *getArgReference_bat(stk, pci, 2);
		b2 = BATdescriptor(bid);
		if (b2 == NULL)
			goto bailout;
		tp2 = b2->ttype;
	}
	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	assert(tp1 == tp2);
	assert(b1 != NULL || b2 != NULL);

	if (pci->argc > 4) {
		assert(pci->argc == 5);
		bid = *getArgReference_bat(stk, pci, 4);
		if (!is_bat_nil(bid)) {
			s2 = BATdescriptor(bid);
			if (s2 == NULL)
				goto bailout;
		}
	}
	if (pci->argc > 3) {
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid)) {
			s1 = BATdescriptor(bid);
			if (s1 == NULL)
				goto bailout;
			if (b1 == NULL) {
				s2 = s1;
				s1 = NULL;
			}
		}
	}

	if (b1)
		canditer_init(&ci1, b1, s1);
	if (b2)
		canditer_init(&ci2, b2, s2);
	ncand = b1 ? ci1.ncand : ci2.ncand;
	off1 = b1 ? b1->hseqbase : 0;
	off2 = b2 ? b2->hseqbase : 0;

	if (b1 == NULL &&
		(tp1 == TYPE_flt ?
		 is_flt_nil(stk->stk[getArg(pci, 1)].val.fval) :
		 is_dbl_nil(stk->stk[getArg(pci, 1)].val.dval))) {
		bn = BATconstant(ci2.hseq, tp1, ATOMnilptr(tp1), ncand, TRANSIENT);
		goto doreturn;
	}
	if (b2 == NULL &&
		(tp1 == TYPE_flt ?
		 is_flt_nil(stk->stk[getArg(pci, 2)].val.fval) :
		 is_dbl_nil(stk->stk[getArg(pci, 2)].val.dval))) {
		bn = BATconstant(ci1.hseq, tp1, ATOMnilptr(tp1), ncand, TRANSIENT);
		goto doreturn;
	}
	if (b1)
		bn = COLnew(ci1.hseq, tp1, ncand, TRANSIENT);
	else
		bn = COLnew(ci2.hseq, tp1, ncand, TRANSIENT);
	if (bn == NULL || ncand == 0)
		goto doreturn;

	b1i = bat_iterator(b1);
	b2i = bat_iterator(b2);
	errno = 0;
	feclearexcept(FE_ALL_EXCEPT);
	switch (tp1) {
	case TYPE_flt:
		if (b1 && b2) {
			const flt *fsrc1 = (const flt *) b1i.base;
			const flt *fsrc2 = (const flt *) b2i.base;
			flt *restrict fdst = (flt *) Tloc(bn, 0);
			for (i = 0; i < ncand; i++) {
				x1 = canditer_next(&ci1) - off1;
				x2 = canditer_next(&ci2) - off2;
				if (is_flt_nil(fsrc1[x1]) ||
					is_flt_nil(fsrc2[x2])) {
					fdst[i] = flt_nil;
					nils++;
				} else {
					fdst[i] = ffunc(fsrc1[x1], fsrc2[x2]);
				}
			}
		} else if (b1) {
			const flt *restrict fsrc1 = (const flt *) b1i.base;
			flt fval2 = stk->stk[getArg(pci, 2)].val.fval;
			flt *restrict fdst = (flt *) Tloc(bn, 0);
			for (i = 0; i < ncand; i++) {
				x1 = canditer_next(&ci1) - off1;
				if (is_flt_nil(fsrc1[x1])) {
					fdst[i] = flt_nil;
					nils++;
				} else {
					fdst[i] = ffunc(fsrc1[x1], fval2);
				}
			}
		} else /* b2 == NULL */ {
			flt fval1 = stk->stk[getArg(pci, 1)].val.fval;
			const flt *restrict fsrc2 = (const flt *) b2i.base;
			flt *restrict fdst = (flt *) Tloc(bn, 0);
			for (i = 0; i < ncand; i++) {
				x2 = canditer_next(&ci2) - off2;
				if (is_flt_nil(fsrc2[x2])) {
					fdst[i] = flt_nil;
					nils++;
				} else {
					fdst[i] = ffunc(fval1, fsrc2[x2]);
				}
			}
		}
		break;
	case TYPE_dbl:
		if (b1 && b2) {
			const dbl *dsrc1 = (const dbl *) b1i.base;
			const dbl *dsrc2 = (const dbl *) b2i.base;
			dbl *restrict ddst = (dbl *) Tloc(bn, 0);
			for (i = 0; i < ncand; i++) {
				x1 = canditer_next(&ci1) - off1;
				x2 = canditer_next(&ci2) - off2;
				if (is_dbl_nil(dsrc1[x1]) ||
					is_dbl_nil(dsrc2[x2])) {
					ddst[i] = dbl_nil;
					nils++;
				} else {
					ddst[i] = dfunc(dsrc1[x1], dsrc2[x2]);
				}
			}
		} else if (b1) {
			const dbl *restrict dsrc1 = (const dbl *) b1i.base;
			dbl dval2 = stk->stk[getArg(pci, 2)].val.dval;
			dbl *restrict ddst = (dbl *) Tloc(bn, 0);
			for (i = 0; i < ncand; i++) {
				x1 = canditer_next(&ci1) - off1;
				if (is_dbl_nil(dsrc1[x1])) {
					ddst[i] = dbl_nil;
					nils++;
				} else {
					ddst[i] = dfunc(dsrc1[x1], dval2);
				}
			}
		} else /* b2 == NULL */ {
			dbl dval1 = stk->stk[getArg(pci, 1)].val.dval;
			const dbl *restrict dsrc2 = (const dbl *) b2i.base;
			dbl *restrict ddst = (dbl *) Tloc(bn, 0);
			for (i = 0; i < ncand; i++) {
				x2 = canditer_next(&ci2) - off2;
				if (is_dbl_nil(dsrc2[x2])) {
					ddst[i] = dbl_nil;
					nils++;
				} else {
					ddst[i] = dfunc(dval1, dsrc2[x2]);
				}
			}
		}
		break;
	default:
		assert(0);
	}
	e = errno;
	ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);

	BATsetcount(bn, ncand);
	bn->tsorted = false;
	bn->trevsorted = false;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	BATkey(bn, false);

  doreturn:
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (bn == NULL)
		throw(MAL, malfunc, GDK_EXCEPTION);
	if (e != 0 || ex != 0) {
		const char *err;
		BBPunfix(bn->batCacheid);
		if (e)
			err = GDKstrerror(e, (char[128]){0}, 128);
		else if (ex & FE_DIVBYZERO)
			err = "Divide by zero";
		else if (ex & FE_OVERFLOW)
			err = "Overflow";
		else
			err = "Invalid result";
		throw(MAL, malfunc, "Math exception: %s", err);
	}
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;

  bailout:
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
/* cannot happen
	if (s1)
		BBPunfix(s1->batCacheid);
*/
	if (s2)
		BBPunfix(s2->batCacheid);
	throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

#define scienceImpl(FUNC)												\
static str																\
CMDscience_bat_##FUNC(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) \
{																		\
	(void) cntxt;														\
	(void) mb;															\
																		\
	return CMDscienceUNARY(stk, pci, FUNC##f, FUNC, "batmmath." #FUNC);	\
}

#define scienceBinaryImpl(FUNC)											\
static str																\
CMDscience_bat_##FUNC(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) \
{																		\
	(void) cntxt;														\
	(void) mb;															\
																		\
	return CMDscienceBINARY(stk, pci, FUNC##f, FUNC, "batmmath." #FUNC); \
}

static str
CMDscience_bat_randintarg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals;
	struct canditer ci = {0};
	bat *res = getArgReference_bat(stk, pci, 0);

	(void) cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *bid = getArgReference_bat(stk, pci, 1), *sid = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;
		if (!(b = BBPquickdesc(*bid))) {
			throw(MAL, "batmmath.rand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (sid && !is_bat_nil(*sid) && !(bs = BATdescriptor(*sid))) {
			throw(MAL, "batmmath.rand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		q = canditer_init(&ci, b, bs);
		if (bs)
			BBPunfix(bs->batCacheid);
	} else {
		q = (BUN) *getArgReference_lng(stk, pci, 1);
	}

	if (!(bn = COLnew(ci.hseq, TYPE_int, q, TRANSIENT))) {
		throw(MAL, "batmmath.rand", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	vals = Tloc(bn, 0);
#ifdef __COVERITY__
	for (BUN i = 0; i < q; i++)
		vals[i] = 0;
#else
	MT_lock_set(&mmath_rse_lock);
	for (BUN i = 0; i < q; i++)
		vals[i] = (int) (next(mmath_rse) >> 33);
	MT_lock_unset(&mmath_rse_lock);
#endif

	BATsetcount(bn, q);
	bn->tnil = false;
	bn->tnonil = true;
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	BBPkeepref(*res = bn->batCacheid);
	return MAL_SUCCEED;
}

scienceImpl(acos)
scienceImpl(asin)
scienceImpl(atan)
scienceImpl(cos)
scienceImpl(sin)
scienceImpl(tan)
scienceImpl(cot)
scienceImpl(cosh)
scienceImpl(sinh)
scienceImpl(tanh)
scienceImpl(radians)
scienceImpl(degrees)
scienceImpl(exp)
scienceImpl(log)
scienceImpl(log10)
scienceImpl(log2)
scienceImpl(sqrt)
scienceImpl(cbrt)
scienceImpl(ceil)
scienceImpl(fabs)
scienceImpl(floor)

scienceBinaryImpl(atan2)
scienceBinaryImpl(pow)
scienceBinaryImpl(logbs)

#include "mel.h"
mel_func batmmath_init_funcs[] = {
 pattern("batmmath", "asin", CMDscience_bat_asin, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "asin", CMDscience_bat_asin, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "asin", CMDscience_bat_asin, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "asin", CMDscience_bat_asin, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "acos", CMDscience_bat_acos, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "acos", CMDscience_bat_acos, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "acos", CMDscience_bat_acos, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "acos", CMDscience_bat_acos, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "atan", CMDscience_bat_atan, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "atan", CMDscience_bat_atan, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "atan", CMDscience_bat_atan, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "atan", CMDscience_bat_atan, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "cos", CMDscience_bat_cos, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "cos", CMDscience_bat_cos, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "cos", CMDscience_bat_cos, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "cos", CMDscience_bat_cos, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "sin", CMDscience_bat_sin, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "sin", CMDscience_bat_sin, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "sin", CMDscience_bat_sin, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "sin", CMDscience_bat_sin, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "tan", CMDscience_bat_tan, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "tan", CMDscience_bat_tan, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "tan", CMDscience_bat_tan, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "tan", CMDscience_bat_tan, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "cot", CMDscience_bat_cot, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "cot", CMDscience_bat_cot, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "cot", CMDscience_bat_cot, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "cot", CMDscience_bat_cot, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "cosh", CMDscience_bat_cosh, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "cosh", CMDscience_bat_cosh, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "cosh", CMDscience_bat_cosh, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "cosh", CMDscience_bat_cosh, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "sinh", CMDscience_bat_sinh, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "sinh", CMDscience_bat_sinh, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "sinh", CMDscience_bat_sinh, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "sinh", CMDscience_bat_sinh, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "tanh", CMDscience_bat_tanh, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "tanh", CMDscience_bat_tanh, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "tanh", CMDscience_bat_tanh, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "tanh", CMDscience_bat_tanh, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "radians", CMDscience_bat_radians, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "radians", CMDscience_bat_radians, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "radians", CMDscience_bat_radians, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "radians", CMDscience_bat_radians, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "degrees", CMDscience_bat_degrees, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "degrees", CMDscience_bat_degrees, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "degrees", CMDscience_bat_degrees, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "degrees", CMDscience_bat_degrees, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "exp", CMDscience_bat_exp, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "exp", CMDscience_bat_exp, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "exp", CMDscience_bat_exp, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "exp", CMDscience_bat_exp, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "log", CMDscience_bat_log, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "log", CMDscience_bat_log, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "log", CMDscience_bat_log, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "log", CMDscience_bat_log, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "log10", CMDscience_bat_log10, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "log10", CMDscience_bat_log10, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "log10", CMDscience_bat_log10, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "log10", CMDscience_bat_log10, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "log2", CMDscience_bat_log2, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "log2", CMDscience_bat_log2, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "log2", CMDscience_bat_log2, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "log2", CMDscience_bat_log2, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),arg("y",dbl))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,4, batarg("",dbl),batarg("x",dbl),arg("y",dbl),batarg("s",oid))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("y",dbl))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,5, batarg("",dbl),batarg("x",dbl),batarg("y",dbl),batarg("s1",oid),batarg("s2",oid))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,3, batarg("",flt),batarg("x",flt),arg("y",flt))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,4, batarg("",flt),batarg("x",flt),arg("y",flt),batarg("s",oid))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("y",flt))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,5, batarg("",flt),batarg("x",flt),batarg("y",flt),batarg("s1",oid),batarg("s2",oid))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,3, batarg("",dbl),arg("x",dbl),batarg("y",dbl))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,4, batarg("",dbl),arg("x",dbl),batarg("y",dbl),batarg("s",oid))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,3, batarg("",flt),arg("x",flt),batarg("y",flt))),
 pattern("batmmath", "log2arg", CMDscience_bat_logbs, false, "", args(1,4, batarg("",flt),arg("x",flt),batarg("y",flt),batarg("s",oid))),
 pattern("batmmath", "sqrt", CMDscience_bat_sqrt, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "sqrt", CMDscience_bat_sqrt, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "sqrt", CMDscience_bat_sqrt, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "sqrt", CMDscience_bat_sqrt, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "cbrt", CMDscience_bat_cbrt, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "cbrt", CMDscience_bat_cbrt, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "cbrt", CMDscience_bat_cbrt, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "cbrt", CMDscience_bat_cbrt, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "ceil", CMDscience_bat_ceil, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "ceil", CMDscience_bat_ceil, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "ceil", CMDscience_bat_ceil, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "ceil", CMDscience_bat_ceil, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "fabs", CMDscience_bat_fabs, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "fabs", CMDscience_bat_fabs, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "fabs", CMDscience_bat_fabs, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "fabs", CMDscience_bat_fabs, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "floor", CMDscience_bat_floor, false, "", args(1,2, batarg("",dbl),batarg("x",dbl))),
 pattern("batmmath", "floor", CMDscience_bat_floor, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("s",oid))),
 pattern("batmmath", "floor", CMDscience_bat_floor, false, "", args(1,2, batarg("",flt),batarg("x",flt))),
 pattern("batmmath", "floor", CMDscience_bat_floor, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("s",oid))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),arg("y",dbl))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,4, batarg("",dbl),batarg("x",dbl),arg("y",dbl),batarg("s",oid))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("y",dbl))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,5, batarg("",dbl),batarg("x",dbl),batarg("y",dbl),batarg("s1",oid),batarg("s2",oid))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,3, batarg("",flt),batarg("x",flt),arg("y",flt))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,4, batarg("",flt),batarg("x",flt),arg("y",flt),batarg("s",oid))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("y",flt))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,5, batarg("",flt),batarg("x",flt),batarg("y",flt),batarg("s1",oid),batarg("s2",oid))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,3, batarg("",dbl),arg("x",dbl),batarg("y",dbl))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,4, batarg("",dbl),arg("x",dbl),batarg("y",dbl),batarg("s",oid))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,3, batarg("",flt),arg("x",flt),batarg("y",flt))),
 pattern("batmmath", "atan2", CMDscience_bat_atan2, false, "", args(1,4, batarg("",flt),arg("x",flt),batarg("y",flt),batarg("s",oid))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),arg("y",dbl))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,4, batarg("",dbl),batarg("x",dbl),arg("y",dbl),batarg("s",oid))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),batarg("y",dbl))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,5, batarg("",dbl),batarg("x",dbl),batarg("y",dbl),batarg("s1",oid),batarg("s2",oid))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,3, batarg("",flt),batarg("x",flt),arg("y",flt))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,4, batarg("",flt),batarg("x",flt),arg("y",flt),batarg("s",oid))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,3, batarg("",flt),batarg("x",flt),batarg("y",flt))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,5, batarg("",flt),batarg("x",flt),batarg("y",flt),batarg("s1",oid),batarg("s2",oid))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,3, batarg("",dbl),arg("x",dbl),batarg("y",dbl))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,4, batarg("",dbl),arg("x",dbl),batarg("y",dbl),batarg("s",oid))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,3, batarg("",flt),arg("x",flt),batarg("y",flt))),
 pattern("batmmath", "pow", CMDscience_bat_pow, false, "", args(1,4, batarg("",flt),arg("x",flt),batarg("y",flt),batarg("s",oid))),
 pattern("batmmath", "rand", CMDscience_bat_randintarg, true, "", args(1,2, batarg("",int),batarg("v",int))),
 pattern("batmmath", "rand", CMDscience_bat_randintarg, true, "", args(1,3, batarg("",int),batarg("v",int),batarg("s",oid))),
 pattern("batmmath", "rand", CMDscience_bat_randintarg, true, "", args(1,2, batarg("",int),arg("card",lng))), /* version with cardinality input */
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batmmath_mal)
{ mal_module("batmmath", NULL, batmmath_init_funcs); }
