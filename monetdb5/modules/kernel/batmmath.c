/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include <math.h>
#include <fenv.h>
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mmath_private.h"
#ifndef FE_INVALID
#define FE_INVALID			0
#endif
#ifndef FE_DIVBYZERO
#define FE_DIVBYZERO		0
#endif
#ifndef FE_OVERFLOW
#define FE_OVERFLOW			0
#endif

static str
CMDscienceUNARY(MalStkPtr stk, InstrPtr pci,
				float (*ffunc)(float), double (*dfunc)(double),
				const char *malfunc)
{
	bat bid;
	BAT *bn, *b, *s = NULL, *r = NULL;
	const bit *rv;
	struct canditer ci;
	oid x;
	BUN i;
	BUN nils = 0;
	int e = 0, ex = 0;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (pci->argc == 4) {
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid)) {
			if ((r = BATdescriptor(bid)) == NULL) {
				BBPunfix(b->batCacheid);
				throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			assert(r->ttype == TYPE_bit);
		}
	}

	if (pci->argc >= 3) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid)) {
			if ((s = BATdescriptor(bid)) == NULL) {
				BBPunfix(b->batCacheid);
				if (r)
					BBPunfix(r->batCacheid);
				throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			if (s->ttype == TYPE_bit) {
				assert(pci->argc == 3);
				assert(r == NULL);
				r = s;
				s = NULL;
			}
		}
	}

	canditer_init(&ci, b, s);
	bn = COLnew(ci.hseq, b->ttype, ci.ncand, TRANSIENT);
	if (bn == NULL || ci.ncand == 0) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (bn == NULL)
			throw(MAL, malfunc, GDK_EXCEPTION);
		goto doreturn;
	}

	rv = r ? Tloc(r, 0) : NULL;

	errno = 0;
	feclearexcept(FE_ALL_EXCEPT);
	switch (b->ttype) {
	case TYPE_flt: {
		const flt *restrict fsrc = (const flt *) Tloc(b, 0);
		flt *restrict fdst = (flt *) Tloc(bn, 0);
		for (i = 0; i < ci.ncand; i++) {
			x = canditer_next(&ci) - b->hseqbase;
			if ((rv != NULL && !rv[i]) || is_flt_nil(fsrc[x])) {
				fdst[i] = flt_nil;
				nils++;
			} else {
				fdst[i] = ffunc(fsrc[x]);
			}
		}
		break;
	}
	case TYPE_dbl: {
		const dbl *restrict dsrc = (const dbl *) Tloc(b, 0);
		dbl *restrict ddst = (dbl *) Tloc(bn, 0);
		for (i = 0; i < ci.ncand; i++) {
			x = canditer_next(&ci) - b->hseqbase;
			if ((rv != NULL && !rv[i]) || is_dbl_nil(dsrc[x])) {
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
	e = errno;
	ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
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

	BATsetcount(bn, ci.ncand);
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
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL, *r = NULL;
	int tp1, tp2;
	struct canditer ci1 = (struct canditer){0}, ci2 = (struct canditer){0};
	const bit *rv;
	oid x1, x2;
	BUN i;
	BUN nils = 0;
	int e = 0, ex = 0;

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

	if (pci->argc > 5) {
		assert(pci->argc == 6);
		bid = *getArgReference_bat(stk, pci, 5);
		if (!is_bat_nil(bid)) {
			r = BATdescriptor(bid);
			if (r == NULL)
				goto bailout;
			assert(r->ttype == TYPE_bit);
		}
	}
	if (pci->argc > 4) {
		bid = *getArgReference_bat(stk, pci, 4);
		if (!is_bat_nil(bid)) {
			s2 = BATdescriptor(bid);
			if (s2 == NULL)
				goto bailout;
			if (s2->ttype == TYPE_bit) {
				assert(pci->argc == 5);
				assert(r == NULL);
				assert(b1 == NULL || b2 == NULL);
				r = s2;
				s2 = NULL;
			}
		}
	}
	if (pci->argc > 3) {
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid)) {
			s1 = BATdescriptor(bid);
			if (s1 == NULL)
				goto bailout;
			if (s1->ttype == TYPE_bit) {
				assert(pci->argc == 4);
				r = s1;
				s1 = NULL;
			} else if (b1 == NULL) {
				s2 = s1;
				s1 = NULL;
			}
		}
	}

	if (b1)
		canditer_init(&ci1, b1, s1);
	if (b2)
		canditer_init(&ci2, b2, s2);

	if (b1 == NULL &&
		(tp1 == TYPE_flt ?
		 is_flt_nil(stk->stk[getArg(pci, 1)].val.fval) :
		 is_dbl_nil(stk->stk[getArg(pci, 1)].val.dval))) {
		bn = BATconstant(ci2.hseq, tp1, ATOMnilptr(tp1), ci2.ncand, TRANSIENT);
		goto doreturn;
	}
	if (b2 == NULL &&
		(tp1 == TYPE_flt ?
		 is_flt_nil(stk->stk[getArg(pci, 2)].val.fval) :
		 is_dbl_nil(stk->stk[getArg(pci, 2)].val.dval))) {
		bn = BATconstant(ci1.hseq, tp1, ATOMnilptr(tp1), ci1.ncand, TRANSIENT);
		goto doreturn;
	}
	if (b1)
		bn = COLnew(ci1.hseq, tp1, ci1.ncand, TRANSIENT);
	else
		bn = COLnew(ci2.hseq, tp1, ci2.ncand, TRANSIENT);
	if (bn == NULL || (b1 ? ci1.ncand : ci2.ncand) == 0) {
		goto doreturn;
	}

	rv = r ? Tloc(r, 0) : NULL;

	errno = 0;
	feclearexcept(FE_ALL_EXCEPT);
	switch (tp1) {
	case TYPE_flt:
		if (b1 && b2) {
			const flt *fsrc1 = (const flt *) Tloc(b1, 0);
			const flt *fsrc2 = (const flt *) Tloc(b2, 0);
			flt *restrict fdst = (flt *) Tloc(bn, 0);
			for (i = 0; i < ci1.ncand; i++) {
				x1 = canditer_next(&ci1) - b1->hseqbase;
				x2 = canditer_next(&ci2) - b2->hseqbase;
				if ((rv != NULL && !rv[i]) ||
					is_flt_nil(fsrc1[x1]) ||
					is_flt_nil(fsrc2[x2])) {
					fdst[i] = flt_nil;
					nils++;
				} else {
					fdst[i] = ffunc(fsrc1[x1], fsrc2[x2]);
				}
			}
		} else if (b1) {
			const flt *restrict fsrc1 = (const flt *) Tloc(b1, 0);
			flt fval2 = stk->stk[getArg(pci, 2)].val.fval;
			flt *restrict fdst = (flt *) Tloc(bn, 0);
			for (i = 0; i < ci1.ncand; i++) {
				x1 = canditer_next(&ci1) - b1->hseqbase;
				if ((rv != NULL && !rv[i]) ||
					is_flt_nil(fsrc1[x1])) {
					fdst[i] = flt_nil;
					nils++;
				} else {
					fdst[i] = ffunc(fsrc1[x1], fval2);
				}
			}
		} else /* b2 == NULL */ {
			flt fval1 = stk->stk[getArg(pci, 1)].val.fval;
			const flt *restrict fsrc2 = (const flt *) Tloc(b2, 0);
			flt *restrict fdst = (flt *) Tloc(bn, 0);
			for (i = 0; i < ci2.ncand; i++) {
				x2 = canditer_next(&ci2) - b2->hseqbase;
				if ((rv != NULL && !rv[i]) ||
					is_flt_nil(fsrc2[x2])) {
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
			const dbl *dsrc1 = (const dbl *) Tloc(b1, 0);
			const dbl *dsrc2 = (const dbl *) Tloc(b2, 0);
			dbl *restrict ddst = (dbl *) Tloc(bn, 0);
			for (i = 0; i < ci1.ncand; i++) {
				x1 = canditer_next(&ci1) - b1->hseqbase;
				x2 = canditer_next(&ci2) - b2->hseqbase;
				if ((rv != NULL && !rv[i]) ||
					is_dbl_nil(dsrc1[x1]) ||
					is_dbl_nil(dsrc2[x2])) {
					ddst[i] = dbl_nil;
					nils++;
				} else {
					ddst[i] = dfunc(dsrc1[x1], dsrc2[x2]);
				}
			}
		} else if (b1) {
			const dbl *restrict dsrc1 = (const dbl *) Tloc(b1, 0);
			dbl dval2 = stk->stk[getArg(pci, 2)].val.dval;
			dbl *restrict ddst = (dbl *) Tloc(bn, 0);
			for (i = 0; i < ci1.ncand; i++) {
				x1 = canditer_next(&ci1) - b1->hseqbase;
				if ((rv != NULL && !rv[i]) ||
					is_dbl_nil(dsrc1[x1])) {
					ddst[i] = dbl_nil;
					nils++;
				} else {
					ddst[i] = dfunc(dsrc1[x1], dval2);
				}
			}
		} else /* b2 == NULL */ {
			dbl dval1 = stk->stk[getArg(pci, 1)].val.dval;
			const dbl *restrict dsrc2 = (const dbl *) Tloc(b2, 0);
			dbl *restrict ddst = (dbl *) Tloc(bn, 0);
			for (i = 0; i < ci2.ncand; i++) {
				x2 = canditer_next(&ci2) - b2->hseqbase;
				if ((rv != NULL && !rv[i]) ||
					is_dbl_nil(dsrc2[x2])) {
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

	BATsetcount(bn, b1 ? ci1.ncand : ci2.ncand);
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
	if (r)
		BBPunfix(r->batCacheid);
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
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

#define scienceImpl(FUNC)												\
str																		\
CMDscience_bat_##FUNC(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) \
{																		\
	(void) cntxt;														\
	(void) mb;															\
																		\
	return CMDscienceUNARY(stk, pci, FUNC##f, FUNC, "batmmath." #FUNC);	\
}

#define scienceBinaryImpl(FUNC)											\
str																		\
CMDscience_bat_##FUNC(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) \
{																		\
	(void) cntxt;														\
	(void) mb;															\
																		\
	return CMDscienceBINARY(stk, pci, FUNC##f, FUNC, "batmmath." #FUNC); \
}

#define scienceNotImpl(FUNC)											\
str																		\
CMDscience_bat_##FUNC(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) \
{																		\
	(void) cntxt;														\
	(void) mb;															\
	(void) stk;															\
	(void) pci;															\
																		\
	throw(MAL, "batmmath." #FUNC, SQLSTATE(0A000) PROGRAM_NYI);			\
}

static double
radians(double x)
{
	return x * (3.14159265358979323846 / 180.0);
}

static float
radiansf(float x)
{
	return (float) (x * (3.14159265358979323846 / 180.0));
}

static double
degrees(double x)
{
	return x * (180.0 / 3.14159265358979323846);
}

static float
degreesf(float x)
{
	return (float) (x * (180.0 / 3.14159265358979323846));
}

mal_export str CMDscience_bat_asin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_acos(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_atan(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_cos(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_sin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_tan(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_cosh(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_sinh(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_tanh(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_radians(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_degrees(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_exp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_log(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_log10(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_log2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_sqrt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#ifdef HAVE_CBRT
mal_export str CMDscience_bat_cbrt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#else
mal_export str CMDscience_bat_cbrt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif
mal_export str CMDscience_bat_ceil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_fabs(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_floor(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

mal_export str CMDscience_bat_atan2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_pow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDscience_bat_logbs(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

scienceImpl(asin)
scienceImpl(acos)
scienceImpl(atan)
scienceImpl(cos)
scienceImpl(sin)
scienceImpl(tan)
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
#ifdef HAVE_CBRT
scienceImpl(cbrt)
#else
scienceNotImpl(cbrt)
#endif
scienceImpl(ceil)
scienceImpl(fabs)
scienceImpl(floor)

scienceBinaryImpl(atan2)
scienceBinaryImpl(pow)
scienceBinaryImpl(logbs)
