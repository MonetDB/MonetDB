/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include <math.h>
#include "mal_exception.h"
#include "mal_interpreter.h"

static str
mythrow(enum malexception type, const char *fcn, const char *msg)
{
	char *errbuf = GDKerrbuf;
	char *s;

	if (errbuf && *errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
		if (strchr(errbuf, '!') == errbuf + 5) {
			s = createException(type, fcn, "%s", errbuf);
		} else if ((s = strchr(errbuf, ':')) != NULL && s[1] == ' ') {
			s = createException(type, fcn, "%s", s + 2);
		} else {
			s = createException(type, fcn, "%s", errbuf);
		}
		GDKclrerr();
		return s;
	}
	return createException(type, fcn, "%s", msg);
}

static str
CMDbatUNARY(MalStkPtr stk, InstrPtr pci,
			BAT *(*batfunc)(BAT *, BAT *), const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL;

	bid = getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bat *sid = getArgReference_bat(stk, pci, 2);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
	}

	bn = (*batfunc)(b, s);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	bid = getArgReference_bat(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatUNARY1(MalStkPtr stk, InstrPtr pci, int abort_on_error,
			 BAT *(*batfunc)(BAT *, BAT *, int), const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL;

	bid = getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bat *sid = getArgReference_bat(stk, pci, 2);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
	}

	bn = (*batfunc)(b, s, abort_on_error);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	bid = getArgReference_bat(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

mal_export str CMDbatISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalciszero, "batcalc.iszero");
}

mal_export str CMDbatISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcisnil, "batcalc.isnil");
}

mal_export str CMDbatISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcisnotnil, "batcalc.isnotnil");
}

mal_export str CMDbatNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnot, "batcalc.not");
}

mal_export str CMDbatABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcabsolute, "batcalc.abs");
}

mal_export str CMDbatINCR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatINCR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, 1, BATcalcincr, "batcalc.incr");
}

mal_export str CMDbatDECR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDECR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, 1, BATcalcdecr, "batcalc.decr");
}

mal_export str CMDbatNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnegate, "batcalc.neg");
}

mal_export str CMDbatSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcsign, "batcalc.sign");
}

static int
calctype(int tp1, int tp2)
{
	int tp1s = ATOMbasetype(tp1);
	int tp2s = ATOMbasetype(tp2);
	if (tp1s == TYPE_str && tp2s == TYPE_str)
		return TYPE_str;
	if (tp1s < TYPE_flt && tp2s < TYPE_flt) {
		if (tp1s > tp2s)
			return tp1;
		if (tp1s < tp2s)
			return tp2;
		return MAX(tp1, tp2);
	}
	if (tp1s == TYPE_dbl || tp2s == TYPE_dbl)
		return TYPE_dbl;
	if (tp1s == TYPE_flt || tp2s == TYPE_flt)
		return TYPE_flt;
#ifdef HAVE_HGE
	if (tp1s == TYPE_hge || tp2s == TYPE_hge)
		return TYPE_hge;
#endif
	return TYPE_lng;
}

static int
calctypeenlarge(int tp1, int tp2)
{
	tp1 = calctype(tp1, tp2);
	switch (tp1) {
	case TYPE_bte:
		return TYPE_sht;
	case TYPE_sht:
		return TYPE_int;
	case TYPE_int:
		return TYPE_lng;
#ifdef HAVE_HGE
	case TYPE_lng:
		return TYPE_hge;
#endif
	case TYPE_flt:
		return TYPE_dbl;
	default:
		/* we shouldn't get here */
		return tp1;
	}
}

static int
calcdivtype(int tp1, int tp2)
{
	/* if right hand side is floating point, the result is floating
	 * point, otherwise the result has the type of the left hand
	 * side */
	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	if (tp1 == TYPE_dbl || tp2 == TYPE_dbl)
		return TYPE_dbl;
	if (tp1 == TYPE_flt || tp2 == TYPE_flt)
		return TYPE_flt;
	return tp1;
}

#if 0
static int
calcdivtypeflt(int tp1, int tp2)
{
	(void) tp1;
	(void) tp2;
	return TYPE_flt;
}

static int
calcdivtypedbl(int tp1, int tp2)
{
	(void) tp1;
	(void) tp2;
	return TYPE_dbl;
}
#endif

static int
calcmodtype(int tp1, int tp2)
{
	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	assert(tp1 > 0 && tp1 < TYPE_str && tp1 != TYPE_bat && tp1 != TYPE_ptr);
	assert(tp2 > 0 && tp2 < TYPE_str && tp2 != TYPE_bat && tp2 != TYPE_ptr);
	if (tp1 == TYPE_dbl || tp2 == TYPE_dbl)
		return TYPE_dbl;
	if (tp1 == TYPE_flt || tp2 == TYPE_flt)
		return TYPE_flt;
	return MIN(tp1, tp2);
}

static str
CMDbatBINARY2(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, int, int),
			  BAT *(batfunc1)(BAT *, const ValRecord *, BAT *, int, int),
			  BAT *(batfunc2)(const ValRecord *, BAT *, BAT *, int, int),
			  int (*typefunc)(int, int),
			  int abort_on_error, const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL;
	int tp1, tp2, tp3;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	tp3 = getArgType(mb, pci, 0);
	assert(isaBatType(tp3));
	tp3 = getBatType(tp3);
	if (pci->argc == 4) {
		bat *sid = getArgReference_bat(stk, pci, 3);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		BAT *b2 = NULL;
		bid = getArgReference_bat(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (tp2 == TYPE_bat || isaBatType(tp2)) {
			bid = getArgReference_bat(stk, pci, 2);
			b2 = BATdescriptor(*bid);
			if (b2 == NULL) {
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
			}
		}
		if (b2) {
			if (tp3 == TYPE_any)
				tp3 = (*typefunc)(b->ttype, b2->ttype);
			bn = (*batfunc)(b, b2, s, tp3, abort_on_error);
			BBPunfix(b2->batCacheid);
		} else {
			if (tp3 == TYPE_any)
				tp3 = (*typefunc)(b->ttype, tp2);
			bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], s,
							 tp3, abort_on_error);
		}
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(tp2 == TYPE_bat || isaBatType(tp2));
		bid = getArgReference_bat(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (tp3 == TYPE_any)
			tp3 = (*typefunc)(tp1, b->ttype);
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, s, tp3, abort_on_error);
	}
	BBPunfix(b->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	bid = getArgReference_bat(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY1(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, int),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *, int),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *, int),
			  int abort_on_error,
			  const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	if (pci->argc == 4) {
		bat *sid = getArgReference_bat(stk, pci, 3);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		BAT *b2 = NULL;
		bid = getArgReference_bat(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (tp2 == TYPE_bat || isaBatType(tp2)) {
			bid = getArgReference_bat(stk, pci, 2);
			b2 = BATdescriptor(*bid);
			if (b2 == NULL) {
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
			}
		}
		if (b2) {
			bn = (*batfunc)(b, b2, s, abort_on_error);
			BBPunfix(b2->batCacheid);
		} else {
			bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], s, abort_on_error);
		}
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(tp2 == TYPE_bat || isaBatType(tp2));
		bid = getArgReference_bat(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, s, abort_on_error);
	}
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	bid = getArgReference_bat(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY0(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *),
			  const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	if (pci->argc == 4) {
		bat *sid = getArgReference_bat(stk, pci, 3);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		BAT *b2 = NULL;
		bid = getArgReference_bat(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (tp2 == TYPE_bat || isaBatType(tp2)) {
			bid = getArgReference_bat(stk, pci, 2);
			b2 = BATdescriptor(*bid);
			if (b2 == NULL) {
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
			}
		}
		if (b2) {
			bn = (*batfunc)(b, b2, s);
			BBPunfix(b2->batCacheid);
		} else if (batfunc1 == NULL) {
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, malfunc, PROGRAM_NYI);
		} else {
			bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], s);
		}
	} else if (batfunc2 == NULL) {
		throw(MAL, malfunc, PROGRAM_NYI);
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(tp2 == TYPE_bat || isaBatType(tp2));
		bid = getArgReference_bat(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, s);
	}
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	bid = getArgReference_bat(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

mal_export str CMDbatMIN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMIN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmin, BATcalcmincst, BATcalccstmin, "batcalc.min");
}

mal_export str CMDbatMIN_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMIN_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmin_no_nil, BATcalcmincst_no_nil, BATcalccstmin_no_nil, "batcalc.min_no_nil");
}

mal_export str CMDbatMAX(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMAX(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmax, BATcalcmaxcst, BATcalccstmax, "batcalc.max");
}

mal_export str CMDbatMAX_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMAX_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmax_no_nil, BATcalcmaxcst_no_nil, BATcalccstmax_no_nil, "batcalc.max_no_nil");
}

mal_export str CMDbatADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 0, "batcalc.add_noerror");
}

mal_export str CMDbatADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 1, "batcalc.+");
}

mal_export str CMDbatADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctypeenlarge, 1, "batcalc.add_enlarge");
}

mal_export str CMDbatSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 0, "batcalc.sub_noerror");
}

mal_export str CMDbatSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 1, "batcalc.-");
}

mal_export str CMDbatSUBenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctypeenlarge, 1, "batcalc.sub_enlarge");
}

mal_export str CMDbatMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 0, "batcalc.mul_noerror");
}

mal_export str CMDbatMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 1, "batcalc.*");
}

mal_export str CMDbatMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctypeenlarge, 1, "batcalc.mul_enlarge");
}

mal_export str CMDbatDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 0, "batcalc.div_noerror");
}

mal_export str CMDbatDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 1, "batcalc./");
}

mal_export str CMDbatMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 0, "batcalc.mod_noerror");
}

mal_export str CMDbatMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 1, "batcalc.%");
}

mal_export str CMDbatXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcxor, BATcalcxorcst, BATcalccstxor,
						 "batcalc.xor");
}

mal_export str CMDbatOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcor, BATcalcorcst, BATcalccstor,
						 "batcalc.or");
}

mal_export str CMDbatAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcand, BATcalcandcst, BATcalccstand,
						 "batcalc.and");
}

mal_export str CMDbatLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh, 0,
						 "batcalc.lsh_noerror");
}

mal_export str CMDbatLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh, 1,
						 "batcalc.<<");
}

mal_export str CMDbatRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh, 0,
						 "batcalc.rsh_noerror");
}

mal_export str CMDbatRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh, 1,
						 "batcalc.>>");
}

mal_export str CMDbatLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalclt, BATcalcltcst, BATcalccstlt,
						 "batcalc.<");
}

mal_export str CMDbatLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcle, BATcalclecst, BATcalccstle,
						 "batcalc.<=");
}

mal_export str CMDbatGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcgt, BATcalcgtcst, BATcalccstgt,
						 "batcalc.>");
}

mal_export str CMDbatGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcge, BATcalcgecst, BATcalccstge,
						 "batcalc.>=");
}

mal_export str CMDbatEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalceq, BATcalceqcst, BATcalccsteq,
						 "batcalc.==");
}

mal_export str CMDbatNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcne, BATcalcnecst, BATcalccstne,
						 "batcalc.!=");
}

mal_export str CMDbatCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalccmp, BATcalccmpcst, BATcalccstcmp,
						 "batcalc.cmp");
}

static str
callbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int sym)
{
	bat *bid;
	BAT *bn, *b, *lo = NULL, *hi = NULL, *s = NULL;
	int tp1, tp2, tp3;

	(void) cntxt;
	(void) mb;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	tp3 = stk->stk[getArg(pci, 3)].vtype;
	if (pci->argc == 5) {
		bat *sid = getArgReference_bat(stk, pci, 4);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
	}

	if (tp1 != TYPE_bat && !isaBatType(tp1)) {
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "batcalc.between", ILLEGAL_ARGUMENT);
	}
	bid = getArgReference_bat(stk, pci, 1);
	b = BATdescriptor(*bid);
	if (b == NULL) {
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
	}

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = getArgReference_bat(stk, pci, 2);
		lo = BATdescriptor(*bid);
		if (lo == NULL) {
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
		}
	}
	if (tp3 == TYPE_bat || isaBatType(tp3)) {
		bid = getArgReference_bat(stk, pci, 3);
		hi = BATdescriptor(*bid);
		if (hi == NULL) {
			BBPunfix(b->batCacheid);
			if (lo)
				BBPunfix(lo->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
		}
	}
	if (lo == NULL) {
		if (hi == NULL) {
			bn = BATcalcbetweencstcst(b, &stk->stk[getArg(pci, 2)],
									  &stk->stk[getArg(pci, 3)], s, sym);
		} else {
			bn = BATcalcbetweencstbat(b, &stk->stk[getArg(pci, 2)], hi, s, sym);
		}
	} else {
		if (hi == NULL) {
			bn = BATcalcbetweenbatcst(b, lo, &stk->stk[getArg(pci, 3)], s, sym);
		} else {
			bn = BATcalcbetween(b, lo, hi, s, sym);
		}
	}
	BBPunfix(b->batCacheid);
	if (lo)
		BBPunfix(lo->batCacheid);
	if (hi)
		BBPunfix(hi->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, "batcalc.between", OPERATION_FAILED);
	}
	bid = getArgReference_bat(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

mal_export str CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return callbatBETWEEN(cntxt, mb, stk, pci, 0);
}

mal_export str CMDbatBETWEENsymmetric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatBETWEENsymmetric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return callbatBETWEEN(cntxt, mb, stk, pci, 1);
}

mal_export str CMDcalcavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDcalcavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	dbl avg;
	BUN vals;
	bat *bid;
	BAT *b, *s = NULL;
	gdk_return ret;

	(void) cntxt;
	(void) mb;

	bid = getArgReference_bat(stk, pci, pci->retc + 0);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.avg", RUNTIME_OBJECT_MISSING);
	if (pci->retc == pci->retc + 2) {
		bat *sid = getArgReference_bat(stk, pci, pci->retc + 1);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "aggr.avg", RUNTIME_OBJECT_MISSING);
		}
	}
	ret = BATcalcavg(b, s, &avg, &vals);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (ret != GDK_SUCCEED)
		return mythrow(MAL, "aggr.avg", OPERATION_FAILED);
	* getArgReference_dbl(stk, pci, 0) = avg;
	if (pci->retc == 2)
		* getArgReference_lng(stk, pci, 1) = vals;
	return MAL_SUCCEED;
}

static str
CMDconvertbat(MalStkPtr stk, InstrPtr pci, int tp, int abort_on_error)
{
	bat *bid;
	BAT *b, *bn, *s = NULL;

	bid = getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.convert", RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bat *sid = getArgReference_bat(stk, pci, 2);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.convert", RUNTIME_OBJECT_MISSING);
		}
	}

	bn = BATconvert(b, s, tp, abort_on_error);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		char buf[20];
		snprintf(buf, sizeof(buf), "batcalc.%s", ATOMname(tp));
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	bid = getArgReference_bat(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

mal_export str CMDconvert_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bit, 0);
}

str
CMDconvertsignal_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bit, 1);
}

mal_export str CMDconvert_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bte, 0);
}

str
CMDconvertsignal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bte, 1);
}

mal_export str CMDconvert_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_sht, 0);
}

str
CMDconvertsignal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_sht, 1);
}

mal_export str CMDconvert_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_int, 0);
}

str
CMDconvertsignal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_int, 1);
}

mal_export str CMDconvert_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_lng, 0);
}

str
CMDconvertsignal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_lng, 1);
}

#ifdef HAVE_HGE
mal_export str CMDconvert_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_hge, 0);
}

str
CMDconvertsignal_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_hge, 1);
}
#endif

mal_export str CMDconvert_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_flt, 0);
}

str
CMDconvertsignal_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_flt, 1);
}

mal_export str CMDconvert_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_dbl, 0);
}

str
CMDconvertsignal_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_dbl, 1);
}

mal_export str CMDconvert_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_oid, 0);
}

str
CMDconvertsignal_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_oid, 1);
}

mal_export str CMDconvert_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_str, 0);
}

str
CMDconvertsignal_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_str, 1);
}

mal_export str CMDifthen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDifthen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *b1 = NULL, *b2 = NULL, *bn;
	int tp0, tp1, tp2;
	bat *ret;
	BUN cnt = BUN_NONE;

	(void) cntxt;
	(void) mb;

	if (pci->argc != 4)
		throw(MAL, "batcalc.ifthen", "Operation not supported.");

	ret = getArgReference_bat(stk, pci, 0);
	tp0 = stk->stk[getArg(pci, 1)].vtype;
	tp1 = stk->stk[getArg(pci, 2)].vtype;
	tp2 = stk->stk[getArg(pci, 3)].vtype;
	if (tp0 == TYPE_bat || isaBatType(tp0)) {
		b = BATdescriptor(* getArgReference_bat(stk, pci, 1));
		if (b == NULL)
			throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
		cnt = BATcount(b);
	}
	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		b1 = BATdescriptor(* getArgReference_bat(stk, pci, 2));
		if (b1 == NULL) {
			if (b)
				BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
		}
		if (cnt == BUN_NONE)
			cnt = BATcount(b1);
		else if (BATcount(b1) != cnt) {
			BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.ifthenelse", ILLEGAL_ARGUMENT);
		}
	}
	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		b2 = BATdescriptor(* getArgReference_bat(stk, pci, 3));
		if (b2 == NULL) {
			if (b)
				BBPunfix(b->batCacheid);
			if (b1)
				BBPunfix(b1->batCacheid);
			throw(MAL, "batcalc.ifthenelse", RUNTIME_OBJECT_MISSING);
		}
		if (cnt == BUN_NONE)
			cnt = BATcount(b2);
		else if (BATcount(b2) != cnt) {
			if (b)
				BBPunfix(b->batCacheid);
			if (b1)
				BBPunfix(b1->batCacheid);
			throw(MAL, "batcalc.ifthenelse", ILLEGAL_ARGUMENT);
		}
	}
	if (b == NULL && b1 == NULL && b2 == NULL) {
		/* at least one BAT required */
		throw(MAL, "batcalc.ifthenelse", ILLEGAL_ARGUMENT);
	}
	if (b != NULL) {
		if (b1 != NULL) {
			if (b2 != NULL) {
				bn = BATcalcifthenelse(b, b1, b2);
			} else {
				bn = BATcalcifthenelsecst(b, b1, &stk->stk[getArg(pci, 3)]);
			}
		} else {
			if (b2 != NULL) {
				bn = BATcalcifthencstelse(b, &stk->stk[getArg(pci, 2)], b2);
			} else {
				bn = BATcalcifthencstelsecst(b, &stk->stk[getArg(pci, 2)], &stk->stk[getArg(pci, 3)]);
			}
		}
	} else {
		bit v = *getArgReference_bit(stk, pci, 1);
		if (v ==  bit_nil) {
			if (b1 != NULL)
				bn = BATconstant(b1->hseqbase, b1->ttype, ATOMnilptr(b1->ttype), BATcount(b1), TRANSIENT);
			else
				bn = BATconstant(b2->hseqbase, b2->ttype, ATOMnilptr(b2->ttype), BATcount(b2), TRANSIENT);
		} else if (v) {
			if (b1 != NULL)
				bn = COLcopy(b1, b1->ttype, 0, TRANSIENT);
			else
				bn = BATconstant(b2->hseqbase, b2->ttype, VALptr(&stk->stk[getArg(pci, 2)]), BATcount(b2), TRANSIENT);
		} else {
			if (b2 != NULL)
				bn = COLcopy(b2, b2->ttype, 0, TRANSIENT);
			else
				bn = BATconstant(b1->hseqbase, b1->ttype, VALptr(&stk->stk[getArg(pci, 3)]), BATcount(b1), TRANSIENT);
		}
	}
	if (b)
		BBPunfix(b->batCacheid);
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, "batcalc.ifthenelse", OPERATION_FAILED);
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}
