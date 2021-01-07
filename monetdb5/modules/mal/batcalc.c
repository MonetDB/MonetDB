/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
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
	bat bid;
	BAT *bn, *b, *s = NULL;

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

	bn = (*batfunc)(b, s);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatUNARY1(MalStkPtr stk, InstrPtr pci, bool abort_on_error,
			 BAT *(*batfunc)(BAT *, BAT *, bool), const char *malfunc)
{
	bat bid;
	BAT *bn, *b, *s = NULL;

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

	bn = (*batfunc)(b, s, abort_on_error);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalciszero, "batcalc.iszero");
}

static str
CMDbatISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcisnil, "batcalc.isnil");
}

static str
CMDbatISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcisnotnil, "batcalc.isnotnil");
}

static str
CMDbatNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnot, "batcalc.not");
}

static str
CMDbatABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcabsolute, "batcalc.abs");
}

static str
CMDbatINCR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, true, BATcalcincr, "batcalc.incr");
}

static str
CMDbatDECR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, true, BATcalcdecr, "batcalc.decr");
}

static str
CMDbatNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnegate, "batcalc.neg");
}

static str
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

/* MAL function has one of the following signatures:
 * # without candidate list
 * func(b1:bat, b2:bat) :bat
 * func(b1:bat, v2:any) :bat
 * func(v1:any, b2:bat) :bat
 * # with candidate list
 * func(b1:bat, b2:bat, s1:bat, s2:bat) :bat
 * func(b1:bat, v2:any, s1:bat) :bat
 * func(v1:any, b2:bat, s2:bat) :bat
 */
static str
CMDbatBINARY2(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, BAT *, int, bool),
			  BAT *(batfunc1)(BAT *, const ValRecord *, BAT *, int, bool),
			  BAT *(batfunc2)(const ValRecord *, BAT *, BAT *, int, bool),
			  int (*typefunc)(int, int),
			  bool abort_on_error, const char *malfunc)
{
	bat bid;
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL;
	int tp1, tp2, tp3;

	tp1 = stk->stk[getArg(pci, 1)].vtype; /* first argument */
	tp2 = stk->stk[getArg(pci, 2)].vtype; /* second argument */
	tp3 = getArgType(mb, pci, 0);		  /* return argument */
	assert(isaBatType(tp3));
	tp3 = getBatType(tp3);

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		bid = *getArgReference_bat(stk, pci, 1);
		b1 = BATdescriptor(bid);
		if (b1 == NULL)
			goto bailout;
	}

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		b2 = BATdescriptor(bid);
		if (b2 == NULL)
			goto bailout;
	}

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

	if (b1 && b2) {
		if (tp3 == TYPE_any)
			tp3 = (*typefunc)(b1->ttype, b2->ttype);
		bn = (*batfunc)(b1, b2, s1, s2, tp3, abort_on_error);
	} else if (b1) {
		if (tp3 == TYPE_any)
			tp3 = (*typefunc)(b1->ttype, tp2);
		bn = (*batfunc1)(b1, &stk->stk[getArg(pci, 2)], s1, tp3, abort_on_error);
	} else if (b2) {
		if (tp3 == TYPE_any)
			tp3 = (*typefunc)(tp1, b2->ttype);
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b2, s2, tp3, abort_on_error);
	} else
		goto bailout;			/* cannot happen */
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, GDK_EXCEPTION);
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

/* MAL function has one of the signatures for CMDbatBINARY2, or one of
 * the following:
 * # without candidate list
 * func(b1:bat, b2:bat, abort_on_error:bit) :bat
 * func(b1:bat, v2:any, abort_on_error:bit) :bat
 * func(v1:any, b2:bat, abort_on_error:bit) :bat
 * # with candidate list
 * func(b1:bat, b2:bat, s1:bat, s2:bat, abort_on_error:bit) :bat
 * func(b1:bat, v2:any, s1:bat, abort_on_error:bit) :bat
 * func(v1:any, b2:bat, s2:bat, abort_on_error:bit) :bat
 */
static str
CMDbatBINARY1(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, BAT *, bool),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *, bool),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *, bool),
			  bool abort_on_error,
			  const char *malfunc)
{
	bat bid;
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype; /* first argument */
	tp2 = stk->stk[getArg(pci, 2)].vtype; /* second argument */

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		bid = *getArgReference_bat(stk, pci, 1);
		b1 = BATdescriptor(bid);
		if (b1 == NULL)
			goto bailout;
	}

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		b2 = BATdescriptor(bid);
		if (b2 == NULL)
			goto bailout;
	}

	if (pci->argc > 5) {
		assert(pci->argc == 6);
		abort_on_error = *getArgReference_bit(stk, pci, 5);
	}
	if (pci->argc > 4) {
		if (stk->stk[getArg(pci, 4)].vtype == TYPE_bat) {
			bid = *getArgReference_bat(stk, pci, 4);
			if (!is_bat_nil(bid)) {
				s2 = BATdescriptor(bid);
				if (s2 == NULL)
					goto bailout;
			}
		} else {
			assert(pci->argc == 5);
			abort_on_error = *getArgReference_bit(stk, pci, 4);
		}
	}
	if (pci->argc > 3) {
		if (stk->stk[getArg(pci, 3)].vtype == TYPE_bat) {
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
		} else {
			assert(pci->argc == 4);
			abort_on_error = *getArgReference_bit(stk, pci, 3);
		}
	}

	if (b1 && b2)
		bn = (*batfunc)(b1, b2, s1, s2, abort_on_error);
	else if (b1)
		bn = (*batfunc1)(b1, &stk->stk[getArg(pci, 2)], s1, abort_on_error);
	else if (b2)
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b2, s2, abort_on_error);
	else
		goto bailout;			/* cannot happen */
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, GDK_EXCEPTION);
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

/* MAL function has one of the signatures for CMDbatBINARY2
 */
static str
CMDbatBINARY0(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, BAT *),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *),
			  const char *malfunc)
{
	bat bid;
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype; /* first argument */
	tp2 = stk->stk[getArg(pci, 2)].vtype; /* second argument */

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		bid = *getArgReference_bat(stk, pci, 1);
		b1 = BATdescriptor(bid);
		if (b1 == NULL)
			goto bailout;
	}

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		b2 = BATdescriptor(bid);
		if (b2 == NULL)
			goto bailout;
	}

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

	if (b1 && b2)
		bn = (*batfunc)(b1, b2, s1, s2);
	else if (b1)
		bn = (*batfunc1)(b1, &stk->stk[getArg(pci, 2)], s1);
	else if (b2)
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b2, s2);
	else
		goto bailout;			/* cannot happen */
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, GDK_EXCEPTION);
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

static str
CMDbatMIN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmin, BATcalcmincst, BATcalccstmin, "batcalc.min");
}

static str
CMDbatMIN_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmin_no_nil, BATcalcmincst_no_nil, BATcalccstmin_no_nil, "batcalc.min_no_nil");
}

static str
CMDbatMAX(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmax, BATcalcmaxcst, BATcalccstmax, "batcalc.max");
}

static str
CMDbatMAX_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmax_no_nil, BATcalcmaxcst_no_nil, BATcalccstmax_no_nil, "batcalc.max_no_nil");
}

static str
CMDbatADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 0, "batcalc.add_noerror");
}

static str
CMDbatADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 1, "batcalc.+");
}

static str
CMDbatADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctypeenlarge, 1, "batcalc.add_enlarge");
}

static str
CMDbatSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 0, "batcalc.sub_noerror");
}

static str
CMDbatSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 1, "batcalc.-");
}

static str
CMDbatSUBenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctypeenlarge, 1, "batcalc.sub_enlarge");
}

static str
CMDbatMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 0, "batcalc.mul_noerror");
}

static str
CMDbatMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 1, "batcalc.*");
}

static str
CMDbatMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctypeenlarge, 1, "batcalc.mul_enlarge");
}

static str
CMDbatDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 0, "batcalc.div_noerror");
}

static str
CMDbatDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 1, "batcalc./");
}

static str
CMDbatMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 0, "batcalc.mod_noerror");
}

static str
CMDbatMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 1, "batcalc.%");
}

static str
CMDbatXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcxor, BATcalcxorcst, BATcalccstxor,
						 "batcalc.xor");
}

static str
CMDbatOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcor, BATcalcorcst, BATcalccstor,
						 "batcalc.or");
}

static str
CMDbatAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcand, BATcalcandcst, BATcalccstand,
						 "batcalc.and");
}

static str
CMDbatLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh,
						 false, "batcalc.lsh_noerror");
}

static str
CMDbatLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh,
						 true, "batcalc.<<");
}

static str
CMDbatRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh,
						 false, "batcalc.rsh_noerror");
}

static str
CMDbatRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh,
						 true, "batcalc.>>");
}

static str
CMDbatLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalclt, BATcalcltcst, BATcalccstlt,
						 "batcalc.<");
}

static str
CMDbatLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcle, BATcalclecst, BATcalccstle,
						 "batcalc.<=");
}

static str
CMDbatGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcgt, BATcalcgtcst, BATcalccstgt,
						 "batcalc.>");
}

static str
CMDbatGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcge, BATcalcgecst, BATcalccstge,
						 "batcalc.>=");
}

static str
CMDbatEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalceq, BATcalceqcst, BATcalccsteq,
						 false, "batcalc.==");
}

static str
CMDbatNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcne, BATcalcnecst, BATcalccstne,
						 false, "batcalc.!=");
}

static str
CMDbatCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalccmp, BATcalccmpcst, BATcalccstcmp,
						 "batcalc.cmp");
}

static str
CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *bn, *b = NULL, *lo = NULL, *hi = NULL, *s = NULL, *slo = NULL, *shi = NULL;
	int tp1, tp2, tp3, tp;
	int bc = 0;					/* number of extra BAT arguments */
	bool symmetric, linc, hinc, nils_false, anti, has_cand = false;

	(void) cntxt;
	(void) mb;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	tp3 = stk->stk[getArg(pci, 3)].vtype;
	if (tp1 != TYPE_bat && !isaBatType(tp1))
		goto bailout;
	bid = *getArgReference_bat(stk, pci, 1);
	b = BATdescriptor(bid);
	if (b == NULL)
		goto bailout;
	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		lo = BATdescriptor(bid);
		if (lo == NULL)
			goto bailout;
	}
	if (tp3 == TYPE_bat || isaBatType(tp3)) {
		bid = *getArgReference_bat(stk, pci, 3);
		hi = BATdescriptor(bid);
		if (hi == NULL)
			goto bailout;
	}
	tp = getArgType(mb, pci, 4);
	if (tp == TYPE_bat || isaBatType(tp)) {
		bid = *getArgReference_bat(stk, pci, 4);
		has_cand = true;
		if (!is_bat_nil(bid)) {
			s = BATdescriptor(bid);
			if (s == NULL)
				goto bailout;
		}
		bc++;
	}
	if (has_cand && lo) {
		tp = getArgType(mb, pci, 4 + bc);
		if (tp == TYPE_bat || isaBatType(tp)) {
			bid = *getArgReference_bat(stk, pci, 4 + bc);
			if (!is_bat_nil(bid)) {
				slo = BATdescriptor(bid);
				if (slo == NULL)
					goto bailout;
			}
			bc++;
		} else {
			if (s == NULL) {
				/* apparently the extra bat was a NIL conditional
				 * execution bat */
				has_cand = false;
			} else
				goto bailout;
		}
	}
	if (has_cand && hi) {
		tp = getArgType(mb, pci, 4 + bc);
		if (tp != TYPE_bat && !isaBatType(tp))
			goto bailout;
		bid = *getArgReference_bat(stk, pci, 4 + bc);
		if (!is_bat_nil(bid)) {
			shi = BATdescriptor(bid);
			if (shi == NULL)
				goto bailout;
		}
		bc++;
	}

	symmetric = *getArgReference_bit(stk, pci, bc + 4);
	linc = *getArgReference_bit(stk, pci, bc + 5);
	hinc = *getArgReference_bit(stk, pci, bc + 6);
	nils_false = *getArgReference_bit(stk, pci, bc + 7);
	anti = *getArgReference_bit(stk, pci, bc + 8);

	if (b && lo && hi)
		bn = BATcalcbetween(b, lo, hi, s, slo, shi,
							symmetric, linc, hinc, nils_false, anti);
	else if (b && lo)
		bn = BATcalcbetweenbatcst(b, lo, &stk->stk[getArg(pci, 3)], s, slo,
								  symmetric, linc, hinc, nils_false, anti);
	else if (b && hi)
		bn = BATcalcbetweencstbat(b, &stk->stk[getArg(pci, 2)], hi, s, shi,
								  symmetric, linc, hinc, nils_false, anti);
	else
		bn = BATcalcbetweencstcst(b, &stk->stk[getArg(pci, 2)],
								  &stk->stk[getArg(pci, 3)], s,
								  symmetric, linc, hinc, nils_false, anti);
	BBPunfix(b->batCacheid);
	if (lo)
		BBPunfix(lo->batCacheid);
	if (hi)
		BBPunfix(hi->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (slo)
		BBPunfix(slo->batCacheid);
	if (shi)
		BBPunfix(shi->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, "batcalc.between", OPERATION_FAILED);
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;

  bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (lo)
		BBPunfix(lo->batCacheid);
	if (hi)
		BBPunfix(hi->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (slo)
		BBPunfix(slo->batCacheid);
/* cannot happen
	if (shi)
		BBPunfix(shi->batCacheid);
*/
	throw(MAL, "batcalc.between", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

static str
CMDcalcavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	dbl avg;
	BUN vals;
	bat bid;
	BAT *b, *s = NULL;
	gdk_return ret;
	int scale = 0;

	(void) cntxt;
	(void) mb;

	bid = *getArgReference_bat(stk, pci, pci->retc + 0);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((pci->argc == pci->retc + 2 &&
		 stk->stk[pci->argv[pci->retc + 1]].vtype == TYPE_bat) ||
		pci->argc == pci->retc + 3) {
		bid = *getArgReference_bat(stk, pci, pci->retc + 1);
		if (!is_bat_nil(bid) && (s = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}
	if (pci->argc >= pci->retc + 2 &&
		stk->stk[pci->argv[pci->argc - 1]].vtype == TYPE_int) {
		scale = *getArgReference_int(stk, pci, pci->argc - 1);
	}
	ret = BATcalcavg(b, s, &avg, &vals, scale);
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
CMDconvertbat(MalStkPtr stk, InstrPtr pci, int tp, bool abort_on_error)
{
	bat bid;
	BAT *b, *bn, *s = NULL;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "batcalc.convert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid) && (s = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.convert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (s && ATOMtype(s->ttype) != TYPE_oid) {
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			throw(MAL, "batcalc.convert", SQLSTATE(42000) ILLEGAL_ARGUMENT);
		}
	}

	bn = BATconvert(b, s, tp, abort_on_error, 0, 0, 0);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL) {
		char buf[20];
		snprintf(buf, sizeof(buf), "batcalc.%s", ATOMname(tp));
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDconvert_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bit, false);
}

static str
CMDconvertsignal_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bit, true);
}

static str
CMDconvert_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bte, false);
}

static str
CMDconvertsignal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bte, true);
}

static str
CMDconvert_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_sht, false);
}

static str
CMDconvertsignal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_sht, true);
}

static str
CMDconvert_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_int, false);
}

static str
CMDconvertsignal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_int, true);
}

static str
CMDconvert_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_lng, false);
}

static str
CMDconvertsignal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_lng, true);
}

#ifdef HAVE_HGE

static str
CMDconvert_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_hge, false);
}

static str
CMDconvertsignal_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_hge, true);
}
#endif

static str
CMDconvert_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_flt, false);
}

static str
CMDconvertsignal_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_flt, true);
}

static str
CMDconvert_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_dbl, false);
}

static str
CMDconvertsignal_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_dbl, true);
}

static str
CMDconvert_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_oid, false);
}

static str
CMDconvertsignal_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_oid, true);
}

static str
CMDconvert_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_str, false);
}

static str
CMDconvertsignal_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_str, true);
}

static str
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
			throw(MAL, "batcalc.ifthenelse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		cnt = BATcount(b);
	}
	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		b1 = BATdescriptor(* getArgReference_bat(stk, pci, 2));
		if (b1 == NULL) {
			if (b)
				BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.ifthenelse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
			throw(MAL, "batcalc.ifthenelse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		bit v;
		if (tp0 == TYPE_msk)
			v = (bit) *getArgReference_msk(stk, pci, 1);
		else
			v = *getArgReference_bit(stk, pci, 1);
		if (is_bit_nil(v)) {
			if (b1 != NULL)
				bn = BATconstant(b1->hseqbase, b1->ttype, ATOMnilptr(b1->ttype), BATcount(b1), TRANSIENT);
			else
				bn = BATconstant(b2->hseqbase, b2->ttype, ATOMnilptr(b2->ttype), BATcount(b2), TRANSIENT);
		} else if (v) {
			if (b1 != NULL)
				bn = COLcopy(b1, b1->ttype, false, TRANSIENT);
			else
				bn = BATconstant(b2->hseqbase, b2->ttype, VALptr(&stk->stk[getArg(pci, 2)]), BATcount(b2), TRANSIENT);
		} else {
			if (b2 != NULL)
				bn = COLcopy(b2, b2->ttype, false, TRANSIENT);
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

#include "mel.h"

static str
batcalc_init(void)
{
	int types[16], cur = 0, *tp;
	int specials[4];
	int *integer, *floats, *extra;

	types[cur++] = TYPE_bit;
	integer = types+cur;
	types[cur++] = TYPE_bte;
	types[cur++] = TYPE_sht;
	types[cur++] = TYPE_int;
	types[cur++] = TYPE_lng;
#ifdef HAVE_HGE
	types[cur++] = TYPE_hge;
#endif
	floats = types+cur;
	types[cur++] = TYPE_flt;
	types[cur++] = TYPE_dbl;
	extra = types+cur;
	types[cur++] = TYPE_oid;
	types[cur++] = TYPE_str;

	cur = 0;
	specials[cur++] = TYPE_bit;
	specials[cur++] = TYPE_oid;
	specials[cur++] = TYPE_str;

	mel_func_arg cand = { .type = TYPE_oid, .isbat=1 };

	int err=0;
	/* for all numeric types, use reverse order */
	for(tp = integer; tp < extra && !err; tp++) {
		mel_func_arg ret = { .type = TYPE_bit, .isbat=1 };
		mel_func_arg arg = { .type = *tp, .isbat=1 };

		err += melFunction(false, "batcalc", "iszero", (fptr)&CMDbatISZERO, "CMDbatISZERO", false, "Unary check for zero over the tail of the bat", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "iszero", (fptr)&CMDbatISZERO, "CMDbatISZERO", false, "Unary check for zero over the tail of the bat with candidates list", 1, 3, ret, arg, cand);
	}
	for(tp = types; tp < extra && !err; tp++) { /* bit + numeric */
		mel_func_arg ret = { .type = *tp, .isbat =1 };
		mel_func_arg arg = { .type = *tp, .isbat =1 };

		err += melFunction(false, "batcalc", "not", (fptr)&CMDbatNOT, "CMDbatNOT", false, "Unary bitwise not over the tail of the bat", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "not", (fptr)&CMDbatNOT, "CMDbatNOT", false, "Unary bitwise not over the tail of the bat with candidates list", 1, 3, ret, arg, cand);
	}
	for(tp = integer; tp < extra && !err; tp++) {
		mel_func_arg ret = { .type = TYPE_bte, .isbat =1 };
		mel_func_arg arg = { .type = *tp, .isbat =1 };

		err += melFunction(false, "batcalc", "sign", (fptr)&CMDbatSIGN, "CMDbatSIGN", false, "Unary sign (-1,0,1) over the tail of the bat", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "sign", (fptr)&CMDbatSIGN, "CMDbatSIGN", false, "Unary sign (-1,0,1) over the tail of the bat with candidates list", 1, 3, ret, arg, cand);
	}
	for(tp = integer; tp < extra && !err; tp++) {
		mel_func_arg ret = { .type = *tp, .isbat =1 };
		mel_func_arg arg = { .type = *tp, .isbat =1 };

		err += melFunction(false, "batcalc", "abs", (fptr)&CMDbatABS, "CMDbatABS", false, "Unary abs over the tail of the bat", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "abs", (fptr)&CMDbatABS, "CMDbatABS", false, "Unary abs over the tail of the bat with candidates list", 1, 3, ret, arg, cand);

		err += melFunction(false, "batcalc", "-", (fptr)&CMDbatNEG, "CMDbatNEG", false, "Unary neg over the tail of the bat", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "-", (fptr)&CMDbatNEG, "CMDbatNEG", false, "Unary neg over the tail of the bat with candidates list", 1, 3, ret, arg, cand);

		err += melFunction(false, "batcalc", "++", (fptr)&CMDbatINCR, "CMDbatINCR", false, "Unary increment over the tail of the bat", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "++", (fptr)&CMDbatINCR, "CMDbatINCR", false, "Unary increment over the tail of the bat with candidates list", 1, 3, ret, arg, cand);

		err += melFunction(false, "batcalc", "--", (fptr)&CMDbatDECR, "CMDbatDECR", false, "Unary decrement over the tail of the bat", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "--", (fptr)&CMDbatDECR, "CMDbatDECR", false, "Unary decrement over the tail of the bat with candidates list", 1, 3, ret, arg, cand);
	}
	/* possibly add the min/max + _no_nil */
	/* binops on numeric types */
	struct {
	   char *op;
	   char *op_ne;
	   char *fname;
	   char *fname_ne;
	   char *fname_el;
           fptr fcn;
           fptr fcn_ne;
           fptr fcn_el;
	   char *comment;
	   char *comment_ne;
	   char *comment_v;
	   char *comment_v_ne;
	   char *comment_v_;
	   char *comment_v__ne;
	   char *comment_el;
	   char *comment_el_v;
	   char *comment_el_v_;
	} funcs[3] = {
	  {
		.op = "+",
		.fcn = (fptr)CMDbatADDsignal,
		.fname = "CMDbatADDsignal",
		.op_ne = "add_noerror",
		.fcn_ne = (fptr)&CMDbatADD,
		.fname_ne = "CMDbatADD",
		.fcn_el = (fptr)&CMDbatADDenlarge,
		.fname_el = "CMDbatADDenlarge",
		.comment = "Return B1 + B2 with candidates list, signal error on overflow",
		.comment_ne = "Return B1 + B2 with candidates list, overflow causes NIL value",
		.comment_v = "Return B + V with candidates list, signal error on overflow",
		.comment_v_ne = "Return B + V with candidates list, overflow causes NIL value",
		.comment_v_ = "Return V + B with candidates list, signal error on overflow",
		.comment_v__ne = "Return V + B with candidates list, overflow causes NIL value",
		.comment_el = "Return B1 + B2 with candidates list, guarantee no overflow by returning larger type",
		.comment_el_v = "Return B + V with candidates list, guarantee no overflow by returning larger type",
		.comment_el_v_ = "Return V + B with candidates list, guarantee no overflow by returning larger type",
	  }, {
		.op = "-",
		.fcn = (fptr)CMDbatSUBsignal,
		.fname = "CMDbatSUBsignal",
		.op_ne = "sub_noerror",
		.fcn_ne = (fptr)&CMDbatSUB,
		.fname_ne = "CMDbatSUB",
		.fcn_el = (fptr)&CMDbatSUBenlarge,
		.fname_el = "CMDbatSUBenlarge",
		.comment = "Return B1 - B2 with candidates list, signal error on overflow",
		.comment_ne = "Return B1 - B2 with candidates list, overflow causes NIL value",
		.comment_v = "Return B - V with candidates list, signal error on overflow",
		.comment_v_ne = "Return B - V with candidates list, overflow causes NIL value",
		.comment_v_ = "Return V - B with candidates list, signal error on overflow",
		.comment_v__ne = "Return V - B with candidates list, overflow causes NIL value",
		.comment_el = "Return B1 - B2 with candidates list, guarantee no overflow by returning larger type",
		.comment_el_v = "Return B - V with candidates list, guarantee no overflow by returning larger type",
		.comment_el_v_ = "Return V - B with candidates list, guarantee no overflow by returning larger type",
	  }, {
		.op = "*",
		.fcn = (fptr)CMDbatMULsignal,
		.fname = "CMDbatMULsignal",
		.op_ne = "mul_noerror",
		.fcn_ne = (fptr)&CMDbatMUL,
		.fname_ne = "CMDbatMUL",
		.fcn_el = (fptr)&CMDbatMULenlarge,
		.fname_el = "CMDbatMULenlarge",
		.comment = "Return B1 * B2 with candidates list, signal error on overflow",
		.comment_ne = "Return B1 * B2 with candidates list, overflow causes NIL value",
		.comment_v = "Return B * V with candidates list, signal error on overflow",
		.comment_v_ne = "Return B * V with candidates list, overflow causes NIL value",
		.comment_v_ = "Return V * B with candidates list, signal error on overflow",
		.comment_v__ne = "Return V * B with candidates list, overflow causes NIL value",
		.comment_el = "Return B1 * B2 with candidates list, guarantee no overflow by returning larger type",
		.comment_el_v = "Return B * V with candidates list, guarantee no overflow by returning larger type",
		.comment_el_v_ = "Return V * B with candidates list, guarantee no overflow by returning larger type",
	  }
	};
	for (int f=0; f<3; f++) {
	  int *tp1, *tp2, *rt;
	  for(tp1 = integer; tp1 < extra && !err; tp1++) {
	    for(tp2 = integer; tp2 < extra && !err; tp2++) {
	      for(rt = extra-1; rt >= integer && !err; rt--) {
		if (f!=3 && (*rt < *tp1 || *rt < *tp2))
			continue;
		mel_func_arg ret = { .type = *rt, .isbat =1 };
		mel_func_arg arg1 = { .type = *tp1, .isbat =1 };
		mel_func_arg arg2 = { .type = *tp2, .isbat =1 };
		mel_func_arg varg1 = { .type = *tp1 };
		mel_func_arg varg2 = { .type = *tp2 };

		if (*rt == *tp1 || *rt == *tp2 || f==3) {
		  err += melFunction(false, "batcalc", funcs[f].op, funcs[f].fcn, funcs[f].fname, false, funcs[f].comment, 1, 5, ret, arg1, arg2, cand, cand);
		  err += melFunction(false, "batcalc", funcs[f].op_ne, funcs[f].fcn_ne, funcs[f].fname_ne, false, funcs[f].comment_ne, 1, 5, ret, arg1, arg2, cand, cand);
		  err += melFunction(false, "batcalc", funcs[f].op, funcs[f].fcn, funcs[f].fname, false, funcs[f].comment_v, 1, 4, ret, arg1, varg2, cand);
		  err += melFunction(false, "batcalc", funcs[f].op_ne, funcs[f].fcn_ne, funcs[f].fname_ne, false, funcs[f].comment_v_ne, 1, 4, ret, arg1, varg2, cand);
		  err += melFunction(false, "batcalc", funcs[f].op, funcs[f].fcn, funcs[f].fname, false, funcs[f].comment_v_, 1, 4, ret, varg1, arg2, cand);
		  err += melFunction(false, "batcalc", funcs[f].op_ne, funcs[f].fcn_ne, funcs[f].fname_ne, false, funcs[f].comment_v__ne, 1, 4, ret, varg1, arg2, cand);
		} else {
		  err += melFunction(false, "batcalc", funcs[f].op, funcs[f].fcn_el, funcs[f].fname_el, false, funcs[f].comment_el, 1, 5, ret, arg1, arg2, cand, cand);
		  err += melFunction(false, "batcalc", funcs[f].op, funcs[f].fcn_el, funcs[f].fname_el, false, funcs[f].comment_el_v, 1, 4, ret, arg1, varg2, cand);
		  err += melFunction(false, "batcalc", funcs[f].op, funcs[f].fcn_el, funcs[f].fname_el, false, funcs[f].comment_el_v_, 1, 4, ret, varg1, arg2, cand);
		}
	      }
	    }
	  }
	}
	{	/* multiplication between integers and floating-points, returning integers */
		int *tp1, *tp2, *tp3;
		for(tp1 = integer; tp1 < floats && !err; tp1++) {
			for(tp2 = floats; tp2 < extra && !err; tp2++) {
				for(tp3 = integer; tp3 < floats && !err; tp3++) {
					int in1 = *tp3, in2 = *tp2;

					for (int i = 0 ; i < 2 ; i++) {
						mel_func_arg ret = { .type = *tp1, .isbat =1 };
						mel_func_arg arg1 = { .type = in1, .isbat =1 };
						mel_func_arg arg2 = { .type = in2, .isbat =1 };
						mel_func_arg varg1 = { .type = in1 };
						mel_func_arg varg2 = { .type = in2 };

						err += melFunction(false, "batcalc", funcs[2].op, funcs[2].fcn, funcs[2].fname, false, funcs[2].comment, 1, 5, ret, arg1, arg2, cand, cand);
						err += melFunction(false, "batcalc", funcs[2].op_ne, funcs[2].fcn_ne, funcs[2].fname_ne, false, funcs[2].comment_ne, 1, 5, ret, arg1, arg2, cand, cand);
						err += melFunction(false, "batcalc", funcs[2].op, funcs[2].fcn, funcs[2].fname, false, funcs[2].comment_v, 1, 4, ret, arg1, varg2, cand);
						err += melFunction(false, "batcalc", funcs[2].op_ne, funcs[2].fcn_ne, funcs[2].fname_ne, false, funcs[2].comment_v_ne, 1, 4, ret, arg1, varg2, cand);
						err += melFunction(false, "batcalc", funcs[2].op, funcs[2].fcn, funcs[2].fname, false, funcs[2].comment_v_, 1, 4, ret, varg1, arg2, cand);
						err += melFunction(false, "batcalc", funcs[2].op_ne, funcs[2].fcn_ne, funcs[2].fname_ne, false, funcs[2].comment_v__ne, 1, 4, ret, varg1, arg2, cand);

						/* swap variables */
						in1 ^= in2;
						in2 ^= in1;
						in1 ^= in2;
					}
				}
			}
		}
	}
	struct {
	   char *op;
	   char *op_ne;
	   char *fname;
	   char *fname_ne;
	   char *fname_el;
           fptr fcn;
           fptr fcn_ne;
           fptr fcn_el;
	   char *comment;
	   char *comment_ne;
	   char *comment_v;
	   char *comment_v_ne;
	   char *comment_v_;
	   char *comment_v__ne;
	   char *comment_el;
	   char *comment_el_v;
	   char *comment_el_v_;
	} div = {
		.op = "/",
		.fcn = (fptr)CMDbatDIVsignal,
		.fname = "CMDbatDIVsignal",
		.op_ne = "div_noerror",
		.fcn_ne = (fptr)&CMDbatDIV,
		.fname_ne = "CMDbatDIV",
		.comment = "Return B1 / B2 with candidates list, signal error on overflow",
		.comment_ne = "Return B1 / B2 with candidates list, overflow causes NIL value",
		.comment_v = "Return B / V with candidates list, signal error on overflow",
		.comment_v_ne = "Return B / V with candidates list, overflow causes NIL value",
		.comment_v_ = "Return V / B with candidates list, signal error on overflow",
		.comment_v__ne = "Return V / B with candidates list, overflow causes NIL value",
	};
	int *tp1, *tp2, *rt;
	for(tp1 = integer; tp1 < extra && !err; tp1++) {
	    for(tp2 = integer; tp2 < extra && !err; tp2++) {
	      for(rt = extra-1; rt >= tp1 && !err; rt--) {
		mel_func_arg ret = { .type = *rt, .isbat =1 };
		mel_func_arg arg1 = { .type = *tp1, .isbat =1 };
		mel_func_arg arg2 = { .type = *tp2, .isbat =1 };
		mel_func_arg varg1 = { .type = *tp1 };
		mel_func_arg varg2 = { .type = *tp2 };

		err += melFunction(false, "batcalc", div.op, div.fcn, div.fname, false, div.comment, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", div.op_ne, div.fcn_ne, div.fname_ne, false, div.comment_ne, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", div.op, div.fcn, div.fname, false, div.comment_v, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", div.op_ne, div.fcn_ne, div.fname_ne, false, div.comment_v_ne, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", div.op, div.fcn, div.fname, false, div.comment_v_, 1, 4, ret, varg1, arg2, cand);
		err += melFunction(false, "batcalc", div.op_ne, div.fcn_ne, div.fname_ne, false, div.comment_v__ne, 1, 4, ret, varg1, arg2, cand);
	      }
	    }
	}
	/* division between integers and floating-points, returning integers */
	for(tp1 = floats; tp1 < extra && !err; tp1++) {
	    for(tp2 = integer; tp2 < floats && !err; tp2++) {
	      for(rt = integer; rt < floats && !err; rt++) {
		mel_func_arg ret = { .type = *rt, .isbat =1 };
		mel_func_arg arg1 = { .type = *tp1, .isbat =1 };
		mel_func_arg arg2 = { .type = *tp2, .isbat =1 };
		mel_func_arg varg1 = { .type = *tp1 };
		mel_func_arg varg2 = { .type = *tp2 };

		err += melFunction(false, "batcalc", div.op, div.fcn, div.fname, false, div.comment, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", div.op_ne, div.fcn_ne, div.fname_ne, false, div.comment_ne, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", div.op, div.fcn, div.fname, false, div.comment_v, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", div.op_ne, div.fcn_ne, div.fname_ne, false, div.comment_v_ne, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", div.op, div.fcn, div.fname, false, div.comment_v_, 1, 4, ret, varg1, arg2, cand);
		err += melFunction(false, "batcalc", div.op_ne, div.fcn_ne, div.fname_ne, false, div.comment_v__ne, 1, 4, ret, varg1, arg2, cand);
	      }
	    }
	}
	struct {
	   char *op;
	   char *op_ne;
	   char *fname;
	   char *fname_ne;
           fptr fcn;
           fptr fcn_ne;
	   char *comment;
	   char *comment_ne;
	   char *comment_v;
	   char *comment_v_ne;
	   char *comment_v_;
	   char *comment_v__ne;
	} mods = {
		.op = "%",
		.fcn = (fptr)CMDbatMODsignal,
		.fname = "CMDbatMODsignal",
		.op_ne = "mod_noerror",
		.fcn_ne = (fptr)&CMDbatMOD,
		.fname_ne = "CMDbatMOD",
		.comment = "Return B1 % B2 with candidates list, signal error on overflow",
		.comment_ne = "Return B1 % B2 with candidates list, overflow causes NIL value",
		.comment_v = "Return B % V with candidates list, signal error on overflow",
		.comment_v_ne = "Return B % V with candidates list, overflow causes NIL value",
		.comment_v_ = "Return V % B with candidates list, signal error on overflow",
		.comment_v__ne = "Return V % B with candidates list, overflow causes NIL value",
	};
	for(tp1 = integer; tp1 < extra && !err; tp1++) {
	    for(tp2 = integer; tp2 < extra && !err; tp2++) {
	      for(rt = extra-1; rt >= integer && !err; rt--) {
		if (rt < tp1 && rt < tp2)
			continue;
		if (*rt == TYPE_dbl) {
			if (*tp1 != TYPE_dbl || *tp2 != TYPE_dbl)
				continue;
		} else if (*rt == TYPE_flt) {
			if (*tp1 != TYPE_flt || *tp2 != TYPE_flt)
				continue;
		} else {
			if (*tp1 == TYPE_flt || *tp2 == TYPE_flt || *tp1 == TYPE_dbl || *tp2 == TYPE_dbl)
				continue;
		}
		mel_func_arg ret = { .type = *rt, .isbat =1 };
		mel_func_arg arg1 = { .type = *tp1, .isbat =1 };
		mel_func_arg arg2 = { .type = *tp2, .isbat =1 };
		mel_func_arg varg1 = { .type = *tp1 };
		mel_func_arg varg2 = { .type = *tp2 };

		err += melFunction(false, "batcalc", mods.op, mods.fcn, mods.fname, false, mods.comment, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", mods.op_ne, mods.fcn_ne, mods.fname_ne, false, mods.comment_ne, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", mods.op, mods.fcn, mods.fname, false, mods.comment_v, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", mods.op_ne, mods.fcn_ne, mods.fname_ne, false, mods.comment_v_ne, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", mods.op, mods.fcn, mods.fname, false, mods.comment_v_, 1, 4, ret, varg1, arg2, cand);
		err += melFunction(false, "batcalc", mods.op_ne, mods.fcn_ne, mods.fname_ne, false, mods.comment_v__ne, 1, 4, ret, varg1, arg2, cand);
	      }
	    }
	}
	struct {
	   char *op;
	   char *fname;
           fptr fcn;
	   char *comment;
	   char *comment_v;
	   char *comment_v_;
	} logops[3] = {
	  {
		.op = "and",
		.fcn = (fptr)CMDbatAND,
		.fname = "CMDbatAND",
		.comment = "Return B1 and B2",
		.comment_v = "Return B and V",
		.comment_v_ = "Return V and B",
	  }, {
		.op = "or",
		.fcn = (fptr)CMDbatOR,
		.fname = "CMDbatOR",
		.comment = "Return B1 or B2",
		.comment_v = "Return B or V",
		.comment_v_ = "Return V or B",
	  }, {
		.op = "xor",
		.fcn = (fptr)CMDbatXOR,
		.fname = "CMDbatXOR",
		.comment = "Return B1 xor B2",
		.comment_v = "Return B xor V",
		.comment_v_ = "Return V xor B",
	  }
	};
	for (int f=0; f<3; f++) {
	  for(tp = types+0; tp < extra && !err; tp++) {
		mel_func_arg ret = { .type = *tp, .isbat =1 };
		mel_func_arg arg = { .type = *tp, .isbat =1 };
		mel_func_arg varg = { .type = *tp };

		err += melFunction(false, "batcalc", logops[f].op, logops[f].fcn, logops[f].fname, false, logops[f].comment, 1, 3, ret, arg, arg);
		err += melFunction(false, "batcalc", logops[f].op, logops[f].fcn, logops[f].fname, false, logops[f].comment, 1, 5, ret, arg, arg, cand, cand);
		err += melFunction(false, "batcalc", logops[f].op, logops[f].fcn, logops[f].fname, false, logops[f].comment_v, 1, 3, ret, arg, varg);
		err += melFunction(false, "batcalc", logops[f].op, logops[f].fcn, logops[f].fname, false, logops[f].comment_v, 1, 4, ret, arg, varg, cand);
		err += melFunction(false, "batcalc", logops[f].op, logops[f].fcn, logops[f].fname, false, logops[f].comment_v_, 1, 3, ret, varg, arg);
		err += melFunction(false, "batcalc", logops[f].op, logops[f].fcn, logops[f].fname, false, logops[f].comment_v_, 1, 4, ret, varg, arg, cand);
	  }
	}
	struct {
	   char *op;
	   char *op_ne;
	   char *fname;
           fptr fcn;
	   char *fname_ne;
           fptr fcn_ne;
	   char *comment;
	   char *comment_v;
	   char *comment_v_;
	   char *comment_ne;
	   char *comment_ne_v;
	   char *comment_ne_v_;
	} shifts[2] = {
	  {
		.op = "<<",
		.op_ne = "lsh_noerror",
		.fcn = (fptr)CMDbatLSHsignal,
		.fname = "CMDbatLSHsignal",
		.fcn_ne = (fptr)CMDbatLSH,
		.fname_ne = "CMDbatLSH",
		.comment = "Return B1 << B2, raise error on out of range second operand",
		.comment_v = "Return B << V, raise error on out of range second operand",
		.comment_v_ = "Return B << V, raise error on out of range second operand",
		.comment_ne = "Return B1 << B2, out of range second operand causes NIL value",
		.comment_ne_v = "Return B << V, out of range second operand causes NIL value",
		.comment_ne_v_ = "Return V << B, out of range second operand causes NIL value",
	  }, {
		.op = ">>",
		.op_ne = "rsh_noerror",
		.fcn = (fptr)CMDbatRSHsignal,
		.fname = "CMDbatRSHsignal",
		.fcn_ne = (fptr)CMDbatRSH,
		.fname_ne = "CMDbatRSH",
		.comment = "Return B1 >> B2, raise error on out of range second operand",
		.comment_v = "Return B >> V, raise error on out of range second operand",
		.comment_v_ = "Return B >> V, raise error on out of range second operand",
		.comment_ne = "Return B1 >> B2, out of range second operand causes NIL value",
		.comment_ne_v = "Return B >> V, out of range second operand causes NIL value",
		.comment_ne_v_ = "Return V >> B, out of range second operand causes NIL value",
	  }
	};
	for (int f=0; f<2; f++) {
	  int *tp1, *tp2;
	  for(tp1 = integer; tp1 < floats && !err; tp1++) {
	    for(tp2 = integer; tp2 < floats && !err; tp2++) {
		mel_func_arg ret = { .type = *tp1, .isbat =1 };
		mel_func_arg arg1 = { .type = *tp1, .isbat =1 };
		mel_func_arg arg2 = { .type = *tp2, .isbat =1 };
		mel_func_arg varg1 = { .type = *tp1 };
		mel_func_arg varg2 = { .type = *tp2 };

		err += melFunction(false, "batcalc", shifts[f].op, shifts[f].fcn, shifts[f].fname, false, shifts[f].comment, 1, 3, ret, arg1, arg2);
		err += melFunction(false, "batcalc", shifts[f].op, shifts[f].fcn, shifts[f].fname, false, shifts[f].comment, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", shifts[f].op_ne, shifts[f].fcn_ne, shifts[f].fname_ne, false, shifts[f].comment_ne, 1, 3, ret, arg1, arg2);
		err += melFunction(false, "batcalc", shifts[f].op_ne, shifts[f].fcn_ne, shifts[f].fname_ne, false, shifts[f].comment_ne, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", shifts[f].op, shifts[f].fcn, shifts[f].fname, false, shifts[f].comment_v, 1, 3, ret, arg1, varg2);
		err += melFunction(false, "batcalc", shifts[f].op, shifts[f].fcn, shifts[f].fname, false, shifts[f].comment_v, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", shifts[f].op_ne, shifts[f].fcn_ne, shifts[f].fname_ne, false, shifts[f].comment_ne_v, 1, 3, ret, arg1, varg2);
		err += melFunction(false, "batcalc", shifts[f].op_ne, shifts[f].fcn_ne, shifts[f].fname_ne, false, shifts[f].comment_ne_v, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", shifts[f].op, shifts[f].fcn, shifts[f].fname, false, shifts[f].comment_v_, 1, 3, ret, varg1, arg2);
		err += melFunction(false, "batcalc", shifts[f].op, shifts[f].fcn, shifts[f].fname, false, shifts[f].comment_v_, 1, 4, ret, varg1, arg2, cand);
		err += melFunction(false, "batcalc", shifts[f].op_ne, shifts[f].fcn_ne, shifts[f].fname_ne, false, shifts[f].comment_ne_v_, 1, 3, ret, varg1, arg2);
		err += melFunction(false, "batcalc", shifts[f].op_ne, shifts[f].fcn_ne, shifts[f].fname_ne, false, shifts[f].comment_ne_v_, 1, 4, ret, varg1, arg2, cand);
	    }
	  }
	}

	struct {
	   char *op;
	   char *fname;
           fptr fcn;
	   char *comment;
	   char *comment_v;
	   char *comment_v_;
	} cmps[6] = {
	  {
		.op = "<",
		.fcn = (fptr)CMDbatLT,
		.fname = "CMDbatLT",
		.comment = "Return B1 < B2",
		.comment_v = "Return B < V",
		.comment_v_ = "Return B < V",
	  }, {
		.op = "<=",
		.fcn = (fptr)CMDbatLE,
		.fname = "CMDbatLE",
		.comment = "Return B1 <= B2",
		.comment_v = "Return B <= V",
		.comment_v_ = "Return B <= V",
	  }, {
		.op = ">",
		.fcn = (fptr)CMDbatGT,
		.fname = "CMDbatGT",
		.comment = "Return B1 > B2",
		.comment_v = "Return B > V",
		.comment_v_ = "Return B > V",
	  }, {
		.op = ">=",
		.fcn = (fptr)CMDbatGE,
		.fname = "CMDbatGE",
		.comment = "Return B1 >= B2",
		.comment_v = "Return B >= V",
		.comment_v_ = "Return B >= V",
	  }, {
		.op = "==",
		.fcn = (fptr)CMDbatEQ,
		.fname = "CMDbatEQ",
		.comment = "Return B1 == B2",
		.comment_v = "Return B == V",
		.comment_v_ = "Return B == V",
	  }, {
		.op = "!=",
		.fcn = (fptr)CMDbatNE,
		.fname = "CMDbatNE",
		.comment = "Return B1 != B2",
		.comment_v = "Return B != V",
		.comment_v_ = "Return B != V",
	  }
	};
	int newtypes[6] = { ATOMindex("json"), ATOMindex("uuid"), TYPE_date, TYPE_daytime, TYPE_timestamp };
	for (int f=0; f<6; f++) {
	  mel_func_arg ret = { .type = TYPE_bit, .isbat =1 };
	  mel_func_arg arg = { .type = TYPE_any, .isbat =1, .nr=1 };
	  mel_func_arg varg = { .type = TYPE_any, .nr=1 };

	  err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 3, ret, arg, arg);
	  err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 5, ret, arg, arg, cand, cand);
	  err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 3, ret, arg, varg);
	  err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 4, ret, arg, varg, cand);
	  err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 3, ret, varg, arg);
	  err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 4, ret, varg, arg, cand);

	  if (strcmp(cmps[f].op,"==")==0 || strcmp(cmps[f].op,"!=")==0) {
		mel_func_arg nil_matches = { .type = TYPE_bit };
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 4, ret, arg, arg, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 6, ret, arg, arg, cand, cand, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 4, ret, arg, varg, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 5, ret, arg, varg, cand, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 4, ret, varg, arg, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 5, ret, varg, arg, cand, nil_matches);
	  }

	  /* uuid, json and mtime (date, daytime, timestamp */
	  for (int nt = 0; nt < 5; nt++) {
	     mel_func_arg ret = { .type = TYPE_bit, .isbat =1 };
	     mel_func_arg arg = { .type = newtypes[nt], .isbat =1, .nr=1 };
	     mel_func_arg varg = { .type = newtypes[nt], .nr=1 };

 	     err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 3, ret, arg, arg);
 	     err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 5, ret, arg, arg, cand, cand);
 	     err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 3, ret, arg, varg);
	     err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 4, ret, arg, varg, cand);
	     err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 3, ret, varg, arg);
	     err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 4, ret, varg, arg, cand);

	     if (strcmp(cmps[f].op,"==")==0 || strcmp(cmps[f].op,"!=")==0) {
		mel_func_arg nil_matches = { .type = TYPE_bit };
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 4, ret, arg, arg, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 6, ret, arg, arg, cand, cand, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 4, ret, arg, varg, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 5, ret, arg, varg, cand, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 4, ret, varg, arg, nil_matches);
	  	err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 5, ret, varg, arg, cand, nil_matches);
	     }
	  }

	  int *tp1, *tp2;
	  for(tp1 = integer; tp1 < floats && !err; tp1++) {
	    for(tp2 = integer; tp2 < floats && !err; tp2++) {
		if (*tp1 == *tp2)
			continue;
		mel_func_arg ret = { .type = TYPE_bit, .isbat =1 };
		mel_func_arg arg1 = { .type = *tp1, .isbat =1 };
		mel_func_arg arg2 = { .type = *tp2, .isbat =1 };
		mel_func_arg varg1 = { .type = *tp1 };
		mel_func_arg varg2 = { .type = *tp2 };

		err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 3, ret, arg1, arg2);
		err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 3, ret, arg1, varg2);
		err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 3, ret, varg1, arg2);
		err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 4, ret, varg1, arg2, cand);

	  	if (strcmp(cmps[f].op,"==")==0 || strcmp(cmps[f].op,"!=")==0) {
			mel_func_arg nil_matches = { .type = TYPE_bit };

			err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 3, ret, arg1, arg2, nil_matches);
			err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment, 1, 5, ret, arg1, arg2, cand, cand, nil_matches);
			err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 3, ret, arg1, varg2, nil_matches);
			err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v, 1, 4, ret, arg1, varg2, cand, nil_matches);
			err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 3, ret, varg1, arg2, nil_matches);
			err += melFunction(false, "batcalc", cmps[f].op, cmps[f].fcn, cmps[f].fname, false, cmps[f].comment_v_, 1, 4, ret, varg1, arg2, cand, nil_matches);
		}
	    }
	  }
	}

	struct {
	   char *op;
	   char *fname;
           fptr fcn;
	   char *comment;
	   char *comment_v;
	   char *comment_v_;
	} cmp = {
		.op = "cmp",
		.fcn = (fptr)CMDbatCMP,
		.fname = "CMDbatCMP",
		.comment = "Return -1/0/1 if B1 </==/> B2",
		.comment_v = "Return -1/0/1 if B </==/> V",
		.comment_v_ = "Return -1/0/1 if V </==/> B",
	};
	for(int i = 0; i < 3 && !err; i++) {
		int tp = specials[i];
		mel_func_arg ret = { .type = TYPE_bte, .isbat =1 };
		mel_func_arg arg = { .type = tp, .isbat =1 };
		mel_func_arg varg = { .type = tp };

		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment, 1, 3, ret, arg, arg);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment, 1, 5, ret, arg, arg, cand, cand);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v, 1, 3, ret, arg, varg);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v, 1, 4, ret, arg, varg, cand);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v_, 1, 3, ret, varg, arg);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v_, 1, 4, ret, varg, arg, cand);
	}
	for(tp1 = integer; tp1 < extra && !err; tp1++) {
	    for(tp2 = integer; tp2 < extra && !err; tp2++) {
		mel_func_arg ret = { .type = TYPE_bte, .isbat =1 };
		mel_func_arg arg1 = { .type = *tp1, .isbat =1 };
		mel_func_arg arg2 = { .type = *tp2, .isbat =1 };
		mel_func_arg varg1 = { .type = *tp1 };
		mel_func_arg varg2 = { .type = *tp2 };

		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment, 1, 3, ret, arg1, arg2);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment, 1, 5, ret, arg1, arg2, cand, cand);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v, 1, 3, ret, arg1, varg2);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v, 1, 4, ret, arg1, varg2, cand);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v_, 1, 3, ret, varg1, arg2);
		err += melFunction(false, "batcalc", cmp.op, cmp.fcn, cmp.fname, false, cmp.comment_v_, 1, 4, ret, varg1, arg2, cand);
	    }
	}
	for(tp = integer; tp < extra && !err; tp++) {
		mel_func_arg ret = { .type = TYPE_dbl };
		mel_func_arg nr = { .type = TYPE_lng };
		mel_func_arg arg = { .type = *tp, .isbat =1 };
		mel_func_arg scale = { .type = TYPE_int };

		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average of non-nil values of B", 1, 2, ret, arg);
		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average of non-nil values of B with candidates list", 1, 3, ret, arg, cand);
		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average and number of non-nil values of B", 2, 3, ret, nr, arg);
		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average and number of non-nil values of B with candidates list", 2, 4, ret, nr, arg, cand);
		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average of non-nil values of B", 1, 3, ret, arg, scale);
		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average of non-nil values of B with candidates list", 1, 4, ret, arg, cand, scale);
		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average and number of non-nil values of B", 2, 4, ret, nr, arg, scale);
		err += melFunction(false, "batcalc", "avg", (fptr)&CMDcalcavg, "CMDcalcavg", false, "average and number of non-nil values of B with candidates list", 2, 5, ret, nr, arg, cand, scale);
	}

	struct {
		int type;
		char *name;
		char *fname;
		fptr fcn;
		char *name_ne;
		char *fname_ne;
		fptr fcn_ne;
	} typeops[10] = {
	  {
		.type = TYPE_bit,
		.name = "bit",
		.name_ne = "bit_noerror",
		.fname = "CMDconvertsignal_bit",
		.fname_ne = "CMDconvert_bit",
		.fcn = (fptr)CMDconvertsignal_bit,
		.fcn_ne = (fptr)CMDconvert_bit,
	  }, {
		.type = TYPE_bte,
		.name = "bte",
		.name_ne = "bte_noerror",
		.fname = "CMDconvertsignal_bte",
		.fname_ne = "CMDconvert_bte",
		.fcn = (fptr)CMDconvertsignal_bte,
		.fcn_ne = (fptr)CMDconvert_bte,
	  }, {
		.type = TYPE_sht,
		.name = "sht",
		.name_ne = "sht_noerror",
		.fname = "CMDconvertsignal_sht",
		.fname_ne = "CMDconvert_sht",
		.fcn = (fptr)CMDconvertsignal_sht,
		.fcn_ne = (fptr)CMDconvert_sht,
	  }, {
		.type = TYPE_int,
		.name = "int",
		.name_ne = "int_noerror",
		.fname = "CMDconvertsignal_int",
		.fname_ne = "CMDconvert_int",
		.fcn = (fptr)CMDconvertsignal_int,
		.fcn_ne = (fptr)CMDconvert_int,
	  }, {
		.type = TYPE_lng,
		.name = "lng",
		.name_ne = "lng_noerror",
		.fname = "CMDconvertsignal_lng",
		.fname_ne = "CMDconvert_lng",
		.fcn = (fptr)CMDconvertsignal_lng,
		.fcn_ne = (fptr)CMDconvert_lng,
#ifdef HAVE_HGE
	  }, {
		.type = TYPE_hge,
		.name = "hge",
		.name_ne = "hge_noerror",
		.fname = "CMDconvertsignal_hge",
		.fname_ne = "CMDconvert_hge",
		.fcn = (fptr)CMDconvertsignal_hge,
		.fcn_ne = (fptr)CMDconvert_hge,
#endif
	  }, {
		.type = TYPE_flt,
		.name = "flt",
		.name_ne = "flt_noerror",
		.fname = "CMDconvertsignal_flt",
		.fname_ne = "CMDconvert_flt",
		.fcn = (fptr)CMDconvertsignal_flt,
		.fcn_ne = (fptr)CMDconvert_flt,
	  }, {
		.type = TYPE_dbl,
		.name = "dbl",
		.name_ne = "dbl_noerror",
		.fname = "CMDconvertsignal_dbl",
		.fname_ne = "CMDconvert_dbl",
		.fcn = (fptr)CMDconvertsignal_dbl,
		.fcn_ne = (fptr)CMDconvert_dbl,
	  }, {
		.type = TYPE_oid,
		.name = "oid",
		.name_ne = "oid_noerror",
		.fname = "CMDconvertsignal_oid",
		.fname_ne = "CMDconvert_oid",
		.fcn = (fptr)CMDconvertsignal_oid,
		.fcn_ne = (fptr)CMDconvert_oid,
	  }, {
		.type = TYPE_str,
		.name = "str",
		.name_ne = "str_noerror",
		.fname = "CMDconvertsignal_str",
		.fname_ne = "CMDconvert_str",
		.fcn = (fptr)CMDconvertsignal_str,
		.fcn_ne = (fptr)CMDconvert_str,
	  }
	};
#ifdef HAVE_HGE
	int typeopslen = 10;
#else
	int typeopslen = 9;
#endif
	for(int t = 0; t<typeopslen; t++) {
		/* from any 2 string */
		mel_func_arg ret = { .type = typeops[t].type, .isbat =1 };
		if (strcmp(typeops[t].name, "str")==0) {
			mel_func_arg arg = { .type = TYPE_any, .isbat =1 };

			err += melFunction(false, "batcalc", typeops[t].name, typeops[t].fcn, typeops[t].fname, false, "", 1, 2, ret, arg);
			err += melFunction(false, "batcalc", typeops[t].name, typeops[t].fcn, typeops[t].fname, false, "", 1, 3, ret, arg, cand);
			err += melFunction(false, "batcalc", typeops[t].name_ne, typeops[t].fcn_ne, typeops[t].fname_ne, false, "", 1, 2, ret, arg);
			err += melFunction(false, "batcalc", typeops[t].name_ne, typeops[t].fcn_ne, typeops[t].fname_ne, false, "", 1, 3, ret, arg, cand);
		} else {
		    for(int p = 0; p<typeopslen; p++) {
				mel_func_arg arg = { .type = typeops[p].type, .isbat =1 };

				err += melFunction(false, "batcalc", typeops[t].name, typeops[t].fcn, typeops[t].fname, false, "", 1, 2, ret, arg);
				err += melFunction(false, "batcalc", typeops[t].name, typeops[t].fcn, typeops[t].fname, false, "", 1, 3, ret, arg, cand);
				err += melFunction(false, "batcalc", typeops[t].name_ne, typeops[t].fcn_ne, typeops[t].fname_ne, false, "", 1, 2, ret, arg);
				err += melFunction(false, "batcalc", typeops[t].name_ne, typeops[t].fcn_ne, typeops[t].fname_ne, false, "", 1, 3, ret, arg, cand);
		    }
		}
	}
	return MAL_SUCCEED;
}

static mel_func batcalc_init_funcs[] = {
 /* batcalc */
 pattern("batcalc", "isnil", CMDbatISNIL, false, "Unary check for nil over the tail of the bat", args(1,2, batarg("",bit),batargany("b",0))),
 pattern("batcalc", "isnil", CMDbatISNIL, false, "Unary check for nil over the tail of the bat with candidates list", args(1,3, batarg("",bit),batargany("b",0),batarg("s",oid))),
 pattern("batcalc", "isnotnil", CMDbatISNOTNIL, false, "Unary check for notnil over the tail of the bat", args(1,2, batarg("",bit),batargany("b",0))),
 pattern("batcalc", "isnotnil", CMDbatISNOTNIL, false, "Unary check for notnil over the tail of the bat with candidates list", args(1,3, batarg("",bit),batargany("b",0),batarg("s",oid))),
 pattern("batcalc", "min", CMDbatMIN, false, "Return bat with minimum value of each pair of inputs", args(1,3, batargany("",1),batargany("b1",1),batargany("b2",1))),
 pattern("batcalc", "min", CMDbatMIN, false, "Return bat with minimum value of each pair of inputs", args(1,5, batargany("",1),batargany("b1",1),batargany("b2",1),batarg("s1",oid),batarg("s2",oid))),
 pattern("batcalc", "min", CMDbatMIN, false, "Return bat with minimum value of each pair of inputs", args(1,3, batargany("",1),batargany("b",1),argany("v",1))),
 pattern("batcalc", "min", CMDbatMIN, false, "Return bat with minimum value of each pair of inputs", args(1,4, batargany("",1),batargany("b",1),argany("v",1),batarg("s",oid))),
 pattern("batcalc", "min", CMDbatMIN, false, "Return bat with minimum value of each pair of inputs", args(1,3, batargany("",1),argany("v",1),batargany("b",1))),
 pattern("batcalc", "min", CMDbatMIN, false, "Return bat with minimum value of each pair of inputs", args(1,4, batargany("",1),argany("v",1),batargany("b",1),batarg("s",oid))),
 pattern("batcalc", "min_no_nil", CMDbatMIN_no_nil, false, "Return bat with minimum value of each pair of inputs, ignoring nil values", args(1,3, batargany("",1),batargany("b1",1),batargany("b2",1))),
 pattern("batcalc", "min_no_nil", CMDbatMIN_no_nil, false, "Return bat with minimum value of each pair of inputs, ignoring nil values", args(1,5, batargany("",1),batargany("b1",1),batargany("b2",1),batarg("s1",oid),batarg("s2",oid))),
 pattern("batcalc", "min_no_nil", CMDbatMIN_no_nil, false, "Return bat with minimum value of each pair of inputs, ignoring nil values", args(1,3, batargany("",1),batargany("b",1),argany("v",1))),
 pattern("batcalc", "min_no_nil", CMDbatMIN_no_nil, false, "Return bat with minimum value of each pair of inputs, ignoring nil values", args(1,4, batargany("",1),batargany("b",1),argany("v",1),batarg("s",oid))),
 pattern("batcalc", "min_no_nil", CMDbatMIN_no_nil, false, "Return bat with minimum value of each pair of inputs, ignoring nil values", args(1,3, batargany("",1),argany("v",1),batargany("b",1))),
 pattern("batcalc", "min_no_nil", CMDbatMIN_no_nil, false, "Return bat with minimum value of each pair of inputs, ignoring nil values", args(1,4, batargany("",1),argany("v",1),batargany("b",1),batarg("s",oid))),
 pattern("batcalc", "max", CMDbatMAX, false, "Return bat with maximum value of each pair of inputs", args(1,3, batargany("",1),batargany("b1",1),batargany("b2",1))),
 pattern("batcalc", "max", CMDbatMAX, false, "Return bat with maximum value of each pair of inputs", args(1,5, batargany("",1),batargany("b1",1),batargany("b2",1),batarg("s1",oid),batarg("s2",oid))),
 pattern("batcalc", "max", CMDbatMAX, false, "Return bat with maximum value of each pair of inputs", args(1,3, batargany("",1),batargany("b",1),argany("v",1))),
 pattern("batcalc", "max", CMDbatMAX, false, "Return bat with maximum value of each pair of inputs", args(1,4, batargany("",1),batargany("b",1),argany("v",1),batarg("s",oid))),
 pattern("batcalc", "max", CMDbatMAX, false, "Return bat with maximum value of each pair of inputs", args(1,3, batargany("",1),argany("v",1),batargany("b",1))),
 pattern("batcalc", "max", CMDbatMAX, false, "Return bat with maximum value of each pair of inputs", args(1,4, batargany("",1),argany("v",1),batargany("b",1),batarg("s",oid))),
 pattern("batcalc", "max_no_nil", CMDbatMAX_no_nil, false, "Return bat with maximum value of each pair of inputs, ignoring nil values", args(1,3, batargany("",1),batargany("b1",1),batargany("b2",1))),
 pattern("batcalc", "max_no_nil", CMDbatMAX_no_nil, false, "Return bat with maximum value of each pair of inputs, ignoring nil values", args(1,5, batargany("",1),batargany("b1",1),batargany("b2",1),batarg("s1",oid),batarg("s2",oid))),
 pattern("batcalc", "max_no_nil", CMDbatMAX_no_nil, false, "Return bat with maximum value of each pair of inputs, ignoring nil values", args(1,3, batargany("",1),batargany("b",1),argany("v",1))),
 pattern("batcalc", "max_no_nil", CMDbatMAX_no_nil, false, "Return bat with maximum value of each pair of inputs, ignoring nil values", args(1,4, batargany("",1),batargany("b",1),argany("v",1),batarg("s",oid))),
 pattern("batcalc", "max_no_nil", CMDbatMAX_no_nil, false, "Return bat with maximum value of each pair of inputs, ignoring nil values", args(1,3, batargany("",1),argany("v",1),batargany("b",1))),
 pattern("batcalc", "max_no_nil", CMDbatMAX_no_nil, false, "Return bat with maximum value of each pair of inputs, ignoring nil values", args(1,4, batargany("",1),argany("v",1),batargany("b",1),batarg("s",oid))),

 pattern("batcalc", "+", CMDbatADD, false, "Return concatenation of B1 and B2 with candidates list", args(1,5, batarg("",str),batarg("b1",str),batarg("b2",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batcalc", "+", CMDbatADD, false, "Return concatenation of B and V with candidates list", args(1,4, batarg("",str),batarg("b",str),arg("v",str),batarg("s",oid))),
 pattern("batcalc", "+", CMDbatADD, false, "Return concatenation of V and B with candidates list", args(1,4, batarg("",str),arg("v",str),batarg("b",str),batarg("s",oid))),

 pattern("batmmath", "fmod", CMDbatMODsignal, false, "", args(1,3, batarg("",dbl),batarg("x",dbl),arg("y",dbl))),
 pattern("batmmath", "fmod", CMDbatMODsignal, false, "", args(1,4, batarg("",dbl),batarg("x",dbl),arg("y",dbl),batarg("s",oid))),
 pattern("batmmath", "fmod", CMDbatMODsignal, false, "", args(1,3, batarg("",flt),batarg("x",flt),arg("y",flt))),
 pattern("batmmath", "fmod", CMDbatMODsignal, false, "", args(1,4, batarg("",flt),batarg("x",flt),arg("y",flt),batarg("s",oid))),

 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa)", args(1,9, batarg("",bit),batargany("b",1),batargany("v1",1),batargany("v2",1),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa) with candidates list", args(1,12, batarg("",bit),batargany("b",1),batargany("v1",1),batargany("v2",1),batarg("s",oid),batarg("s1",oid),batarg("s2",oid),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa)", args(1,9, batarg("",bit),batargany("b",1),batargany("v1",1),argany("v2",1),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa) with candidates list", args(1,11, batarg("",bit),batargany("b",1),batargany("v1",1),argany("v2",1),batarg("s",oid),batarg("s1",oid),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa)", args(1,9, batarg("",bit),batargany("b",1),argany("v1",1),batargany("v2",1),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa) with candidates list", args(1,11, batarg("",bit),batargany("b",1),argany("v1",1),batargany("v2",1),batarg("s",oid),batarg("s2",oid),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa)", args(1,9, batarg("",bit),batargany("b",1),argany("v1",1),argany("v2",1),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("batcalc", "between", CMDbatBETWEEN, false, "B between V1 and V2 (or vice versa) with candidates list", args(1,10, batarg("",bit),batargany("b",1),argany("v1",1),argany("v2",1),batarg("s",oid),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),

 pattern("aggr", "avg", CMDcalcavg, false, "Gives the avg of all tail values", args(1,2, arg("",dbl),batargany("b",2))),
 pattern("aggr", "avg", CMDcalcavg, false, "Gives the avg of all tail values", args(1,3, arg("",dbl),batargany("b",2),arg("scale",int))),

 pattern("batcalc", "ifthenelse", CMDifthen, false, "If-then-else operation to assemble a conditional result", args(1,4, batargany("",1),arg("v",bit),batargany("b1",1),batargany("b2",1))),
 pattern("batcalc", "ifthenelse", CMDifthen, false, "If-then-else operation to assemble a conditional result", args(1,4, batargany("",1),arg("v",bit),argany("v1",1),batargany("b2",1))),
 pattern("batcalc", "ifthenelse", CMDifthen, false, "If-then-else operation to assemble a conditional result", args(1,4, batargany("",1),arg("v",bit),batargany("b1",1),argany("v2",1))),
 pattern("batcalc", "ifthenelse", CMDifthen, false, "If-then-else operation to assemble a conditional result", args(1,4, batargany("",1),batarg("b",bit),argany("v1",1),argany("v2",1))),
 pattern("batcalc", "ifthenelse", CMDifthen, false, "If-then-else operation to assemble a conditional result", args(1,4, batargany("",1),batarg("b",bit),batargany("b1",1),argany("v2",1))),
 pattern("batcalc", "ifthenelse", CMDifthen, false, "If-then-else operation to assemble a conditional result", args(1,4, batargany("",1),batarg("b",bit),argany("v1",1),batargany("b2",1))),
 pattern("batcalc", "ifthenelse", CMDifthen, false, "If-then-else operation to assemble a conditional result", args(1,4, batargany("",1),batarg("b",bit),batargany("b1",1),batargany("b2",1))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batcalc_mal)
{ mal_module2("batcalc", NULL, batcalc_init_funcs, &batcalc_init, NULL); }
